import json
import re
from abc import ABC, abstractmethod

from diffrev.processDiff.chunking import chunk_diff
from diffrev.reviewUtils.events import add_event, add_findings_events
from diffrev.reviewUtils.queries import get_job
from diffrev.shared.config import CHUNK_BYTES
from diffrev.shared.db import conn_cursor

HUNK_HEADER_RE = re.compile(r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@", re.MULTILINE)


class ProviderError(Exception):
    """Raised when a provider cannot complete a review."""


def normalize_finding(raw):
    """Coerce a provider's raw finding into the canonical Finding object."""
    rule_id = raw["ruleId"]
    path = raw["path"]
    line = int(raw["line"])
    return {
        "id": f"{rule_id}:{path}:{line}",
        "ruleId": rule_id,
        "path": path,
        "line": line,
        "severity": raw["severity"],
        "category": raw["category"],
        "title": raw["title"],
        "evidence": raw["evidence"],
    }


def finding_sort_key(finding):
    """Order findings by path, then line number, then rule id."""
    return (finding["path"], finding["line"], finding["ruleId"])


def process_findings(findings, max_findings):
    """Deduplicate by id, sort by (path, line, ruleId), truncate to max."""
    seen = set()
    ordered = []
    for finding in findings:
        fid = finding["id"]
        if fid in seen:
            continue
        seen.add(fid)
        ordered.append(finding)
    ordered.sort(key=finding_sort_key)
    return ordered[:max_findings]


async def load_diff(job_id):
    """Reassemble the stored diff from its chunks, in order."""
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT chunk_cont FROM chunks WHERE job_id = %s ORDER BY chunk_num",
            (job_id,),
        )
        rows = await cur.fetchall()
    return "".join(r["chunk_cont"] for r in rows)


async def persist_result(job_id, findings, usage):
    """Persist the final findings plus the 'done' event, then mark the job done,
    all in one transaction so SSE replay sees a consistent log. The 'finding'
    events were already streamed per chunk by the pipeline."""
    async with conn_cursor() as (conn, cur):
        await conn.begin()
        try:
            for f in findings:
                await cur.execute(
                    "INSERT INTO findings "
                    "(job_id, finding_id, rule_id, path, line, severity, category, "
                    "title, evidence) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (job_id, f["id"], f["ruleId"], f["path"], f["line"],
                     f["severity"], f["category"], f["title"], f["evidence"]),
                )

            await cur.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS max_seq "
                "FROM job_events WHERE job_id = %s",
                (job_id,),
            )
            seq = int((await cur.fetchone())["max_seq"]) + 1
            await cur.execute(
                "INSERT INTO job_events (job_id, sequence, event, payload) "
                "VALUES (%s, %s, 'done', %s)",
                (job_id, seq, json.dumps(
                    {"total": len(findings), "usage": usage},
                    separators=(",", ":"),
                )),
            )
            await cur.execute(
                "UPDATE jobs SET status = 'done', finished_at = NOW(6) "
                "WHERE job_id = %s",
                (job_id,),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


async def fail_job(job_id, message):
    """Mark a job failed and append a status event for SSE."""
    async with conn_cursor() as (conn, cur):
        await conn.begin()
        try:
            await cur.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS max_seq "
                "FROM job_events WHERE job_id = %s",
                (job_id,),
            )
            seq = int((await cur.fetchone())["max_seq"]) + 1
            await cur.execute(
                "UPDATE jobs SET status = 'failed', error_message = %s, "
                "finished_at = NOW(6) WHERE job_id = %s",
                (message, job_id),
            )
            await cur.execute(
                "INSERT INTO job_events (job_id, sequence, event, payload) "
                "VALUES (%s, %s, 'status', %s)",
                (job_id, seq, json.dumps(
                    {"status": "failed", "error": message},
                    separators=(",", ":"),
                )),
            )
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


class BaseProvider(ABC):
    """Shared diff-review pipeline; subclasses plug in the analyzer.

    The pipeline runs after the submission-time validation: validate the diff
    again, chunk it, analyze every chunk through the provider, build the
    canonical Finding objects, then persist findings and the running/done
    status events that back the SSE stream.
    """

    name = "base"
    chunk_bytes = CHUNK_BYTES

    @abstractmethod
    async def analyze(self, chunk):
        """Review one chunk and return raw findings (Finding fields, no id)."""
        raise NotImplementedError

    async def run(self, job_id):
        """Validate, chunk, analyze, build findings, and persist the result.

        Each chunk's findings are streamed over SSE as soon as that chunk is
        analyzed ("as discovered"), before the job is marked done. The final
        ordered, deduplicated, maxFindings-truncated list is persisted with the
        'done' event. Any provider failure still ends in a failed job.
        """
        job = await get_job(job_id)
        if job is None:
            return
        await add_event(job_id, "status", {"status": "running"})
        try:
            diff = await load_diff(job_id)
            self.validate(diff)
            chunks = chunk_diff(diff, self.chunk_bytes)
            all_findings = []
            emitted_ids = set()
            for chunk in chunks:
                chunk_raw = await self.analyze(chunk)
                chunk_findings = self.build_findings(chunk_raw, None)
                all_findings.extend(chunk_findings)
                surviving = process_findings(all_findings, job["max_findings"])
                surviving_ids = {f["id"] for f in surviving}
                to_stream = [
                    f for f in chunk_findings
                    if f["id"] in surviving_ids and f["id"] not in emitted_ids
                ]
                emitted_ids.update(f["id"] for f in to_stream)
                await add_findings_events(job_id, to_stream)
            findings = process_findings(all_findings, job["max_findings"])
            usage = {
                "inputBytes": job["input_bytes"],
                "chunks": len(chunks),
                "cacheHit": False,
            }
            await persist_result(job_id, findings, usage)
        except ProviderError as exc:
            await fail_job(job_id, str(exc))
        except Exception as exc:
            await fail_job(job_id, f"provider failed: {exc}")

    def validate(self, diff):
        """Reject a diff that cannot be chunked or parsed."""
        if not isinstance(diff, str) or not diff.strip():
            raise ProviderError("diff is empty")
        if not HUNK_HEADER_RE.search(diff):
            raise ProviderError("diff is not parseable as a unified diff")

    def build_findings(self, raw, max_findings):
        normalized = []
        for finding in raw:
            normalized.append(normalize_finding(finding))
        return process_findings(normalized, max_findings)


def get_provider(name):
    """Return a provider instance for a provider name."""
    if name == "mock":
        from diffrev.processDiff.mock import MockProvider
        return MockProvider()
    if name == "llm":
        from diffrev.processDiff.llm import LlmProvider
        return LlmProvider()
    raise ProviderError(f"unknown provider: {name}")
