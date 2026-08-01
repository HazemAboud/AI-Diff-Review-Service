import asyncio
import json
import logging

from diffrev.processDiff.base import ProviderError, get_provider
from diffrev.reviewUtils.events import add_event
from diffrev.reviewUtils.queries import get_job
from diffrev.shared.db import conn_cursor

MAX_CONCURRENT = 4
POLL_INTERVAL = 0.5

log = logging.getLogger("diffrev.worker")


async def worker_loop() -> None:
    """Single dispatcher that claims queued jobs and processes up to
    MAX_CONCURRENT of them in parallel."""
    sem = asyncio.Semaphore(MAX_CONCURRENT)
    while True:
        await sem.acquire()
        job = await claim_next_job()
        if job is None:
            sem.release()
            await asyncio.sleep(POLL_INTERVAL)
            continue
        asyncio.create_task(_process_wrapper(job["job_id"], sem))


async def _process_wrapper(job_id: str, sem: asyncio.Semaphore) -> None:
    try:
        await _process(job_id)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("job %s crashed", job_id)
        await _fail(job_id, "internal error while processing job")
    finally:
        sem.release()


async def claim_next_job() -> dict | None:
    """Atomically move the oldest queued job to running, or return None."""
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT job_id FROM jobs WHERE status = 'queued' "
            "ORDER BY created_at ASC, job_id ASC LIMIT 1"
        )
        row = await cur.fetchone()
        if row is None:
            return None
        await cur.execute(
            "UPDATE jobs SET status = 'running', started_at = NOW(6) "
            "WHERE job_id = %s AND status = 'queued'",
            (row["job_id"],),
        )
        if cur.rowcount == 0:
            return None
        return {"job_id": row["job_id"]}


async def _process(job_id: str) -> None:
    job = await get_job(job_id)
    if job is None:
        return

    await add_event(job_id, "status", {"status": "running"})

    diff = await _load_diff(job_id)
    try:
        provider = get_provider(job["provider"])
        raw_findings = await provider.review(diff)
    except ProviderError as exc:
        await _fail(job_id, str(exc))
        return
    except Exception as exc:
        await _fail(job_id, f"provider failed: {exc}")
        return

    findings = process_findings(raw_findings, job["max_findings"])
    usage = {
        "inputBytes": job["input_bytes"],
        "chunks": job["chunks"],
        "cacheHit": job["cacheHit"],
    }
    await _persist_result(job_id, findings, usage)


async def _load_diff(job_id: str) -> str:
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT chunk_cont FROM chunks WHERE job_id = %s ORDER BY chunk_num",
            (job_id,),
        )
        rows = await cur.fetchall()
    return "".join(r["chunk_cont"] for r in rows)


def process_findings(findings: list[dict], max_findings: int) -> list[dict]:
    """Deduplicate by id, sort by (path, line, ruleId), truncate to max."""
    seen: set[str] = set()
    ordered: list[dict] = []
    for f in findings:
        fid = f.get("id")
        if fid in seen:
            continue
        seen.add(fid)
        ordered.append(f)
    ordered.sort(key=lambda f: (f["path"], f["line"], f["ruleId"]))
    return ordered[:max_findings]


async def _persist_result(job_id: str, findings: list[dict], usage: dict) -> None:
    """Write findings and the finding/done events, then mark the job done,
    all in one transaction."""
    async with conn_cursor() as (conn, cur):
        await conn.begin()
        try:
            await cur.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS max_seq "
                "FROM job_events WHERE job_id = %s",
                (job_id,),
            )
            seq = int((await cur.fetchone())["max_seq"]) + 1

            for f in findings:
                await cur.execute(
                    "INSERT INTO findings "
                    "(job_id, finding_id, rule_id, path, line, severity, category, "
                    "title, evidence) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (job_id, f["id"], f["ruleId"], f["path"], f["line"],
                     f["severity"], f["category"], f["title"], f["evidence"]),
                )
                await cur.execute(
                    "INSERT INTO job_events (job_id, sequence, event, payload) "
                    "VALUES (%s, %s, 'finding', %s)",
                    (job_id, seq, json.dumps(f, separators=(",", ":"))),
                )
                seq += 1

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


async def _fail(job_id: str, message: str) -> None:
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
