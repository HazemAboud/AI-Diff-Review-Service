import hashlib
import json
import uuid

from aiomysql import IntegrityError
from fastapi import status

from diffrev.shared.db import conn_cursor
from diffrev.shared.errors import raise_error

DEFAULT_MAX_FINDINGS = 100
VALID_PROVIDERS = ("mock", "llm")


def normalize_options(data: dict) -> dict:
    """Canonical {provider, maxFindings} used both for hashing and storage."""
    options = data.get("options")
    if not isinstance(options, dict):
        options = {}
    provider = options.get("provider", "mock")
    if provider not in VALID_PROVIDERS:
        provider = "mock"
    try:
        max_findings = int(options.get("maxFindings", DEFAULT_MAX_FINDINGS))
    except (TypeError, ValueError):
        max_findings = DEFAULT_MAX_FINDINGS
    return {"provider": provider, "maxFindings": max(0, max_findings)}


def compute_body_hash(data: dict) -> str:
    """sha256 of the canonical {diff, options}, so byte-identical intent hashes equal."""
    options = normalize_options(data)
    dat = {"diff": data.get("diff"), "options": options}
    raw = json.dumps(dat, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def resolve_idempotency(key: str, body_hash: str) -> dict | None:
    """
    Return {job_id, status} if the key maps to a job with the same body else
    raise 409. Returns None when the key has never been seen.
    """
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT k.job_id, j.body_hash, j.status "
            "FROM idempotency_keys k JOIN jobs j ON j.job_id = k.job_id "
            "WHERE k.idem_key = %s",
            (key,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    if row["body_hash"] != body_hash:
        raise_error(
            status.HTTP_409_CONFLICT,
            "idempotency_conflict",
            f"Idempotency-Key {key!r} was already used with a different request body",
        )
    return {"job_id": row["job_id"], "status": row["status"]}


async def bind_idempotency_key(job_id: str, key: str) -> bool:
    """Map an idempotency key to an existing job; False if the key is taken."""
    async with conn_cursor() as (conn, cur):
        try:
            await cur.execute(
                "INSERT INTO idempotency_keys (idem_key, job_id) VALUES (%s, %s)",
                (key, job_id),
            )
            return True
        except IntegrityError:
            return False


async def find_cached_job(body_hash: str) -> dict | None:
    """Return the most recent job for `body_hash`, marked as a cache hit."""
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT job_id, status FROM jobs "
            "WHERE body_hash = %s ORDER BY created_at DESC, job_id DESC LIMIT 1",
            (body_hash,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "UPDATE jobs SET cacheHit = TRUE WHERE job_id = %s",
            (row["job_id"],),
        )
    return {"job_id": row["job_id"], "status": row["status"], "created": False}


async def create_job(
    *,
    body_hash: str,
    input_bytes: int,
    provider: str,
    max_findings: int,
    idempotency_key: str | None,
    chunks: list[str],
) -> dict:
    """Insert a new queued job (with its chunks and initial status event) in one
    transaction; on a duplicate-key race, re-resolve instead."""
    job_id = str(uuid.uuid4())
    async with conn_cursor() as (conn, cur):
        try:
            await conn.begin()
            await cur.execute(
                "INSERT INTO jobs "
                "(job_id, status, provider, max_findings, input_bytes, chunks, body_hash) "
                "VALUES (%s, 'queued', %s, %s, %s, %s, %s)",
                (job_id, provider, max_findings, input_bytes, len(chunks), body_hash),
            )
            for num, chunk in enumerate(chunks, start=1):
                await cur.execute(
                    "INSERT INTO chunks (job_id, chunk_num, chunk_cont) "
                    "VALUES (%s, %s, %s)",
                    (job_id, num, chunk),
                )
            await cur.execute(
                "INSERT INTO job_events (job_id, sequence, event, payload) "
                "VALUES (%s, 1, 'status', %s)",
                (job_id, json.dumps({"status": "queued"})),
            )
            if idempotency_key:
                await cur.execute(
                    "INSERT INTO idempotency_keys (idem_key, job_id) VALUES (%s, %s)",
                    (idempotency_key, job_id),
                )
            await conn.commit()
        except IntegrityError:
            await conn.rollback()
            resolved = None
            if idempotency_key:
                resolved = await resolve_idempotency(idempotency_key, body_hash)
            if resolved is None:
                resolved = await find_cached_job(body_hash)
            if resolved is None:
                raise
            return resolved
    return {"job_id": job_id, "status": "queued", "created": True}
