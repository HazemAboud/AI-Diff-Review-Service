import hashlib
import json
import uuid

from aiomysql import IntegrityError
from fastapi import status

from diffrev.shared.db import conn_cursor
from diffrev.shared.errors import raise_error


def compute_body_hash(data):
    """sha256 of the canonical {diff, options}, so byte-identical intent hashes equal."""
    dat = {"diff": data.get("diff"), "options": data.get("options")}
    raw = json.dumps(dat, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


STATUS_FOR_JOB_SQL = (
    "SELECT j.status FROM jobs j WHERE j.job_id = %s "
    "UNION ALL "
    "SELECT src.status FROM cache_hits ch "
    "JOIN jobs src ON src.job_id = ch.source_job_id "
    "WHERE ch.job_id = %s"
)


async def resolve_idempotency(key, body_hash):
    """
    Return {job_id, status} if the key maps to a submission with the same body
    else raise 409. Returns None when the key has never been seen.

    A key may be bound to a real job or to a cache-hit row (record_cache_hit
    binds keys to the latter, and a cache_hits row never has a jobs row of its
    own), so resolve through both tables instead of only jobs.
    """
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT "
            "  COALESCE(j.job_id, ch.job_id) AS job_id, "
            "  COALESCE(j.body_hash, src.body_hash) AS body_hash, "
            "  COALESCE(j.status, src.status) AS status "
            "FROM idempotency_keys k "
            "LEFT JOIN jobs j ON j.job_id = k.job_id "
            "LEFT JOIN cache_hits ch ON ch.job_id = k.job_id "
            "LEFT JOIN jobs src ON src.job_id = ch.source_job_id "
            "WHERE k.idem_key = %s",
            (key,),
        )
        row = await cur.fetchone()
    if row is None or row["job_id"] is None:
        return None
    if row["body_hash"] != body_hash:
        raise_error(
            status.HTTP_409_CONFLICT,
            "idempotency_conflict",
            f"Idempotency-Key {key!r} was already used with a different request body",
        )
    return {"job_id": row["job_id"], "status": row["status"]}


async def bind_idempotency_key(key, body_hash, job_id):
    """Persist `key -> job_id` and return the final {job_id, status}.

    `job_id` may be a real job or a cache-hit row. On a duplicate-key race the
    earlier binding wins: if it resolves to a submission for the same body the
    request returns that job, otherwise it raises 409. A binding left pointing
    at a row that no longer exists (e.g. a source job removed by dedup) is
    dropped so the key can be re-bound instead of failing forever.
    """
    async with conn_cursor() as (conn, cur):
        try:
            await conn.begin()
            await cur.execute(
                "INSERT INTO idempotency_keys (idem_key, job_id) VALUES (%s, %s)",
                (key, job_id),
            )
            await cur.execute(STATUS_FOR_JOB_SQL, (job_id, job_id))
            row = await cur.fetchone()
            await conn.commit()
            return {"job_id": job_id, "status": row["status"] if row else "queued"}
        except IntegrityError:
            await conn.rollback()
    resolved = await resolve_idempotency(key, body_hash)
    if resolved is not None:
        return resolved
    async with conn_cursor() as (conn, cur):
        await conn.begin()
        await cur.execute(
            "DELETE FROM idempotency_keys WHERE idem_key = %s", (key,)
        )
        try:
            await cur.execute(
                "INSERT INTO idempotency_keys (idem_key, job_id) VALUES (%s, %s)",
                (key, job_id),
            )
        except IntegrityError:
            await conn.rollback()
            return await resolve_idempotency(key, body_hash)
        await cur.execute(STATUS_FOR_JOB_SQL, (job_id, job_id))
        row = await cur.fetchone()
        await conn.commit()
    return {"job_id": job_id, "status": row["status"] if row else "queued"}


async def find_cached_job(body_hash):
    """Return the source job for `body_hash`, if any. Does not mutate the job;
    a cache hit is recorded per submission, not on the shared row."""
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT job_id, status FROM jobs "
            "WHERE body_hash = %s LIMIT 1",
            (body_hash,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return {"job_id": row["job_id"], "status": row["status"]}


async def record_cache_hit(source_job_id, input_bytes):
    """Register a duplicate submission that was served from the source job's
    cache. Returns the opaque jobId handed to that submitter; its GET/SSE
    resolve to the source job's live state with cacheHit=True."""
    job_id = str(uuid.uuid4())
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "INSERT INTO cache_hits (job_id, source_job_id, input_bytes) "
            "VALUES (%s, %s, %s)",
            (job_id, source_job_id, input_bytes),
        )
    return job_id


async def create_job(
    *,
    body_hash,
    input_bytes,
    provider,
    max_findings,
    idempotency_key,
    chunks,
):
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
                cached = await find_cached_job(body_hash)
                if cached is not None:
                    job_id = await record_cache_hit(cached["job_id"], input_bytes)
                    if idempotency_key:
                        bound = await bind_idempotency_key(
                            idempotency_key, body_hash, job_id
                        )
                        if bound is not None:
                            return {**bound, "created": False}
                    return {"job_id": job_id, "status": cached["status"],
                            "created": False}
                raise
            return resolved
    return {"job_id": job_id, "status": "queued", "created": True}
