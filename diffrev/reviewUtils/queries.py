from diffrev.shared.db import conn_cursor


def row_to_finding(row):
    return {
        "id": row["finding_id"],
        "ruleId": row["rule_id"],
        "path": row["path"],
        "line": int(row["line"]),
        "severity": row["severity"],
        "category": row["category"],
        "title": row["title"],
        "evidence": row["evidence"],
    }


def job_from_row(row, *, cache_hit, job_id_override=None, input_bytes_override=None,
                 findings_job_id=None):
    return {
        "job_id": job_id_override if job_id_override is not None else row["job_id"],
        "status": row["status"],
        "provider": row["provider"],
        "max_findings": int(row["max_findings"]),
        "input_bytes": int(input_bytes_override
                           if input_bytes_override is not None else row["input_bytes"]),
        "chunks": int(row["chunks"]),
        "cacheHit": cache_hit,
        "findings_job_id": findings_job_id or row["job_id"],
        "error_message": row["error_message"],
    }


async def get_job(job_id):
    """Fetch a real jobs row (the original computation), not a cache_hits row.

    Used by the worker and provider pipeline, which only ever process real jobs.
    """
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT job_id, status, provider, max_findings, input_bytes, chunks, "
            "error_message FROM jobs WHERE job_id = %s",
            (job_id,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return job_from_row(row, cache_hit=False)


async def get_submission(job_id):
    """Fetch whatever a client-facing id refers to: either a real job (a fresh
    computation, cacheHit=False) or a cache_hits row (a duplicate submission,
    cacheHit=True) that resolves to its source job's live state."""
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT job_id, source_job_id, input_bytes FROM cache_hits WHERE job_id = %s",
            (job_id,),
        )
        hit = await cur.fetchone()
        if hit is None:
            await cur.execute(
                "SELECT job_id, status, provider, max_findings, input_bytes, chunks, "
                "error_message FROM jobs WHERE job_id = %s",
                (job_id,),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            return job_from_row(row, cache_hit=False)
        await cur.execute(
            "SELECT status, provider, max_findings, input_bytes, chunks, error_message "
            "FROM jobs WHERE job_id = %s",
            (hit["source_job_id"],),
        )
        source = await cur.fetchone()
        if source is None:
            return None
        return job_from_row(
            source,
            cache_hit=True,
            job_id_override=job_id,
            input_bytes_override=hit["input_bytes"],
            findings_job_id=hit["source_job_id"],
        )


async def get_findings(job_id):
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT finding_id, rule_id, path, line, severity, category, title, evidence "
            "FROM findings WHERE job_id = %s ORDER BY path, line, rule_id",
            (job_id,),
        )
        rows = await cur.fetchall()
    return [row_to_finding(r) for r in rows]
