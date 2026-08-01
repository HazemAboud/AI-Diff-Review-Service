from diffrev.shared.db import conn_cursor


def _row_to_finding(row: dict) -> dict:
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


async def get_job(job_id: str) -> dict | None:
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT job_id, status, provider, max_findings, input_bytes, chunks, "
            "cacheHit, error_message FROM jobs WHERE job_id = %s",
            (job_id,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return {
        "job_id": row["job_id"],
        "status": row["status"],
        "provider": row["provider"],
        "max_findings": int(row["max_findings"]),
        "input_bytes": int(row["input_bytes"]),
        "chunks": int(row["chunks"]),
        "cacheHit": bool(row["cacheHit"]),
        "error_message": row["error_message"],
    }


async def get_findings(job_id: str) -> list[dict]:
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT finding_id, rule_id, path, line, severity, category, title, evidence "
            "FROM findings WHERE job_id = %s ORDER BY path, line, rule_id",
            (job_id,),
        )
        rows = await cur.fetchall()
    return [_row_to_finding(r) for r in rows]
