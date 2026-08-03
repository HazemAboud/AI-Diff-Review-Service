import json

from diffrev.shared.db import conn_cursor


async def add_event(job_id, event, payload):
    """Append one event to a job's event log; returns its sequence number."""
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT COALESCE(MAX(sequence), 0) + 1 AS next_seq "
            "FROM job_events WHERE job_id = %s",
            (job_id,),
        )
        row = await cur.fetchone()
        seq = int(row["next_seq"])
        await cur.execute(
            "INSERT INTO job_events (job_id, sequence, event, payload) "
            "VALUES (%s, %s, %s, %s)",
            (job_id, seq, event, json.dumps(payload, separators=(",", ":"))),
        )
        return seq


async def add_findings_events(job_id, findings):
    """Append one 'finding' event per finding, in a single transaction."""
    if not findings:
        return
    async with conn_cursor() as (conn, cur):
        await conn.begin()
        try:
            await cur.execute(
                "SELECT COALESCE(MAX(sequence), 0) AS max_seq "
                "FROM job_events WHERE job_id = %s",
                (job_id,),
            )
            seq = int((await cur.fetchone())["max_seq"]) + 1
            for finding in findings:
                await cur.execute(
                    "INSERT INTO job_events (job_id, sequence, event, payload) "
                    "VALUES (%s, %s, 'finding', %s)",
                    (job_id, seq, json.dumps(finding, separators=(",", ":"))),
                )
                seq += 1
            await conn.commit()
        except Exception:
            await conn.rollback()
            raise


async def fetch_events_after(job_id, after_seq):
    """Return (sequence, event, payload) rows strictly after `after_seq`."""
    async with conn_cursor() as (conn, cur):
        await cur.execute(
            "SELECT sequence, event, payload FROM job_events "
            "WHERE job_id = %s AND sequence > %s ORDER BY sequence",
            (job_id, after_seq),
        )
        rows = await cur.fetchall()
    return [
        (int(r["sequence"]), r["event"], json.loads(r["payload"]))
        for r in rows
    ]
