import asyncio
import logging

from diffrev.processDiff.base import ProviderError, fail_job, get_provider
from diffrev.reviewUtils.queries import get_job
from diffrev.shared.config import MAX_CONCURRENT_JOBS
from diffrev.shared.db import conn_cursor

POLL_INTERVAL = 0.5

log = logging.getLogger("diffrev.worker")


async def worker_loop():
    """Single dispatcher that claims queued jobs and processes up to
    MAX_CONCURRENT_JOBS of them in parallel.

    Every step that touches the database is guarded: a transient claim
    failure is logged and the loop keeps going, so a DB blip can never
    silently kill the dispatcher and strand queued jobs forever.
    """
    sem = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
    while True:
        await sem.acquire()
        try:
            job = await claim_next_job()
        except asyncio.CancelledError:
            sem.release()
            raise
        except Exception:
            log.exception("failed to claim the next job; continuing")
            sem.release()
            await asyncio.sleep(POLL_INTERVAL)
            continue
        if job is None:
            sem.release()
            await asyncio.sleep(POLL_INTERVAL)
            continue
        asyncio.create_task(process_wrapper(job["job_id"], sem))


async def process_wrapper(job_id, sem):
    try:
        await process(job_id)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("job %s crashed", job_id)
        await fail_job(job_id, "internal error while processing job")
    finally:
        sem.release()


async def claim_next_job():
    """Atomically move the oldest queued job to running, or return None.

    The claim runs in one explicit transaction: if anything fails mid-way the
    UPDATE rolls back, so the job is never left half-claimed as 'running'
    without a processor.
    """
    async with conn_cursor() as (conn, cur):
        await conn.begin()
        try:
            await cur.execute(
                "SELECT job_id FROM jobs WHERE status = 'queued' "
                "ORDER BY created_at ASC, job_id ASC LIMIT 1"
            )
            row = await cur.fetchone()
            if row is None:
                await conn.commit()
                return None
            await cur.execute(
                "UPDATE jobs SET status = 'running', started_at = NOW(6) "
                "WHERE job_id = %s AND status = 'queued'",
                (row["job_id"],),
            )
            if cur.rowcount == 0:
                await conn.commit()
                return None
            await conn.commit()
            return {"job_id": row["job_id"]}
        except Exception:
            await conn.rollback()
            raise


async def process(job_id):
    job = await get_job(job_id)
    if job is None:
        return
    try:
        provider = get_provider(job["provider"])
    except ProviderError as exc:
        await fail_job(job_id, str(exc))
        return
    await provider.run(job_id)
