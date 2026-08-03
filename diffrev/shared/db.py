import os
from contextlib import asynccontextmanager
from pathlib import Path

import aiomysql

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
DB_NAME = os.getenv("DB_NAME", "diffrev_db")

pool = None


async def init_pool():
    """Create the database if needed, apply db.sql, and open a connection pool."""
    global pool
    if pool is not None:
        return pool

    conn = await aiomysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`")
            await conn.commit()
    finally:
        conn.close()

    pool = await aiomysql.create_pool(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        autocommit=True,
        minsize=1,
        maxsize=10,
        cursorclass=aiomysql.cursors.DictCursor,
    )
    await apply_schema(pool)
    return pool


async def apply_schema(pool):
    ddl_path = Path(__file__).resolve().parents[2] / "db.sql"
    if not ddl_path.exists():
        return
    statements = [
        s.strip() for s in ddl_path.read_text(encoding="utf-8").split(";") if s.strip()
    ]
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            for stmt in statements:
                await cur.execute(stmt)
            await ensure_unique_body_hash(cur)
            await drop_idem_job_fk(cur)


async def ensure_unique_body_hash(cur):
    """Backfill the UNIQUE index on jobs.body_hash for older databases."""
    await cur.execute(
        "SELECT COUNT(*) AS c FROM information_schema.statistics "
        "WHERE table_schema = DATABASE() AND table_name = 'jobs' "
        "AND index_name = 'uk_body_hash'"
    )
    row = await cur.fetchone()
    if row is None or row["c"]:
        return
    await cur.execute(
        "DELETE j FROM jobs j "
        "LEFT JOIN ("
        "  SELECT job_id FROM ("
        "    SELECT job_id, ROW_NUMBER() OVER ("
        "      PARTITION BY body_hash ORDER BY created_at DESC, job_id DESC"
        "    ) AS rn FROM jobs"
        "  ) t WHERE t.rn = 1"
        ") keep ON j.job_id = keep.job_id "
        "WHERE keep.job_id IS NULL"
    )
    await cur.execute("ALTER TABLE jobs ADD UNIQUE KEY uk_body_hash (body_hash)")


async def drop_idem_job_fk(cur):
    """Drop the legacy idempotency_keys->jobs foreign key."""
    await cur.execute(
        "SELECT constraint_name FROM information_schema.table_constraints "
        "WHERE table_schema = DATABASE() AND table_name = 'idempotency_keys' "
        "AND constraint_type = 'FOREIGN KEY'"
    )
    rows = await cur.fetchall()
    for row in rows:
        name = row.get("constraint_name") or row.get("CONSTRAINT_NAME")
        if not name:
            continue
        await cur.execute(
            f"ALTER TABLE idempotency_keys DROP FOREIGN KEY `{name}`"
        )


@asynccontextmanager
async def conn_cursor():
    """Yield (connection, cursor) from the shared pool, releasing on exit."""
    if pool is None:
        await init_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            yield conn, cur
