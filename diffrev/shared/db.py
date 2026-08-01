import os
from contextlib import asynccontextmanager
from pathlib import Path

import aiomysql

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "root")
DB_NAME = os.getenv("DB_NAME", "diffrev_db")

_pool = None


async def init_pool() -> aiomysql.Pool:
    """Create the database if needed, apply db.sql, and open a connection pool."""
    global _pool
    if _pool is not None:
        return _pool

    conn = await aiomysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD
    )
    try:
        async with conn.cursor() as cur:
            await cur.execute(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`")
            await conn.commit()
    finally:
        conn.close()

    _pool = await aiomysql.create_pool(
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
    await _apply_schema(_pool)
    return _pool


async def _apply_schema(pool: aiomysql.Pool) -> None:
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


@asynccontextmanager
async def conn_cursor():
    """Yield (connection, cursor) from the shared pool, releasing on exit."""
    if _pool is None:
        await init_pool()
    async with _pool.acquire() as conn:
        async with conn.cursor() as cur:
            yield conn, cur
