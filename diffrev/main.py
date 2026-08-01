import asyncio
import json
import math
import os
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse

from diffrev.processDiff.chunking import chunk_diff
from diffrev.reviewUtils.events import fetch_events_after
from diffrev.reviewUtils.queries import get_findings, get_job
from diffrev.reviewUtils.unique import (
    bind_idempotency_key,
    compute_body_hash,
    create_job,
    find_cached_job,
    normalize_options,
    resolve_idempotency,
)
from diffrev.reviewUtils.validity import validate_payload
from diffrev.shared.db import init_pool
from diffrev.shared.errors import error_envelope, raise_error
from diffrev.shared.ratelimit import RateLimiter
from diffrev.worker import worker_loop


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_pool()
    worker_task = asyncio.create_task(worker_loop())
    yield
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.detail,
            headers=exc.headers,
        )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail},
        headers=exc.headers,
    )


VERSION = "1.0.0"

START = time.time()

SPEC_LIMITS = {
    "maxPayloadBytes": 1048576,
    "chunkBytes": 65536,
    "maxConcurrentJobs": 4,
    "rateLimitPerMinute": 30,
}

VALID_TOKEN = os.getenv("API_BEARER_TOKEN", "your-secret-token-here")

RATE_LIMITER = RateLimiter(SPEC_LIMITS["rateLimitPerMinute"])

SSE_POLL_INTERVAL = 0.4


async def verify_bearer_token(request: Request):
    """Check the user's token, returns 401 if missing or wrong."""
    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        raise_error(status.HTTP_401_UNAUTHORIZED, "unauthorized",
                    "Missing or invalid authorization header")

    token = auth_header.split(" ")[1]
    if token != VALID_TOKEN:
        raise_error(status.HTTP_401_UNAUTHORIZED, "unauthorized", "Invalid bearer token")


@app.get("/health", status_code=status.HTTP_200_OK)
def health():
    uptime = time.time() - START
    return {"status": "ok", "version": VERSION, "uptimeSeconds": uptime}


@app.get("/spec", status_code=status.HTTP_200_OK)
def spec():
    return {
        "specVersion": "1.0",
        "providers": ["mock", "llm"],
        "limits": SPEC_LIMITS,
    }


@app.post("/v1/reviews", status_code=status.HTTP_202_ACCEPTED, dependencies=[Depends(verify_bearer_token)])
async def review(request: Request):
    """Initiates the diff review process after checking request validity."""
    allowed, retry_after = await RATE_LIMITER.check()
    if not allowed:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            headers={"Retry-After": str(max(1, int(math.ceil(retry_after))))},
            detail=error_envelope("rate_limited", "Rate limit exceeded, slow down"),
        )

    raw = await request.body()
    data = validate_payload(raw, SPEC_LIMITS["maxPayloadBytes"])

    options = normalize_options(data)
    provider = options["provider"]
    max_findings = options["maxFindings"]
    body_hash = compute_body_hash(data)
    idempotency_key = request.headers.get("Idempotency-Key")

    if idempotency_key:
        existing = await resolve_idempotency(idempotency_key, body_hash)
        if existing:
            return {"jobId": existing["job_id"], "status": existing["status"]}

    cached = await find_cached_job(body_hash)
    if cached:
        if idempotency_key:
            bound = await bind_idempotency_key(cached["job_id"], idempotency_key)
            if not bound:
                existing = await resolve_idempotency(idempotency_key, body_hash)
                return {"jobId": existing["job_id"], "status": existing["status"]}
        return {"jobId": cached["job_id"], "status": cached["status"]}

    chunks = chunk_diff(data["diff"])
    job = await create_job(
        body_hash=body_hash,
        input_bytes=len(raw),
        provider=provider,
        max_findings=max_findings,
        idempotency_key=idempotency_key,
        chunks=chunks,
    )
    return {"jobId": job["job_id"], "status": job["status"]}


@app.get("/v1/reviews/{job_id}", status_code=status.HTTP_200_OK,
         dependencies=[Depends(verify_bearer_token)])
async def get_review(job_id: str):
    job = await get_job(job_id)
    if job is None:
        raise_error(status.HTTP_404_NOT_FOUND, "not_found",
                    "No job found for the given jobId")

    resp = {
        "jobId": job["job_id"],
        "status": job["status"],
        "usage": {
            "inputBytes": job["input_bytes"],
            "chunks": job["chunks"],
            "cacheHit": job["cacheHit"],
        },
    }
    if job["status"] == "done":
        resp["findings"] = await get_findings(job_id)
    if job["status"] == "failed" and job["error_message"]:
        resp["error"] = job["error_message"]
    return resp


def _sse_event(event: str, payload: dict) -> str:
    return (f"event: {event}\ndata: "
            f"{json.dumps(payload, separators=(',', ':'))}\n\n")


@app.get("/v1/reviews/{job_id}/stream", status_code=status.HTTP_200_OK,
         dependencies=[Depends(verify_bearer_token)])
async def stream_review(job_id: str):
    job = await get_job(job_id)
    if job is None:
        raise_error(status.HTTP_404_NOT_FOUND, "not_found",
                    "No job found for the given jobId")

    async def gen():
        last_seq = 0
        while True:
            events = await fetch_events_after(job_id, last_seq)
            terminal = False
            for seq, event, payload in events:
                last_seq = seq
                if event == "status":
                    yield _sse_event("status", payload)
                    if payload.get("status") == "failed":
                        terminal = True
                elif event == "finding":
                    yield _sse_event("finding", payload)
                elif event == "done":
                    yield _sse_event("done", payload)
                    terminal = True
            if terminal:
                return

            current = await get_job(job_id)
            if current is None:
                return
            if current["status"] in ("done", "failed") and not events:
                for seq, event, payload in await fetch_events_after(job_id, last_seq):
                    last_seq = seq
                    yield _sse_event(event, payload)
                return
            await asyncio.sleep(SSE_POLL_INTERVAL)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
