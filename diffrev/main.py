from diffrev.shared.env import load_env

load_env()

import asyncio
import hmac
import json
import math
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from diffrev.processDiff.chunking import chunk_diff
from diffrev.reviewUtils.events import fetch_events_after
from diffrev.reviewUtils.queries import get_findings, get_submission
from diffrev.reviewUtils.unique import (
    bind_idempotency_key,
    compute_body_hash,
    create_job,
    find_cached_job,
    record_cache_hit,
    resolve_idempotency,
)
from diffrev.reviewUtils.validity import validate_options, validate_payload

from diffrev.shared.config import SPEC_LIMITS
from diffrev.shared.db import init_pool
from diffrev.shared.errors import error_envelope, raise_error
from diffrev.shared.ratelimit import RateLimiter
from diffrev.worker import worker_loop


@asynccontextmanager
async def lifespan(app):
    await init_pool()
    worker_task = asyncio.create_task(worker_loop())
    yield
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


HTTP_STATUS_TO_CODE = {
    400: "invalid_json",
    401: "unauthorized",
    404: "not_found",
    405: "not_found",
    409: "idempotency_conflict",
    413: "payload_too_large",
    422: "invalid_diff",
    429: "rate_limited",
}


@app.exception_handler(HTTPException)
@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request, exc):
    """Every non-2xx response uses the error envelope.

    Registered for both `fastapi` (raised by route handlers/dependencies) and
    `starlette` (raised by the router itself for 404/405) HTTPException so no
    /v1 response ever escapes as the raw {"detail": ...} body.
    """
    if isinstance(exc.detail, dict) and "error" in exc.detail:
        code = exc.detail["error"]["code"]
        message = exc.detail["error"]["message"]
    else:
        code = HTTP_STATUS_TO_CODE.get(exc.status_code, "internal")
        message = str(exc.detail)
    return JSONResponse(
        status_code=exc.status_code,
        content=error_envelope(code, message),
        headers=exc.headers,
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc):
    """Catch anything unexpected so the client never sees a raw 500."""
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=error_envelope("internal", "internal server error"),
    )


VERSION = "1.0.0"

START = time.time()

DEFAULT_MAX_FINDINGS = 100

VALID_TOKEN = os.getenv("API_BEARER_TOKEN")
if not VALID_TOKEN:
    raise RuntimeError(
        "API_BEARER_TOKEN must be set to a non-empty value before starting the service"
    )

RATE_LIMITER = RateLimiter(SPEC_LIMITS["rateLimitPerMinute"])

SSE_POLL_INTERVAL = 0.4


@app.middleware("http")
async def require_bearer_on_v1(request, call_next):
    """Require a valid bearer token on every /v1 path and method, so even a
    request that would otherwise be a 404 or 405 is checked for auth first.
    This is the single auth implementation; token comparison is constant-time.
    """
    if request.url.path.startswith("/v1"):
        auth_header = request.headers.get("Authorization")
        token_ok = False
        if auth_header and auth_header.startswith("Bearer "):
            parts = auth_header.split(" ")
            if len(parts) >= 2 and hmac.compare_digest(parts[1], VALID_TOKEN):
                token_ok = True
        if not token_ok:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content=error_envelope(
                    "unauthorized", "Missing or invalid authorization header"
                ),
            )
    return await call_next(request)


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


@app.post("/v1/reviews", status_code=status.HTTP_202_ACCEPTED)
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

    options = data.get("options", {})
    provider, max_findings = validate_options(options, DEFAULT_MAX_FINDINGS)
    body_hash = compute_body_hash(data)
    idempotency_key = request.headers.get("Idempotency-Key")
    input_bytes = len(raw)

    if idempotency_key:
        existing = await resolve_idempotency(idempotency_key, body_hash)
        if existing:
            return {"jobId": existing["job_id"], "status": existing["status"]}

    cached = await find_cached_job(body_hash)
    if cached:
        job_id = await record_cache_hit(cached["job_id"], input_bytes)
        if idempotency_key:
            bound = await bind_idempotency_key(idempotency_key, body_hash, job_id)
            if bound is not None:
                return {"jobId": bound["job_id"], "status": bound["status"]}
        return {"jobId": job_id, "status": cached["status"]}

    chunks = chunk_diff(data["diff"], SPEC_LIMITS["chunkBytes"])
    job = await create_job(
        body_hash=body_hash,
        input_bytes=input_bytes,
        provider=provider,
        max_findings=max_findings,
        idempotency_key=idempotency_key,
        chunks=chunks,
    )
    return {"jobId": job["job_id"], "status": job["status"]}


@app.get("/v1/reviews/{job_id}", status_code=status.HTTP_200_OK)
async def get_review(job_id):
    job = await get_submission(job_id)
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
        resp["findings"] = await get_findings(job["findings_job_id"])
    return resp


def sse_event(event, payload):
    return (f"event: {event}\ndata: "
            f"{json.dumps(payload, separators=(',', ':'))}\n\n")


@app.get("/v1/reviews/{job_id}/stream", status_code=status.HTTP_200_OK)
async def stream_review(job_id):
    job = await get_submission(job_id)
    if job is None:
        raise_error(status.HTTP_404_NOT_FOUND, "not_found",
                    "No job found for the given jobId")

    events_job_id = job["findings_job_id"]
    cache_hit = job["cacheHit"]

    def emit(event, payload):
        if event == "done" and cache_hit:
            payload = {
                **payload,
                "usage": {**payload.get("usage", {}), "cacheHit": True},
            }
        return sse_event(event, payload)

    async def gen():
        last_seq = 0
        while True:
            events = await fetch_events_after(events_job_id, last_seq)
            terminal = False
            for seq, event, payload in events:
                last_seq = seq
                if event == "status":
                    yield emit("status", payload)
                    if payload.get("status") == "failed":
                        terminal = True
                elif event == "finding":
                    yield emit("finding", payload)
                elif event == "done":
                    yield emit("done", payload)
                    terminal = True
            if terminal:
                return

            current = await get_submission(job_id)
            if current is None:
                return
            if current["status"] in ("done", "failed") and not events:
                for seq, event, payload in await fetch_events_after(events_job_id, last_seq):
                    last_seq = seq
                    yield emit(event, payload)
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
