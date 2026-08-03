# SUBMISSION.md

## Architecture

FastAPI app with MySQL DB. A submission is either a **real job** (a row in `jobs`, with child rows in `chunks`, `findings`, and
`job_events`) or a **cache hit** (a row in the `cache_hits` table that points at the
real job which already has a result). `POST /v1/reviews`
validates, chunks, and inserts a `queued` job — or, for a duplicate
`{diff, options}`, records a cache hit — then returns `202`.
A single background `worker_loop` polls for queued jobs and runs up to `MAX_CONCURRENT_JOBS` of them
concurrently via an `asyncio.Semaphore`. Each job is processed by a
`Provider` (`mock` or `llm`) through one shared pipeline in
`BaseProvider.run`: validate → chunk → analyze per chunk → normalize → store. All state — job status, findings, and the
SSE event log — lives in MySQL, not in memory, so `GET` and `/stream` are
stateless reads that are correct across worker restarts and would remain
correct with multiple app instances behind a load balancer.

The API was deployed as a container on a GCP VPS.

---

## Database schema

### `jobs` — one row per unique computation

| column        | type          | notes |
|---------------|---------------|-------|
| `job_id`      | `CHAR(36)`    | opaque client-facing id (UUID); PK |
| `status`      | `ENUM('queued','running','done','failed')` | lifecycle; drives worker claim and SSE |
| `provider`    | `ENUM('mock','llm')` | provider chosen at submit |
| `max_findings`| `INT UNSIGNED`| truncation ceiling |
| `input_bytes` | `INT UNSIGNED`| raw payload byte size (reported as `usage.inputBytes`) |
| `chunks`      | `INT UNSIGNED`| chunk count (reported as `usage.chunks`) |
| `body_hash`   | `CHAR(64)`    | SHA-256 of `{diff, options}`; **UNIQUE** |
| `error_message` | `TEXT NULL` | set when the job fails |
| `created_at` / `started_at` / `finished_at` | `DATETIME(6)` | timestamps |

Tracks all details required to drive and report on a job.

### `idempotency_keys` — `Idempotency-Key` → submission binding

| column     | type          | notes |
|------------|---------------|-------|
| `idem_key` | `VARCHAR(255)`| the client's header; PK |
| `job_id`   | `CHAR(36)`    | a real job **or** a `cache_hits` row |

This ensures idempotency even for cache-hit jobs (new ids with the same body are also tracked, so an idempotency key reused with a different body correctly returns `409`).

### `cache_hits` — one row per duplicate submission

| column          | type          | notes |
|-----------------|---------------|-------|
| `job_id`        | `CHAR(36)`    | the jobId returned to *this* submitter; PK |
| `source_job_id` | `CHAR(36)`    | FK → `jobs`, `ON DELETE CASCADE` |
| `input_bytes`   | `INT UNSIGNED`| the size of *this* submission's payload |
| `created_at`    | `DATETIME(6)` | |

Tracks cache hits and stores a reference to the original job; used along with `idempotency_keys` to produce the required behavior.

### `chunks` — the stored diff, split at file boundaries

| column       | type       | notes |
|--------------|------------|-------|
| `job_id`     | `CHAR(36)` | FK → `jobs`, `ON DELETE CASCADE` |
| `chunk_num`  | `INT UNSIGNED` | 1-based ordinal |
| `chunk_content` | `MEDIUMTEXT` | the chunk text |

Chunks are ordered with their raw content.

### `findings` — the persisted result

| column     | type       | notes |
|------------|------------|-------|
| `job_id`   | `CHAR(36)` | the *source* job; FK → `jobs`, `ON DELETE CASCADE` |
| `finding_id` | `VARCHAR(512)` | `"ruleId:path:line"` — the spec's dedup id |
| `rule_id`  | `VARCHAR(20)` | |
| `path`     | `TEXT`     | |
| `line`     | `INT UNSIGNED` | line number in the new file |
| `severity` | `ENUM('critical','high','medium','low')` | |
| `category` | `ENUM('security','correctness','performance','style')` | |
| `title`    | `VARCHAR(255)` | |
| `evidence` | `TEXT`     | the offending added line, verbatim |

Stores all required finding fields.

### `job_events` — the SSE event log

| column     | type       | notes |
|------------|------------|-------|
| `job_id`   | `CHAR(36)` | |
| `sequence` | `INT UNSIGNED` | monotonic per job |
| `event`    | `ENUM('status','finding','done')` | |
| `payload`  | `JSON`     | the event body |
| `ev_time`  | `DATETIME(6)` | |

Stores the events for each job, in order.

---

## Provider design

Both providers extend `BaseProvider` and implement one method:
`async def analyze(chunk) -> list[raw_finding]`. Everything else — diff
re-validation, chunking, building `Finding` objects,
deduplication by `id`, sorting by `(path, line, ruleId)`, `maxFindings` truncation, DB storage, and SSE event emission — lives once in the base
class.

- **`mock`** (`mock.py`): regex/string matching over lines
  reconstructed from the unified diff, based on the rules table in the spec.

- **`llm`** (`llm.py`): calls the Gemini `generateContent` API with a
  fixed prompt requesting a JSON findings array, once per chunk. The model is accessed through two environment variables: `LLM_API_KEY` (credential) and `LLM_BASE_URL` (endpoint). Both are configured only on the server; requests from clients carry only the service's own bearer token.

Both providers return "raw" findings; `normalize_finding` in `base.py` is the one
place that coerces them into the `Finding` object shape and builds the
`id` (`{ruleId}:{path}:{line}`) used for dedup — mainly to handle variance in LLM output.

---

## Route algorithms

### `GET /health` — public

1. Compute `uptime = time.time() - START` (`START` captured once at startup).
2. Return `{"status": "ok", "version": "1.0.0", "uptimeSeconds": uptime}`.

### `GET /spec` — public

1. Returns the declared spec constants, which are imported across the app so they act as the single source of truth for both the `/spec` response and actual enforcement (rate limiting, concurrency, chunk size, payload limit).

### Auth

1. If `request.url.path.startswith("/v1")`:
   - Read the `Authorization` header.
   - Accept if it starts with `Bearer ` **and**
     `hmac.compare_digest(token, VALID_TOKEN)` holds (constant-time).
   - Otherwise return `401 {"error":{"code":"unauthorized",...}}`.
2. Else pass through.

`require_bearer_on_v1` runs as middleware before FastAPI's router, so an unauthenticated request to a nonexistent `/v1/...` path still gets `401`, never a `404` that would leak route existence.

### `POST /v1/reviews` — algorithm

1. **Rate limit.** `RATE_LIMITER.check()` (sliding 60 s window). Over limit → `429` with a `Retry-After`
   header and the error envelope.
2. **Payload validation** (`validate_payload`):
   - Raw byte length > 1 MiB → `413 payload_too_large`.
   - Not valid JSON → `400 invalid_json`.
   - `diff` missing/empty/not a string → `422 invalid_diff`.
   - No unified-diff hunk header (`@@ -a,b +c,d @@`) → `422 invalid_diff`.
3. **Options** (`validate_options`): `provider` must be `mock`/`llm`
   (default `mock`); `maxFindings` must be an `int`, default `100`. Bad values → `422`.
4. **Body hash**: SHA-256 of a normalized JSON of `{diff, options}`, to guarantee the same hash for identical requests regardless of extra fields.
5. **Idempotency-Key**:
   - same key + same body hash → return that submission's `{jobId, status}`
     immediately, doing no new work;
   - same key + different body hash → `409 idempotency_conflict`;
   - unseen key → continue.
6. **Cache**: `find_cached_job(body_hash)` (unique, ≤1 row). If found,
   `record_cache_hit(source_job_id, input_bytes)` **always** creates a new
   `jobId` for this submission (a `cache_hits` row) — regardless of whether an idempotency key was supplied. If a key *was* supplied,
   `bind_idempotency_key` additionally binds it to this new hit id.
   Either way, respond `{jobId: <hit id>, status: <source job's status>}`.
7. **New job**: `chunk_diff(diff, 65536)`. `create_job`
   inserts the `jobs` row, its `chunks` rows, an initial `status: queued`
   `job_events` row, and the idempotency binding, all in one
   transaction.
8. Return `202 {"jobId": ..., "status": "queued"}` (or the resolved status
   for an idempotent/cached submission — which may already be `done`).

The worker claims the job asynchronously; this handler never blocks on
provider work.

### `GET /v1/reviews/{jobId}` — algorithm

1. `get_submission(job_id)`: look up `cache_hits` first, then `jobs`.
   Neither → `404 not_found`. (This checks both the original and cache-hit ids for the same underlying job.)
2. For a cache hit: report the *source* job's live `status`, `chunks`, and
   provider, but this hit's own `input_bytes` and `cacheHit: true`.
3. Build the response: `{jobId, status, usage:{inputBytes, chunks,
   cacheHit}}`; when `status == "done"`, fetch `findings` via
   `get_findings(findings_job_id)` — for a cache hit that's the *source*
   job id — pre-sorted by `path, line, rule_id` at the SQL level.

### `GET /v1/reviews/{jobId}/stream` — algorithm

1. Look up the job; unknown id → `404`.
2. Loop through `job_events` for rows with `sequence` greater than the last one sent, polling every 0.4 s.
3. Send each row as an SSE event and stop once status is terminal (`done`/`failed`).
4. A finished job replays all events identically in one query.

---

## Chunking (`chunking.py`)

`chunk_diff` splits only on file boundaries (`diff --git ` / `--- `),
never mid-file:
- ≤ 64 KiB, or a single file (even over budget) → one chunk.
- Otherwise the first file becomes its own chunk and the process repeats on
  the rest.

Detection is hunk-aware: `@@ -a,b +c,d @@` line counts are consumed before
checking for a boundary, so added lines that merely look like headers are
never mistaken for file boundaries (this logic is reused in `mock.py`).

---

## Concurrency & rate limiting

### Concurrency

- **Queue**: jobs queue as DB rows, so a 5th+ submission is
  queued and processed later.
- **Bounded parallelism**: one dispatcher task holds
  `asyncio.Semaphore(MAX_CONCURRENT_JOBS)` (= 4, the same constant declared in `/spec`).
- **Atomic claim** (`claim_next_job`): a single transaction
  (`SELECT ... WHERE status='queued' LIMIT 1` then `UPDATE ... WHERE status='queued'`) with the
  update's `rowcount` checked, so racing workers never double-claim a job.
- **Non-blocking**: the `llm` provider's HTTP call runs in a thread
  (`asyncio.to_thread`), so one job's network wait never stalls the loop.

### Rate limiting

- Sliding 60 s window over a `deque` of hit timestamps (`ratelimit.py`);
  over limit → `429 rate_limited` with `Retry-After`, never a `5xx`.
- Applied to `POST /v1/reviews` only (before the body is read); GETs are
  never limited.
- The limit is imported from the same `SPEC_LIMITS` constant used by `/spec`.

---

## Verification of cross-cutting behaviors

An extensive AI-generated test suite (`test_cases.py`) verifies the API behavior:

- **Chunking**: multi-file diffs over the 64 KiB boundary are
  never split mid-file, every chunk is ≤64 KiB except a single oversized file, and
  findings are correct.
- **Findings ordering/dedup**: the same diff with default and tiny chunk sizes
  yields identical final lists — ordered by `(path, line, ruleId)`, deduped
  by `id`, truncated by `maxFindings`.
- **Idempotency and caching**: same key + same body → same `jobId`; same key
  + different body → `409`; no-key duplicate → `cacheHit: true` with
  identical findings and no new `jobs` row; a key whose first use was a
  cache hit resolves to the same original `job_id` on replay.
- **SSE replay**: `/stream` on a finished real job and on a cache-hit
  submission both replay `status → finding... → done` identically to a live stream.
- **Rate limiting**: 30/min succeeds; excess → `429` +
  `Retry-After`, never a `5xx`.
- **Injection inertness**: `MOCK-INJ` trigger phrases only produce the
  `MOCK-INJ` finding — they never alter the API's behavior.
- **LLM graceful degradation**: an unset/invalid `LLM_API_KEY` and an
  unreachable `LLM_BASE_URL` all end in a `failed` job with a descriptive
  `error_message`.

---

## AI tools used

- **OpenCode CLI (Big Pickle)** — primary coding agent
- **Claude** — brainstorming architectural choices and checking test coverage
- **Gemini** — general web search for learning

---

## AI suggestions I rejected, and why

1. **Separating routes into multiple files.** I rejected this because the way I envisioned the app is a unified main file holding all routes, which then import helper functions from other files to perform the business logic — this made it easier for me to track the features in the API.

2. **Using in-memory dictionaries.** I opted for a database mainly for persistence, even though it's not explicitly required — I'd expect users of an API like this to want access to old jobs after a server restart. The database also provides a built-in way to guarantee uniqueness for relevant columns, like `body_hash`.

---

## What I'd do next with more time

1. Write a more detailed prompt for the LLM provider to generate specific error codes.
2. Add auto-retry for failed jobs.
3. Add a hard max limit on findings, or pagination.
4. Build a UI to interact with the API that allows direct file uploads and presents feedback in a readable form.
