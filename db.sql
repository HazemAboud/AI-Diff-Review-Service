CREATE TABLE IF NOT EXISTS jobs (
  job_id          CHAR(36)      NOT NULL,
  status          ENUM('queued','running','done','failed') NOT NULL DEFAULT 'queued',
  provider        ENUM('mock','llm') NOT NULL DEFAULT 'mock',
  max_findings    INT UNSIGNED  NOT NULL DEFAULT 100,
  input_bytes     INT UNSIGNED  NOT NULL,
  chunks          INT UNSIGNED  NOT NULL DEFAULT 0,
  body_hash       CHAR(64)      NOT NULL,
  error_message   TEXT          NULL,
  created_at      DATETIME(6)   NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  started_at      DATETIME(6)   NULL,
  finished_at     DATETIME(6)   NULL,
  PRIMARY KEY (job_id),
  UNIQUE KEY uk_body_hash (body_hash),
  KEY idx_body_hash_status (body_hash, status),
  KEY idx_queue (status, created_at)
);

CREATE TABLE IF NOT EXISTS idempotency_keys (
  idem_key  VARCHAR(255)  NOT NULL,
  job_id    CHAR(36)      NOT NULL,
  PRIMARY KEY (idem_key),
  KEY idx_idem_job (job_id)
);

CREATE TABLE IF NOT EXISTS cache_hits (
  job_id         CHAR(36)     NOT NULL,
  source_job_id  CHAR(36)     NOT NULL,
  input_bytes    INT UNSIGNED NOT NULL,
  created_at     DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (job_id),
  KEY idx_cache_hits_source (source_job_id),
  CONSTRAINT fk_cache_hits_source FOREIGN KEY (source_job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS chunks (
  job_id     CHAR(36)      NOT NULL,
  chunk_num  INT UNSIGNED  NOT NULL,
  chunk_cont MEDIUMTEXT    NOT NULL,
  PRIMARY KEY (job_id, chunk_num),
  CONSTRAINT fk_chunks_job FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS findings (
  job_id      CHAR(36)      NOT NULL,
  finding_id  VARCHAR(512)  NOT NULL,   -- "ruleId:path:line"
  rule_id     VARCHAR(20)   NOT NULL,
  path        TEXT NOT NULL,
  line        INT UNSIGNED  NOT NULL,
  severity    ENUM('critical','high','medium','low') NOT NULL,
  category    ENUM('security','correctness','performance','style') NOT NULL,
  title       VARCHAR(255)  NOT NULL,
  evidence    TEXT          NOT NULL,
  PRIMARY KEY (job_id, finding_id),
  CONSTRAINT fk_findings_job FOREIGN KEY (job_id) REFERENCES jobs(job_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS job_events (
  job_id    CHAR(36)     NOT NULL,
  sequence     INT UNSIGNED NOT NULL,
  event     ENUM('status','finding','done') NOT NULL,
  payload   JSON         NOT NULL,
  ev_time DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (job_id, sequence)
);