#!/usr/bin/env bash
set -euo pipefail

# Start MySQL using the official image entrypoint, which handles datadir
# initialization (MYSQL_ROOT_PASSWORD/MYSQL_DATABASE/MYSQL_USER/MYSQL_PASSWORD)
# on first run, then execs mysqld in the foreground of this background job.
/usr/local/bin/docker-entrypoint.sh mysqld &
MYSQL_PID=$!

# Wait for the final mysqld to accept TCP connections as the app's DB user.
# (mysqladmin ping is not used: it reports the temp init server as "alive",
# before the real server is listening on 3306.)
export DB_HOST=127.0.0.1
export DB_PORT=3306
export DB_NAME="${MYSQL_DATABASE:-diffrev_db}"
export DB_USER="${MYSQL_USER:-diffrev}"
export DB_PASSWORD="${MYSQL_PASSWORD:-diffrev_pass}"

READY=""
for _ in $(seq 1 120); do
  if mysql --protocol=tcp -h "$DB_HOST" -P "$DB_PORT" -u"$DB_USER" \
      -p"$DB_PASSWORD" -e "SELECT 1" >/dev/null 2>&1; then
    READY=1
    break
  fi
  if ! kill -0 "$MYSQL_PID" 2>/dev/null; then
    echo "[entrypoint] mysqld exited during startup" >&2
    exit 1
  fi
  sleep 1
done

if [ -z "$READY" ]; then
  echo "[entrypoint] MySQL did not become ready in time" >&2
  exit 1
fi

exec uvicorn diffrev.main:app --host 0.0.0.0 --port 8000
