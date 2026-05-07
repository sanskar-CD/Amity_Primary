#!/usr/bin/env bash
# Deploy Amity_Primary on Linux: pull origin/main, install deps, restart gunicorn on :5001.
# Run as root (or adjust APP_DIR). Usage: ./scripts/deploy_amity.sh

set -euo pipefail

APP_DIR="${APP_DIR:-/root/Amity_Primary}"
PORT="${PORT:-5001}"
PYTHON="${APP_DIR}/.venv/bin/python"
GUNICORN="${APP_DIR}/.venv/bin/gunicorn"
LOG_DIR="${APP_DIR}/logs"
RUN_DIR="${APP_DIR}/run"
BACKUP_DIR="${APP_DIR}/backups/$(date +%Y%m%d_%H%M%S)"

cd "${APP_DIR}"

echo "===> [1/9] Sanity"
test -d "${APP_DIR}/.git" || { echo "ERROR: not a git repo: ${APP_DIR}"; exit 1; }
test -x "${PYTHON}" || { echo "ERROR: venv python missing: ${PYTHON}"; exit 1; }

echo "===> [2/9] Backup .env + SQLite"
mkdir -p "${BACKUP_DIR}"
for f in .env leads.db leads.db-wal leads.db-shm; do
  if [[ -f "${f}" ]]; then
    cp -a "${f}" "${BACKUP_DIR}/"
  fi
done
echo "    backup: ${BACKUP_DIR}"

echo "===> [3/9] git fetch + reset to origin/main"
git fetch origin
git reset --hard origin/main
git log --oneline -n 3

echo "===> [4/9] pip install (requirements.txt includes gunicorn)"
"${PYTHON}" -m pip install --upgrade pip --quiet
"${PYTHON}" -m pip install -r requirements.txt --quiet

if [[ ! -x "${GUNICORN}" ]]; then
  echo "ERROR: gunicorn missing after pip: ${GUNICORN}"
  exit 1
fi

echo "===> [5/9] Playwright Chromium (scraper / cron)"
if "${PYTHON}" -m playwright install chromium --with-deps 2>/dev/null; then
  :
else
  "${PYTHON}" -m playwright install chromium || true
fi

echo "===> [6/9] Stop old gunicorn + free port ${PORT}"
if command -v fuser >/dev/null 2>&1; then
  fuser -k -TERM "${PORT}/tcp" 2>/dev/null || true
fi
pkill -TERM -f "${APP_DIR}/.venv/bin/gunicorn" 2>/dev/null || true
sleep 2
if command -v fuser >/dev/null 2>&1; then
  fuser -k -KILL "${PORT}/tcp" 2>/dev/null || true
fi
pkill -KILL -f "${APP_DIR}/.venv/bin/gunicorn" 2>/dev/null || true

echo "===> [7/9] Start gunicorn"
mkdir -p "${LOG_DIR}" "${RUN_DIR}"
nohup "${GUNICORN}" -w 2 -b "0.0.0.0:${PORT}" \
  --access-logfile "${LOG_DIR}/access.log" \
  --error-logfile "${LOG_DIR}/error.log" \
  --pid "${RUN_DIR}/gunicorn.pid" \
  app:app >>"${LOG_DIR}/app.log" 2>&1 &
disown

sleep 3

echo "===> [8/9] Listen check"
if command -v ss >/dev/null 2>&1; then
  ss -tlnp | grep ":${PORT} " || { echo "FAIL: nothing on :${PORT}"; tail -n 60 "${LOG_DIR}/error.log" 2>/dev/null || true; exit 1; }
elif command -v netstat >/dev/null 2>&1; then
  netstat -tlnp 2>/dev/null | grep ":${PORT} " || { echo "FAIL: nothing on :${PORT}"; exit 1; }
else
  echo "WARN: no ss/netstat; skip port check"
fi

echo "===> [9/9] HTTP check"
if command -v curl >/dev/null 2>&1; then
  curl -sS -o /dev/null -w "HTTP %{http_code} /login\n" "http://127.0.0.1:${PORT}/login" || true
fi

echo "DONE. Logs: ${LOG_DIR}/{app,error,access}.log"
