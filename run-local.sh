#!/usr/bin/env bash
# YoinkT: verify source, run (Docker if available, else native python), self-test.
# Usage: ./run-local.sh [youtube-url-for-download-test]
set -e
cd "$(dirname "$0")"

echo "== 1/5 verify source =="
grep -q "APP_VERSION" backend/app.py || { echo "FAIL: stale app.py (pre-v6)"; exit 1; }
echo "source OK ($(grep -o 'v[0-9]*-always-mp4' backend/app.py | head -1))"

echo "== 2/5 stop old server =="
# kill anything previously serving this app; old processes serve OLD code forever
pkill -f "uvicorn app:app" 2>/dev/null && echo "killed old uvicorn" || true
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
  MODE=docker
  docker compose down --remove-orphans 2>/dev/null || true
else
  MODE=native
  PIDS=$(lsof -ti :8000 2>/dev/null || true)
  if [ -n "$PIDS" ]; then
    echo "port 8000 busy — killing old server(s):"
    for pid in $PIDS; do ps -p "$pid" -o command= 2>/dev/null || true; done
    echo "$PIDS" | xargs kill 2>/dev/null || true
    sleep 1
    LEFT=$(lsof -ti :8000 2>/dev/null || true)
    [ -n "$LEFT" ] && { echo "$LEFT" | xargs kill -9 2>/dev/null || true; sleep 1; }
    lsof -ti :8000 >/dev/null 2>&1 && { echo "FAIL: can't free port 8000"; exit 1; }
  fi
fi
echo "mode: $MODE"

echo "== 3/5 start =="
if [ "$MODE" = docker ]; then
  docker compose build --no-cache
  docker compose up -d
else
  command -v ffmpeg >/dev/null || { echo "FAIL: ffmpeg missing. macOS: brew install ffmpeg"; exit 1; }
  command -v aria2c >/dev/null || echo "note: aria2c not found — ⚡ Turbo mode will be disabled (brew install aria2 to enable)."
  command -v deno >/dev/null || command -v node >/dev/null || echo "note: no JS runtime (deno/node) — YouTube may throttle to ~50 KB/s (brew install deno to fix)."
  # PO-token provider = the main anti-throttle lever (full-speed googlevideo
  # servers instead of the ~2-4 MB/s per-connection cap). Auto-start it if
  # docker is available and the user hasn't already pointed at one.
  if [ -z "${POT_PROVIDER_URL:-}" ] && command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    if ! docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^YoinkT-pot$'; then
      echo "starting PO-token provider (bgutil) for full-speed downloads…"
      docker rm -f YoinkT-pot >/dev/null 2>&1 || true
      docker run -d --name YoinkT-pot -p 4416:4416 \
        brainicism/bgutil-ytdlp-pot-provider:latest >/dev/null 2>&1 \
        && sleep 2 || echo "note: could not start PO provider (continuing without it)."
    fi
    curl -sf http://localhost:4416/ping >/dev/null 2>&1 && export POT_PROVIDER_URL="http://localhost:4416"
  fi
  if [ -n "${POT_PROVIDER_URL:-}" ]; then
    echo "PO-token provider: $POT_PROVIDER_URL (full-speed mode)"
  else
    echo "tip: install Docker to auto-run the PO-token provider (full speed), or set POT_PROVIDER_URL yourself."
    echo "     without it, YouTube may throttle single-connection downloads to ~2-4 MB/s."
  fi
  command -v deno >/dev/null || echo "note: install Deno for the n-challenge solver: brew install deno"
  command -v python3 >/dev/null || { echo "FAIL: python3 missing"; exit 1; }
  [ -d .venv ] || python3 -m venv .venv
  ./.venv/bin/pip install -q --upgrade pip
  ./.venv/bin/pip install -q -r backend/requirements.txt
  ./.venv/bin/pip install -q --upgrade yt-dlp     # stale yt-dlp = throttled/broken
  # PATH must include venv bin: app.py spawns the yt-dlp CLI by name
  ( cd backend && PATH="$PWD/../.venv/bin:$PATH" \
      nohup ../.venv/bin/uvicorn app:app --host 127.0.0.1 --port 8000 \
      > ../YoinkT.log 2>&1 & )
  echo "logs: $(pwd)/YoinkT.log"
fi

echo "== 4/5 wait for health =="
ok=""
for i in $(seq 1 30); do
  if curl -sf http://localhost:8000/api/health >/dev/null 2>&1; then ok=1; break; fi
  sleep 1
done
[ -n "$ok" ] || { echo "FAIL: no health response."; [ "$MODE" = native ] && tail -20 YoinkT.log || docker compose logs --tail 30; exit 1; }
curl -s http://localhost:8000/api/health; echo
curl -s http://localhost:8000/api/health | grep -q "always-mp4" || { echo "VERDICT: OLD SERVER still answering — send me this output"; exit 1; }
echo "VERDICT: NEW CODE RUNNING (v6-always-mp4)"
command -v open >/dev/null && open "http://localhost:8000" || true

echo "== 5/5 live check =="
URL="${1:-}"
if [ -z "$URL" ]; then
  echo "Open http://localhost:8000 (footer must show v6-always-mp4)."
  echo "Auto-test: ./run-local.sh 'https://www.youtube.com/watch?v=...'"
  exit 0
fi
echo "-- /api/info exts (merged must all be mp4):"
curl -s "http://localhost:8000/api/info?url=$URL" | grep -o '"ext":"[^"]*"' | sort | uniq -c
echo "-- served filename for m:1080:"
curl -s -D - -o /dev/null --max-time 8 \
  "http://localhost:8000/api/download?url=$URL&kind=m:1080" | grep -i content-disposition || true
echo "-- 15s speed sample of merged 1080p:"
curl -s -o /dev/null --max-time 15 -w "downloaded %{size_download} bytes in %{time_total}s (%{speed_download} B/s)\n" \
  "http://localhost:8000/api/download?url=$URL&kind=m:1080" || true
echo "Done."
