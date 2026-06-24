#!/bin/bash
# Ensure bgutil-pot-server is running on http://127.0.0.1:4416 before starting yt-dlp scripts.
# - Starts it via systemd (user service) if available.
# - Falls back to a backgrounded `node` process if systemd is unavailable.
# - Idempotent: safe to call multiple times; will NOT spawn a second instance.

set -e

PORT=4416
LOG=/tmp/bgutil-pot.log
SRV_DIR=/home/hientran/bgutil-pot-server/server
NODE_BIN=/home/hientran/.nvm/versions/node/v24.15.0/bin/node

is_up() {
  curl -s -o /dev/null -w "%{http_code}" --max-time 2 "http://127.0.0.1:${PORT}/ping" 2>/dev/null | grep -q '^200$'
}

if is_up; then
  echo "[bgutil] already running on port ${PORT}"
  exit 0
fi

if systemctl --user >/dev/null 2>&1 && [ -f "$HOME/.config/systemd/user/bgutil-pot.service" ]; then
  echo "[bgutil] starting via systemd --user"
  systemctl --user daemon-reload
  systemctl --user enable --now bgutil-pot.service
else
  echo "[bgutil] starting as background node process"
  nohup "$NODE_BIN" build/main.js --port "$PORT" >"$LOG" 2>&1 &
  disown || true
fi

# Wait up to 15s for the server to come up
for i in $(seq 1 60); do
  if is_up; then
    echo "[bgutil] ready on http://127.0.0.1:${PORT}"
    exit 0
  fi
  sleep 0.5
done

echo "[bgutil] FAILED to start; check ${LOG}" >&2
exit 1
