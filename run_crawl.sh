#!/bin/bash
# Usage:
#   bash /home/hientran/sythetic_crawl_data/run_crawl.sh
# Wrapper for: youtube_researcher_youtube_subs_multi.py
#   - Ensures bgutil-pot-server is up on http://127.0.0.1:4416 first
#     (auto-spawn via start_bgutil.sh if systemd service not running)
#   - Then runs the python crawler with the channels list (khoa_hoc_6 by default).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY=/home/hientran/miniconda3/envs/crawl/bin/python3
CHANNELS_FILE=/home/hientran/sythetic_crawl_data/channels_audio/channels_lich_su.txt
MAX_RESULTS=1000
MAX_FETCH=10000
VIDEO_DELAY=5

# 1. Make sure bgutil-pot-server is alive
bash "$SCRIPT_DIR/start_bgutil.sh"

# 2. Quick sanity check before launching the long crawl
if ! curl -sf --max-time 3 http://127.0.0.1:4416/ping >/dev/null; then
  echo "[run_crawl] bgutil POT server is NOT responding at http://127.0.0.1:4416" >&2
  echo "[run_crawl] aborting to avoid 'sign in to confirm you're not a bot' errors" >&2
  exit 1
fi

# 3. Make sure node >= 23.5.0 is on PATH for the EJS challenge solver
#    (yt-dlp refuses to use older node versions, which is what caused the sign-in failures)
if ! command -v node >/dev/null 2>&1; then
  export PATH="$HOME/.local/bin:$PATH"
fi
NODE_VER=$(node --version 2>/dev/null | sed 's/^v//' | cut -d. -f1,2)
NODE_MAJOR=$(echo "$NODE_VER" | cut -d. -f1)
NODE_MINOR=$(echo "$NODE_VER" | cut -d. -f2)
if [ "${NODE_MAJOR:-0}" -lt 23 ] || { [ "${NODE_MAJOR:-0}" -eq 23 ] && [ "${NODE_MINOR:-0}" -lt 5 ]; }; then
  echo "[run_crawl] WARNING: detected node v${NODE_VER:-?}, yt-dlp needs >= 23.5.0"
  echo "[run_crawl] forcing nvm node 24..."
  export PATH="/home/hientran/.nvm/versions/node/v24.15.0/bin:$PATH"
fi
echo "[run_crawl] node $(node --version) on PATH"

# 4. Launch crawler
echo "[run_crawl] bgutil OK, starting crawler..."
cd "$SCRIPT_DIR"
exec "$PY" \
  "$SCRIPT_DIR/youtube_researcher_audio_subs_multi_rotator_v5.py" \
  --channels-file "$CHANNELS_FILE" \
  --max-results "$MAX_RESULTS" \
  --max-fetch "$MAX_FETCH" \
  --video-delay "$VIDEO_DELAY" \
  # --no-transcribe \
  --skip-existing

