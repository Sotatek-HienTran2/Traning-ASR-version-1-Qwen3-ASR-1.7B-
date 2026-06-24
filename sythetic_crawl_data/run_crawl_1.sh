#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY=/home/hientran/miniconda3/envs/crawl/bin/python3
CHANNELS_FILE=/home/hientran/sythetic_crawl_data/channels_audio/channels_lich_su.txt
MAX_RESULTS=5000
MAX_FETCH=10000
VIDEO_DELAY=5

# Ensure node >= 23.5.0
if ! command -v node >/dev/null 2>&1; then
  export PATH="$HOME/.local/bin:$PATH"
fi

NODE_VER=$(node --version 2>/dev/null | sed 's/^v//' | cut -d. -f1,2)
NODE_MAJOR=$(echo "$NODE_VER" | cut -d. -f1)
NODE_MINOR=$(echo "$NODE_VER" | cut -d. -f2)

if [ "${NODE_MAJOR:-0}" -lt 23 ] || \
   { [ "${NODE_MAJOR:-0}" -eq 23 ] && [ "${NODE_MINOR:-0}" -lt 5 ]; }; then
  echo "[run_crawl] WARNING: detected node v${NODE_VER:-?}, forcing node 24..."
  export PATH="/home/hientran/.nvm/versions/node/v24.15.0/bin:$PATH"
fi

echo "[run_crawl] node $(node --version) on PATH"

# Launch crawler directly
echo "[run_crawl] starting crawler..."
cd "$SCRIPT_DIR"

exec "$PY" \
  "$SCRIPT_DIR/youtube_researcher_audio_subs_multi_rotator_v3.py" \
  --channels-file "$CHANNELS_FILE" \
  --max-results "$MAX_RESULTS" \
  --max-fetch "$MAX_FETCH" \
  --video-delay "$VIDEO_DELAY" \
  --skip-existing