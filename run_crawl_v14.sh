#!/bin/bash
set -euo pipefail

# =============================================================================
# run_crawl_v14.sh — clone của run_crawl_v13.sh + tối ưu lấy VietSub.
# -----------------------------------------------------------------------------
# v14 khác v13:
#   1) KHÔNG có --audio-only (v14 phải tạo JSON sub cho audio đã tải).
#   2) Thêm --vi-sub-priority (auto_first | manual_first).
#   3) Thêm --retry-no-transcript: ép retry video đã bị mark sai do rate-limit.
#   4) Thêm --no-marker-ttl-days N (default 7).
#   5) --respect-no-transcript-marker: giữ hành vi v13 (skip vĩnh viễn).
#   6) KHÔNG xóa audio đã tải: Bucket C tự skip khi đã có audio hợp lệ.
#
# Mặc định KHÔNG xóa audio đã tải → an toàn khi re-run nhiều lần.
# Override qua env, ví dụ:
#   VI_SUB_PRIORITY=manual_first \
#   RETRY_NO_TRANSCRIPT=1 \
#   ./run_crawl_v14.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
# Đảm bảo các module "anh em" (vd: vpn_rotator_v4 nằm ở thư mục cha) cũng
# import được khi chạy crawler từ trong channels_audio/.
export PYTHONPATH="$PARENT_DIR${PYTHONPATH:+:$PYTHONPATH}"
PY=/home/hientran/miniconda3/envs/crawl/bin/python3
CHANNELS_FILE=/home/hientran/sythetic_crawl_data/channels_audio/channels_AI_7_ok.txt
MAX_RESULTS=5000
MAX_FETCH=3000
VIDEO_DELAY=8

# === v14: Mid-download slow-speed rotation config (giống v13) ===
AUDIO_SLOW_SPEED_KBPS="${AUDIO_SLOW_SPEED_KBPS:-500.0}"
AUDIO_SLOW_WINDOW_SECONDS="${AUDIO_SLOW_WINDOW_SECONDS:-30.0}"
AUDIO_MAX_ROTATE_PER_VIDEO="${AUDIO_MAX_ROTATE_PER_VIDEO:-3}"

# === v14: VietSub-specific config ===
# Priority: "auto_first" (default — phù hợp video Việt Nam auto-generated)
#           hoặc "manual_first" (ưu tiên manual cho video VTV, FAPTV...).
VI_SUB_PRIORITY="${VI_SUB_PRIORITY:-auto_first}"

# Retry video đã mark .no_transcript ở run trước (mặc định: 1 = BẬT).
#   RETRY_NO_TRANSCRIPT=0: tắt → giữ hành vi skip như cũ.
#   RETRY_NO_TRANSCRIPT=1: BẬT → retry video có marker CŨ (> TTL).
#   RETRY_NO_TRANSCRIPT=2: BẬT MẠNH → retry cả marker MỚI (giống --no-marker-skip).
RETRY_NO_TRANSCRIPT="${RETRY_NO_TRANSCRIPT:-1}"

# TTL cho marker .no_transcript (ngày). Marker cũ hơn TTL sẽ bị bỏ qua.
NO_MARKER_TTL_DAYS="${NO_MARKER_TTL_DAYS:-7}"

# Respect marker = luôn skip video có marker (giống v13). Default: 0 (tắt).
RESPECT_NO_TRANSCRIPT_MARKER="${RESPECT_NO_TRANSCRIPT_MARKER:-0}"

# === v14: youtube-transcript-api fallback config ===
# BẬT mặc định: sau khi yt-dlp fail hết retries → thử youtube-transcript-api
# (engine khác, gọi timedtext API — bypass captcha tốt hơn).
#   NO_API_FALLBACK=1: tắt fallback (chỉ dùng yt-dlp).
#   API_FALLBACK_LANGS: danh sách langs ưu tiên (mặc định "vi,en").
NO_API_FALLBACK="${NO_API_FALLBACK:-0}"
API_FALLBACK_LANGS="${API_FALLBACK_LANGS:-vi,en}"

# Output: dùng youtube_dataset (cũ) hoặc youtube_dataset_v14 (mới).
# Mặc định: youtube_dataset_v14 để tránh ghi đè data v13 đang chạy.
OUTPUT_DIR="${OUTPUT_DIR:-/home/hientran/sythetic_crawl_data/youtube_dataset}"

# v11: Per-instance tunnel isolation.
INSTANCE_ID="${INSTANCE_ID:-pid$$_t$(date +%s)}"

# =============================================================================
# kill_vpn_fake_ips() và kill_vpn_by_instance() — giống run_crawl_v13.sh
# =============================================================================
kill_vpn_by_instance() {
  local instance_id="$1"
  if [ -z "$instance_id" ]; then
    echo "[kill-by-inst] instance_id rỗng → skip"
    return 0
  fi

  echo "[kill-by-inst] Kill tunnel OpenVPN của instance='$instance_id'..."

  local killed=0

  shopt -s nullglob
  local pid_files=( /tmp/openvpn-proton-${instance_id}*.pid.*.* )
  shopt -u nullglob

  if [ "${#pid_files[@]}" -eq 0 ]; then
    echo "[kill-by-inst]   (không có PID file nào cho instance='$instance_id')"
    return 0
  fi

  echo "[kill-by-inst]   Tìm thấy ${#pid_files[@]} PID file → kill theo PID chính xác..."
  for pf in "${pid_files[@]}"; do
    local pid
    pid="$(cat "$pf" 2>/dev/null || true)"
    if [ -z "$pid" ] || ! [[ "$pid" =~ ^[0-9]+$ ]]; then
      continue
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      continue
    fi
    local proc_uid
    proc_uid="$(awk -v p="$pid" '$1==p {print $2}' /proc/$pid/status 2>/dev/null || true)"
    if [ -z "$proc_uid" ] || [ "$proc_uid" != "$(id -u)" ]; then
      echo "[kill-by-inst]     • PID $pid không thuộc user hiện tại → skip"
      continue
    fi
    local cmdline
    cmdline="$(tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null || true)"
    if ! [[ "$cmdline" == *openvpn*proton_config* ]]; then
      echo "[kill-by-inst]     • PID $pid KHÔNG phải openvpn+proton_config → skip"
      continue
    fi
    echo "[kill-by-inst]     • PID $pid ($(basename "$pf")) → SIGTERM"
    kill -15 "$pid" 2>/dev/null || true
    killed=$((killed + 1))

    local waited=0
    while [ $waited -lt 4 ]; do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 0.5
      waited=$((waited + 1))
    done
    if kill -0 "$pid" 2>/dev/null; then
      echo "[kill-by-inst]     • PID $pid vẫn sống sau 2s → SIGKILL"
      kill -9 "$pid" 2>/dev/null || true
    fi
  done

  echo "[kill-by-inst] Killed $killed tunnel(s) cho instance='$instance_id'"
  sleep 1
}

# Đảm bảo node >= 23.5.0
if ! command -v node >/dev/null 2>&1; then
  export PATH="$HOME/.local/bin:$PATH"
fi

NODE_VER=$(node --version 2>/dev/null | sed 's/^v//' | cut -d. -f1,2)
NODE_MAJOR=$(echo "$NODE_VER" | cut -d. -f1)
NODE_MINOR=$(echo "$NODE_VER" | cut -d. -f2)

if [ "${NODE_MAJOR:-0}" -lt 23 ] || \
   { [ "${NODE_MAJOR:-0}" -eq 23 ] && [ "${NODE_MINOR:-0}" -lt 5 ]; }; then
  echo "[run_crawl] WARNING: detected node v${NODE_VER:-?}, yêu cầu >= 23.5.0."
  echo "[run_crawl] Hãy cài đặt node >= 23.5.0 (vd qua nvm) rồi chạy lại."
fi

echo "[run_crawl] node $(node --version) on PATH"
echo "[run_crawl] Instance ID: $INSTANCE_ID"
echo "[run_crawl] *** v14: TỐI ƯU LẤY VIETSUB (giữ audio đã tải) ***"
echo "[run_crawl]   vi_sub_priority=${VI_SUB_PRIORITY}"
echo "[run_crawl]   retry_no_transcript=${RETRY_NO_TRANSCRIPT}"
echo "[run_crawl]   no_marker_ttl_days=${NO_MARKER_TTL_DAYS}"
echo "[run_crawl]   respect_no_transcript_marker=${RESPECT_NO_TRANSCRIPT_MARKER}"
echo "[run_crawl]   no_api_fallback=${NO_API_FALLBACK}"
echo "[run_crawl]   api_fallback_langs=${API_FALLBACK_LANGS}"
echo "[run_crawl]   output_dir=${OUTPUT_DIR}"

# v11: Chỉ kill tunnel CŨ của CÙNG instance_id (nếu run trước crash để lại).
kill_vpn_by_instance "$INSTANCE_ID"

# Launch crawler directly (KHÔNG --audio-only: v14 phải tạo JSON sub cho audio).
echo "[run_crawl] starting crawler (v14: tối ưu Vietsub)..."
echo "[run_crawl]   slow_speed_kbps=${AUDIO_SLOW_SPEED_KBPS} KB/s"
echo "[run_crawl]   slow_window=${AUDIO_SLOW_WINDOW_SECONDS}s"
echo "[run_crawl]   max_rotate_per_video=${AUDIO_MAX_ROTATE_PER_VIDEO}"
cd "$SCRIPT_DIR"

# Build CLI args
ARGS=(
  "$SCRIPT_DIR/youtube_researcher_audio_subs_multi_rotator_v14.py"
  --channels-file "$CHANNELS_FILE"
  --max-results "$MAX_RESULTS"
  --max-fetch "$MAX_FETCH"
  --video-delay "$VIDEO_DELAY"
  --instance-id "$INSTANCE_ID"
  --skip-existing
  --output "$OUTPUT_DIR"
  --audio-slow-speed-kbps "$AUDIO_SLOW_SPEED_KBPS"
  --audio-slow-window-seconds "$AUDIO_SLOW_WINDOW_SECONDS"
  --audio-max-rotate-per-video "$AUDIO_MAX_ROTATE_PER_VIDEO"
  --vi-sub-priority "$VI_SUB_PRIORITY"
  --no-marker-ttl-days "$NO_MARKER_TTL_DAYS"
)

# Thêm --retry-no-transcript nếu RETRY_NO_TRANSCRIPT=1 hoặc 2
if [ "$RETRY_NO_TRANSCRIPT" = "1" ]; then
  ARGS+=(--retry-no-transcript)
fi

# Thêm --respect-no-transcript-marker nếu set 1
if [ "$RESPECT_NO_TRANSCRIPT_MARKER" = "1" ]; then
  ARGS+=(--respect-no-transcript-marker)
fi

# Thêm --no-api-fallback nếu NO_API_FALLBACK=1
if [ "$NO_API_FALLBACK" = "1" ]; then
  ARGS+=(--no-api-fallback)
fi
ARGS+=(--api-fallback-langs "$API_FALLBACK_LANGS")

# RETRY_NO_TRANSCRIPT=2 → thêm cờ mạnh (retry cả marker mới)
if [ "$RETRY_NO_TRANSCRIPT" = "2" ]; then
  ARGS+=(--retry-no-transcript --retry-no-transcript-force)
fi

echo "[run_crawl] args count=${#ARGS[@]}"
exec "$PY" "${ARGS[@]}"
