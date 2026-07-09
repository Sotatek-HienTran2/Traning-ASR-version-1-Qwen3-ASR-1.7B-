#!/bin/bash
set -euo pipefail

# =============================================================================
# run_crawl_v15.sh — clone của run_crawl_v14.sh + PLAYER_CLIENT ROTATION (vietsub optimization).
# -----------------------------------------------------------------------------
# v15 khác v14:
#   1) PLAYER_CLIENT ROTATION: rotate qua tv_embedded, web_embedded, tv, android, ios.
#   2) CLIENT_EMPTY DETECTION: khi 1 client trả subs=0+auto=0 → retry client
#      khác (không return "no_subs" → tránh MISS vietsub).
#   3) OPTIMIZED ROTATION LIST: bỏ web, web_safari, web_creator, mweb
#      (EMPTY), bỏ android_vr (FAIL); thêm tv_embedded, web_embedded, tv (TỐT NHẤT).
#
# Mặc định KHÔNG xóa audio đã tải → an toàn khi re-run nhiều lần.
# Override qua env, ví dụ:
#   VI_SUB_PRIORITY=manual_first \
#   PLAYER_CLIENTS="tv_embedded,android" \
#   ./run_crawl_v15.sh
#
# === AUDIO_ONLY MODE (--audio-only) ===
# Mặc định: TẮT (0) → chạy đầy đủ pipeline (metadata + subs + audio).
# BẬT khi cần chỉ tải audio (đã có subs trước đó, không cần tạo JSON sub):
#   AUDIO_ONLY=1 ./run_crawl_v15.sh
# Khi BẬT, code sẽ:
#   - Skip bước tạo JSON sub (vì đã có từ run trước).
#   - Chỉ tải audio cho video chưa có audio hợp lệ.
#   - Vẫn áp dụng player_client rotation + API fallback (để verify subs có sẵn).
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
# Đảm bảo các module "anh em" (vd: vpn_rotator_v4 nằm ở thư mục cha) cũng
# import được khi chạy crawler từ trong channels_audio/.
export PYTHONPATH="$PARENT_DIR${PYTHONPATH:+:$PYTHONPATH}"
PY=/home/hientran/miniconda3/envs/crawl/bin/python3
CHANNELS_FILE=/home/hientran/sythetic_crawl_data/channels_audio/channels_phan_mem_6_ok_dich.txt
MAX_RESULTS=100
MAX_FETCH=100
VIDEO_DELAY=6

# === v14: Mid-download slow-speed rotation config (giống v13/v14) ===
AUDIO_SLOW_SPEED_KBPS="${AUDIO_SLOW_SPEED_KBPS:-500.0}"
AUDIO_SLOW_WINDOW_SECONDS="${AUDIO_SLOW_WINDOW_SECONDS:-30.0}"
AUDIO_MAX_ROTATE_PER_VIDEO="${AUDIO_MAX_ROTATE_PER_VIDEO:-3}"

# === v14: VietSub-specific config ===
# Priority: "auto_first" (default — phù hợp video Việt Nam auto-generated)
#           hoặc "manual_first" (ưu tiên manual cho video VTV, FAPTV...).
# MAX RECALL: giữ auto_first vì VN video hầu hết là auto-translated.
VI_SUB_PRIORITY="${VI_SUB_PRIORITY:-auto_first}"

# Retry video đã mark .no_transcript ở run trước (mặc định: 2 = FORCE).
#   RETRY_NO_TRANSCRIPT=0: tắt → giữ hành vi skip như cũ.
#   RETRY_NO_TRANSCRIPT=1: BẬT → retry video có marker CŨ (> TTL).
#   RETRY_NO_TRANSCRIPT=2: BẬT MẠNH → retry cả marker MỚI (MAX RECALL).
RETRY_NO_TRANSCRIPT="${RETRY_NO_TRANSCRIPT:-2}"

# TTL cho marker .no_transcript (ngày). Marker cũ hơn TTL sẽ bị bỏ qua.
# MAX RECALL: TTL ngắn (1 ngày) → retry video đã skip nhanh hơn.
NO_MARKER_TTL_DAYS="${NO_MARKER_TTL_DAYS:-1}"

# Respect marker = luôn skip video có marker (giống v13). Default: 0 (tắt).
# MAX RECALL: tắt để KHÔNG skip cứng video nào.
RESPECT_NO_TRANSCRIPT_MARKER="${RESPECT_NO_TRANSCRIPT_MARKER:-0}"

# === v14: youtube-transcript-api fallback config ===
# BẬT mặc định: sau khi yt-dlp fail hết retries → thử youtube-transcript-api
# (engine khác, gọi timedtext API — bypass captcha tốt hơn).
#   NO_API_FALLBACK=1: tắt fallback (chỉ dùng yt-dlp).
#   API_FALLBACK_LANGS: danh sách langs ưu tiên (mặc định "vi,en").
# MAX RECALL: BẬT fallback + mở rộng langs để tăng cơ hội có VI.
NO_API_FALLBACK="${NO_API_FALLBACK:-0}"
API_FALLBACK_LANGS="${API_FALLBACK_LANGS:-vi,en,vi-orig,vi-vn}"

# === v15: PLAYER_CLIENT ROTATION config ===
# BẬT mặc định: rotate qua tv_embedded → web_embedded → tv → android → ios
# để tăng tỷ lệ lấy được vietsub.
#   NO_PLAYER_CLIENT_ROTATE=1: tắt → giữ hành vi v14 (chỉ dùng web_safari, web).
# MAX RECALL: BẮT BUỘC bật (đã test: tăng từ 0% → 60% video có VI).
NO_PLAYER_CLIENT_ROTATE="${NO_PLAYER_CLIENT_ROTATE:-0}"

# Custom player_clients (comma-separated). Bỏ trống → dùng list tối ưu của v15.
#   PLAYER_CLIENTS="tv_embedded,android,ios" → chỉ dùng 3 client này.
# MAX RECALL: set explicit đầy đủ 5 client tốt nhất (theo test 10 video).
PLAYER_CLIENTS="${PLAYER_CLIENTS:-tv_embedded,web_embedded,tv,android,ios}"

# === v15: API MODE SUBS POPULATE config ===
# BẬT mặc định: populate subs song song sau API mode để Bucket B có cache.
#   NO_SUBS_POPULATE=1: tắt populate (Bucket B sẽ tự gọi yt-dlp).
# MAX RECALL: BẮT BUỘC bật để Bucket B có cache ngay (không phải gọi lại yt-dlp).
NO_SUBS_POPULATE="${NO_SUBS_POPULATE:-0}"
# MAX RECALL: concurrency=3 (giảm từ 8) để tránh YouTube rate-limit →
# lỗi "Requested format is not available" khi 8 request cùng lúc.
SUBS_POPULATE_CONCURRENCY="${SUBS_POPULATE_CONCURRENCY:-12}"

# === v14: AUDIO_MAX_ROTATE_PER_VIDEO ===
# MAX RECALL: tăng từ 3 → 5 để retry thêm khi IP/proxy fail (tránh skip sớm).
AUDIO_MAX_ROTATE_PER_VIDEO="${AUDIO_MAX_ROTATE_PER_VIDEO:-5}"

# === AUDIO_ONLY MODE ===
# 0 (default): chạy đầy đủ pipeline (metadata + subs + audio).
# 1: chỉ tải audio cho video chưa có audio (skip tạo JSON sub mới).
AUDIO_ONLY="${AUDIO_ONLY:-0}"


# v11: Per-instance tunnel isolation.
INSTANCE_ID="${INSTANCE_ID:-pid$$_t$(date +%s)}"

# =============================================================================
# kill_vpn_fake_ips() và kill_vpn_by_instance() — giống run_crawl_v14.sh
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
echo "[run_crawl] *** v15: MAX RECALL MODE (lấy VI subs tối đa) ***"
echo "[run_crawl]   vi_sub_priority=${VI_SUB_PRIORITY}"
echo "[run_crawl]   retry_no_transcript=${RETRY_NO_TRANSCRIPT} (2=force, retry cả marker mới)"
echo "[run_crawl]   no_marker_ttl_days=${NO_MARKER_TTL_DAYS} (1 ngày → retry sớm)"
echo "[run_crawl]   respect_no_transcript_marker=${RESPECT_NO_TRANSCRIPT_MARKER}"
echo "[run_crawl]   no_api_fallback=${NO_API_FALLBACK} (0=BẬT fallback youtube-transcript-api)"
echo "[run_crawl]   api_fallback_langs=${API_FALLBACK_LANGS}"
echo "[run_crawl]   no_player_client_rotate=${NO_PLAYER_CLIENT_ROTATE} (0=BẮT BUỘC bật)"
echo "[run_crawl]   player_clients=${PLAYER_CLIENTS} (5 client tốt nhất)"
echo "[run_crawl]   no_subs_populate=${NO_SUBS_POPULATE} (0=BẬT populate Bucket B cache)"
echo "[run_crawl]   subs_populate_concurrency=${SUBS_POPULATE_CONCURRENCY}"
echo "[run_crawl]   audio_max_rotate_per_video=${AUDIO_MAX_ROTATE_PER_VIDEO}"
echo "[run_crawl]   audio_only=${AUDIO_ONLY} (1=chỉ tải audio, skip tạo subs JSON)"
# echo "[run_crawl]   output_dir=${OUTPUT_DIR}"

# v11: Chỉ kill tunnel CŨ của CÙNG instance_id (nếu run trước crash để lại).
kill_vpn_by_instance "$INSTANCE_ID"

# Launch crawler directly (KHÔNG --audio-only: v15 phải tạo JSON sub cho audio).
echo "[run_crawl] starting crawler (v15: player_client rotation + vietsub optimization)..."
echo "[run_crawl]   slow_speed_kbps=${AUDIO_SLOW_SPEED_KBPS} KB/s"
echo "[run_crawl]   slow_window=${AUDIO_SLOW_WINDOW_SECONDS}s"
echo "[run_crawl]   max_rotate_per_video=${AUDIO_MAX_ROTATE_PER_VIDEO}"
cd "$SCRIPT_DIR"

# Build CLI args
ARGS=(
  "$SCRIPT_DIR/youtube_researcher_audio_subs_multi_rotator_v15.py"
  --channels-file "$CHANNELS_FILE"
  --max-results "$MAX_RESULTS"
  --max-fetch "$MAX_FETCH"
  --video-delay "$VIDEO_DELAY"
  --instance-id "$INSTANCE_ID"
  --skip-existing
  # --output "$OUTPUT_DIR"
  --audio-slow-speed-kbps "$AUDIO_SLOW_SPEED_KBPS"
  --audio-slow-window-seconds "$AUDIO_SLOW_WINDOW_SECONDS"
  --audio-max-rotate-per-video "$AUDIO_MAX_ROTATE_PER_VIDEO"
  --vi-sub-priority "$VI_SUB_PRIORITY"
  --no-marker-ttl-days "$NO_MARKER_TTL_DAYS"
)

# Thêm --audio-only nếu AUDIO_ONLY=1 (chỉ tải audio, skip tạo subs JSON)
if [ "$AUDIO_ONLY" = "1" ]; then
  ARGS+=(--audio-only)
fi

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

# === v15: player_client rotation ===
if [ "$NO_PLAYER_CLIENT_ROTATE" = "1" ]; then
  ARGS+=(--no-player-client-rotate)
fi

# Thêm --player-clients nếu set (custom list)
if [ -n "$PLAYER_CLIENTS" ]; then
  ARGS+=(--player-clients "$PLAYER_CLIENTS")
fi

# === v15: subs populate ===
if [ "$NO_SUBS_POPULATE" = "1" ]; then
  ARGS+=(--no-subs-populate)
fi
ARGS+=(--subs-populate-concurrency "$SUBS_POPULATE_CONCURRENCY")

# RETRY_NO_TRANSCRIPT=2 → thêm cờ mạnh (retry cả marker mới)
if [ "$RETRY_NO_TRANSCRIPT" = "2" ]; then
  ARGS+=(--retry-no-transcript --retry-no-transcript-force)
fi

echo "[run_crawl] args count=${#ARGS[@]}"
exec "$PY" "${ARGS[@]}"