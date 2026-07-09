#!/bin/bash
set -euo pipefail

# =============================================================================
# run_crawl_v16.sh — clone của run_crawl_v15.sh + VIETSUB PRE-FILTER (tiết kiệm bandwidth).
# -----------------------------------------------------------------------------
# v16 khác v15:
#   1) VIETSUB PRE-FILTER (Bucket C): TRƯỚC khi tải audio (~50-500MB), check
#      video có vietsub hay không. Nếu KHÔNG có → SKIP download audio luôn.
#      3 nguồn check (ưu tiên giảm dần):
#        a) video.subtitles/automatic_captions đã populate (Phase 2 API mode)
#        b) cache file /tmp/vi_subs_check_cache/<id>.json (TTL 3 ngày)
#        c) yt-dlp extract_info(skip_download=True) + _score_vi_subs()
#           (chỉ metadata, timeout 8s, ~1-3s/video)
#   2) NEW MARKER .no_vi_subs (TTL 7 ngày): song song với .no_transcript.
#      Lần chạy sau skip ngay video không có VI → KHÔNG tốn yt-dlp.
#   3) CACHE FILE /tmp/vi_subs_check_cache/<id>.json (TTL 3 ngày).
#   4) CLI flags mới:
#        --require-vietsub / --no-require-vietsub (default: True)
#        --vi-subs-check-langs (default: 'vi')
#        --retry-no-vi-subs
#        --no-vi-subs-marker-ttl-days (default: 7.0)
#        --vi-subs-check-cache-ttl-days (default: 3.0)
#
# Mặc định KHÔNG xóa audio đã tải → an toàn khi re-run nhiều lần.
# Override qua env, ví dụ:
#   REQUIRE_VIETSUB=0 \
#   VI_SUBS_CHECK_LANGS="vi,en" \
#   ./run_crawl_v16.sh
#
# === TẮT VIETSUB FILTER (giống v15) ===
#   REQUIRE_VIETSUB=0 ./run_crawl_v16.sh
# Khi đó code sẽ tải audio hết kể cả video không có VI subs (giống v15).
#
# === AUDIO_ONLY MODE (--audio-only) ===
# Mặc định: TẮT (0) → chạy đầy đủ pipeline (metadata + subs + audio).
# BẬT khi cần chỉ tải audio (đã có subs trước đó, không cần tạo JSON sub):
#   AUDIO_ONLY=1 ./run_crawl_v16.sh
# Lưu ý: AUDIO_ONLY=1 + REQUIRE_VIETSUB=1 có thể skip audio cho video không có VI
#        → kết hợp AUDIO_ONLY=1 + REQUIRE_VIETSUB=0 để chỉ tải audio (giống v15).
#
# === RETRY VIDEO ĐÃ SKIP VÌ NO_VI_SUBS (force check lại) ===
#   RETRY_NO_VI_SUBS=1 ./run_crawl_v16.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PARENT_DIR="$(dirname "$SCRIPT_DIR")"
# Đảm bảo các module "anh em" (vd: vpn_rotator_v4 nằm ở thư mục cha) cũng
# import được khi chạy crawler từ trong channels_audio/.
export PYTHONPATH="$PARENT_DIR${PYTHONPATH:+:$PYTHONPATH}"
PY=/home/hientran/miniconda3/envs/crawl/bin/python3
CHANNELS_FILE=/home/hientran/sythetic_crawl_data/channels_audio/channels_AI_5_ok_dich.txt
MAX_RESULTS=5000
MAX_FETCH=10000
VIDEO_DELAY=3

# === v14: Mid-download slow-speed rotation config (giống v13/v14) ===
AUDIO_SLOW_SPEED_KBPS="${AUDIO_SLOW_SPEED_KBPS:-500.0}"
AUDIO_SLOW_WINDOW_SECONDS="${AUDIO_SLOW_WINDOW_SECONDS:-30.0}"
AUDIO_MAX_ROTATE_PER_VIDEO="${AUDIO_MAX_ROTATE_PER_VIDEO:-5}"

# === v14: VietSub-specific config ===
# Priority: "auto_first" (default — phù hợp video Việt Nam auto-generated)
#           hoặc "manual_first" (ưu tiên manual cho video VTV, FAPTV...).
# MAX RECALL: giữ auto_first vì VN video hầu hết là auto-translated.
VI_SUB_PRIORITY="${VI_SUB_PRIORITY:-auto_first}"

# Retry video đã mark .no_transcript ở run trước (mặc định: 2 = FORCE).
#   RETRY_NO_TRANSCRIPT=0: tắt → giữ hành vi skip như cũ.
#   RETRY_NO_TRANSCRIPT=1: BẬT → retry video có marker CŨ (> TTL).
#   RETRY_NO_TRANSCRIPT=2: BẬT MẠNH → retry cả marker MỚI (MAX RECALL).
RETRY_NO_TRANSCRIPT="${RETRY_NO_TRANSCRIPT:-10}"

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
# MAX RECALL: concurrency=5 (giảm từ 12) để tránh YouTube rate-limit.
# v17: 12 → 5 vì empty_streak cao khi concurrency=12 + IP VPN warm-up chậm.
SUBS_POPULATE_CONCURRENCY="${SUBS_POPULATE_CONCURRENCY:-1}"

# === v16: VIETSUB PRE-FILTER config (MỚI) ===
# REQUIRE_VIETSUB: CHỈ download audio nếu video có vietsub (any VI key).
#   REQUIRE_VIETSUB=1 (default): BẬT filter → skip audio cho video không có VI.
#                                 Tiết kiệm ~50-500MB/video bandwidth.
#   REQUIRE_VIETSUB=0          : TẮT filter → tải audio hết (giống v15).
# MAX RECALL cho kênh VN:    REQUIRE_VIETSUB=1 (skip EN video nhanh).
# MAX RECALL cho kênh mix:   REQUIRE_VIETSUB=0 (tải audio hết, transcribe
#                            sau sẽ vẫn có kết quả từ API fallback).
if [ "${REQUIRE_VIETSUB:-1}" = "1" ]; then
  REQUIRE_VIETSUB_FLAG=(--require-vietsub)
else
  REQUIRE_VIETSUB_FLAG=(--no-require-vietsub)
fi

# VI_SUBS_CHECK_LANGS: danh sách lang code ưu tiên (phân cách dấu phẩy).
#   "vi"     (default): chỉ chấp nhận video có VI subs.
#   "vi,en"  : chấp nhận EN subs khi không có VI (ASR multilingual).
# MAX RECALL: "vi" (mặc định, vì dataset Qwen3-ASR VI-focused).
VI_SUBS_CHECK_LANGS="${VI_SUBS_CHECK_LANGS:-vi}"

# RETRY_NO_VI_SUBS: Bỏ qua marker .no_vi_subs và check lại từ đầu.
#   RETRY_NO_VI_SUBS=0 (default): tôn trọng marker.
#   RETRY_NO_VI_SUBS=1          : force check lại (sau khi user upload VI subs mới).
RETRY_NO_VI_SUBS="${RETRY_NO_VI_SUBS:-0}"

# NO_VI_SUBS_MARKER_TTL_DAYS: TTL (ngày) cho marker .no_vi_subs.
# Sau TTL sẽ check lại → catch trường hợp user upload VI subs sau.
# MAX RECALL: 7 ngày (default hợp lý).
NO_VI_SUBS_MARKER_TTL_DAYS="${NO_VI_SUBS_MARKER_TTL_DAYS:-7.0}"

# VI_SUBS_CHECK_CACHE_TTL_DAYS: TTL (ngày) cho cache file check VI subs.
# MAX RECALL: 3 ngày (check lại thường xuyên để catch user upload sub mới).
VI_SUBS_CHECK_CACHE_TTL_DAYS="${VI_SUBS_CHECK_CACHE_TTL_DAYS:-3.0}"

# === v16: VI CONTENT VERIFY (langdetect) ===
# NO_VI_CONTENT_VERIFY: TẮT verify nội dung (chỉ check key label, giống v15).
#   0 (default): BẬT verify nội dung bằng langdetect (Tier 4 mới).
#   1: TẮT verify (chỉ check key label, nhanh hơn ~1-3s/video).
# MAX RECALL: BẬT (verify chính xác hơn, chỉ chậm thêm 1-3s ở Tier 4).
NO_VI_CONTENT_VERIFY="${NO_VI_CONTENT_VERIFY:-0}"

# VI_CONTENT_VERIFY_MIN_PROB: Ngưỡng xác suất VI từ langdetect (0.0-1.0).
# Default 0.50 = langdetect top1 phải là 'vi' với prob >=50%.
# MAX RECALL: 0.50 (vừa phải, không quá strict gây miss VI hợp lệ).
VI_CONTENT_VERIFY_MIN_PROB="${VI_CONTENT_VERIFY_MIN_PROB:-0.50}"

# VI_CONTENT_VERIFY_TIMEOUT: Timeout (giây) cho download sub sample verify.
VI_CONTENT_VERIFY_TIMEOUT="${VI_CONTENT_VERIFY_TIMEOUT:-8}"

# === AUDIO_ONLY MODE ===
# 0 (default): chạy đầy đủ pipeline (metadata + subs + audio).
# 1: chỉ tải audio cho video chưa có audio (skip tạo JSON sub mới).
AUDIO_ONLY="${AUDIO_ONLY:-0}"


# v11: Per-instance tunnel isolation.
INSTANCE_ID="${INSTANCE_ID:-pid$$_t$(date +%s)}"

# =============================================================================
# kill_vpn_by_instance() — giống run_crawl_v14.sh, v15 (helper để cleanup).
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

# === v17: ROUTING FIX — kiểm tra policy-based routing rule ===
# Bug: openvpn add route `0.0.0.0/1 + 128.0.0.0/1` qua tun0 (metric 0), nhưng
# SERVER ĐÃ CÀI SẴN rule `from 172.16.198.60 lookup 100` (priority 32760-32765)
# ép MỌI traffic từ IP thật 172.16.198.60 đi qua table 100 → chỉ có default
# via enp3s0 → BỎ QUA VPN hoàn toàn → YouTube thấy IP thật → block.
#
# Fix (Cách 1 — khuyến nghị): admin chạy 1 LẦN:
#   sudo ip rule add uidrange <uid>-<uid> priority 100 lookup main
# → packet từ UID hiện tại sẽ MATCH rule uidrange TRƯỚC (priority 100 < 32760)
# → đi main table → dùng `0.0.0.0/1 + 128.0.0.0/1 via tun0` → qua VPN ✓
# → SSH (root UID 0) không bị ảnh hưởng vì rule uidrange chỉ match UID user.

CURRENT_UID="$(id -u)"
if command -v ip >/dev/null 2>&1 && command -v getcap >/dev/null 2>&1; then
  # Check rule uidrange đã có chưa
  if ! ip rule show 2>/dev/null | grep -q "uidrange ${CURRENT_UID}-${CURRENT_UID}.*lookup main"; then
    echo ""
    echo "================================================================"
    echo "[run_crawl] ⚠️  THIẾU policy-based routing rule cho UID=${CURRENT_UID}"
    echo "================================================================"
    echo "Server có sẵn rule 'from 172.16.198.60 lookup 100' (priority 32760)"
    echo "ép traffic từ IP thật đi qua enp3s0 (IP thật) → bypass VPN."
    echo ""
    echo "FIX (admin chạy 1 LẦN với sudo):"
    echo "    sudo ip rule add uidrange ${CURRENT_UID}-${CURRENT_UID} priority 100 lookup main"
    echo ""
    echo "Verify sau khi admin chạy:"
    echo "    ip rule show"
    echo "    curl https://ifconfig.me   # phải ra IP VPN (không phải IP thật)"
    echo ""
    echo "Để xóa rule (nếu cần rollback):"
    echo "    sudo ip rule del uidrange ${CURRENT_UID}-${CURRENT_UID} priority 100"
    echo ""
    echo "Script vẫn chạy tiếp nhưng CẦN admin fix trước khi crawler work đúng."
    echo "================================================================"
    echo ""
  else
    echo "[run_crawl] ✓ uidrange rule đã có (UID=${CURRENT_UID} đi main table, qua VPN OK)"
  fi

  # Check setcap /bin/ip (cần cho xóa bypass host routes)
  if ! getcap /bin/ip 2>/dev/null | grep -q "cap_net_admin"; then
    echo ""
    echo "[run_crawl] ⚠️  THIẾU cap_net_admin cho /bin/ip"
    echo "[run_crawl]   Cần để xóa các host routes bypass (142.250.0.0/15, ...)."
    echo "[run_crawl]   Chạy 1 LẦN: sudo setcap cap_net_admin+ep /bin/ip"
    echo ""
  else
    echo "[run_crawl] ✓ /bin/ip đã có cap_net_admin (route fix sẽ hoạt động)"
  fi
fi

# === v17: VPN IP HEALTH CHECK ===
# Check IP hiện tại (qua VPN) có sạch không (không bị YouTube flag).
# Nếu DIRTY → cảnh báo user nên rotate IP trước khi chạy.
# Nếu RISKY → cảnh báo CÓ THỂ bị flag sau 5-30 request.
if [ -f "$SCRIPT_DIR/check_vpn_ip_health.py" ]; then
  echo ""
  echo "[run_crawl] Check VPN IP health..."
  IP_CHECK_OUTPUT=$("$PY" "$SCRIPT_DIR/check_vpn_ip_health.py" 2>&1 || true)
  IP_CHECK_EXIT=$?
  echo "$IP_CHECK_OUTPUT" | grep -E "IP:|SUMMARY|IP_OK|IP_RISKY|IP_DIRTY" | head -5
  case $IP_CHECK_EXIT in
    0)
      echo "[run_crawl] ✓ IP sạch — bắt đầu crawler"
      ;;
    1)
      echo "[run_crawl] ⚠️  IP là datacenter (RISKY) — YouTube có thể flag sau 5-30 request"
      echo "[run_crawl]     Nên rotate VPN sang server khác trước khi chạy"
      ;;
    2)
      echo "[run_crawl] ✗ IP BỊ DIRTY (YouTube flag) — crawler sẽ fail ngay"
      echo "[run_crawl]     CẦN rotate VPN sang server khác trước khi chạy"
      ;;
  esac
  echo ""
fi

echo "[run_crawl] node $(node --version) on PATH"
echo "[run_crawl] Instance ID: $INSTANCE_ID"
echo "[run_crawl] *** v16: MAX RECALL MODE (lấy VI subs tối đa) + VIETSUB PRE-FILTER (tiết kiệm bandwidth) ***"
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
echo "[run_crawl]   === v16: VIETSUB PRE-FILTER (MỚI) ==="
echo "[run_crawl]   require_vietsub=${REQUIRE_VIETSUB:-1} (1=CHỈ tải audio nếu có VI subs)"
echo "[run_crawl]   vi_subs_check_langs=${VI_SUBS_CHECK_LANGS} (vd: 'vi' hoặc 'vi,en')"
echo "[run_crawl]   retry_no_vi_subs=${RETRY_NO_VI_SUBS} (1=force check lại từ đầu)"
echo "[run_crawl]   no_vi_subs_marker_ttl_days=${NO_VI_SUBS_MARKER_TTL_DAYS}"
echo "[run_crawl]   vi_subs_check_cache_ttl_days=${VI_SUBS_CHECK_CACHE_TTL_DAYS}"
echo "[run_crawl]   === v16: VI CONTENT VERIFY (langdetect, MỚI) ==="
echo "[run_crawl]   no_vi_content_verify=${NO_VI_CONTENT_VERIFY} (0=BẬT verify nội dung)"
echo "[run_crawl]   vi_content_verify_min_prob=${VI_CONTENT_VERIFY_MIN_PROB}"
echo "[run_crawl]   vi_content_verify_timeout=${VI_CONTENT_VERIFY_TIMEOUT}s"
# echo "[run_crawl]   output_dir=${OUTPUT_DIR}"

# v11: Chỉ kill tunnel CŨ của CÙNG instance_id (nếu run trước crash để lại).
kill_vpn_by_instance "$INSTANCE_ID"

# Launch crawler directly (KHÔNG --audio-only: v16 phải tạo JSON sub cho audio).
echo "[run_crawl] starting crawler (v16: vietsub pre-filter + player_client rotation)..."
echo "[run_crawl]   slow_speed_kbps=${AUDIO_SLOW_SPEED_KBPS} KB/s"
echo "[run_crawl]   slow_window=${AUDIO_SLOW_WINDOW_SECONDS}s"
echo "[run_crawl]   max_rotate_per_video=${AUDIO_MAX_ROTATE_PER_VIDEO}"
cd "$SCRIPT_DIR"

# Build CLI args
ARGS=(
  "$SCRIPT_DIR/youtube_researcher_audio_subs_multi_rotator_v16.py"
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
  # === v16: VIETSUB PRE-FILTER (MỚI) ===
  "${REQUIRE_VIETSUB_FLAG[@]}"
  --vi-subs-check-langs "$VI_SUBS_CHECK_LANGS"
  --no-vi-subs-marker-ttl-days "$NO_VI_SUBS_MARKER_TTL_DAYS"
  --vi-subs-check-cache-ttl-days "$VI_SUBS_CHECK_CACHE_TTL_DAYS"
  # === v16: VI CONTENT VERIFY (langdetect) ===
)

# Thêm --no-vi-content-verify nếu NO_VI_CONTENT_VERIFY=1
if [ "$NO_VI_CONTENT_VERIFY" = "1" ]; then
  ARGS+=(--no-vi-content-verify)
fi
ARGS+=(
  --vi-content-verify-min-prob "$VI_CONTENT_VERIFY_MIN_PROB"
  --vi-content-verify-timeout "$VI_CONTENT_VERIFY_TIMEOUT"
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

# === v16: retry-no-vi-subs ===
if [ "$RETRY_NO_VI_SUBS" = "1" ]; then
  ARGS+=(--retry-no-vi-subs)
fi

echo "[run_crawl] args count=${#ARGS[@]}"
exec "$PY" "${ARGS[@]}"
