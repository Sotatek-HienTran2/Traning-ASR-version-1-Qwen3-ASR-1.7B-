#!/bin/bash
set -euo pipefail

# === CACHE SUDO 1 LẦN ĐẦU (cần cho route add/del qua VPN tunnel) ===
echo "[run_crawl] 🔑 Cần quyền sudo để quản lý route VPN (nhập password 1 lần):"
sudo -v
# Giữ sudo alive suốt session (refresh mỗi 4 phút, chạy nền)
(while true; do sudo -n true; sleep 240; done) 2>/dev/null &
SUDO_KEEP_ALIVE_PID=$!

# === v15: Auto-detect default gateway + interface (thay vì hardcode wifi nhà) ===
# Lưu lại gateway/interface TẠI THỜI ĐIỂM KHỞI ĐỘNG để cleanup đúng khi exit.
DEFAULT_GW="$(ip route show default | awk '/default/ {print $3; exit}')"
DEFAULT_IFACE="$(ip route show default | awk '/default/ {print $5; exit}')"
echo "[run_crawl] Auto-detected: gateway=${DEFAULT_GW:-?} iface=${DEFAULT_IFACE:-?}"

# === TRAP EXIT: dọn route/rule rác khi script kết thúc ===
cleanup_vpn_routes() {
  echo "[cleanup] Dọn policy routing rules/tables rác của instance=${INSTANCE_ID}..."
  # Kill sudo keep-alive background process
  kill "$SUDO_KEEP_ALIVE_PID" 2>/dev/null || true

  # v15: Cleanup chỉ ip rule/table của instance NÀY.
  # Strategy: scan ip rule show, tìm tất cả rule có table trong range 100-249,
  # match source IP với tun device của instance này (nếu tun còn sống),
  # HOẶC fallback: nếu tun đã die thì grep openvpn log để tìm IP đã assign.
  _DEV_PREFIX="${INSTANCE_ID:0:8}"

  # Approach 1: tun device còn sống → lấy IP trực tiếp
  for _suffix in m a s; do
    _tundev="tun_${_DEV_PREFIX}_${_suffix}"
    _tun_ip=$(ip -4 addr show dev "$_tundev" 2>/dev/null | grep -oP 'inet \K[0-9.]+')
    if [ -n "$_tun_ip" ]; then
      _table=$(ip rule show | grep "from $_tun_ip" | grep -oP 'lookup \K[0-9]+' | head -1)
      if [ -n "$_table" ]; then
        sudo ip rule del from "$_tun_ip" table "$_table" 2>/dev/null || true
        sudo ip route flush table "$_table" 2>/dev/null || true
      fi
    fi
  done

  # Approach 2: tun device đã die → scan ip rule cho orphan rules
  # Orphan rules: source IP không match bất kỳ interface nào → safe to remove
  while IFS= read -r _rule_line; do
    _src_ip=$(echo "$_rule_line" | grep -oP 'from \K[0-9.]+')
    _tbl=$(echo "$_rule_line" | grep -oP 'lookup \K[0-9]+')
    [ -z "$_src_ip" ] || [ -z "$_tbl" ] && continue
    # Chỉ xử lý table range 100-249 (policy routing tables)
    [ "$_tbl" -lt 100 ] 2>/dev/null && continue
    [ "$_tbl" -gt 249 ] 2>/dev/null && continue
    # Kiểm tra IP có còn trên hệ thống không
    if ! ip -4 addr show 2>/dev/null | grep -q "inet ${_src_ip}/"; then
      sudo ip rule del from "$_src_ip" table "$_tbl" 2>/dev/null || true
      sudo ip route flush table "$_tbl" 2>/dev/null || true
    fi
  done < <(ip rule show 2>/dev/null | grep "lookup 1[0-9][0-9]\|lookup 2[0-4][0-9]")

  # Xóa route 0.0.0.0/1 và 128.0.0.0/1 main table (legacy v14, nếu còn sót)
  sudo ip route del 0.0.0.0/1 2>/dev/null || true
  sudo ip route del 128.0.0.0/1 2>/dev/null || true
  # Xóa rule priority 50 nếu còn sót từ version cũ
  sudo ip rule del priority 50 2>/dev/null || true
  sudo ip route flush table 300 2>/dev/null || true

  sudo ip route flush cache 2>/dev/null || true
  echo "[cleanup] Done."
}
trap cleanup_vpn_routes EXIT

# =============================================================================
# run_crawl_v15.sh — clone của run_crawl_v14.sh
# -----------------------------------------------------------------------------
# v15 khác v14: Policy routing (custom table per instance, KHÔNG sửa main table).
#               Auto-detect gateway/interface.
#               Chạy được trên mạng nội bộ công ty, nhiều instance song song.
#
# v14 khác v13: VIETSUB ENGINE NÂNG CẤP + youtube-transcript-api FALLBACK.
#
# v14 CẢI TIẾN LOGIC TRANSCRIBE (lấy vietsub):
#   1) VI-sub scoring engine (`_score_vi_subs`): SCORE TẤT CẢ key VI
#      (vi-orig > vi-VN > vi-VN-x-* > vi manual > vi auto > vi-*)
#      → Tăng tỉ lệ tìm được vi-orig (auto-gen gốc của YouTube).
#   2) Best sub-URL picker (`_pick_best_sub_url`): ưu tiên json3 > vtt >
#      ttml > srv3/2/1, check URL hợp lệ trước khi dùng.
#   3) Cookies hot-reload (`_reload_cookies_if_changed`): check mtime
#      cookies.txt mỗi 60s → reload nếu user cập nhật cookies.
#   4) youtube-transcript-api FALLBACK (`_get_youtube_transcript_via_api`):
#      khi yt-dlp fail (captcha/bot check), fallback sang
#      `youtube-transcript-api` library gọi timedtext API (endpoint KHÁC
#      yt-dlp, thường bypass captcha tốt hơn).
#   5) Multi-key fallback: nếu key A fail download/parse → thử key B, ...
#   6) transcribe_with_youtube(): 2 attempts yt-dlp + API fallback chain.
#
# GIỮ NGUYÊN TỪ V13:
#   - SmartDownloader + MidDownloadRotate fix
#   - HTTP500Detector stall fire mỗi 30s
#   - AudioIPController force_real_after_2_fake_fails
#   - Per-instance tunnel kill
#   - 3 rotator tách biệt (audio / transcript / metadata)
#
# Các flag mới truyền xuống crawler (đều có default nên KHÔNG bắt buộc):
#   --vi-sub-priority {auto_first,manual_first}
#                                          # default: auto_first
#                                          # auto_first: ưu tiên vi-orig/vi-VN auto
#                                          # manual_first: ưu tiên sub manual
#   --no-marker-ttl-days N                # TTL (ngày) cho marker .no_transcript
#                                          # default: 7.0
#   --respect-no-transcript-marker        # tôn trọng marker (skip nếu < TTL)
#   --retry-no-transcript                 # retry video có marker cũ (> TTL)
#   --retry-no-transcript-force           # retry bỏ qua cả marker mới
#   --no-api-fallback                     # TẮT fallback youtube-transcript-api
#                                          # default: BẬT (fallback)
#   --api-fallback-langs LANGS            # default: 'vi,en'
#
# Override qua env, ví dụ:
#   VI_SUB_PRIORITY=manual_first \
#   NO_API_FALLBACK=1 \
#   API_FALLBACK_LANGS='vi,en,zh' \
#   ./run_crawl_v14.sh
#
# AUDIO-ONLY MODE (giữ từ v13):
#   AUDIO_ONLY=1 ./run_crawl_v14.sh
#     → Crawler chỉ tải audio, KHÔNG lấy transcript (YouTube subs + ASR).
#     → Bucket A/B (đã có audio từ run cũ): SKIP (không transcribe, không save JSON).
#     → Bucket C (chưa có audio): chỉ download audio, KHÔNG transcribe + save JSON.
#     → Tương đương truyền --audio-only cho youtube_researcher_audio_subs_multi_rotator_v14.py
#     → Dùng khi: muốn xây bộ audio-only dataset (sẽ transcribe sau bằng script riêng
#       như Whisper/NeMo/Qwen3-ASR), hoặc tiết kiệm quota API YouTube (không gọi
#       captions.list + timedtext API), hoặc giảm tải transcript_rotator.
#     → Mặc định: 0 (crawl đầy đủ audio + transcript + JSON).
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY=/home/hientran/miniconda3/envs/crawl/bin/python3
CHANNELS_FILE=/home/hientran/sythetic_crawl_data/channels_audio/channels_khoa_hoc_3_ok_dich.txt
MAX_RESULTS=5000
MAX_FETCH=5000
VIDEO_DELAY=5

# === v13: Mid-download slow-speed rotation config ===
# Có thể override qua env var. Default hợp lý cho audio YouTube 192kbps.
# v13: default 100 KB/s (v12 default: 400 KB/s — quá cao, gần như không bao giờ fire).
# Set thấp hơn nếu muốn phát hiện IP chậm sớm hơn (vd: 50 KB/s).
AUDIO_SLOW_SPEED_KBPS="${AUDIO_SLOW_SPEED_KBPS:-500.0}"
AUDIO_SLOW_WINDOW_SECONDS="${AUDIO_SLOW_WINDOW_SECONDS:-30.0}"
# v14-AGGRESSIVE: giảm từ 3 → 1 lần rotate do slow-speed/video.
# Lý do: fail nhanh thay vì vòng lặp lâu, tiết kiệm thời gian/audio bandwidth.
AUDIO_MAX_ROTATE_PER_VIDEO="${AUDIO_MAX_ROTATE_PER_VIDEO:-1}"
# v14-AGGRESSIVE: giảm stall timeout từ 30s → 15s.
# Lý do: phát hiện IP stuck nhanh hơn, fire rotate sớm hơn.
AUDIO_STALL_SECONDS="${AUDIO_STALL_SECONDS:-15.0}"

# === AUDIO-ONLY MODE (chỉ lấy audio, KHÔNG lấy bản dịch transcribe) ===
# Mặc định TẮT (0 = crawl đầy đủ audio + transcript + JSON).
# Set AUDIO_ONLY=1 để chỉ tải audio:
#   - Bucket A/B (đã có audio từ run cũ) → SKIP (không transcribe + không save JSON)
#   - Bucket C (chưa có audio) → chỉ download audio, KHÔNG transcribe + save JSON
# Override qua env, ví dụ:
#   AUDIO_ONLY=1 ./run_crawl_v14.sh
AUDIO_ONLY="${AUDIO_ONLY:-0}"

# === v14: VIETSUB + API FALLBACK CONFIG ===
# VI_SUB_PRIORITY: "auto_first" | "manual_first"
#   - auto_first (default): auto-captions (vi-orig, vi-VN) được ưu tiên
#   - manual_first: manual subs được bump lên top (VTV, FAPTV style)
VI_SUB_PRIORITY="${VI_SUB_PRIORITY:-auto_first}"

# NO_MARKER_TTL_DAYS: TTL (ngày) cho marker .no_transcript.
#   Sau TTL → retry video. Mặc định 7.0 ngày.
NO_MARKER_TTL_DAYS="${NO_MARKER_TTL_DAYS:-7.0}"

# RESPECT_NO_TRANSCRIPT_MARKER: tôn trọng marker (skip video < TTL).
#   Mặc định 0 = bỏ qua marker, luôn retry.
RESPECT_NO_TRANSCRIPT_MARKER="${RESPECT_NO_TRANSCRIPT_MARKER:-0}"

# RETRY_NO_TRANSCRIPT: retry video có marker cũ (> TTL).
#   Mặc định 0 = tắt (chỉ retry khi respect_no_transcript_marker=0).
RETRY_NO_TRANSCRIPT="${RETRY_NO_TRANSCRIPT:-0}"

# RETRY_NO_TRANSCRIPT_FORCE: retry bỏ qua cả marker MỚI (ghi đè TTL).
#   Mặc định 0 = tắt.
RETRY_NO_TRANSCRIPT_FORCE="${RETRY_NO_TRANSCRIPT_FORCE:-0}"

# NO_API_FALLBACK: tắt fallback sang youtube-transcript-api khi yt-dlp fail.
#   Mặc định 0 = BẬT fallback (khuyến nghị để tăng tỉ lệ lấy được vietsub).
NO_API_FALLBACK="${NO_API_FALLBACK:-0}"

# API_FALLBACK_LANGS: danh sách ngôn ngữ ưu tiên cho API fallback.
#   Mặc định 'vi,en' (Tiếng Việt trước, Tiếng Anh sau).
API_FALLBACK_LANGS="${API_FALLBACK_LANGS:-vi,en}"

# === v14-AGGRESSIVE: Transcript retry config (đọc bởi transcribe_with_youtube) ===
# TRANSCRIPT_MAX_ATTEMPTS: số lần thử yt-dlp tối đa trước khi fallback API.
#   Mặc định v13 = 2. v14-AGGRESSIVE: giảm xuống 1 → fail nhanh, fallback API ngay.
TRANSCRIPT_MAX_ATTEMPTS="${TRANSCRIPT_MAX_ATTEMPTS:-5}"
# TRANSCRIPT_BACKOFF_SECONDS: sleep giữa các attempts (chỉ áp dụng khi max>1).
#   Mặc định v13 = 2s. v14-AGGRESSIVE: giảm xuống 1s. Set 0 = skip sleep.
TRANSCRIPT_BACKOFF_SECONDS="${TRANSCRIPT_BACKOFF_SECONDS:-1}"
# TRANSCRIPT_VPN_RETRY: nếu DIRECT fail → tự động retry với VPN rotator.
#   1 (default) = BẬT, 0 = tắt. Khi BẬT: 1 attempt DIRECT → fail → 1 attempt VPN.
TRANSCRIPT_VPN_RETRY="${TRANSCRIPT_VPN_RETRY:-1}"
# YTDLP_EXTRACT_TIMEOUT: timeout (giây) cho yt-dlp extract_info (lấy subtitle URL).
#   Default Python = 15s. Giảm xuống để fail nhanh hơn khi IP bị block ở tầng HTTP.
#   Pre-check chỉ test TCP connect — nếu pass pre-check nhưng YouTube block HTTP
#   response thì yt-dlp vẫn treo hết timeout này. 10s đủ để phân biệt block vs chậm.
YTDLP_EXTRACT_TIMEOUT="${YTDLP_EXTRACT_TIMEOUT:-10}"
# YTDLP_PRECHECK_TIMEOUT: timeout (giây) cho pre-check TCP connect.
#   Default = 5s. Giảm xuống nếu muốn phát hiện block nhanh hơn.
YTDLP_PRECHECK_TIMEOUT="${YTDLP_PRECHECK_TIMEOUT:-5}"
# YTDLP_PRECHECK_MAX_FAILS: số lần pre-check fail liên tiếp trước khi skip pre-check
#   và để yt-dlp tự thử. Default = 1 (fail 1 lần → attempt 2+ bỏ qua pre-check ngay).
#   Set 0 = disable pre-check hoàn toàn. Set 2 = cần fail 2 lần mới skip.
#   Lý do default=1: pre-check TCP timeout = YouTube drop SYN ở DC level (toàn subnet
#   ProtonVPN free bị chặn), không phải IP cụ thể bị block. Rotate IP không giúp gì.
#   Skip sớm để yt-dlp tự thử (code path khác, TLS reuse, HTTP/2).
YTDLP_PRECHECK_MAX_FAILS="${YTDLP_PRECHECK_MAX_FAILS:-1}"
# TRANSCRIPT_ROTATE_EVERY: cứ mỗi N audio → tự động force_rotate IP fake transcript rotator.
#   Default 10 (cứ 10 audio thì đổi IP 1 lần, tránh Google rate-limit).
#   Set 0 = tắt.
TRANSCRIPT_ROTATE_EVERY="${TRANSCRIPT_ROTATE_EVERY:-10}"

# === v17: YT-DLP REQUEST SPACING (anti rate-limit) ===
# Các tham số này được Python đọc qua os.environ để override default.
# Tăng các giá trị này lên nếu thấy nhiều lỗi 429/rate-limit.
#   YTDLP_SLEEP_REQUESTS: sleep N giây giữa các HTTP requests (default 1).
#   YTDLP_SLEEP_INTERVAL: sleep N giây trước mỗi video download (default 3).
#   YTDLP_MAX_SLEEP_INTERVAL: max sleep (default 8).
#   YTDLP_THROTTLED_RATE: cap tốc độ download (bytes/s, default 5242880 = 5MB/s).
YTDLP_SLEEP_REQUESTS="${YTDLP_SLEEP_REQUESTS:-1}"
YTDLP_SLEEP_INTERVAL="${YTDLP_SLEEP_INTERVAL:-3}"
YTDLP_MAX_SLEEP_INTERVAL="${YTDLP_MAX_SLEEP_INTERVAL:-8}"
YTDLP_THROTTLED_RATE="${YTDLP_THROTTLED_RATE:-5242880}"

# v11: Per-instance tunnel isolation.
# Mỗi instance PHẢI có ID riêng để:
#   - Tunnel của instance A không bị instance B kill nhầm.
#   - Cleanup khi exit chỉ kill tunnel của đúng instance đó.
# Có thể override bằng env var INSTANCE_ID (mặc định: pid{os.getpid()}_t{time}):
#   ./run_crawl_v14.sh                         # auto
#   INSTANCE_ID=inst_a ./run_crawl_v14.sh      # dùng id cố định
INSTANCE_ID="${INSTANCE_ID:-pid$$_t$(date +%s)}"

# =============================================================================
# kill_vpn_fake_ips()
# -----------------------------------------------------------------------------
# Kill TẤT CẢ tunnel OpenVPN "fake IP" đang chạy của user hiện tại, do các run
# trước để lại (các instance crawler cũ crash / bị kill giữa chừng, các tunnel
# OpenVPN session cũ chưa được dọn, ...).
#
# Tại sao cần?
#   - AudioIPController v5 yêu cầu "IP đầu tiên trong session LUÔN là IP thật"
#     (state=REAL, default route, KHÔNG qua VPN). Nếu tunnel OpenVPN cũ của
#     session trước còn sống, system routing vẫn qua VPN → IP đầu tiên SAI
#     là IP fake, phá vỡ logic REAL→FAKE state machine.
#   - Ngoài ra các openvpn process cũ còn ngốm tài nguyên + có thể chiếm
#     /dev/net/tun → block tunnel mới không start được.
#
# Cách làm (an toàn, CHỈ kill process CỦA user hiện tại):
#   1) Đọc tất cả PID file `/tmp/openvpn-proton-*.pid.*.*` (v4/v5 rotator tạo
#      file này qua openvpn --writepid). Với mỗi PID còn sống → SIGTERM,
#      đợi 2s, nếu vẫn sống → SIGKILL.
#   2) Fallback: pkill -u <uid> openvpn để dọn các openvpn process của user
#      (chỉ khi bước 1 chưa dọn hết).
#   3) Verify: nếu vẫn còn interface `tun0` (hoặc các tun*) → cảnh báo.
#   4) KHÔNG xóa file .pid / .log cũ (để debug nếu cần truy vết run trước).
# =============================================================================
kill_vpn_fake_ips() {
  echo "[kill-vpn] Dọn tất cả tunnel OpenVPN fake-IP của user $(id -un) (uid=$(id -u))..."

  local killed_pid=0
  local killed_pkill=0

  # --- Bước 1: Kill theo PID file (chính xác, không sợ nhầm) ---
  # Pattern: /tmp/openvpn-proton-<instance_id>.pid.<idx>.<retry>
  #         /tmp/openvpn-proton-<instance_id>[_role].pid.<idx>.<retry>
  # Dùng nullglob để pattern rỗng không phải là chuỗi nguyên văn.
  shopt -s nullglob
  local pid_files=( /tmp/openvpn-proton-*.pid.*.* )
  shopt -u nullglob

  if [ "${#pid_files[@]}" -eq 0 ]; then
    echo "[kill-vpn]   (không có PID file nào trong /tmp/openvpn-proton-*.pid.*.*)"
  else
    echo "[kill-vpn]   Tìm thấy ${#pid_files[@]} PID file → kill theo PID chính xác..."
    for pf in "${pid_files[@]}"; do
      # PID file chỉ chứa 1 dòng = PID (số)
      local pid
      pid="$(cat "$pf" 2>/dev/null || true)"
      if [ -z "$pid" ] || ! [[ "$pid" =~ ^[0-9]+$ ]]; then
        continue
      fi
      # Check process còn sống không (kill -0 chỉ test, không kill)
      if ! kill -0 "$pid" 2>/dev/null; then
        continue  # đã chết từ trước
      fi
      # Check process thuộc user hiện tại (tránh nhầm PID của user khác)
      local proc_uid
      proc_uid="$(awk -v p="$pid" '$1==p {print $2}' /proc/$pid/status 2>/dev/null || true)"
      if [ -z "$proc_uid" ] || [ "$proc_uid" != "$(id -u)" ]; then
        echo "[kill-vpn]     • PID $pid không thuộc user hiện tại (uid=$proc_uid) → skip"
        continue
      fi
      # SAFETY CHECK: Verify PID này là openvpn dùng proton_config.
      # Đọc /proc/<pid>/cmdline (các argument cách nhau bằng NUL) → phải chứa
      # "openvpn" + "proton_config". Nếu KHÔNG → skip (có thể PID file cũ trỏ
      # vào process khác, hoặc PID được tái sử dụng bởi process khác).
      local cmdline
      cmdline="$(tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null || true)"
      if ! [[ "$cmdline" == *openvpn*proton_config* ]]; then
        echo "[kill-vpn]     • PID $pid KHÔNG phải openvpn+proton_config (cmdline='${cmdline:0:80}...') → skip"
        continue
      fi
      echo "[kill-vpn]     • PID $pid ($(basename "$pf")) → SIGTERM"
      kill -15 "$pid" 2>/dev/null || true
      killed_pid=$((killed_pid + 1))

      # Đợi tối đa 2s cho process chết
      local waited=0
      while [ $waited -lt 4 ]; do
        if ! kill -0 "$pid" 2>/dev/null; then
          break
        fi
        sleep 0.5
        waited=$((waited + 1))
      done
      # Vẫn sống → SIGKILL
      if kill -0 "$pid" 2>/dev/null; then
        echo "[kill-vpn]     • PID $pid vẫn sống sau 2s → SIGKILL"
        kill -9 "$pid" 2>/dev/null || true
      fi
    done
  fi

  # --- Bước 2: Fallback pkill cho các openvpn process của user (còn sót) ---
  # Filter CHẶT: chỉ kill các process openvpn CỦA USER HIỆN TẠI + chỉ các
  # process dùng proton_config (tránh kill openvpn của tool khác).
  if pgrep -u "$(id -u)" -f "openvpn.*proton_config" >/dev/null 2>&1; then
    echo "[kill-vpn]   Fallback: pkill -u $(id -u) -f 'openvpn.*proton_config'..."
    pkill -9 -u "$(id -u)" -f "openvpn.*proton_config" 2>/dev/null || true
    killed_pkill=$((killed_pkill + 1))
  fi

  # --- Bước 3: Verify còn tunnel openvpn-proton nào không ---
  # CHỈ đợi khi openvpn-proton process đã hết → tun interfaces của nó sẽ tự
  # được kernel cleanup. KHÔNG đợi các tun của tool khác (WireGuard, etc.).
  local wait_tun=0
  while [ $wait_tun -lt 10 ]; do
    # Nếu không còn openvpn-proton process nào → xong
    if ! pgrep -u "$(id -u)" -f "openvpn.*proton_config" >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
    wait_tun=$((wait_tun + 1))
  done

  # Liệt kê tun interfaces hiện tại (CHỈ để info, KHÔNG dùng để quyết định kill)
  local remaining_tun
  remaining_tun="$(ip link show 2>/dev/null | awk '/tun[0-9]+:/ {print $2}' | tr -d ':' | tr '\n' ' ' || true)"
  local remaining_procs
  remaining_procs="$(pgrep -u "$(id -u)" -f "openvpn.*proton_config" 2>/dev/null | tr '\n' ' ' || true)"

  if [ -n "$remaining_procs" ]; then
    # Vẫn còn openvpn-proton process → thật sự có vấn đề
    echo "[kill-vpn]   ⚠️  openvpn-proton còn process: '$remaining_procs'"
  elif [ -n "$remaining_tun" ]; then
    # Không còn openvpn-proton process nhưng vẫn còn tun → đó là tun của TOOL KHÁC
    # (WireGuard, OpenVPN của app khác, ...) → KHÔNG kill, chỉ thông báo
    echo "[kill-vpn]   ✅ Đã dọn sạch openvpn-proton. Còn tun: '$remaining_tun' (CỦA TOOL KHÁC, không đụng)."
  else
    echo "[kill-vpn]   ✅ Đã dọn sạch. Không còn tunnel OpenVPN fake-IP nào."
  fi

  echo "[kill-vpn] Tổng: ${killed_pid} PID (theo file) + ${killed_pkill} nhóm fallback pkill"

  # Đợi thêm 1s cho network route hết hẳn
  sleep 1
}

# =============================================================================
# kill_vpn_by_instance(instance_id)
# -----------------------------------------------------------------------------
# v11: Kill CHỈ tunnel OpenVPN của MỘT instance cụ thể (per-instance kill).
#
# Khác với kill_vpn_fake_ips() ở trên: kill_vpn_fake_ips() giết TẤT CẢ tunnel
# của user hiện tại (kể cả tunnel của instance khác đang chạy). Hàm này chỉ
# kill tunnel của instance_id được chỉ định, an toàn cho multi-instance.
#
# Pattern PID file: /tmp/openvpn-proton-{instance_id}.*.pid.*.*
# =============================================================================
kill_vpn_by_instance() {
  local instance_id="$1"
  if [ -z "$instance_id" ]; then
    echo "[kill-by-inst] instance_id rỗng → skip"
    return 0
  fi

  echo "[kill-by-inst] Kill tunnel OpenVPN của instance='$instance_id'..."

  local killed=0

  # Lọc PID file matching instance_id
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

# === POT Server (PO Token) — dùng bản đã build sẵn tại /home/hientran/bgutil-pot-server ===
# Lý do: thư viện /home/sotatek/bgutil-ytdlp-pot-provider KHÔNG truy cập được
# (permission denied với user hientran). Bản đã build sẵn ở:
#   /home/hientran/bgutil-pot-server/server/build/generate_once.js
# Nếu generate_once.js bị mất → script sẽ tự động build lại (npm install + tsc).
POT_SERVER_DIR="/home/hientran/bgutil-pot-server/server"
POT_SCRIPT="$POT_SERVER_DIR/build/generate_once.js"

if [ ! -f "$POT_SCRIPT" ]; then
  echo "[run_crawl] POT: generate_once.js chưa có → build..."
  if [ ! -d "$POT_SERVER_DIR" ]; then
    echo "[run_crawl] POT: Clone bgutil-ytdlp-pot-provider..."
    git clone https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git "$POT_SERVER_DIR"
  fi
  if [ ! -d "$POT_SERVER_DIR/node_modules" ]; then
    echo "[run_crawl] POT: npm install..."
    (cd "$POT_SERVER_DIR" && npm install)
  fi
  echo "[run_crawl] POT: Compiling TypeScript → build/generate_once.js..."
  (cd "$POT_SERVER_DIR" && npx tsc --outDir build)
  if [ -f "$POT_SCRIPT" ]; then
    echo "[run_crawl] POT: Build OK → $POT_SCRIPT"
  else
    echo "[run_crawl] POT: ⚠️  Build FAILED — subtitle extraction sẽ không có PO Token!"
  fi
else
  echo "[run_crawl] POT: generate_once.js OK ($POT_SCRIPT)"
fi

# === Khởi động POT server trên http://127.0.0.1:4416 (nếu chưa chạy) ===
# Gọi start_bgutil.sh (idempotent — sẽ không spawn instance thứ 2 nếu đã lên).
# Sau đó sanity check /ping để chắc chắn server thực sự sẵn sàng, tránh
# yt-dlp gặp "Sign in to confirm you're not a bot".
echo "[run_crawl] Ensuring POT server is up on http://127.0.0.1:4416..."
bash "$SCRIPT_DIR/start_bgutil.sh"
if ! curl -sf --max-time 3 http://127.0.0.1:4416/ping >/dev/null; then
  echo "[run_crawl] ⚠️  bgutil POT server is NOT responding at http://127.0.0.1:4416" >&2
  echo "[run_crawl]     (yt-dlp sẽ không có PO Token → có thể fail với 'Sign in to confirm')" >&2
fi

# v15: Startup orphan cleanup — dọn ip rule/table rác từ lần chạy trước bị kill -9
echo "[run_crawl] Scanning orphan policy routing rules..."
_orphan_cleaned=0
while IFS= read -r _rule_line; do
  _src_ip=$(echo "$_rule_line" | grep -oP 'from \K[0-9.]+')
  _tbl=$(echo "$_rule_line" | grep -oP 'lookup \K[0-9]+')
  [ -z "$_src_ip" ] || [ -z "$_tbl" ] && continue
  [ "$_tbl" -lt 100 ] 2>/dev/null && continue
  [ "$_tbl" -gt 249 ] 2>/dev/null && continue
  if ! ip -4 addr show 2>/dev/null | grep -q "inet ${_src_ip}/"; then
    sudo ip rule del from "$_src_ip" table "$_tbl" 2>/dev/null || true
    sudo ip route flush table "$_tbl" 2>/dev/null || true
    _orphan_cleaned=$((_orphan_cleaned + 1))
  fi
done < <(ip rule show 2>/dev/null | grep "lookup 1[0-9][0-9]\|lookup 2[0-4][0-9]")
[ "$_orphan_cleaned" -gt 0 ] && echo "[run_crawl] Cleaned $_orphan_cleaned orphan rule(s)" || echo "[run_crawl] No orphan rules found"

# v11: Chỉ kill tunnel CŨ của CÙNG instance_id (nếu run trước crash để lại).
# KHÔNG gọi kill_vpn_fake_ips() nữa — nó giết cả tunnel của instance khác.
kill_vpn_by_instance "$INSTANCE_ID"

# Launch crawler directly
echo "[run_crawl] starting crawler (v15: auto-detect network + VIETSUB engine + API fallback, AGGRESSIVE retry)..."
echo "[run_crawl]   slow_speed_kbps=${AUDIO_SLOW_SPEED_KBPS} KB/s"
echo "[run_crawl]   slow_window=${AUDIO_SLOW_WINDOW_SECONDS}s"
echo "[run_crawl]   max_rotate_per_video=${AUDIO_MAX_ROTATE_PER_VIDEO}"
echo "[run_crawl]   stall_seconds=${AUDIO_STALL_SECONDS}s (fire rotate khi stuck)"
echo "[run_crawl]   audio_only_mode=${AUDIO_ONLY} (1=chỉ tải audio, KHÔNG transcribe)"
echo "[run_crawl]   transcript_max_attempts=${TRANSCRIPT_MAX_ATTEMPTS} (yt-dlp retry trước khi fallback API)"
echo "[run_crawl]   transcript_backoff_seconds=${TRANSCRIPT_BACKOFF_SECONDS}s"
echo "[run_crawl]   transcript_vpn_retry=${TRANSCRIPT_VPN_RETRY} (1=DIRECT fail → retry VPN)"
echo "[run_crawl]   transcript_rotate_every=${TRANSCRIPT_ROTATE_EVERY} (cứ N audio → force_rotate IP fake)"
echo "[run_crawl]   ytdlp_extract_timeout=${YTDLP_EXTRACT_TIMEOUT}s (yt-dlp extract_info timeout)"
echo "[run_crawl]   ytdlp_precheck_timeout=${YTDLP_PRECHECK_TIMEOUT}s (TCP pre-check timeout)"
echo "[run_crawl]   ytdlp_precheck_max_fails=${YTDLP_PRECHECK_MAX_FAILS} (skip pre-check sau N fail liên tiếp)"
echo "[run_crawl]   *** v14-AGGRESSIVE: max_rotate=1, stall=15s, transcript_attempts=1, backoff=1s ***"
echo "[run_crawl]   *** v14: VI-sub scoring engine + multi-key fallback ***"
echo "[run_crawl]   *** v14: youtube-transcript-api fallback (mặc định BẬT) ***"
echo "[run_crawl]   vi_sub_priority=${VI_SUB_PRIORITY}"
echo "[run_crawl]   no_marker_ttl_days=${NO_MARKER_TTL_DAYS}"
echo "[run_crawl]   respect_no_transcript_marker=${RESPECT_NO_TRANSCRIPT_MARKER}"
echo "[run_crawl]   retry_no_transcript=${RETRY_NO_TRANSCRIPT}"
echo "[run_crawl]   retry_no_transcript_force=${RETRY_NO_TRANSCRIPT_FORCE}"
echo "[run_crawl]   no_api_fallback=${NO_API_FALLBACK} (1=tắt fallback)"
echo "[run_crawl]   api_fallback_langs=${API_FALLBACK_LANGS}"
echo "[run_crawl]   ytdlp_sleep_requests=${YTDLP_SLEEP_REQUESTS}s (sleep giữa HTTP requests)"
echo "[run_crawl]   ytdlp_sleep_interval=${YTDLP_SLEEP_INTERVAL}s (sleep trước mỗi video)"
echo "[run_crawl]   ytdlp_max_sleep_interval=${YTDLP_MAX_SLEEP_INTERVAL}s"
echo "[run_crawl]   ytdlp_throttled_rate=$((YTDLP_THROTTLED_RATE / 1024 / 1024))MB/s (cap tốc độ)"
cd "$SCRIPT_DIR"

# === v14-AGGRESSIVE: Export transcript retry config để Python đọc qua os.environ ===
export TRANSCRIPT_MAX_ATTEMPTS
export TRANSCRIPT_BACKOFF_SECONDS
export TRANSCRIPT_VPN_RETRY
export TRANSCRIPT_ROTATE_EVERY
export YTDLP_EXTRACT_TIMEOUT
export YTDLP_PRECHECK_TIMEOUT
export YTDLP_PRECHECK_MAX_FAILS
# === v17: Export yt-dlp request spacing vars để Python đọc ===
export YTDLP_SLEEP_REQUESTS
export YTDLP_SLEEP_INTERVAL
export YTDLP_MAX_SLEEP_INTERVAL
export YTDLP_THROTTLED_RATE

# === v17: Bucket B parallel workers ===
BUCKET_B_WORKERS="${BUCKET_B_WORKERS:-1}"
export BUCKET_B_WORKERS

# Build CLI args
CLI_ARGS=(
  --channels-file "$CHANNELS_FILE"
  --max-results "$MAX_RESULTS"
  --max-fetch "$MAX_FETCH"
  --video-delay "$VIDEO_DELAY"
  --instance-id "$INSTANCE_ID"
  --skip-existing
  --audio-slow-speed-kbps "$AUDIO_SLOW_SPEED_KBPS"
  --audio-slow-window-seconds "$AUDIO_SLOW_WINDOW_SECONDS"
  --audio-max-rotate-per-video "$AUDIO_MAX_ROTATE_PER_VIDEO"
  --audio-stall-seconds "$AUDIO_STALL_SECONDS"
  # v14: VIETSUB
  --vi-sub-priority "$VI_SUB_PRIORITY"
  --no-marker-ttl-days "$NO_MARKER_TTL_DAYS"
)

# === AUDIO-ONLY MODE: chỉ tải audio, KHÔNG lấy bản dịch transcribe ===
if [ "$AUDIO_ONLY" = "1" ]; then
  echo "[run_crawl]   ⚠️  AUDIO-ONLY MODE: chỉ tải audio, KHÔNG transcribe + save JSON"
  CLI_ARGS+=(--audio-only)
fi

# === v14: RESPECT NO_TRANSCRIPT MARKER ===
# Nếu user set RESPECT_NO_TRANSCRIPT_MARKER=1 → tôn trọng marker cũ (< TTL)
if [ "$RESPECT_NO_TRANSCRIPT_MARKER" = "1" ]; then
  echo "[run_crawl]   📌 RESPECT_NO_TRANSCRIPT_MARKER: tôn trọng marker cũ (< TTL)"
  CLI_ARGS+=(--respect-no-transcript-marker)
fi

# === v14: RETRY NO_TRANSCRIPT (cũ hoặc force) ===
if [ "$RETRY_NO_TRANSCRIPT" = "1" ]; then
  echo "[run_crawl]   🔄 RETRY_NO_TRANSCRIPT: retry video có marker cũ (> TTL)"
  CLI_ARGS+=(--retry-no-transcript)
fi

if [ "$RETRY_NO_TRANSCRIPT_FORCE" = "1" ]; then
  echo "[run_crawl]   🔄 RETRY_NO_TRANSCRIPT_FORCE: retry bỏ qua cả marker mới"
  CLI_ARGS+=(--retry-no-transcript-force)
fi

exec "$PY" \
  "$SCRIPT_DIR/youtube_researcher_audio_subs_multi_rotator_v17.py" \
  "${CLI_ARGS[@]}"