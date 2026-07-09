#!/bin/bash
set -euo pipefail

# =============================================================================
# run_crawl_v13.sh — clone của run_crawl_v12.sh
# -----------------------------------------------------------------------------
# v13 khác v12: FIX bug "MidDownloadRotate bị outer try/except nuốt".
#
# v12 BUG (đã fix trong v13):
#   - Hook raise MidDownloadRotate ngay giữa chunk download
#   - Inner except re-raise đúng
#   - NHƯNG outer `except Exception as hook_err` ở cuối hook catch lại, chỉ in log
#   - → yt-dlp KHÔNG nhận được exception
#   - → SmartDownloader KHÔNG BAO GIỜ catch MidDownloadRotate
#   - → KHÔNG BAO GIỜ rotate mid-download
#
# v13 FIX (3 lớp):
#   1. MidDownloadRotate giờ kế thừa DownloadError → yt-dlp propagate đúng
#   2. Hook KHÔNG còn outer try/except nuốt exception
#   3. Có marker file /tmp/v13_mid_download_rotate.* làm fallback cuối cùng
#
# Các flag mới truyền xuống crawler (đều có default nên KHÔNG bắt buộc):
#   --audio-slow-speed-kbps N        # rolling avg < N KB/s → rotate
#                                    # default: 100.0 (v13 tăng từ 50.0), 0 = tắt
#   --audio-slow-window-seconds N    # cửa sổ tính rolling avg
#                                    # default: 30.0
#   --audio-max-rotate-per-video N   # số lần rotate tối đa do slow-speed1/video
#                                    # default: 3, 0 = không giới hạn
#
# Override qua env, ví dụ:
#   AUDIO_SLOW_SPEED_KBPS=30 AUDIO_SLOW_WINDOW_SECONDS=45 \
#   AUDIO_MAX_ROTATE_PER_VIDEO=5 ./run_crawl_v13.sh
#
# AUDIO-ONLY MODE (mới thêm):
#   AUDIO_ONLY=1 ./run_crawl_v13.sh
#     → Crawler chỉ tải audio, KHÔNG lấy transcript (YouTube subs + ASR).
#     → Bucket A/B (đã có audio từ run cũ): SKIP (không transcribe, không save JSON).
#     → Bucket C (chưa có audio): chỉ download audio, KHÔNG transcribe + save JSON.
#     → Tương đương truyền --audio-only cho youtube_researcher_audio_subs_multi_rotator_v13.py
#     → Dùng khi: muốn xây bộ audio-only dataset (sẽ transcribe sau bằng script riêng
#       như Whisper/NeMo/Qwen3-ASR), hoặc tiết kiệm quota API YouTube (không gọi
#       captions.list + timedtext API), hoặc giảm tải transcript_rotator.
#     → Mặc định: 0 (crawl đầy đủ audio + transcript + JSON).
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# Python từ conda env 'crawl' (chứa yt-dlp, google-api-python-client, dotenv, ...)
# env gốc /home/sotatek/miniconda3/envs/crawl không tồn tại trên máy này.
# Có thể override bằng env var PY, ví dụ: PY=python3 ./run_crawl_v13.sh
PY="${PY:-/home/hientran/miniconda3/envs/crawl/bin/python3}"
# Channels file: máy này KHÔNG có /home/sotatek/.../channels_marketing.txt.
# Dùng file có sẵn trong /home/hientran/sythetic_crawl_data/channels_audio/.
# Có thể override bằng env var CHANNELS_FILE.
CHANNELS_FILE=/home/hientran/sythetic_crawl_data/channels_audio/channels_tong_hop_1.txt
MAX_RESULTS=1000
MAX_FETCH=500
VIDEO_DELAY=5

# === v13: Mid-download slow-speed rotation config ===
# Có thể override qua env var. Default hợp lý cho audio YouTube 192kbps.
# v13: default 100 KB/s (v12 default: 400 KB/s — quá cao, gần như không bao giờ fire).
# Set thấp hơn nếu muốn phát hiện IP chậm sớm hơn (vd: 50 KB/s).
AUDIO_SLOW_SPEED_KBPS="${AUDIO_SLOW_SPEED_KBPS:-500.0}"
AUDIO_SLOW_WINDOW_SECONDS="${AUDIO_SLOW_WINDOW_SECONDS:-30.0}"
AUDIO_MAX_ROTATE_PER_VIDEO="${AUDIO_MAX_ROTATE_PER_VIDEO:-3}"

# === AUDIO-ONLY MODE (chỉ lấy audio, KHÔNG lấy bản dịch transcribe) ===
# Mặc định TẮT (0 = crawl đầy đủ audio + transcript + JSON).
# Set AUDIO_ONLY=1 để chỉ tải audio:
#   - Bucket A/B (đã có audio từ run cũ) → SKIP (không transcribe + không save JSON)
#   - Bucket C (chưa có audio) → chỉ download audio, KHÔNG transcribe + save JSON
# Override qua env, ví dụ:
#   AUDIO_ONLY=1 ./run_crawl_v13.sh
AUDIO_ONLY="${AUDIO_ONLY:-1}"

# v11: Per-instance tunnel isolation.
# Mỗi instance PHẢI có ID riêng để:
#   - Tunnel của instance A không bị instance B kill nhầm.
#   - Cleanup khi exit chỉ kill tunnel của đúng instance đó.
# Có thể override bằng env var INSTANCE_ID (mặc định: pid{os.getpid()}_t{time}):
#   ./run_crawl_v11.sh                         # auto
#   INSTANCE_ID=inst_a ./run_crawl_v11.sh      # dùng id cố định
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

# v11: Chỉ kill tunnel CŨ của CÙNG instance_id (nếu run trước crash để lại).
# KHÔNG gọi kill_vpn_fake_ips() nữa — nó giết cả tunnel của instance khác.
kill_vpn_by_instance "$INSTANCE_ID"

# Launch crawler directly
echo "[run_crawl] starting crawler (v13: FIX mid-download slow-speed rotation)..."
echo "[run_crawl]   slow_speed_kbps=${AUDIO_SLOW_SPEED_KBPS} KB/s"
echo "[run_crawl]   slow_window=${AUDIO_SLOW_WINDOW_SECONDS}s"
echo "[run_crawl]   max_rotate_per_video=${AUDIO_MAX_ROTATE_PER_VIDEO}"
echo "[run_crawl]   audio_only_mode=${AUDIO_ONLY} (1=chỉ tải audio, KHÔNG transcribe)"
echo "[run_crawl]   *** v13 FIX: hook raise MidDownloadRotate KHÔNG bị outer try/except nuốt nữa ***"
echo "[run_crawl]   *** v13 FIX: default threshold 100 KB/s (v12: 50 KB/s) ***"
cd "$SCRIPT_DIR"

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
)

# === AUDIO-ONLY MODE: chỉ tải audio, KHÔNG lấy bản dịch transcribe ===
if [ "$AUDIO_ONLY" = "1" ]; then
  echo "[run_crawl]   ⚠️  AUDIO-ONLY MODE: chỉ tải audio, KHÔNG transcribe + save JSON"
  CLI_ARGS+=(--audio-only)
fi

exec "$PY" \
  "$SCRIPT_DIR/youtube_researcher_audio_subs_multi_rotator_v13.py" \
  "${CLI_ARGS[@]}"


