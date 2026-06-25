#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PY=/home/hientran/miniconda3/envs/crawl/bin/python3
CHANNELS_FILE=/home/hientran/sythetic_crawl_data/channels_audio/channels_lich_su_1.txt
MAX_RESULTS=5000
MAX_FETCH=50000
VIDEO_DELAY=5

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

# Đảm bảo node >= 23.5.0
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

# === Kill hết tunnel OpenVPN fake-IP của các run cũ ===
# Lý do: AudioIPController v5 yêu cầu IP đầu tiên LUÔN là IP THẬT (state=REAL).
# Nếu tunnel OpenVPN cũ còn sống → system routing đi qua VPN → IP đầu tiên
# SAI là IP fake, phá vỡ state machine REAL→FAKE.
kill_vpn_fake_ips

# Launch crawler directly
echo "[run_crawl] starting crawler..."
cd "$SCRIPT_DIR"

exec "$PY" \
  "$SCRIPT_DIR/youtube_researcher_audio_subs_multi_rotator_v6.py" \
  --channels-file "$CHANNELS_FILE" \
  --max-results "$MAX_RESULTS" \
  --max-fetch "$MAX_FETCH" \
  --video-delay "$VIDEO_DELAY" \
  --skip-existing \
  --audio-only 


  # exec "$PY" \
  # "$SCRIPT_DIR/youtube_researcher_audio_subs_multi_rotator_v5.py" \
  # --channels-file "$CHANNELS_FILE" \
  # --max-results "$MAX_RESULTS" \
  # --max-fetch "$MAX_FETCH" \
  # --video-delay "$VIDEO_DELAY" \
  # --skip-existing \
  # --audio-only