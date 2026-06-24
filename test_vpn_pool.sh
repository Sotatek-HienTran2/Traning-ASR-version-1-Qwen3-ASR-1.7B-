#!/bin/bash
# Test từng ProtonVPN server xem cái nào YouTube còn cho qua, cái nào bị block.
# Output: proton_config/.test_result_<pid>.log
#   GOOD    → IP sạch, dùng được
#   BLOCKED → IP bị YouTube flag, NÊN XÓA khỏi proton_config/
#
# Mỗi server tốn ~40-60s (connect OpenVPN + curl test + disconnect).
# 62 server × 60s ≈ 1 giờ. Chạy nền (run_in_background).
#
# Usage:
#   bash /home/hientran/sythetic_crawl_data/test_vpn_pool.sh [--limit N]
#   bash /home/hientran/sythetic_crawl_data/test_vpn_pool.sh --country us
#   bash /home/hientran/sythetic_crawl_data/test_vpn_pool.sh --remove-blocked

set -uo pipefail

CONFIG_DIR=/home/hientran/sythetic_crawl_data/proton_config
AUTH_FILE="$CONFIG_DIR/auth.txt"
TEST_URL="https://www.youtube.com/watch?v=jNQXAC9IVRw"  # "Me at the zoo" - video public, stable
OUTLOG="$CONFIG_DIR/.test_result_$$.log"
PROGRESS_LOG="$CONFIG_DIR/.test_progress_$$.log"
SUMMARY="$CONFIG_DIR/.test_summary_$$.txt"

# ----- parse args -----
COUNTRY_FILTER=""
LIMIT=0
REMOVE_BLOCKED=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --country) COUNTRY_FILTER="$2"; shift 2;;
    --limit)   LIMIT="$2"; shift 2;;
    --remove-blocked) REMOVE_BLOCKED=1; shift;;
    *) echo "Unknown arg: $1"; exit 1;;
  esac
done

# ----- list server to test -----
mapfile -t SERVERS < <(ls "$CONFIG_DIR"/*.ovpn 2>/dev/null | xargs -n1 basename | grep -v "^\." | sort)
if [[ -n "$COUNTRY_FILTER" ]]; then
  SERVERS=("${SERVERS[@]/$COUNTRY_FILTER-*}")  # crude filter
  # rebuild properly
  SERVERS=()
  for s in $(ls "$CONFIG_DIR"/${COUNTRY_FILTER}-*.ovpn 2>/dev/null | xargs -n1 basename); do
    SERVERS+=("$s")
  done
fi
TOTAL=${#SERVERS[@]}
if [[ $LIMIT -gt 0 && $LIMIT -lt $TOTAL ]]; then
  SERVERS=("${SERVERS[@]:0:$LIMIT}")
  TOTAL=$LIMIT
fi

echo "===================================================" | tee "$SUMMARY"
echo "ProtonVPN Pool Test" | tee -a "$SUMMARY"
echo "  Total to test: $TOTAL" | tee -a "$SUMMARY"
echo "  Country filter: ${COUNTRY_FILTER:-ALL}" | tee -a "$SUMMARY"
echo "  Started: $(date)" | tee -a "$SUMMARY"
echo "  Output: $OUTLOG" | tee -a "$SUMMARY"
echo "===================================================" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"

# ----- helpers -----
kill_all_vpn() {
  # Kill CHỈ tunnel test của script này (dựa vào log path /tmp/openvpn-test-vpn.log
  # hoặc pid file). KHÔNG động vào tunnel của crawl (openvpn-proton-*.log).
  if [[ -f /tmp/openvpn-test-vpn.pid ]]; then
    PID=$(cat /tmp/openvpn-test-vpn.pid 2>/dev/null)
    if [[ -n "$PID" ]]; then
      kill -TERM "$PID" 2>/dev/null || true
      # đợi tunnel xuống
      for i in 1 2 3 4 5; do
        kill -0 "$PID" 2>/dev/null || break
        sleep 1
      done
      kill -KILL "$PID" 2>/dev/null || true
    fi
    rm -f /tmp/openvpn-test-vpn.pid
  fi
  # backup: kill anything listening on test log
  pkill -f "openvpn.*--log /tmp/openvpn-test-vpn\.log" 2>/dev/null
  sleep 2
}

cleanup() {
  kill_all_vpn
  # Cleanup routes added by this script (chỉ xóa route do script này tạo)
  for r in $(ip route 2>/dev/null | grep "^0\.0\.0\.0/1 via.*dev tun[0-9]" | grep -v "tun0 "); do
    ip route del $r 2>/dev/null || true
  done
  for r in $(ip route 2>/dev/null | grep "^128\.0\.0\.0/1 via.*dev tun[0-9]" | grep -v "tun0 "); do
    ip route del $r 2>/dev/null || true
  done
  echo ""
  echo ">>> Done. Summary saved to $SUMMARY"
  echo ">>> Full log:      $OUTLOG"
}

trap cleanup EXIT

# ----- check pre-req -----
if [[ ! -f "$AUTH_FILE" ]]; then
  echo "FATAL: $AUTH_FILE not found"
  exit 1
fi
if ! command -v curl >/dev/null; then
  echo "FATAL: curl not installed"
  exit 1
fi

# ----- test loop -----
GOOD=0
BLOCKED=0
FAIL=0
BLOCKED_LIST=()

for idx in "${!SERVERS[@]}"; do
  server="${SERVERS[$idx]}"
  num=$((idx + 1))
  printf "[%2d/%2d] %-50s ... " "$num" "$TOTAL" "$server" | tee -a "$PROGRESS_LOG"

  # 1. kill old tunnel + wait
  kill_all_vpn
  sleep 2

  # 2. prepare ovpn: strip update-resolv-conf (DNS update script gây
  #    "Failed to set DNS configuration" vì cần interactive auth).
  #    Strip cả route IPv6 (gây "File exists" khi add).
  TEST_OVPN=/tmp/openvpn-test-vpn.ovpn
  cp "$CONFIG_DIR/$server" "$TEST_OVPN"
  # Loại bỏ các dòng up/down/up-pre/down-pre trỏ tới update-resolv-conf
  sed -i '/^[[:space:]]*\(up\|down\|up-pre\|down-pre\)[[:space:]]/d' "$TEST_OVPN"
  # Loại bỏ route-ipv6 để tránh "File exists"
  sed -i '/^[[:space:]]*route-ipv6/d' "$TEST_OVPN"
  # Loại bỏ redirect-gateway def1 (để script tự add route)
  sed -i '/^[[:space:]]*redirect-gateway/d' "$TEST_OVPN"
  # Thêm script-security + up/down noop
  echo "script-security 2" >> "$TEST_OVPN"
  echo "up /bin/true" >> "$TEST_OVPN"
  echo "down /bin/true" >> "$TEST_OVPN"

  # 3. connect new tunnel (daemon mode)
  TEST_LOG=/tmp/openvpn-test-vpn.log
  rm -f "$TEST_LOG" /tmp/openvpn-test-vpn.pid
  /usr/sbin/openvpn \
    --config "$TEST_OVPN" \
    --auth-user-pass "$AUTH_FILE" \
    --auth-retry nointeract --auth-nocache \
    --daemon \
    --log "$TEST_LOG" \
    --writepid /tmp/openvpn-test-vpn.pid \
    --dev tun \
    >/dev/null 2>&1

  # 4. wait for tunnel ready (max 60s)
  # Tunnel mới sẽ có IP 10.x.x.x, do openvpn process mới (PID trong
  # /tmp/openvpn-test-vpn.pid) tạo ra. Tìm tun device do PID này tạo.
  NEW_VPN_PID=$(cat /tmp/openvpn-test-vpn.pid 2>/dev/null)
  ready=0
  TUN_DEV=""
  for i in $(seq 1 60); do
    # Tìm tun devices
    while IFS= read -r dev; do
      [[ -z "$dev" ]] && continue
      # Get ifindex của dev này
      IFINDEX=$(cat "/sys/class/net/$dev/ifindex" 2>/dev/null)
      [[ -z "$IFINDEX" ]] && continue
      # Check openvpn process mới có fd open tới tun này không
      if [[ -n "$NEW_VPN_PID" ]] && \
         ls -l /proc/$NEW_VPN_PID/fd/ 2>/dev/null | grep -q "tun"; then
        # Tìm dev tun mà process này đang giữ
        OWNED_TUN=$(ls -l /proc/$NEW_VPN_PID/fd/ 2>/dev/null | \
                    grep -oE 'tun[0-9]+' | head -1)
        if [[ -n "$OWNED_TUN" && "$OWNED_TUN" == "$dev" ]]; then
          TUN_DEV="$dev"
          break
        fi
      fi
    done < <(ls /sys/class/net/ | grep -E '^tun[0-9]+$' | sort -V)
    if [[ -n "$TUN_DEV" ]]; then
      DEV_IP=$(ip -o addr show dev "$TUN_DEV" 2>/dev/null | awk '$3=="inet" {print $4}' | head -1)
      if [[ -n "$DEV_IP" ]] && [[ "$DEV_IP" == 10.* ]]; then
        TUN_IP="${DEV_IP%/*}"
        TUN_GW="${TUN_IP%.*}.1"
        # Dùng metric cao hơn metric của route cũ (nếu có) để route mới ưu tiên
        # Lấy metric hiện tại của 0.0.0.0/1
        EXISTING_METRIC=$(ip route 2>/dev/null | grep "^0\.0\.0\.0/1 via" | awk '{for(i=1;i<=NF;i++) if($i=="metric") print $(i+1)}' | head -1)
        [[ -z "$EXISTING_METRIC" ]] && EXISTING_METRIC=0
        NEW_METRIC=$((EXISTING_METRIC - 10))
        if ip route add 0.0.0.0/1 via "$TUN_GW" dev "$TUN_DEV" metric "$NEW_METRIC" 2>/dev/null; then
          ip route add 128.0.0.0/1 via "$TUN_GW" dev "$TUN_DEV" metric "$NEW_METRIC" 2>/dev/null
          ready=1
        else
          # Fallback: add without metric
          ip route add 0.0.0.0/1 via "$TUN_GW" dev "$TUN_DEV" 2>/dev/null && \
          ip route add 128.0.0.0/1 via "$TUN_GW" dev "$TUN_DEV" 2>/dev/null && \
          ready=1
        fi
        break
      fi
    fi
    sleep 1
  done

  if [[ $ready -eq 0 ]]; then
    KNOWN=$(ls /sys/class/net/ | grep -E '^tun[0-9]+' | tr '\n' ',' )
    echo "FAIL (no tunnel) | tun_devs=$KNOWN | vpn_pid=$NEW_VPN_PID" | tee -a "$PROGRESS_LOG" "$OUTLOG"
    FAIL=$((FAIL + 1))
    continue
  fi

  # 4. test YouTube - lấy IP + check redirect sign-in
  CURRENT_IP=$(curl -s --max-time 10 --proxy "" https://api.ipify.org 2>/dev/null || echo "?")
  HTTP_CODE=$(curl -s -o /tmp/youtube-test.html -w "%{http_code}" --max-time 25 "$TEST_URL" 2>/dev/null || echo "000")

  # Check response có dấu hiệu bị chặn
  # - HTTP 200 + có "consent.youtube.com" hoặc "Sign in to confirm" → blocked
  # - HTTP 200 + có "Me at the zoo" trong title → OK
  if grep -q "Sign in to confirm" /tmp/youtube-test.html 2>/dev/null \
     || grep -q "consent\.youtube\.com" /tmp/youtube-test.html 2>/dev/null \
     || grep -q "before you continue" /tmp/youtube-test.html 2>/dev/null; then
    VERDICT="BLOCKED"
    BLOCKED=$((BLOCKED + 1))
    BLOCKED_LIST+=("$server")
  elif grep -q "Me at the zoo" /tmp/youtube-test.html 2>/dev/null; then
    VERDICT="GOOD"
    GOOD=$((GOOD + 1))
  elif [[ "$HTTP_CODE" == "200" ]]; then
    # 200 nhưng không match title → coi như ambiguous, check tiếp
    if grep -qi "video" /tmp/youtube-test.html 2>/dev/null; then
      VERDICT="GOOD"
      GOOD=$((GOOD + 1))
    else
      VERDICT="AMBIGUOUS (HTTP $HTTP_CODE)"
      FAIL=$((FAIL + 1))
    fi
  else
    VERDICT="FAIL (HTTP $HTTP_CODE)"
    FAIL=$((FAIL + 1))
  fi

  echo "$VERDICT | ip=$CURRENT_IP | http=$HTTP_CODE" | tee -a "$PROGRESS_LOG" "$OUTLOG"
done

# ----- summary -----
{
  echo ""
  echo "==================================================="
  echo "RESULT SUMMARY"
  echo "  GOOD:     $GOOD"
  echo "  BLOCKED:  $BLOCKED"
  echo "  FAIL:     $FAIL"
  echo "  Total:    $TOTAL"
  echo "  Ended:    $(date)"
  echo "==================================================="
  if [[ ${#BLOCKED_LIST[@]} -gt 0 ]]; then
    echo ""
    echo ">>> BLOCKED servers (NÊN XÓA khỏi proton_config/):"
    for s in "${BLOCKED_LIST[@]}"; do
      echo "    rm \"$CONFIG_DIR/$s\""
    done
  fi
} | tee -a "$SUMMARY"

# ----- optionally remove blocked -----
if [[ $REMOVE_BLOCKED -eq 1 && ${#BLOCKED_LIST[@]} -gt 0 ]]; then
  echo ""
  echo ">>> --remove-blocked: moving ${#BLOCKED_LIST[@]} blocked server(s) to $CONFIG_DIR/.blocked/"
  mkdir -p "$CONFIG_DIR/.blocked"
  for s in "${BLOCKED_LIST[@]}"; do
    mv "$CONFIG_DIR/$s" "$CONFIG_DIR/.blocked/$s" 2>&1
    echo "    moved: $s"
  done
fi