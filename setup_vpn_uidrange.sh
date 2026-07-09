#!/bin/bash
# =============================================================================
# setup_vpn_uidrange.sh — Helper để ADMIN chạy 1 LẦN, fix policy-based routing
# -----------------------------------------------------------------------------
# Vấn đề:
#   Server có sẵn rule `from 172.16.198.60 lookup 100` (priority 32760-32765)
#   ép MỌI traffic từ IP thật đi qua enp3s0 (IP thật) → bypass hoàn toàn VPN.
#
# Fix Cách 1 (khuyến nghị):
#   Thêm rule `uidrange <UID>-<UID> priority 100 lookup main`
#   → packet từ UID hiện tại match rule uidrange TRƯỚC (priority 100 < 32760)
#   → đi main table → dùng `0.0.0.0/1 + 128.0.0.0/1 via tun0` → qua VPN ✓
#   → SSH (root, UID 0) không bị ảnh hưởng vì rule uidrange chỉ match UID user.
#
# Cách dùng:
#   sudo bash setup_vpn_uidrange.sh add     # add rule
#   sudo bash setup_vpn_uidrange.sh remove  # remove rule (rollback)
#   sudo bash setup_vpn_uidrange.sh check   # check status
#   sudo bash setup_vpn_uidrange.sh help    # in hướng dẫn
#
# KHÔNG cần sudo cho check.
# =============================================================================

set -euo pipefail

ACTION="${1:-check}"
USER_UID="${SUDO_UID:-$(id -u)}"  # UID của user gốc (khi chạy qua sudo)
USER_NAME="${SUDO_USER:-$(whoami)}"

print_header() {
  echo ""
  echo "================================================================"
  echo "$1"
  echo "================================================================"
}

cmd_check() {
  print_header "Check policy-based routing rules"
  echo "Current UID: $USER_UID (user: $USER_NAME)"
  echo ""
  echo "=== Current ip rule show ==="
  ip rule show
  echo ""

  # Check rule uidrange
  if ip rule show 2>/dev/null | grep -q "uidrange ${USER_UID}-${USER_UID}.*lookup main"; then
    echo "✓ Rule 'uidrange ${USER_UID}-${USER_UID} priority 100 lookup main' ĐÃ CÓ."
    echo "  → Packet từ UID ${USER_UID} sẽ đi main table → có thể qua VPN."
    return 0
  else
    echo "✗ Rule 'uidrange ${USER_UID}-${USER_UID} priority 100 lookup main' CHƯA CÓ."
    echo "  → Packet từ UID ${USER_UID} sẽ bị rule 'from 172.16.198.60' bắt → bypass VPN."
    echo ""
    echo "FIX: chạy 'sudo bash $0 add' để add rule."
    return 1
  fi
}

cmd_add() {
  if [ "$EUID" -ne 0 ]; then
    echo "[ERROR] Cần chạy với sudo: sudo bash $0 add"
    exit 1
  fi

  print_header "Add uidrange rule (Cách 1 — khuyến nghị)"
  echo "Target UID: $USER_UID (user: $USER_NAME)"
  echo ""

  # Check đã có chưa
  if ip rule show 2>/dev/null | grep -q "uidrange ${USER_UID}-${USER_UID}.*lookup main"; then
    echo "[WARN] Rule đã tồn tại. Skip."
    cmd_check
    exit 0
  fi

  echo "[STEP 1/3] Add rule: ip rule add uidrange ${USER_UID}-${USER_UID} priority 100 lookup main"
  ip rule add uidrange "${USER_UID}-${USER_UID}" priority 100 lookup main
  echo "  → Done."

  echo ""
  echo "[STEP 2/3] Verify"
  if ip rule show 2>/dev/null | grep -q "uidrange ${USER_UID}-${USER_UID}.*lookup main"; then
    echo "  ✓ Rule đã có trong ip rule show."
  else
    echo "  ✗ KHÔNG thấy rule. Lỗi."
    exit 1
  fi

  echo ""
  echo "[STEP 3/3] Setcap cho /bin/ip (cần cho vpn_rotator_v4 xóa bypass host routes)"
  if getcap /bin/ip 2>/dev/null | grep -q "cap_net_admin"; then
    echo "  ✓ /bin/ip đã có cap_net_admin. Skip."
  else
    echo "  → setcap cap_net_admin+ep /bin/ip"
    setcap cap_net_admin+ep /bin/ip
    if getcap /bin/ip 2>/dev/null | grep -q "cap_net_admin"; then
      echo "  ✓ Done."
    else
      echo "  ✗ setcap FAIL. Chạy thủ công: sudo setcap cap_net_admin+ep /bin/ip"
    fi
  fi

  echo ""
  print_header "DONE!"
  echo "Để test:"
  echo "    su - ${USER_NAME} -c 'curl https://ifconfig.me'"
  echo "    # phải ra IP VPN (không phải 172.16.198.60)"
  echo ""
  echo "Để rollback:"
  echo "    sudo bash $0 remove"
}

cmd_remove() {
  if [ "$EUID" -ne 0 ]; then
    echo "[ERROR] Cần chạy với sudo: sudo bash $0 remove"
    exit 1
  fi

  print_header "Remove uidrange rule (rollback)"
  echo "Target UID: $USER_UID (user: $USER_NAME)"
  echo ""

  if ! ip rule show 2>/dev/null | grep -q "uidrange ${USER_UID}-${USER_UID}.*lookup main"; then
    echo "[WARN] Rule CHƯA có. Skip."
    exit 0
  fi

  echo "[STEP 1/2] Remove rule: ip rule del uidrange ${USER_UID}-${USER_UID} priority 100"
  if ip rule del uidrange "${USER_UID}-${USER_UID}" priority 100 lookup main 2>&1; then
    echo "  → Done."
  else
    echo "  ✗ FAIL. Thử với priority khác:"
    PRIORITY=$(ip rule show 2>/dev/null | grep "uidrange ${USER_UID}-${USER_UID}" | grep -oP 'priority \K\d+' | head -1 || echo "")
    if [ -n "$PRIORITY" ]; then
      echo "  → Retry: ip rule del uidrange ${USER_UID}-${USER_UID} priority ${PRIORITY}"
      ip rule del uidrange "${USER_UID}-${USER_UID}" priority "$PRIORITY" lookup main 2>&1 || echo "  ✗ Vẫn fail."
    fi
  fi

  echo ""
  echo "[STEP 2/2] Verify"
  if ip rule show 2>/dev/null | grep -q "uidrange ${USER_UID}-${USER_UID}.*lookup main"; then
    echo "  ✗ Rule VẪN CÒN. Kiểm tra thủ công: ip rule show"
  else
    echo "  ✓ Rule đã được xóa."
  fi

  echo ""
  echo "[OPTIONAL] unsetcap cho /bin/ip:"
  echo "    sudo setcap -r /bin/ip"
  echo "  (rollback về trạng thái ban đầu, không cần thiết)"

  echo ""
  print_header "DONE (rollback)"
}

cmd_help() {
  cat <<EOF
Cách dùng:
  sudo bash $0 add     # add rule 'uidrange <uid>-<uid> priority 100 lookup main'
                       # + setcap cap_net_admin+ep /bin/ip
                       # (CHẠY 1 LẦN DUY NHẤT)

  sudo bash $0 remove  # remove rule (rollback nếu cần)

  bash $0 check        # check status (không cần sudo)

  bash $0 help         # in hướng dẫn này

Tại sao cần fix:
  Server có sẵn 6 rules 'from 172.16.198.60 lookup 100' (priority 32760-32765).
  Rule này ép traffic từ IP thật 172.16.198.60 đi qua enp3s0 (IP thật) →
  bypass hoàn toàn VPN tun0. Kết quả: YouTube luôn thấy IP thật → bị block
  "Sign in to confirm you're not a bot" liên tục.

Cách fix hoạt động:
  Rule uidrange priority 100 sẽ MATCH TRƯỚC rule 'from 172.16.198.60'
  (priority 32760) → packet từ UID <uid> đi main table → có
  '0.0.0.0/1 + 128.0.0.0/1 via tun0' → qua VPN.

  SSH (root, UID 0) không bị ảnh hưởng vì rule uidrange chỉ match UID user.
EOF
}

case "$ACTION" in
  add)
    cmd_add
    ;;
  remove)
    cmd_remove
    ;;
  check)
    cmd_check || exit 1
    ;;
  help|--help|-h)
    cmd_help
    ;;
  *)
    echo "[ERROR] Unknown action: $ACTION"
    cmd_help
    exit 1
    ;;
esac