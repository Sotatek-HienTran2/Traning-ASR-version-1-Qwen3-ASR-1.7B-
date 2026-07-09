#!/bin/bash
# =============================================================================
# delete_bypass_routes.sh — Admin chạy 1 LẦN để xóa bypass host routes
# -----------------------------------------------------------------------------
# Vấn đề:
#   Sau khi add uidrange rule (xem setup_vpn_uidrange.sh), user-level traffic
#   đi main table. NHƯNG main table có các host routes bypass cụ thể
#   (142.250.0.0/15, 104.16.0.0/20, ...) qua enp3s0 — match TRƯỚC VPN routes
#   (0.0.0.0/1 + 128.0.0.0/1) → traffic YouTube VẪN ra IP thật.
#
# Fix: Admin chạy script này 1 LẦN để xóa các bypass host routes. Sau đó
# khi crawler chạy, VPN routes 0.0.0.0/1 + 128.0.0.0/1 sẽ match YouTube IPs.
#
# KHÔNG ảnh hưởng đến SSH:
#   - SSH session đang established, không cần lookup route mới.
#   - Khi SSH tạo connection MỚI tới IP ngoài (vd: github.com), vẫn dùng
#     default route qua enp3s0 (do host routes bypass đã xóa, default route
#     qua enp3s0 vẫn còn nhưng có metric 100 thấp hơn VPN → vẫn đi IP thật
#     cho ROOT user — root không match uidrange rule).
#
# CÁCH DÙNG:
#   sudo bash /home/hientran/sythetic_crawl_data/delete_bypass_routes.sh
#   sudo bash /home/hientran/sythetic_crawl_data/delete_bypass_routes.sh restore
# =============================================================================

set -euo pipefail

if [ "$EUID" -ne 0 ]; then
  echo "[ERROR] Cần chạy với sudo:"
  echo "    sudo bash $0 [restore]"
  exit 1
fi

ACTION="${1:-delete}"
BACKUP_FILE="/tmp/bypass-routes-backup.txt"

# Các bypass routes cần xóa (dựa trên IP_good.txt + scan thực tế từ router).
# Nếu router có thêm route khác, có thể edit list này.
TARGET_ROUTES=(
  "104.16.0.0/20"
  "118.70.190.141/32"
  "138.199.7.234/32"
  "142.250.0.0/15"
  "149.40.58.146/32"
  "156.146.51.129/32"
  "160.79.104.10/32"
  # Thêm các IP khác từ IP_good.txt nếu cần
)

cmd_delete() {
  echo "================================================================"
  echo "Bypass Host Routes Killer — fix VPN routing"
  echo "================================================================"
  echo ""
  echo "[STEP 1] Snapshot current bypass routes (để restore nếu cần)..."
  : > "$BACKUP_FILE"
  ip route show | grep "via 172.16.198.1 dev enp3s0" | grep -v "default via\|172.16.198.0/24" >> "$BACKUP_FILE"
  echo "  → Backup saved to $BACKUP_FILE"
  echo ""
  echo "[STEP 2] Delete bypass host routes..."
  local deleted=0
  local failed=0
  for dst in "${TARGET_ROUTES[@]}"; do
    # Tìm via/dev chính xác
    line=$(ip route show | grep "^${dst} " | grep "via 172.16.198.1 dev enp3s0" | head -1 || true)
    if [ -z "$line" ]; then
      echo "  [SKIP] $dst (không tìm thấy hoặc không qua enp3s0)"
      continue
    fi
    if ip route del $line 2>/dev/null; then
      echo "  [DEL]  $line"
      deleted=$((deleted + 1))
    else
      echo "  [FAIL] $line"
      failed=$((failed + 1))
    fi
  done
  echo ""
  echo "  → Deleted: $deleted, Failed: $failed"
  echo ""
  echo "[STEP 3] Verify YouTube IP giờ đi qua tun0..."
  YIP="142.250.198.142"
  route_result=$(ip route get "$YIP" 2>/dev/null || true)
  echo "  ip route get $YIP:"
  echo "    $route_result"
  if echo "$route_result" | grep -q "tun0"; then
    echo "  ✓ YouTube IP đi qua tun0 (VPN) — fix thành công!"
  elif echo "$route_result" | grep -q "enp3s0"; then
    echo "  ✗ YouTube IP VẪN đi qua enp3s0 (IP thật)"
    echo "    → Có thể có route bypass khác chưa được list. Check 'ip route show'"
  else
    echo "  ? Kết quả không xác định. Check 'ip route get $YIP'"
  fi
  echo ""
  echo "================================================================"
  echo "DONE"
  echo "================================================================"
  echo ""
  echo "Test:"
  echo "    su - ${SUDO_USER:-hientran} -c 'curl https://ifconfig.me'"
  echo "    # phải ra IP VPN"
  echo ""
  echo "Để restore (nếu cần):"
  echo "    sudo bash $0 restore"
}

cmd_restore() {
  echo "================================================================"
  echo "Restore bypass routes from backup"
  echo "================================================================"
  if [ ! -f "$BACKUP_FILE" ]; then
    echo "[ERROR] Backup file not found: $BACKUP_FILE"
    exit 1
  fi
  echo ""
  local restored=0
  local failed=0
  while IFS= read -r line; do
    [ -z "$line" ] && continue
    if ip route add $line 2>/dev/null; then
      echo "  [ADD]  $line"
      restored=$((restored + 1))
    else
      echo "  [FAIL] $line"
      failed=$((failed + 1))
    fi
  done < "$BACKUP_FILE"
  echo ""
  echo "  → Restored: $restored, Failed: $failed"
  echo ""
  echo "================================================================"
  echo "DONE"
  echo "================================================================"
}

case "$ACTION" in
  delete|"")
    cmd_delete
    ;;
  restore)
    cmd_restore
    ;;
  *)
    echo "Usage: sudo bash $0 [delete|restore]"
    exit 1
    ;;
esac