#!/usr/bin/env python3
"""
Test từng server ProtonVPN xem có bị YouTube block không.
- Connect qua từng .ovpn file
- Check IP thật (đã đổi chưa)
- Test gọi YouTube xem có 200/429/403/Sign in
- Log kết quả ra file

Không xóa file - chỉ thống kê.
"""

import sys
import time
import subprocess
from pathlib import Path

PROTON_CONFIG_DIR = Path("/home/hientran/sythetic_crawl_data/proton_config")
PROTON_AUTH_FILE = PROTON_CONFIG_DIR / "auth.txt"
OPENVPN_BIN = "/usr/sbin/openvpn"
OPENVPN_LOG = "/tmp/openvpn-proton.log"
IP_CHECK_URL = "https://ifconfig.me"

# YouTube test endpoint - dùng RSS feed (ít bị block nhất)
TEST_URL = "https://www.youtube.com/feeds/videos.xml?channel_id=UCBJycsmduvYEL83R_U4JriQ"

# Cũng test YouTube homepage để check "Sign in to confirm"
TEST_HOMEPAGE = "https://www.youtube.com/"


def disconnect_openvpn():
    """Kill openvpn hiện tại."""
    subprocess.run(["pkill", "-f", "openvpn.*proton_config"], capture_output=True)
    time.sleep(3)


def connect_ovpn(ovpn_path: Path, timeout: int = 40) -> tuple:
    """Connect 1 .ovpn file. Returns (success, log_path)"""
    disconnect_openvpn()
    time.sleep(2)

    # Strip DNS update script (giống vpn_rotator.py)
    original = ovpn_path.read_text(encoding="utf-8", errors="replace")
    cleaned_lines = []
    for line in original.splitlines():
        stripped = line.strip()
        if (stripped.startswith("up ") or stripped.startswith("down ")) and "/etc/openvpn/update-resolv-conf" in stripped:
            cleaned_lines.append(f"# {line}")
        else:
            cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines) + "\n"

    temp_path = PROTON_CONFIG_DIR / f".{ovpn_path.stem}.no-dns.ovpn"
    temp_path.write_text(cleaned, encoding="utf-8")

    pid_file = Path("/tmp") / f"test_openvpn.pid.{ovpn_path.stem}"
    try:
        pid_file.unlink(missing_ok=True)
    except Exception:
        pass

    log_path = f"/tmp/test_openvpn_{ovpn_path.stem}.log"

    try:
        proc = subprocess.Popen(
            [
                OPENVPN_BIN,
                "--config", str(temp_path),
                "--auth-user-pass", str(PROTON_AUTH_FILE),
                "--auth-retry", "nointeract",
                "--auth-nocache",
                "--daemon",
                "--log", log_path,
                "--writepid", str(pid_file),
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        proc.wait()
    except Exception as e:
        return False, log_path

    # Đợi tunnel lên
    start = time.time()
    while time.time() - start < timeout:
        r = subprocess.run(
            ["ip", "link", "show", "tun0"],
            capture_output=True, text=True, timeout=3,
        )
        if r.returncode == 0 and "state " in r.stdout:
            state_part = r.stdout.split("state ")[1].split("\n")[0]
            if "DOWN" not in state_part:
                return True, log_path
        time.sleep(1)
    return False, log_path


def get_current_ip() -> str:
    """Lấy IP hiện tại qua tunnel."""
    try:
        r = subprocess.run(
            ["curl", "-s", "--max-time", "8", IP_CHECK_URL],
            capture_output=True, text=True, timeout=10,
        )
        return r.stdout.strip() if r.returncode == 0 else "?"
    except Exception:
        return "?"


def test_youtube_rss() -> tuple:
    """
    Test gọi YouTube RSS feed qua tunnel.
    Returns: (status, http_code, response_size)
    """
    try:
        r = subprocess.run(
            [
                "curl", "-s", "-o", "/tmp/youtube_test.xml",
                "-w", "%{http_code}",
                "--max-time", "15",
                TEST_URL,
            ],
            capture_output=True, text=True, timeout=20,
        )
        http_code = r.stdout.strip()
        size = Path("/tmp/youtube_test.xml").stat().st_size if Path("/tmp/youtube_test.xml").exists() else 0
        return ("ok", http_code, size)
    except subprocess.TimeoutExpired:
        return ("timeout", "?", 0)
    except Exception as e:
        return (f"err:{e}", "?", 0)


def test_youtube_homepage() -> str:
    """Test gọi YouTube homepage - nếu trả về 'Sign in' hoặc ít nội dung thì bị chặn."""
    try:
        r = subprocess.run(
            [
                "curl", "-s", "-o", "/tmp/youtube_home.html",
                "-w", "%{http_code}",
                "--max-time", "15",
                TEST_HOMEPAGE,
            ],
            capture_output=True, text=True, timeout=20,
        )
        http_code = r.stdout.strip()
        if Path("/tmp/youtube_home.html").exists():
            content = Path("/tmp/youtube_home.html").read_text(errors="replace")
            if "Sign in to confirm you're not a bot" in content:
                return f"BLOCKED_SIGNIN({http_code})"
            if len(content) < 5000:
                return f"SHORT({http_code}, {len(content)}B)"
            return f"OK({http_code}, {len(content)//1024}KB)"
        return f"NOFILE({http_code})"
    except subprocess.TimeoutExpired:
        return "TIMEOUT"
    except Exception as e:
        return f"ERR:{e}"


def main():
    # === Gioống vpn_rotator.py: filter file ẩn ===
    ovpn_files = sorted(
        p for p in PROTON_CONFIG_DIR.glob("*.ovpn") if not p.name.startswith(".")
    )
    if not ovpn_files:
        print("Khong tim thay file .ovpn")
        return 1

    # Limit số servers (override bằng env var TEST_VPN_LIMIT=0 = full)
    import os
    limit = int(os.environ.get("TEST_VPN_LIMIT", "5"))
    if limit > 0:
        ovpn_files = ovpn_files[:limit]
        print(f"Limit test {limit} servers (set TEST_VPN_LIMIT=0 de test full)")
    print(f"Test {len(ovpn_files)} servers ProtonVPN\n")
    print(f"{'Server':<40} {'IP':<16} {'RSS':<25} {'Homepage':<25}")
    print("-" * 110)

    print(f"\n{'='*110}")
    print(f"  Server          Status         IP            RSS                  Homepage")
    print(f"{'='*110}")

    results = []
    for ovpn in ovpn_files:
        name = ovpn.name
        print(f"\n  Testing {name}...", end=" ", flush=True)

        ok, log_path = connect_ovpn(ovpn, timeout=60)
        if not ok:
            # Đọc 5 dòng cuối log openvpn để debug
            log_tail = ""
            try:
                log_tail = Path(log_path).read_text(errors="replace").strip().splitlines()[-3:]
                log_tail = " | ".join(log_tail)[:80]
            except Exception:
                log_tail = "no-log"
            print(f"[TUNNEL_FAIL] log: {log_tail}")
            results.append((name, "?", "tunnel_fail", "tunnel_fail", "tunnel_fail"))
            continue

        ip = get_current_ip()
        rss_status, http_code, size = test_youtube_rss()
        homepage_status = test_youtube_homepage()

        # Phan biet 3 trang thai:
        # - youtube_blocked: tunnel len nhung YouTube chan (RSS fail / BLOCKED_SIGNIN)
        # - ok: tunnel len + YouTube OK
        is_youtube_blocked = (
            rss_status != "ok"
            or "BLOCKED" in homepage_status
            or homepage_status == "TIMEOUT"
        )

        status_label = "YOUTUBE_BLOCKED" if is_youtube_blocked else "OK"
        marker = "❌" if is_youtube_blocked else "✅"
        print(f"\n  {marker} [{status_label}] {name}")
        print(f"     IP={ip}, RSS={rss_status}({http_code},{size}B), Homepage={homepage_status}")
        results.append((name, ip, status_label, rss_status, homepage_status))

        # Cleanup temp file
        temp_path = PROTON_CONFIG_DIR / f".{ovpn.stem}.no-dns.ovpn"
        try:
            temp_path.unlink()
        except Exception:
            pass

    # Cleanup cuối
    disconnect_openvpn()

    # In tổng kết
    print(f"\n{'='*110}")
    print(f"TONG KET: {len(results)} servers test")
    ok_count = sum(1 for r in results if r[2] == "OK")
    tunnel_fail = sum(1 for r in results if r[2] == "tunnel_fail")
    yt_blocked = sum(1 for r in results if r[2] == "YOUTUBE_BLOCKED")
    print(f"  OK (YouTube accessible):       {ok_count}")
    print(f"  TUNNEL_FAIL (openvpn khong len): {tunnel_fail}")
    print(f"  YOUTUBE_BLOCKED (YouTube chan):  {yt_blocked}")

    if tunnel_fail > 0:
        print(f"\n=== TUNNEL_FAIL ({tunnel_fail}) - co the do config/auth/server ProtonVPN ===")
        for name, ip, status, rss, home in results:
            if status == "tunnel_fail":
                print(f"  {name}")

    if yt_blocked:
        print(f"\n=== YOUTUBE_BLOCKED ({len(yt_blocked)}) - can xoa file .ovpn ===")
        for name, ip, status, rss, home in results:
            if status == "YOUTUBE_BLOCKED":
                print(f"  {name:<40} IP={ip:<16} RSS={rss:<10} Home={home}")

    print("\n(Chi thong ke - KHONG xoa file. Neu muon xoa, goi lenh rieng.)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[INTERRUPTED]")
        disconnect_openvpn()
        sys.exit(130)
