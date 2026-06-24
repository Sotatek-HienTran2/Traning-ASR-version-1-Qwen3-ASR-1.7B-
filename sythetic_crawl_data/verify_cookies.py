#!/usr/bin/env python3
"""Verify cookies.txt có hợp lệ cho yt-dlp không."""
import sys
from pathlib import Path

def verify(cookies_path: str) -> int:
    p = Path(cookies_path)
    print(f"File: {p}")
    print(f"Exists: {p.exists()}")
    if not p.exists():
        print("[FAIL] File không tồn tại")
        return 1
    print(f"Size: {p.stat().st_size} bytes")

    # Đọc thử
    try:
        # Thử nhiều encoding (file Netscape thường là ASCII/UTF-8)
        for enc in ("utf-8", "ascii", "latin-1"):
            try:
                content = p.read_text(encoding=enc)
                print(f"Encoding OK: {enc}")
                break
            except UnicodeDecodeError:
                continue
        else:
            print("[FAIL] Không đọc được file (encoding lỗi)")
            return 1
    except Exception as e:
        print(f"[FAIL] Lỗi đọc: {e}")
        return 1

    lines = content.splitlines()
    print(f"Lines: {len(lines)}")

    # Phải có header
    if not lines or not lines[0].startswith("# Netscape HTTP Cookie File"):
        print(f"[WARN] Header không đúng. Dòng đầu: {lines[0] if lines else '(empty)'!r}")
        print("       File hợp lệ phải bắt đầu bằng: # Netscape HTTP Cookie File")
    else:
        print("[OK] Header Netscape đúng format")

    # Đếm cookie YouTube
    youtube_lines = [l for l in lines if "youtube.com" in l]
    google_lines = [l for l in lines if "google.com" in l]
    print(f"YouTube.com cookies: {len(youtube_lines)}")
    print(f"Google.com cookies:  {len(google_lines)}")

    # Cookies cần thiết cho YouTube login
    # Các cookie quan trọng: LOGIN_INFO, SID, HSID, SSID, APISID, SAPISID
    important = ["LOGIN_INFO", "SID", "HSID", "SSID", "APISID", "SAPISID", "VISITOR_INFO1_LIVE", "YSC", "PREF"]
    found_important = []
    for cookie_name in important:
        if any(cookie_name in line for line in lines):
            found_important.append(cookie_name)

    print(f"\nImportant cookies found: {len(found_important)}/{len(important)}")
    for c in found_important:
        print(f"  [+] {c}")

    if len(youtube_lines) == 0:
        print("\n[FAIL] KHÔNG CÓ cookie YouTube nào -> file rỗng hoặc sai nguồn")
        return 1

    if "LOGIN_INFO" not in str(found_important):
        print("\n[WARN] Không thấy LOGIN_INFO -> có thể CHƯA đăng nhập YouTube khi export")
        print("        Cookie không có LOGIN_INFO thường không đủ để bypass check")
        return 2

    print("\n[OK] File cookies.txt hợp lệ cho yt-dlp")
    return 0


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "./cookies.txt"
    sys.exit(verify(path))
