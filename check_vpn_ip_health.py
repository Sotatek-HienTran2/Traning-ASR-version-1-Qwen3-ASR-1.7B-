
#!/usr/bin/env python3
"""
check_vpn_ip_health.py — Check if current VPN IP is "clean" cho YouTube crawl.

Check 3 tiêu chí:
  1. IP thuộc datacenter range (ProtonVPN/VPN/hosting) → biết ngay là IP fake
  2. HTTP 200 + có subs JSON (không phải consent page)
  3. Test với player_client tv_embedded + tv (3 client phổ biến nhất)

Output:
  - IP_OK: IP hiện tại sạch, có thể dùng để crawl
  - IP_DIRTY: IP bị YouTube flag, nên rotate sang IP khác ngay

Usage:
  python check_vpn_ip_health.py
  python check_vpn_ip_health.py --target-video 94wV0n__SP8
"""

import sys
import json
import time
import subprocess
import urllib.request
import urllib.error
from pathlib import Path

# Default test video — Vietsub (nếu bị flag sẽ rất rõ)
DEFAULT_TEST_VIDEO = "94wV0n__SP8"  # 5 Bi An Co That O Trung Quoc

# IP ranges của các datacenter / VPN phổ biến (heuristic check)
KNOWN_DATACENTER_RANGES = {
    "ProtonVPN (AS212238)": [
        "138.199.",  # ProtonVPN US
        "146.70.",   # ProtonVPN US
        "149.40.",   # ProtonVPN US
        "156.146.",  # ProtonVPN
        "185.183.",  # ProtonVPN EU
        "89.187.",   # ProtonVPN EU
        "89.238.",   # ProtonVPN
    ],
    "Datacenter / hosting (heuristic)": [
        "45.",       # Often hosting
        "104.",      # Cloudflare
        "138.199.",  # Often ProtonVPN
        "146.70.",   # Often ProtonVPN
    ],
}


def get_current_ip(timeout=5) -> str:
    """Lấy IP hiện tại qua default route (đã qua VPN nếu tunnel up)."""
    try:
        result = subprocess.run(
            ["curl", "-s", "--max-time", str(timeout), "https://ifconfig.me"],
            capture_output=True, text=True, timeout=timeout + 2,
        )
        return result.stdout.strip() if result.returncode == 0 else ""
    except Exception as e:
        return f"ERROR: {e}"


def check_ip_datacenter(ip: str) -> tuple[bool, str]:
    """
    Check IP có thuộc datacenter/VPN range không.
    Returns: (is_datacenter, provider_name)
    """
    if not ip:
        return False, "unknown"
    for provider, prefixes in KNOWN_DATACENTER_RANGES.items():
        for prefix in prefixes:
            if ip.startswith(prefix):
                return True, provider
    return False, "residential"


def test_youtube_http_200(video_id: str, timeout=8) -> tuple[int, str]:
    """
    Test watch page trả về HTTP 200 và có watch page thật (không phải consent).
    Returns: (http_code, response_excerpt)
    """
    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            code = resp.getcode()
            body = resp.read(2000).decode("utf-8", errors="replace")
            return code, body[:500]
    except urllib.error.HTTPError as e:
        return e.code, f"HTTPError: {e.reason}"
    except Exception as e:
        return 0, f"ERROR: {e}"


def test_ytdlp_extract(video_id: str, timeout=15) -> tuple[bool, str]:
    """
    Test yt-dlp extract_info có thành công không.
    Returns: (success, info_summary)
    """
    try:
        from yt_dlp import YoutubeDL
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extractor_args": {"youtube": {"player_client": ["tv_embedded"]}},
            "socket_timeout": timeout,
        }
        with YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(
                f"https://www.youtube.com/watch?v={video_id}",
                download=False,
            )
            if info is None:
                return False, "extract_info returned None"
            title = info.get("title", "(no title)")
            subs = info.get("subtitles", {}) or {}
            auto = info.get("automatic_captions", {}) or {}
            return True, f"title='{title[:60]}', subs={list(subs.keys())[:3]}, auto={list(auto.keys())[:3]}"
    except Exception as e:
        return False, f"ERROR: {str(e)[:200]}"


def main():
    target_video = DEFAULT_TEST_VIDEO
    if len(sys.argv) > 1:
        target_video = sys.argv[1]
    elif "--target-video" in sys.argv:
        idx = sys.argv.index("--target-video")
        if idx + 1 < len(sys.argv):
            target_video = sys.argv[idx + 1]

    print("=" * 70)
    print(f"VPN IP Health Check — target={target_video}")
    print("=" * 70)

    # 1. Get current IP
    print("\n[1/4] Current IP via default route...")
    ip = get_current_ip()
    print(f"  IP: {ip or '(empty)'}")
    if not ip or ip.startswith("ERROR"):
        print("  ✗ Cannot get IP. Check VPN tunnel status.")
        sys.exit(2)

    # 2. Check datacenter
    print("\n[2/4] Check if IP is datacenter/VPN...")
    is_dc, provider = check_ip_datacenter(ip)
    if is_dc:
        print(f"  → IP is DATACENTER (provider: {provider})")
        print(f"  → YouTube SẼ detect là VPN IP — có khả năng bị flag/limit")
    else:
        print(f"  → IP appears RESIDENTIAL ({provider})")

    # 3. HTTP 200 check
    print(f"\n[3/4] Test HTTP 200 on watch page...")
    code, body = test_youtube_http_200(target_video)
    print(f"  HTTP code: {code}")
    if code == 200:
        # Check content thật
        if "consent" in body.lower() or "verify you" in body.lower():
            print(f"  ✗ Trang CONSENT/VERIFY (bị YouTube flag)")
            print(f"  body excerpt: {body[:200]}")
        elif '"title"' in body:
            print(f"  ✓ Trang watch thật (có title JSON)")
        else:
            print(f"  ? Trang 200 nhưng content lạ")
            print(f"  body excerpt: {body[:200]}")
    else:
        print(f"  ✗ HTTP {code} — block/error")

    # 4. yt-dlp extract test
    print(f"\n[4/4] Test yt-dlp extract (player_client=tv_embedded)...")
    ok, summary = test_ytdlp_extract(target_video)
    if ok:
        print(f"  ✓ Extract OK: {summary}")
    else:
        print(f"  ✗ Extract FAILED: {summary}")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    if ok and code == 200 and not is_dc:
        print(f"  ✓ IP_OK: {ip} — sạch, có thể dùng crawl")
        sys.exit(0)
    elif ok and code == 200 and is_dc:
        print(f"  ⚠ IP_RISKY: {ip} là datacenter nhưng hiện tại YouTube vẫn cho qua.")
        print(f"     Có thể bị flag sau 5-30 request. Nên rotate IP để an toàn.")
        sys.exit(1)
    else:
        print(f"  ✗ IP_DIRTY: {ip} — bị YouTube flag. NÊN ROTATE NGAY!")
        sys.exit(2)


if __name__ == "__main__":
    main()