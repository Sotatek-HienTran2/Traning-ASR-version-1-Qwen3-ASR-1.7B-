#!/usr/bin/env python3
"""
Test tốc độ tải AUDIO từ YouTube qua từng IP fake (ProtonVPN).

Mục đích:
- Đo throughput (MB/s) khi tải audio qua từng VPN server
- Đo thời gian setup tunnel + yt-dlp handshake
- Phát hiện IP nào bị YouTube block (sign in / 429 / captcha)
- Phát hiện IP nào download tốt, IP nào chậm

CÁCH CHẠY:
    # Test nhanh 5 servers đầu (mặc định)
    python test_vpn_audio_speed.py

    # Test TẤT CẢ servers trong proton_config
    TEST_VPN_LIMIT=0 python test_vpn_audio_speed.py

    # Test 1 server cụ thể
    python test_vpn_audio_speed.py --ovpn us-free-5.protonvpn.udp.ovpn

    # Test với audio URL khác
    python test_vpn_audio_speed.py --audio-url "https://www.youtube.com/watch?v=XXXXX"

Output:
    /home/hientran/sythetic_crawl_data/vpn_audio_speed_test_<timestamp>/
        ├── results.json        # raw data từng server
        ├── results.csv         # dạng bảng để mở Excel
        ├── summary.txt         # ranking tốc độ + đề xuất
        └── download_speed_test/  # audio tải về (cleanup sau khi xong)
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import shutil
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

# ================= CONFIG =================
PROTON_CONFIG_DIR = Path("/home/hientran/sythetic_crawl_data/proton_config")
PROTON_AUTH_FILE = PROTON_CONFIG_DIR / "auth.txt"
COOKIES_FILE = Path("/home/hientran/sythetic_crawl_data/cookies.txt")
OPENVPN_BIN = "/usr/sbin/openvpn"
IP_CHECK_URL = "https://ifconfig.me"
IP_CHECK_FALLBACK = "https://api.ipify.org"

# Audio test mặc định: video ~5 phút, audio ~5MB (đo speed đủ chính xác)
# "Never Gonna Give You Up" Rick Astley - 3:32, ~5MB audio ở 128kbps
DEFAULT_AUDIO_URL = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
# Format: ưu tiên audio-only, fallback video. Dùng "wa*" để tương thích web client.
DEFAULT_AUDIO_FORMAT = "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best"
# Thời gian test throughput tối đa cho mỗi IP (giây)
SPEED_TEST_DURATION_S = 15


@dataclass
class SpeedTestResult:
    """Kết quả test 1 server."""
    server_file: str
    country: str = "?"        # us / nl / jp / ca / sg ...
    server_num: str = "?"     # 5 / 14 / 165 ...
    tunnel_ok: bool = False
    tunnel_time_s: float = 0.0
    public_ip: str = "?"
    download_ok: bool = False
    download_status: str = "?"   # ok / blocked / timeout / error
    download_http_code: str = "?"
    download_time_s: float = 0.0
    download_bytes: int = 0
    download_speed_mbps: float = 0.0  # MB/s
    download_speed_kbps: float = 0.0  # kB/s
    error_msg: str = ""
    log_path: str = ""


def parse_ovpn_name(ovpn_name: str) -> tuple[str, str]:
    """Parse tên file .ovpn -> (country, server_num).
    vd: 'us-free-5.protonvpn.udp.ovpn' -> ('us', '5')
        'nl-free-165.protonvpn.udp.ovpn' -> ('nl', '165')
    """
    stem = ovpn_name.replace(".protonvpn.udp.ovpn", "")
    parts = stem.split("-")
    country = parts[0] if parts else "?"
    server_num = parts[-1] if len(parts) >= 3 else "?"
    return country, server_num


def disconnect_openvpn():
    """Kill openvpn hiện tại (an toàn cho multi-instance)."""
    # Chỉ kill openvpn của user hiện tại, KHÔNG pkill all
    subprocess.run(
        ["pkill", "-9", "-u", str(os.getuid()), "-f", "openvpn.*proton_config"],
        capture_output=True,
    )
    time.sleep(2)


def prepare_no_dns_config(ovpn_path: Path) -> Path:
    """Tạo temp .ovpn file đã strip DNS update script.
    Tránh lỗi 'Interactive authentication required' khi user không có root.
    """
    original = ovpn_path.read_text(encoding="utf-8", errors="replace")
    cleaned_lines = []
    for line in original.splitlines():
        stripped = line.strip()
        if (stripped.startswith("up ") or stripped.startswith("down ")) and \
           "/etc/openvpn/update-resolv-conf" in stripped:
            cleaned_lines.append(f"# {line}")
        else:
            cleaned_lines.append(line)
    cleaned = "\n".join(cleaned_lines) + "\n"

    temp_path = ovpn_path.parent / f".{ovpn_path.stem}.no-dns.ovpn"
    temp_path.write_text(cleaned, encoding="utf-8")
    return temp_path


def connect_ovpn(ovpn_path: Path, instance_id: str, timeout: int = 45) -> tuple[bool, float, str]:
    """
    Connect 1 .ovpn file.
    Returns: (success, setup_time_seconds, log_path)
    """
    start = time.time()
    disconnect_openvpn()
    time.sleep(1)

    prepared_config = prepare_no_dns_config(ovpn_path)

    log_path = f"/tmp/test_vpn_audio_{instance_id}.log"
    pid_file = Path(f"/tmp/test_vpn_audio_{instance_id}.pid")
    try:
        pid_file.unlink(missing_ok=True)
    except Exception:
        pass

    try:
        proc = subprocess.Popen(
            [
                OPENVPN_BIN,
                "--config", str(prepared_config),
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
        proc.wait(timeout=5)
    except Exception as e:
        return False, time.time() - start, log_path

    # Đợi tunnel lên
    while time.time() - start < timeout:
        try:
            r = subprocess.run(
                ["ip", "link", "show", "tun0"],
                capture_output=True, text=True, timeout=3,
            )
            if r.returncode == 0 and "state " in r.stdout:
                state_part = r.stdout.split("state ")[1].split("\n")[0]
                if "DOWN" not in state_part:
                    return True, time.time() - start, log_path
        except Exception:
            pass
        time.sleep(1)

    return False, time.time() - start, log_path


def get_current_ip() -> str:
    """Lấy IP public hiện tại qua tunnel (fallback 2 endpoint)."""
    for url in [IP_CHECK_URL, IP_CHECK_FALLBACK]:
        try:
            r = subprocess.run(
                ["curl", "-s", "--max-time", "8", url],
                capture_output=True, text=True, timeout=10,
            )
            if r.returncode == 0 and r.stdout.strip():
                return r.stdout.strip()
        except Exception:
            continue
    return "?"


def get_audio_direct_url(url: str, proxy_url: Optional[str] = None) -> tuple[str, str, str]:
    """
    Dùng yt-dlp extract_info để lấy direct URL của audio.
    Returns: (direct_url, status, error_msg)
        - status: 'ok' / 'blocked' / 'error'
    """
    import yt_dlp

    ydl_opts = {
        "format": DEFAULT_AUDIO_FORMAT,
        "quiet": True,
        "no_warnings": True,
        "no_color": True,
        "noprogress": True,
        "socket_timeout": 30,
        "retries": 3,
        "skip_download": True,
    }
    if COOKIES_FILE.exists():
        ydl_opts["cookiefile"] = str(COOKIES_FILE)
    try:
        ydl_opts["js_runtimes"] = {"node": {}}
    except Exception:
        pass
    if proxy_url:
        ydl_opts["proxy"] = proxy_url

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            if not info:
                return "", "error", "extract_info returned None"
            direct = info.get("url")
            if not direct:
                return "", "error", "no direct url in info"
            return direct, "ok", ""
    except yt_dlp.utils.DownloadError as e:
        err_str = str(e)
        if any(s in err_str.lower() for s in ["sign in", "bot", "captcha"]):
            return "", "blocked", err_str[:200]
        if "429" in err_str or "too many requests" in err_str.lower():
            return "", "blocked", "429: " + err_str[:200]
        if "403" in err_str or "forbidden" in err_str.lower():
            return "", "blocked", "403: " + err_str[:200]
        return "", "error", err_str[:200]
    except Exception as e:
        return "", "error", f"{type(e).__name__}: {str(e)[:200]}"


def curl_speed_test(
    direct_url: str,
    output_path: Path,
    duration_s: int = SPEED_TEST_DURATION_S,
) -> tuple[str, float, int, float, float]:
    """
    Dùng curl tải file audio trong tối đa `duration_s` giây.
    Returns: (status, elapsed_seconds, bytes_downloaded, avg_speed_mbps, peak_speed_mbps)
        - status: 'ok' / 'timeout' / 'error'
        - avg_speed_mbps: tốc độ trung bình (MB/s) trong cả khoảng test
        - peak_speed_mbps: tốc độ đỉnh (MB/s) trong cửa sổ 2s

    Lấy speed trung bình bằng cách: chạy curl với -y (rate limit) rồi parse %{speed_download}
    từ curl progress, hoặc đơn giản là đo bytes/s tổng cộng.
    """
    start = time.time()
    # Touch file để tạo sẵn
    output_path.touch()

    try:
        proc = subprocess.Popen(
            [
                "curl",
                "-s",                          # silent
                "-L",                          # follow redirect
                "--max-time", str(duration_s + 5),  # hard cap
                "--connect-timeout", "10",
                "-o", str(output_path),
                "-w", "%{http_code}|%{size_download}|%{speed_download}|%{time_total}\n",
                direct_url,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        stdout, _ = proc.communicate(timeout=duration_s + 10)
        elapsed = time.time() - start

        try:
            out = stdout.decode(errors="replace").strip()
            parts = out.split("|")
            http_code = parts[0] if len(parts) > 0 else "?"
            size_dl = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
            speed_b = float(parts[2]) if len(parts) > 2 else 0.0  # bytes/s
            curl_time = float(parts[3]) if len(parts) > 3 else elapsed
        except Exception:
            http_code, size_dl, speed_b, curl_time = "?", 0, 0.0, elapsed

        # Nếu file trên disk > size_dl từ curl, dùng size trên disk (curl chưa flush)
        try:
            actual_size = output_path.stat().st_size
        except Exception:
            actual_size = 0
        bytes_dl = max(size_dl, actual_size)

        # Tính avg speed dùng thời gian thực
        actual_elapsed = min(elapsed, duration_s) if bytes_dl > 0 else elapsed
        if actual_elapsed <= 0:
            actual_elapsed = curl_time if curl_time > 0 else 0.001
        avg_mbps = bytes_dl / actual_elapsed / (1024 * 1024)
        # peak = curl instantaneous speed
        peak_mbps = speed_b / (1024 * 1024)

        if bytes_dl <= 0:
            return "error", elapsed, 0, 0.0, 0.0
        if http_code.startswith("4") or http_code.startswith("5"):
            return "blocked", elapsed, bytes_dl, avg_mbps, peak_mbps
        return "ok", elapsed, bytes_dl, avg_mbps, peak_mbps

    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        # Lấy bytes đã tải được trước khi timeout
        try:
            bytes_dl = output_path.stat().st_size
        except Exception:
            bytes_dl = 0
        # Nếu đạt >= duration_s thì coi như OK, tính speed trong khoảng thời gian đó
        if elapsed > 0 and bytes_dl > 0:
            avg_mbps = bytes_dl / min(elapsed, duration_s) / (1024 * 1024)
        else:
            avg_mbps = 0.0
        if bytes_dl > 0:
            return "ok", elapsed, bytes_dl, avg_mbps, 0.0
        return "timeout", elapsed, 0, 0.0, 0.0
    except Exception as e:
        return "error", time.time() - start, 0, 0.0, 0.0


def measure_avg_speed_over_window(
    direct_url: str,
    output_path: Path,
    duration_s: int = SPEED_TEST_DURATION_S,
    sample_every_s: float = 2.0,
) -> dict:
    """
    Đo tốc độ trung bình qua nhiều cửa sổ thời gian (1s, 3s, 5s, 10s, 15s).
    Trả về dict với các metric chi tiết để debug.
    """
    start = time.time()
    output_path.touch()

    proc = subprocess.Popen(
        [
            "curl", "-s", "-L",
            "--max-time", str(duration_s + 5),
            "--connect-timeout", "10",
            "-o", str(output_path),
            "-w", "%{http_code}|%{size_download}|%{speed_download}|%{time_total}\n",
            direct_url,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )

    samples = []  # [(t_elapsed, bytes_so_far), ...]
    try:
        while True:
            elapsed = time.time() - start
            if elapsed > duration_s + 5:
                break
            try:
                size = output_path.stat().st_size
            except Exception:
                size = 0
            samples.append((elapsed, size))
            if elapsed >= duration_s:
                break
            time.sleep(sample_every_s)
    finally:
        try:
            proc.communicate(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    total_elapsed = time.time() - start
    try:
        final_size = output_path.stat().st_size
    except Exception:
        final_size = 0

    # Tính avg speed qua các window
    def avg_in_window(window_s: float) -> float:
        if not samples or final_size <= 0:
            return 0.0
        # Lấy sample ở thời điểm gần window_s nhất
        target_t = window_s
        base = None
        for t, sz in samples:
            if t <= 0.5:
                base = sz
                break
        if base is None:
            base = 0
        # bytes trong window
        end_size = None
        for t, sz in samples:
            if t >= target_t:
                end_size = sz
                break
        if end_size is None:
            end_size = final_size
        actual_window = target_t
        if actual_window <= 0:
            return 0.0
        return max(0, end_size - base) / actual_window / (1024 * 1024)

    return {
        "total_elapsed": total_elapsed,
        "total_bytes": final_size,
        "avg_speed_1s_mbps": avg_in_window(1.0),
        "avg_speed_3s_mbps": avg_in_window(3.0),
        "avg_speed_5s_mbps": avg_in_window(5.0),
        "avg_speed_10s_mbps": avg_in_window(10.0),
        "avg_speed_15s_mbps": avg_in_window(15.0),
        "samples_count": len(samples),
    }


def download_audio(
    url: str,
    output_dir: Path,
    proxy_url: Optional[str] = None,
    timeout: int = 120,
) -> tuple[str, str, float, int, str]:
    """
    Download audio từ YouTube qua yt-dlp, đo tốc độ trung bình.
    Returns: (status, http_code, elapsed_seconds, bytes_downloaded, error_msg)
        - status: 'ok' / 'blocked' / 'timeout' / 'error'
        - http_code: HTTP code từ yt-dlp (vd: '200', '403', '429') hoặc '?'

    Lưu ý: SPEED_TEST_DURATION_S = 15s là window chuẩn để tính avg throughput.
    Hàm này vẫn cho phép tải tối đa `timeout` giây (default 120s như cũ) để có
    file hoàn chỉnh; tốc độ "avg 15s" sẽ được cap ở 15s khi tính ở ngoài.
    """
    import yt_dlp

    output_template = str(output_dir / "%(id)s.%(ext)s")
    ydl_opts = {
        "format": DEFAULT_AUDIO_FORMAT,
        "outtmpl": output_template,
        "quiet": True,
        "no_warnings": True,
        "no_color": True,
        "noprogress": True,
        "socket_timeout": 30,
        "retries": 3,
        "ignoreerrors": False,
    }

    # Thêm cookies nếu có (giống youtube_researcher_youtube_subs_multi_vpn.py)
    if COOKIES_FILE.exists():
        ydl_opts["cookiefile"] = str(COOKIES_FILE)

    # Bật JS runtime để bypass bot detection (giống production code)
    try:
        ydl_opts["js_runtimes"] = {"node": {}}
    except Exception:
        pass

    if proxy_url:
        ydl_opts["proxy"] = proxy_url

    start = time.time()
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            elapsed = time.time() - start

            if info is None:
                return "error", "?", elapsed, 0, "extract_info returned None"

            # Tính tổng bytes downloaded
            video_id = info.get("id", "unknown")
            total_bytes = 0
            for ext in [".m4a", ".webm", ".mp3", ".opus", ".wav", ".mp4", ".mkv"]:
                f = output_dir / f"{video_id}{ext}"
                if f.exists():
                    total_bytes += f.stat().st_size
                    break

            # Nếu không tìm được file, check theo filename yt-dlp prepare
            if total_bytes == 0:
                try:
                    filename = ydl.prepare_filename(info)
                    f = Path(filename)
                    if f.exists():
                        total_bytes = f.stat().st_size
                except Exception:
                    pass

            if total_bytes > 0:
                return "ok", "200", elapsed, total_bytes, ""
            else:
                return "error", "?", elapsed, 0, "no output file found"

    except yt_dlp.utils.DownloadError as e:
        elapsed = time.time() - start
        err_str = str(e)
        # Check các dấu hiệu bị YouTube block
        if any(s in err_str.lower() for s in ["sign in", "bot", "captcha"]):
            return "blocked", "?", elapsed, 0, err_str[:200]
        if "429" in err_str or "too many requests" in err_str.lower():
            return "blocked", "429", elapsed, 0, err_str[:200]
        if "403" in err_str or "forbidden" in err_str.lower():
            return "blocked", "403", elapsed, 0, err_str[:200]
        return "error", "?", elapsed, 0, err_str[:200]

    except subprocess.TimeoutExpired:
        return "timeout", "?", time.time() - start, 0, "subprocess timeout"
    except Exception as e:
        return "error", "?", time.time() - start, 0, f"{type(e).__name__}: {str(e)[:200]}"


def test_one_server(
    ovpn_path: Path,
    audio_url: str,
    test_dir: Path,
    instance_id: str,
) -> SpeedTestResult:
    """Test đầy đủ 1 server: tunnel up + get IP + download audio."""
    country, server_num = parse_ovpn_name(ovpn_path.name)
    result = SpeedTestResult(
        server_file=ovpn_path.name,
        country=country,
        server_num=server_num,
    )

    # === Bước 1: kết nối tunnel ===
    tunnel_ok, tunnel_time, log_path = connect_ovpn(ovpn_path, instance_id=instance_id)
    result.tunnel_ok = tunnel_ok
    result.tunnel_time_s = round(tunnel_time, 2)
    result.log_path = log_path

    if not tunnel_ok:
        # Đọc tail log openvpn
        try:
            tail = Path(log_path).read_text(errors="replace").strip().splitlines()[-3:]
            result.error_msg = "tunnel_fail: " + " | ".join(tail)[:200]
        except Exception:
            result.error_msg = "tunnel_fail: no log"
        return result

    # === Bước 2: lấy IP public ===
    result.public_ip = get_current_ip()

    # === Bước 3: download audio để đo tốc độ ===
    server_test_dir = test_dir / f"{country}_{server_num}_{Path(ovpn_path).stem}"
    server_test_dir.mkdir(parents=True, exist_ok=True)

    status, http_code, elapsed, bytes_dl, err = download_audio(
        url=audio_url,
        output_dir=server_test_dir,
        proxy_url=None,  # Khi dùng VPN tunnel thì KHÔNG set proxy (đi qua default route)
        timeout=120,
    )

    result.download_status = status
    result.download_http_code = http_code
    result.download_time_s = round(elapsed, 2)
    result.download_bytes = bytes_dl
    result.download_ok = (status == "ok" and bytes_dl > 0)
    if result.download_ok:
        # Tính tốc độ trung bình trong cửa sổ SPEED_TEST_DURATION_S (15s).
        # Nếu tải xong nhanh hơn 15s → dùng elapsed thật (file nhỏ nên OK).
        # Nếu tải lâu hơn 15s → cap ở 15s để có con số "avg 15s" công bằng giữa các IP.
        effective_window = min(elapsed, SPEED_TEST_DURATION_S)
        if effective_window <= 0:
            effective_window = 0.001
        result.download_speed_mbps = round(bytes_dl / effective_window / (1024 * 1024), 3)
        result.download_speed_kbps = round(bytes_dl / effective_window / 1024, 1)
    if err:
        result.error_msg = err

    # Cleanup file audio để tiết kiệm disk
    try:
        shutil.rmtree(server_test_dir, ignore_errors=True)
    except Exception:
        pass

    return result


def print_progress(idx: int, total: int, result: SpeedTestResult):
    """In 1 dòng progress cho 1 server."""
    flag = {
        True: "✅",
        False: "❌",
    }[result.download_ok]

    if not result.tunnel_ok:
        print(f"  [{idx:3d}/{total}] ❌ {result.server_file:<40}  TUNNEL_FAIL ({result.tunnel_time_s:.1f}s)")
        return

    if result.download_ok:
        print(
            f"  [{idx:3d}/{total}] {flag} {result.server_file:<40}  "
            f"IP={result.public_ip:<16}  "
            f"{result.download_bytes//1024}KB in {result.download_time_s:.1f}s  "
            f"= {result.download_speed_mbps:.2f} MB/s  "
            f"({result.download_speed_kbps:.0f} KB/s)"
        )
    else:
        print(
            f"  [{idx:3d}/{total}] {flag} {result.server_file:<40}  "
            f"IP={result.public_ip:<16}  "
            f"status={result.download_status}  {result.error_msg[:60]}"
        )


def save_results(results: list, output_dir: Path, audio_url: str, test_duration_s: float):
    """Lưu kết quả ra JSON, CSV, và summary text."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir.mkdir(parents=True, exist_ok=True)

    # === 1. JSON raw ===
    json_data = {
        "test_metadata": {
            "timestamp": datetime.now().isoformat(),
            "audio_url": audio_url,
            "total_servers_tested": len(results),
            "test_duration_seconds": round(test_duration_s, 1),
        },
        "results": [asdict(r) for r in results],
    }
    json_path = output_dir / f"results_{timestamp}.json"
    json_path.write_text(json.dumps(json_data, ensure_ascii=False, indent=2), encoding="utf-8")

    # === 2. CSV ===
    csv_path = output_dir / f"results_{timestamp}.csv"
    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(
            "server_file,country,server_num,tunnel_ok,tunnel_time_s,public_ip,"
            "download_ok,download_status,download_http_code,download_time_s,"
            "download_bytes,download_speed_mbps,download_speed_kbps,error_msg\n"
        )
        for r in results:
            f.write(
                f"{r.server_file},{r.country},{r.server_num},"
                f"{r.tunnel_ok},{r.tunnel_time_s},{r.public_ip},"
                f"{r.download_ok},{r.download_status},{r.download_http_code},"
                f"{r.download_time_s},{r.download_bytes},"
                f"{r.download_speed_mbps},{r.download_speed_kbps},"
                f"\"{r.error_msg.replace(chr(34), chr(39))}\"\n"
            )

    # === 3. Summary text ===
    ok_results = [r for r in results if r.download_ok]
    tunnel_fail = [r for r in results if not r.tunnel_ok]
    blocked = [r for r in results if r.tunnel_ok and r.download_status == "blocked"]
    errors = [r for r in results if r.tunnel_ok and r.download_status not in ("ok", "blocked")]

    summary_path = output_dir / f"summary_{timestamp}.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("=" * 90 + "\n")
        f.write("  VPN AUDIO SPEED TEST - SUMMARY\n")
        f.write("=" * 90 + "\n")
        f.write(f"Test time: {datetime.now().isoformat()}\n")
        f.write(f"Audio URL: {audio_url}\n")
        f.write(f"Duration:  {test_duration_s:.1f}s\n")
        f.write(f"Total servers: {len(results)}\n")
        f.write(f"  ✅ Tốc độ OK:        {len(ok_results)}\n")
        f.write(f"  ❌ Tunnel FAIL:      {len(tunnel_fail)}\n")
        f.write(f"  🚫 YouTube BLOCKED:  {len(blocked)}\n")
        f.write(f"  ⚠️  Lỗi khác:        {len(errors)}\n")
        f.write("\n")

        # === Top 10 nhanh nhất ===
        f.write("=" * 90 + "\n")
        f.write(f"  TOP 10 NHANH NHẤT (OK only)\n")
        f.write("=" * 90 + "\n")
        f.write(f"{'Rank':<6} {'Server':<40} {'IP':<16} {'MB/s':<10} {'KB/s':<10} {'Time(s)':<10}\n")
        f.write("-" * 90 + "\n")
        for rank, r in enumerate(sorted(ok_results, key=lambda x: x.download_speed_mbps, reverse=True)[:10], 1):
            f.write(
                f"#{rank:<5} {r.server_file:<40} {r.public_ip:<16} "
                f"{r.download_speed_mbps:<10.3f} {r.download_speed_kbps:<10.0f} "
                f"{r.download_time_s:<10.2f}\n"
            )
        f.write("\n")

        # === Group theo country ===
        f.write("=" * 90 + "\n")
        f.write(f"  THỐNG KÊ THEO COUNTRY\n")
        f.write("=" * 90 + "\n")
        by_country = {}
        for r in results:
            by_country.setdefault(r.country, []).append(r)
        for country in sorted(by_country.keys()):
            items = by_country[country]
            ok_items = [i for i in items if i.download_ok]
            avg_speed = (
                sum(i.download_speed_mbps for i in ok_items) / len(ok_items)
                if ok_items else 0.0
            )
            f.write(
                f"  {country.upper():<5}  total={len(items):<3}  "
                f"ok={len(ok_items):<3}  blocked={sum(1 for i in items if i.tunnel_ok and i.download_status == 'blocked'):<3}  "
                f"tunnel_fail={sum(1 for i in items if not i.tunnel_ok):<3}  "
                f"avg_speed={avg_speed:.2f} MB/s\n"
            )
        f.write("\n")

        # === Tunnel failures ===
        if tunnel_fail:
            f.write("=" * 90 + "\n")
            f.write(f"  TUNNEL FAIL ({len(tunnel_fail)} servers)\n")
            f.write("=" * 90 + "\n")
            for r in tunnel_fail:
                f.write(f"  - {r.server_file:<40}  ({r.tunnel_time_s:.1f}s)\n")
                if r.error_msg:
                    f.write(f"      {r.error_msg[:200]}\n")
            f.write("\n")

        # === Blocked ===
        if blocked:
            f.write("=" * 90 + "\n")
            f.write(f"  YOUTUBE BLOCKED ({len(blocked)} servers) - NÊN XÓA KHỎI proton_config/\n")
            f.write("=" * 90 + "\n")
            for r in blocked:
                f.write(f"  - {r.server_file:<40}  IP={r.public_ip:<16}  http={r.download_http_code}\n")
                if r.error_msg:
                    f.write(f"      {r.error_msg[:200]}\n")
            f.write("\n")

        f.write("=" * 90 + "\n")
        f.write(f"  Files:\n")
        f.write(f"    JSON:    {json_path}\n")
        f.write(f"    CSV:     {csv_path}\n")
        f.write(f"    Summary: {summary_path}\n")
        f.write("=" * 90 + "\n")

    return json_path, csv_path, summary_path


def main():
    parser = argparse.ArgumentParser(
        description="Test tốc độ tải AUDIO qua từng IP fake (ProtonVPN)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--ovpn",
        type=str,
        help="Test 1 server cụ thể (vd: us-free-5.protonvpn.udp.ovpn)",
    )
    parser.add_argument(
        "--audio-url",
        type=str,
        default=DEFAULT_AUDIO_URL,
        help=f"URL audio để test (default: {DEFAULT_AUDIO_URL})",
    )
    parser.add_argument(
        "--country",
        type=str,
        choices=["us", "nl", "jp", "ca", "sg"],
        help="Chỉ test country này (vd: us, nl, jp)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="/home/hientran/sythetic_crawl_data/vpn_audio_speed_test",
        help="Folder output cho results",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Số server test tối đa (default: env TEST_VPN_LIMIT hoặc 5)",
    )
    args = parser.parse_args()

    # === Xác định list servers cần test ===
    # Filter file ẩn (bắt đầu bằng '.') và folder _remove
    all_ovpn = sorted(
        p for p in PROTON_CONFIG_DIR.glob("*.ovpn")
        if not p.name.startswith(".") and "_remove" not in p.name
    )

    if not all_ovpn:
        print(f"❌ Không tìm thấy file .ovpn nào trong {PROTON_CONFIG_DIR}")
        sys.exit(1)

    if args.ovpn:
        # Test 1 server cụ thể
        target = PROTON_CONFIG_DIR / args.ovpn
        if not target.exists():
            print(f"❌ File {args.ovpn} không tồn tại trong {PROTON_CONFIG_DIR}")
            sys.exit(1)
        ovpn_files = [target]
    elif args.country:
        # Filter theo country
        ovpn_files = [p for p in all_ovpn if p.name.startswith(args.country + "-")]
        if not ovpn_files:
            print(f"❌ Không có file .ovpn nào của country '{args.country}'")
            sys.exit(1)
    else:
        # Apply limit
        limit = args.limit
        if limit is None:
            limit = int(os.environ.get("TEST_VPN_LIMIT", "65"))
        ovpn_files = all_ovpn if limit == 0 else all_ovpn[:limit]

    print("=" * 90)
    print("  VPN AUDIO SPEED TEST")
    print("=" * 90)
    print(f"  Config dir:  {PROTON_CONFIG_DIR}")
    print(f"  Total files: {len(all_ovpn)} (sau filter)")
    print(f"  Test:        {len(ovpn_files)} servers")
    print(f"  Audio URL:   {args.audio_url}")
    print(f"  Output:      {args.output_dir}")
    print(f"  Country:     {args.country or 'ALL'}")
    print(f"  Limit:       {args.limit or 'env TEST_VPN_LIMIT (default 5)'}")
    print("=" * 90)
    print()

    # === Output dir cho file audio tạm (sẽ cleanup) ===
    test_audio_dir = Path(args.output_dir) / "_audio_tmp"
    test_audio_dir.mkdir(parents=True, exist_ok=True)

    # === Run tests ===
    results: list[SpeedTestResult] = []
    instance_id = f"audiospeed_{datetime.now().strftime('%H%M%S')}"
    test_start = time.time()

    try:
        for idx, ovpn in enumerate(ovpn_files, 1):
            result = test_one_server(
                ovpn_path=ovpn,
                audio_url=args.audio_url,
                test_dir=test_audio_dir,
                instance_id=f"{instance_id}_{idx}",
            )
            results.append(result)
            print_progress(idx, len(ovpn_files), result)
            sys.stdout.flush()
    except KeyboardInterrupt:
        print("\n\n[INTERRUPTED] Cleanup tunnel...")
    finally:
        # Cleanup tunnel + audio tmp
        disconnect_openvpn()
        try:
            shutil.rmtree(test_audio_dir, ignore_errors=True)
        except Exception:
            pass

    test_duration = time.time() - test_start

    # === Lưu kết quả ===
    print()
    json_path, csv_path, summary_path = save_results(
        results=results,
        output_dir=Path(args.output_dir),
        audio_url=args.audio_url,
        test_duration_s=test_duration,
    )

    # === In summary ra stdout ===
    print()
    print("=" * 90)
    print("  KẾT QUẢ TỔNG HỢP")
    print("=" * 90)

    ok_results = [r for r in results if r.download_ok]
    tunnel_fail = [r for r in results if not r.tunnel_ok]
    blocked = [r for r in results if r.tunnel_ok and r.download_status == "blocked"]
    errors = [r for r in results if r.tunnel_ok and r.download_status not in ("ok", "blocked")]

    print(f"  ✅ Tốc độ OK:        {len(ok_results)}/{len(results)}")
    print(f"  ❌ Tunnel FAIL:      {len(tunnel_fail)}/{len(results)}")
    print(f"  🚫 YouTube BLOCKED:  {len(blocked)}/{len(results)}")
    print(f"  ⚠️  Lỗi khác:        {len(errors)}/{len(results)}")
    print(f"  ⏱️  Tổng thời gian:  {test_duration:.1f}s")
    print()

    if ok_results:
        print("  Top 5 NHANH NHẤT:")
        for rank, r in enumerate(sorted(ok_results, key=lambda x: x.download_speed_mbps, reverse=True)[:5], 1):
            print(
                f"    #{rank}  {r.server_file:<40}  "
                f"IP={r.public_ip:<16}  "
                f"{r.download_speed_mbps:.2f} MB/s  "
                f"({r.download_bytes//1024}KB in {r.download_time_s:.1f}s)"
            )
        print()

    print(f"  📁 Output files:")
    print(f"     {json_path}")
    print(f"     {csv_path}")
    print(f"     {summary_path}")
    print("=" * 90)

    return 0 if ok_results else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[INTERRUPTED]")
        disconnect_openvpn()
        sys.exit(130)
