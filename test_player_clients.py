#!/usr/bin/env python3
"""
Test player_client performance: chạy 100 video qua từng client,
đo tỷ lệ thành công (có subs/auto) và tỷ lệ VI keys.

Usage:
  /home/hientran/miniconda3/envs/crawl/bin/python test_player_clients.py
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
os.chdir(SCRIPT_DIR)
sys.path.insert(0, str(SCRIPT_DIR))

# Add parent dir to PYTHONPATH (cho vpn_rotator import)
PARENT_DIR = str(SCRIPT_DIR.parent)
if PARENT_DIR not in sys.path:
    sys.path.insert(0, PARENT_DIR)

# Test config
NUM_VIDEOS = 20
TIMEOUT = 12  # seconds per video per client
CLIENTS = [
    "tv_embedded", "tv", "ios", "android",
    "web_embedded", "web",
]
COOKIES = str(SCRIPT_DIR / "cookies.txt")


def get_video_ids(n: int = NUM_VIDEOS) -> list:
    """Lấy N video IDs từ channels_tong_hop qua YouTube Data API."""
    api_keys_env = os.environ.get("YOUTUBE_API_KEYS", "")
    if not api_keys_env:
        # Thử từ file .env
        env_file = SCRIPT_DIR / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("YOUTUBE_API_KEYS="):
                    api_keys_env = line.split("=", 1)[1].strip().strip('"\'')
    if not api_keys_env:
        # Fallback: đọc từ sys.argv hoặc hardcode
        print("WARN: YOUTUBE_API_KEYS chưa set. Lấy từ channels_AI_5_ok_dich.txt")
        # Fallback: dùng yt-dlp
        return get_video_ids_ytdlp(n)

    keys = [k.strip() for k in api_keys_env.split(",") if k.strip()]
    import requests
    video_ids = []

    channels = (SCRIPT_DIR / "channels_audio/channels_tong_hop.txt").read_text().splitlines()
    channels = [c.strip() for c in channels if c.strip()][:2]  # 2 channels đầu

    for ch_url in channels:
        for key in keys[:2]:  # chỉ thử 2 key đầu
            try:
                # Resolve channel
                handle = ch_url.split("@")[-1].split("/")[0]
                resp = requests.get(
                    "https://www.googleapis.com/youtube/v3/channels",
                    params={"key": key, "part": "id",
                            "forHandle": "@" + handle},
                    timeout=10,
                )
                if resp.status_code != 200:
                    continue
                items = resp.json().get("items", [])
                if not items:
                    continue
                channel_id = items[0]["id"]
                playlist_id = "UU" + channel_id[2:]

                # Lấy video IDs
                resp = requests.get(
                    "https://www.googleapis.com/youtube/v3/playlistItems",
                    params={"key": key, "playlistId": playlist_id,
                            "part": "contentDetails", "maxResults": 50},
                    timeout=10,
                )
                if resp.status_code != 200:
                    continue
                for item in resp.json().get("items", []):
                    vid = item.get("contentDetails", {}).get("videoId")
                    if vid:
                        video_ids.append(vid)
                if len(video_ids) >= n:
                    return video_ids[:n]
            except Exception as e:
                print(f"  Resolve {ch_url} error: {e}")
                continue
    return video_ids[:n]


def get_video_ids_ytdlp(n: int) -> list:
    """Fallback: dùng yt-dlp để lấy video IDs."""
    import yt_dlp
    channels = (SCRIPT_DIR / "channels_audio/channels_tong_hop.txt").read_text().splitlines()
    channels = [c.strip() for c in channels if c.strip()][:2]
    video_ids = []
    ydl_opts = {"quiet": True, "no_warnings": True,
                "extract_flat": True, "playlistend": 50}
    for ch in channels:
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(ch + "/videos", download=False)
                for e in info.get("entries", []):
                    if e and e.get("id"):
                        video_ids.append(e["id"])
        except Exception as e:
            print(f"  yt-dlp {ch} error: {e}")
    return video_ids[:n]


def test_client(video_id: str, client: str) -> dict:
    """Test 1 video với 1 client. Return dict with result."""
    cli_args = [
        "yt-dlp",
        "--skip-download", "--ignore-errors", "--quiet",
        "--no-warnings", "--no-playlist", "--no-color",
        "--js-runtimes", "node",
        "--extractor-args", f"youtube:player_client={client}",
        "--dump-json",
    ]
    if Path(COOKIES).exists():
        cli_args.extend(["--cookies", COOKIES])
    cli_args.append(f"https://www.youtube.com/watch?v={video_id}")

    t0 = time.time()
    try:
        proc = subprocess.Popen(
            cli_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            bufsize=0,  # unbuffered
        )
        try:
            stdout, stderr = proc.communicate(timeout=TIMEOUT)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=2)
            except Exception:
                pass
            return {"client": client, "status": "TIMEOUT", "elapsed": TIMEOUT,
                    "subs": 0, "auto": 0, "vi_keys": 0}
    except FileNotFoundError:
        return {"client": client, "status": "NO_YT_DLP", "elapsed": 0,
                "subs": 0, "auto": 0, "vi_keys": 0}
    except Exception as e:
        return {"client": client, "status": f"ERROR:{str(e)[:30]}",
                "elapsed": time.time() - t0, "subs": 0, "auto": 0, "vi_keys": 0}

    elapsed = time.time() - t0
    if proc.returncode != 0 or not stdout.strip():
        return {"client": client, "status": "EMPTY", "elapsed": elapsed,
                "subs": 0, "auto": 0, "vi_keys": 0}

    try:
        info = json.loads(stdout)
    except json.JSONDecodeError:
        return {"client": client, "status": "PARSE_ERR", "elapsed": elapsed,
                "subs": 0, "auto": 0, "vi_keys": 0}

    subs = info.get("subtitles") or {}
    auto = info.get("automatic_captions") or {}
    n_subs = sum(len(v) for v in subs.values())
    n_auto = sum(len(v) for v in auto.values())
    vi_keys = sum(1 for k in (list(subs.keys()) + list(auto.keys()))
                  if k.lower().startswith("vi"))
    status = "OK" if (subs or auto) else "EMPTY"
    return {"client": client, "status": status, "elapsed": elapsed,
            "subs": n_subs, "auto": n_auto, "vi_keys": vi_keys}


def main():
    print(f"=== Test player_clients: {NUM_VIDEOS} videos × {len(CLIENTS)} clients ===")
    print(f"Timeout per video: {TIMEOUT}s")
    print()

    video_ids = get_video_ids(NUM_VIDEOS)
    if not video_ids:
        print("❌ Không lấy được video IDs. Set YOUTUBE_API_KEYS env hoặc "
              "đảm bảo yt-dlp + cookies work.")
        return
    print(f"✅ Lấy được {len(video_ids)} video IDs")
    print(f"   First 5: {video_ids[:5]}")
    print()

    # Test mỗi client trên tất cả video (tuần tự để tránh rate-limit)
    results = {c: {"OK": 0, "EMPTY": 0, "TIMEOUT": 0, "ERROR": 0,
                   "total_subs": 0, "total_auto": 0, "total_vi": 0,
                   "total_elapsed": 0.0, "videos_with_vi": 0}
               for c in CLIENTS}

    for i, vid in enumerate(video_ids):
        print(f"\n[{i+1}/{len(video_ids)}] {vid}")
        for client in CLIENTS:
            r = test_client(vid, client)
            c = r["client"]
            results[c][r["status"] if r["status"] in ("OK", "EMPTY", "TIMEOUT")
                      else "ERROR"] += 1
            results[c]["total_subs"] += r["subs"]
            results[c]["total_auto"] += r["auto"]
            results[c]["total_vi"] += r["vi_keys"]
            results[c]["total_elapsed"] += r["elapsed"]
            if r["vi_keys"] > 0:
                results[c]["videos_with_vi"] += 1
            mark = {"OK": "✅", "EMPTY": "⚠️", "TIMEOUT": "⏱️", "ERROR": "❌"}.get(
                r["status"], "❓")
            print(f"   {mark} {client:15s} {r['status']:8s} "
                  f"subs={r['subs']:4d} auto={r['auto']:4d} VI={r['vi_keys']:2d} "
                  f"({r['elapsed']:.1f}s)")
        # In summary mỗi 10 video
        if (i + 1) % 10 == 0:
            print_summary(results, i + 1, len(video_ids))

    print("\n" + "=" * 80)
    print("FINAL SUMMARY")
    print("=" * 80)
    print_summary(results, len(video_ids), len(video_ids), final=True)

    # Đề xuất
    print("\n" + "=" * 80)
    print("ĐỀ XUẤT (sắp xếp theo tỷ lệ VI)")
    print("=" * 80)
    ranked = sorted(CLIENTS,
                    key=lambda c: (-results[c]["videos_with_vi"],
                                    -results[c]["OK"] / max(1, results[c]["OK"] + results[c]["EMPTY"]),
                                    results[c]["total_elapsed"]))
    for rank, c in enumerate(ranked, 1):
        n_total = results[c]["OK"] + results[c]["EMPTY"] + results[c]["TIMEOUT"]
        if n_total == 0:
            continue
        ok_rate = results[c]["OK"] / n_total * 100
        vi_rate = results[c]["videos_with_vi"] / n_total * 100
        print(f"  {rank}. {c:15s} OK={ok_rate:5.1f}%  VI={vi_rate:5.1f}%  "
              f"avg_time={results[c]['total_elapsed']/n_total:.1f}s")


def print_summary(results, done, total, final=False):
    print(f"\n--- Summary ({done}/{total}) ---")
    print(f"{'Client':15s} {'OK':>5s} {'EMPTY':>6s} {'TIMEOUT':>8s} "
          f"{'OK%':>5s} {'VI%':>5s} {'avg_t':>6s}")
    for c in CLIENTS:
        r = results[c]
        n = r["OK"] + r["EMPTY"] + r["TIMEOUT"]
        if n == 0:
            continue
        ok_pct = r["OK"] / n * 100
        vi_pct = r["videos_with_vi"] / n * 100
        avg_t = r["total_elapsed"] / n
        print(f"  {c:15s} {r['OK']:5d} {r['EMPTY']:6d} {r['TIMEOUT']:8d} "
              f"{ok_pct:5.1f} {vi_pct:5.1f} {avg_t:6.1f}")


if __name__ == "__main__":
    main()
