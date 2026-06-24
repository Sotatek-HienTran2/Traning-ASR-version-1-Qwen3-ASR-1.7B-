#!/usr/bin/env python3
"""
YouTube Researcher - Channel-based (yt-dlp version, NO API KEY needed)
Lấy video từ kênh YouTube bằng yt-dlp thay vì YouTube Data API v3.
- Không cần YOUTUBE_API_KEY
- Lấy danh sách video qua yt-dlp (flat-playlist)
- Lấy metadata chi tiết qua yt-dlp (extract_info per video)
- Lấy comments: optional (yt-dlp --write-comments, hoặc bỏ qua)
- **Dùng bản dịch / phụ đề SẴN CÓ của YouTube (tiếng Việt) thay cho Soniox API**
    + Lấy qua `youtube-transcript-api` (ưu tiên: vi manual > vi auto > en > auto > any)
    + Fallback qua yt-dlp subtitles nếu cần
    + Không tải audio về -> không tốn dung lượng disk
    + Không cần SONIOX_API_KEY
- **Fake IP CHỈ qua ProtonVPN OpenVPN tunnel** (./proton_config/*.ovpn).
  Truyền --use-vpn để bật. Mặc định KHÔNG bật → dùng IP thật.

Usage:
    # Mode nhiều kênh (file txt, mỗi kênh 1 dòng)
    python youtube_researcher_youtube_subs.py

    # Mode 1 kênh
    python youtube_researcher_youtube_subs.py --channel "https://www.youtube.com/@vietnh1009"

    # Bật ProtonVPN để rotate IP (5 server free: CA/MX/NL/SG/US)
    python youtube_researcher_youtube_subs.py --use-vpn

    # Rotate IP VPN mỗi 10 request
    python youtube_researcher_youtube_subs.py --use-vpn --vpn-rotate-every 10
"""

import json
import os
import re
import sys
import time
import threading
import numpy as np
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ================= VPN ROTATOR =================
# Chỉ dùng ProtonVPN OpenVPN tunnel để fake IP (xem ./proton_config/).
# - 5 server free (CA/MX/NL/SG/US), rotate random theo --vpn-strategy
# - Auth: ./proton_config/auth.txt (chmod 600)
# - Cần: sudo setcap cap_net_admin+ep /usr/sbin/openvpn (chạy 1 lần)
try:
    from vpn_rotator import (
        get_vpn_rotator_from_config,
        VPNRotator,
        is_proxy_dead_error,
    )
except ImportError:
    # Fallback nếu vpn_rotator.py không ở cùng folder
    sys.path.insert(0, str(Path(__file__).parent))
    from vpn_rotator import (  # type: ignore
        get_vpn_rotator_from_config,
        VPNRotator,
        is_proxy_dead_error,
    )

# ================= COOKIES =================
# Tự động tìm cookies.txt cùng folder script để bypass "Sign in to confirm you're not a bot"
COOKIES_FILE = Path(__file__).parent / "cookies.txt"
COOKIES_FILE_STR = str(COOKIES_FILE) if COOKIES_FILE.exists() else None

# ================= CONFIG =================
# YouTube API v3 key rotation: dùng tất cả key có trong .env
# Khi 1 key bị quota (403) → tự chuyển sang key tiếp theo
_YOUTUBE_API_KEYS: list = []
for _k in ["YOUTUBE_API_KEY", "YOUTUBE_API_KEY_1", "YOUTUBE_API_KEY_2", "YOUTUBE_API_KEY_3"]:
    _v = os.environ.get(_k, "")
    if _v and _v not in _YOUTUBE_API_KEYS:
        _YOUTUBE_API_KEYS.append(_v)
YOUTUBE_API_KEY = _YOUTUBE_API_KEYS[0] if _YOUTUBE_API_KEYS else ""
SONIOX_API_KEY = ""
ANTHROPIC_API_KEY = ""

MINIMAX_MODEL = os.environ.get(
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "MiniMax/MiniMax-M2.7"
)

# ================= FILTER CONFIG =================
FILTER_PUBLISHED_DAYS = 36500
FILTER_MIN_DURATION = 0
FILTER_MAX_DURATION = 1000000
FILTER_MIN_VIEW_COUNT = 50
FILTER_MIN_LIKE_COUNT = 0
FILTER_MIN_COMMENT_COUNT = 0


@dataclass
class VideoCandidate:
    video_id: str
    title: str
    channel: str
    description: str
    published_at: str
    duration: str

    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0

    transcript: Optional[str] = None
    transcript_language: str = ""
    transcript_is_auto: bool = False

    thumbnail: str = ""
    url: str = ""

    tags: list = field(default_factory=list)
    category_id: str = ""
    default_language: str = ""
    default_audio_language: str = ""

    caption_available: bool = False
    definition: str = ""
    dimension: str = ""
    licensed_content: bool = False
    projection: str = ""

    privacy_status: str = ""
    embeddable: bool = True
    made_for_kids: bool = False

    live_broadcast_content: str = ""
    concurrent_viewers: int = 0

    topic_categories: list = field(default_factory=list)
    recording_location_description: str = ""
    top_comments: list = field(default_factory=list)

    estimated_speech_ratio: float = 0.0
    detected_languages: list = field(default_factory=list)
    avg_confidence: float = 0.0
    dataset_score: float = 0.0

    niche: str = ""
    llm_score: float = 0.0
    llm_reason: str = ""

    # === Extra fields from yt-dlp (khong co trong YouTube Data API) ===
    channel_id: str = ""                      # Channel ID (UCxxx)
    channel_url: str = ""                    # URL kenh
    channel_follower_count: int = 0          # So follower cua kenh
    uploader: str = ""                       # Ten uploader
    uploader_id: str = ""                    # Handle (@xxx)
    uploader_url: str = ""                   # URL uploader
    location: str = ""                       # Vi tri (vd: "BÉC-LIN")
    width: int = 0                           # Video width (px)
    height: int = 0                          # Video height (px)
    fps: float = 0.0                         # Frame per second
    vcodec: str = ""                         # Video codec (vd: av01)
    acodec: str = ""                         # Audio codec (vd: opus)
    tbr: float = 0.0                         # Total bitrate (kbps)
    abr: float = 0.0                         # Audio bitrate (kbps)
    vbr: float = 0.0                         # Video bitrate (kbps)
    filesize_approx: int = 0                 # File size (bytes)
    release_year: int = 0                    # Nam phat hanh
    release_date: str = ""                   # Ngay phat hanh (YYYYMMDD)
    live_status: str = ""                    # "not_live" | "is_live" | "was_live" | "is_upcoming"
    was_live: bool = False                   # Da live truoc do
    age_limit: int = 0                       # Gioi han tuoi
    playable_in_embed: bool = True           # Co the embed khong
    chapters: list = field(default_factory=list)            # Chapters neu co
    heatmap: list = field(default_factory=list)            # Heatmap neu co
    duration_string: str = ""                # Duration hien thi (vd: "2:54")
    aspect_ratio: float = 0.0                # Ty le khung hinh
    categories: list = field(default_factory=list)         # Tat ca categories (khong chi first)
    automatic_captions: dict = field(default_factory=dict) # Dict ngon ngu auto-caption

    audio_filename: str = ""  # Ten file audio (vd: "Gh1Sgknc6Fg.wav") - dong nhat giua disk / json / csv

    passed_filters: list = field(default_factory=list)
    failed_filters: list = field(default_factory=list)

    @property
    def video_url(self) -> str:
        return f"https://www.youtube.com/watch?v={self.video_id}"


@dataclass
class FilterCriteria:
    published_after: Optional[datetime] = None
    published_before: Optional[datetime] = None

    min_duration: Optional[int] = None
    max_duration: Optional[int] = None

    min_view_count: int = 0
    max_view_count: Optional[int] = None

    min_like_count: int = 0
    min_comment_count: int = 0

    require_transcript: bool = False
    exclude_keywords: list = field(default_factory=list)
    include_keywords: list = field(default_factory=list)
    language_hint: str = "vi"


# ================= HELPERS =================

def parse_duration(duration_str: str) -> int:
    if not duration_str:
        return 0
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration_str)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def format_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


def format_number(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def api_call_with_retry(func, max_retries=5, delay=10):
    """Retry API calls on SSL/connection errors"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            error_str = str(e)
            if any(kw in error_str.lower() for kw in ['ssl', 'timeout', 'connection', 'reset', 'broken pipe']):
                if attempt < max_retries - 1:
                    wait = delay * (attempt + 1)
                    print(f"  Connection error, retrying in {wait}s... (attempt {attempt+1}/{max_retries})")
                    time.sleep(wait)
                else:
                    raise
            else:
                raise


def _channel_id_cache_path() -> Path:
    """Path luu cache channel_id theo handle/URL (tang toc resolve_channel_id)."""
    return Path(__file__).parent / ".channel_id_cache.json"


def _load_channel_id_cache() -> dict:
    p = _channel_id_cache_path()
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_channel_id_cache(cache: dict):
    p = _channel_id_cache_path()
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  [warn] khong luu duoc channel_id cache: {e}")


def resolve_channel_id(api_key: str, channel_input: str,
                        proxy_url: Optional[str] = None) -> Optional[str]:
    """
    Resolve channel URL/handle/ID to channel ID bang yt-dlp (khong can API key).

    Supports:
        - https://www.youtube.com/@ChannelHandle
        - https://www.youtube.com/channel/UCxxxxx
        - https://www.youtube.com/c/ChannelName
        - UCxxxxx (direct channel ID)

    Co cache: neu da resolve truoc do thi tra ve luon (khong goi yt-dlp lai).
    """
    # Direct channel ID
    if channel_input.startswith("UC") and len(channel_input) == 24:
        return channel_input

    # Extract from URL
    url = channel_input.strip().rstrip("/")

    # /channel/UCxxxxx: trich truc tiep tu URL
    channel_match = re.search(r'youtube\.com/channel/([^/\s?]+)', url)
    if channel_match:
        return channel_match.group(1)

    # Check cache truoc (theo key la URL goc)
    cache = _load_channel_id_cache()
    if channel_input in cache:
        return cache[channel_input]

    # Dung yt-dlp de lay channel_id tu handle/c/name
    # Them /videos de dam bao lay channel (khong phai playlist)
    if not url.endswith("/videos"):
        test_url = url + "/videos"
    else:
        test_url = url

    # ====== FAST PATH: dùng YouTube Data API nếu có key (50 units) ======
    # Nhanh hơn yt-dlp ~10x: ~0.5-1s vs 5-20s
    if _YOUTUBE_API_KEYS:
        import requests as _req
        # Trich handle/user từ URL
        handle_match = re.search(r"youtube\.com/@([^/\s?]+)", url)
        custom_match = re.search(r"youtube\.com/c/([^/\s?]+)", url)
        user_match = re.search(r"youtube\.com/user/([^/\s?]+)", url)
        try:
            for api_key in _YOUTUBE_API_KEYS:
                if handle_match:
                    resp = _req.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"key": api_key, "part": "id", "forHandle": "@" + handle_match.group(1)},
                        timeout=10,
                    )
                elif custom_match:
                    resp = _req.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"key": api_key, "part": "id", "forUsername": custom_match.group(1)},
                        timeout=10,
                    )
                elif user_match:
                    resp = _req.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"key": api_key, "part": "id", "forUsername": user_match.group(1)},
                        timeout=10,
                    )
                else:
                    break  # Không match format nào, fallback yt-dlp
                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get("items", [])
                    if items:
                        cid = items[0].get("id", "")
                        if cid.startswith("UC") and len(cid) == 24:
                            cache[channel_input] = cid
                            _save_channel_id_cache(cache)
                            return cid
                elif resp.status_code == 403:
                    # Quota hết → thử key tiếp
                    continue
                else:
                    break  # Lỗi khác → fallback yt-dlp
        except Exception as e:
            print(f"  [API] resolve_channel_id error: {e}, fallback yt-dlp")
    # ====== END FAST PATH ======

    try:
        import yt_dlp
    except ImportError:
        print("pip install yt-dlp")
        return None

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": True,  # chi lay metadata, khong extract tung video
        "skip_download": True,
        "js_runtimes": {"node": {}},
    }
    YouTubeResearcher._apply_auth_skip(ydl_opts)
    # Cũng skip authcheck cho youtubetab (channel listing)
    ydl_opts["extractor_args"].setdefault("youtubetab", {}).setdefault("skip", []).append("authcheck")
    YouTubeResearcher._apply_cookies(ydl_opts)
    YouTubeResearcher._apply_timeouts(ydl_opts, socket_timeout=60)
    if proxy_url:
        ydl_opts["proxy"] = proxy_url

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(test_url, download=False)
            if info:
                # info co the la playlist/channel
                channel_id = info.get("channel_id") or info.get("id")
                # Neu la UCxxx thi tra ve luon
                if channel_id and channel_id.startswith("UC") and len(channel_id) == 24:
                    return channel_id
                # Thu lay tu first entry
                entries = info.get("entries") or []
                if entries:
                    first = entries[0]
                    if isinstance(first, dict):
                        cid = first.get("channel_id")
                        if cid and cid.startswith("UC") and len(cid) == 24:
                            return cid
                # Channel URL co format /@handle -> channel_id = info.get('channel_id')
                result_id = channel_id
                if result_id:
                    cache[channel_input] = result_id
                    _save_channel_id_cache(cache)
                return result_id
    except Exception as e:
        print(f"  [yt-dlp] resolve_channel_id error: {e}")

    return None


def fetch_channel_via_rss(channel_id: str, max_results: int = 50,
                          proxy_url: Optional[str] = None,
                          rss_page_delay: int = 2) -> list[dict]:
    """
    Lay video tu kenh qua YouTube RSS feed.
    RSS feed cua YouTube tra 15 video moi nhat, co the lap lai nhieu lan
    bang cach dung <link rel='next-archive' href='...start-index=N'/> de lay
    15 video cu hon moi lan.

    Args:
        channel_id: channel ID (UCxxxxx)
        max_results: so video toi da. Neu > 15, script se loop nhieu trang RSS.
        proxy_url: proxy URL (optional)
        rss_page_delay: delay (giây) giữa các page RSS, tránh bị YouTube rate limit.

    Returns:
        list of dict voi keys: id, title, upload_date, url, thumbnail
    """
    import urllib.request
    import xml.etree.ElementTree as ET

    rss_url = f"https://www.youtube.com/feeds/videos.xml?channel_id={channel_id}"
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "media": "http://search.yahoo.com/mrss/",
    }

    all_entries = []
    seen_ids = set()
    next_url = rss_url
    # RSS chi tra 15 video moi nhat moi page. Voi max_results=20000,
    # can 1334 pages (qua lon). Cap o 700 pages (10500 video toi da tu RSS).
    max_pages = min((max_results // 15) + 2, 700)
    print(f"  [RSS] Target: {max_results} videos, max_pages={max_pages}")

    for page_idx in range(max_pages):
        if len(all_entries) >= max_results:
            print(f"  [RSS] Da dat max_results={max_results}, dung.")
            break

        # Delay giữa các page RSS (tránh bị YouTube rate limit khi loop nhiều trang)
        if page_idx > 0 and rss_page_delay > 0:
            time.sleep(rss_page_delay)

        try:
            # Dùng requests nếu có proxy (urllib cần custom handler, requests đơn giản hơn)
            if proxy_url:
                import requests
                r = requests.get(next_url, proxies={"http": proxy_url, "https": proxy_url},
                                 headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
                raw = r.content
            else:
                req = urllib.request.Request(next_url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    raw = resp.read()
        except Exception as e:
            print(f"  [RSS] page {page_idx + 1} fetch error: {e}")
            break

        try:
            root = ET.fromstring(raw)
        except Exception as e:
            print(f"  [RSS] page {page_idx + 1} parse error: {e}")
            break

        # Lay cac entry trong page nay
        entries = root.findall("atom:entry", ns)
        new_in_page = 0
        for entry in entries:
            try:
                video_id = entry.find("atom:id", ns).text.split(":")[-1]
                if video_id in seen_ids:
                    continue
                seen_ids.add(video_id)
                title = entry.find("atom:title", ns).text
                published = entry.find("atom:published", ns).text
                upload_date = published[:10].replace("-", "")
                media_group = entry.find("media:group", ns)
                thumbnail = ""
                if media_group is not None:
                    media_thumb = media_group.find("media:thumbnail", ns)
                    if media_thumb is not None:
                        thumbnail = media_thumb.get("url", "")
                all_entries.append({
                    "id": video_id,
                    "title": title,
                    "upload_date": upload_date,
                    "url": f"https://www.youtube.com/watch?v={video_id}",
                    "thumbnail": thumbnail,
                })
                new_in_page += 1
                if len(all_entries) >= max_results:
                    break
            except Exception:
                continue

        print(f"  [RSS] page {page_idx + 1}: {new_in_page} new entries (total: {len(all_entries)})")

        if new_in_page == 0:
            # Het entry moi -> dung
            break

        # Tim link next-archive de lay trang tiep theo
        next_url = None
        for link in root.findall("atom:link", ns):
            if link.get("rel") == "next-archive":
                next_url = link.get("href")
                break
        if not next_url:
            # Het trang
            break

    return all_entries[:max_results]


def fetch_video_info_via_ytdlp(video_id: str,
                               proxy_url: Optional[str] = None) -> dict | None:
    """
    Lay full metadata cho 1 video (views, likes, duration, ...) qua yt-dlp.
    """
    try:
        import yt_dlp
    except ImportError:
        return None

    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "ignoreerrors": True,
        "js_runtimes": {"node": {}},
    }
    YouTubeResearcher._apply_auth_skip(ydl_opts)
    YouTubeResearcher._apply_cookies(ydl_opts)
    YouTubeResearcher._apply_timeouts(ydl_opts, socket_timeout=60)
    if proxy_url:
        ydl_opts["proxy"] = proxy_url
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
        return info
    except Exception as e:
        print(f"  [yt-dlp] video {video_id} error: {e}")
        return None


class YouTubeResearcher:
    """
    YouTube Researcher - Lấy video theo kênh (channel URL)
    Thay vì search keyword, nhập URL kênh YouTube -> lấy video -> lọc -> pipeline
    """

    def __init__(self, api_key: str, output_dir: str = "./researched_videos",
                 proxy_rotator: Optional[VPNRotator] = None):
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._videos: list[VideoCandidate] = []
        self._filtered_videos: list[VideoCandidate] = []
        # Proxy rotator: mỗi request gọi self._next_proxy() để lấy IP mới
        self._rotator = proxy_rotator
        self._direct_blocked = False

    def _next_proxy(self) -> Optional[str]:
        """Lấy proxy URL tiếp theo từ rotator (hoặc None nếu không có rotator)."""
        if not self._rotator:
            return None
        url = self._rotator.next()
        if url:
            # Log ngắn gọn: ẩn user:pass, chỉ show ip:port
            try:
                from urllib.parse import urlparse
                p = urlparse(url)
                short = f"{p.hostname}:{p.port}"
            except Exception:
                short = url[:40]
            print(f"    [proxy] → {short}")
        return url

    def _proxy_guard(self):
        """
        Context manager bảo vệ tunnel: chỉ rotate VPN khi KHÔNG có request
        nào đang dùng tunnel.

        Dùng đúng cách — wrap TOÀN BỘ đoạn code dùng tunnel:
            with self._proxy_guard():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(...)

        Nếu không có rotator (direct IP) → return dummy context (no-op).
        """
        if not self._rotator:
            # Dummy context manager khi không có rotator (chạy direct)
            class _NoOp:
                def __enter__(self): return self
                def __exit__(self, *a): return False
            return _NoOp()
        return self._rotator.acquire()

    def _guard_and_proxy(self):
        """
        Helper: acquire tunnel + lấy proxy URL trong 1 bước.

        Dùng cho pattern:
            with self._guard_and_proxy() as (guard, proxy_url):
                # proxy_url có thể None (dùng default route qua tunnel)
                ...
        """
        guard = self._proxy_guard()
        # Phải vào guard TRƯỚC rồi mới gọi next() (vì next() có thể trigger rotate,
        # rotate sẽ chờ idle nếu guard chưa enter)
        class _GuardProxy:
            def __init__(self, g, owner):
                self._g = g
                self._owner = owner
                self.proxy_url = None
            def __enter__(self):
                self._g.__enter__()
                self.proxy_url = self._owner._next_proxy()
                return self
            def __exit__(self, *a):
                return self._g.__exit__(*a)
        return _GuardProxy(guard, self)

    def _mark_proxy_failed(self, proxy_url: Optional[str]):
        """Đánh dấu proxy fail (khi gặp 429/timeout/SSL)."""
        if self._rotator and proxy_url:
            self._rotator.mark_failed(proxy_url)

    def _mark_proxy_dead(self, proxy_url: Optional[str]):
        """Xóa proxy thực sự chết khỏi pool (vĩnh viễn).
        Dùng khi gặp: connect timeout, SSL, 5xx, 'Sign in to confirm'.
        KHÔNG dùng cho 429/403 rate limit (proxy vẫn sống, chỉ bị rate limit).
        """
        if self._rotator and proxy_url:
            self._rotator.remove_proxy(proxy_url)

    def _proxy_for_fallback(self) -> Optional[str]:
        """Direct-first strategy: trả None (direct) nếu chưa bị block,
        trả proxy nếu direct đã bị rate-limit/block.
        Nếu proxy pool rỗng (bị xóa hết do chết) → KHÔNG quay lại direct,
        trả None để caller tự quyết định backoff."""
        if self._direct_blocked and self._rotator and len(self._rotator) > 0:
            proxy = self._next_proxy()
            if proxy:
                return proxy
            # Pool rỗng → KHÔNG quay lại direct (tránh loop vô hạn)
            return None
        return None

    def _escalate_to_proxy(self):
        """Đánh dấu direct IP đã bị block, chuyển sang dùng proxy."""
        if self._rotator:
            self._direct_blocked = True
            print("    [strategy] Direct IP blocked/rate-limited → switching to proxy")
        else:
            print("    [strategy] Direct IP blocked but no proxy available, will keep retrying direct")

    @staticmethod
    def _test_proxy_fast(proxy_url: str, timeout: float = 5.0) -> bool:
        """
        Test proxy còn sống không qua TCP connect (không qua HTTPS, nhanh).
        Return True nếu connect được trong `timeout` giây.
        """
        if not proxy_url:
            return True  # no proxy = luôn OK
        try:
            from urllib.parse import urlparse
            import socket
            p = urlparse(proxy_url)
            host = p.hostname
            port = p.port or (443 if p.scheme == "https" else 80)
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            try:
                sock.connect((host, port))
                sock.close()
                return True
            except (socket.timeout, OSError):
                return False
        except Exception:
            return False

    @staticmethod
    def _apply_cookies(ydl_opts: dict) -> dict:
        """Inject cookiefile vào ydl_opts nếu cookies.txt tồn tại."""
        if COOKIES_FILE_STR:
            ydl_opts["cookiefile"] = COOKIES_FILE_STR
        return ydl_opts

    @staticmethod
    def _apply_auth_skip(ydl_opts: dict) -> dict:
        """
        Inject các option cần thiết để bypass "Sign in to confirm you're not a bot"
        và pass challenge `n` của YouTube:

        1. `extractor_args.youtube.skip = ["authcheck"]` — KHÔNG gửi authcheck request
           (IP bị flag / cookies không đủ vẫn không bị redirect sign-in).

        2. `extractor_args.youtube.player_client = ["web_safari", "web"]` — Chỉ định
           client dùng để giải challenge `n`. Mặc định `tv` client KHÔNG trigger
           challenge `n` nhưng cũng không trả format đầy đủ. `web_safari` + `web`
           cho đầy đủ formats + pass challenge khi có bgutil-pot-server + EJS.

        Cần: bgutil-pot-server chạy ở http://127.0.0.1:4416 (đã cài plugin
        getpot_bgutil_http tự động gọi). Cần: yt-dlp-ejs package + node JS runtime
        để giải JS challenge.

        KHÔNG ghi đè extractor_args.youtube hiện có (vd: max_comments).
        """
        ydl_opts.setdefault("extractor_args", {})
        if "youtube" not in ydl_opts["extractor_args"]:
            ydl_opts["extractor_args"]["youtube"] = {}

        yt_args = ydl_opts["extractor_args"]["youtube"]

        # 1. Skip authcheck (nếu chưa có)
        if "skip" not in yt_args:
            yt_args["skip"] = []
        if "authcheck" not in yt_args["skip"]:
            yt_args["skip"].append("authcheck")

        # 2. Player client để pass challenge n (nếu chưa có)
        # web_safari: nhẹ, hỗ trợ đầy đủ formats
        # web: fallback, dùng khi web_safari fail
        if "player_client" not in yt_args:
            yt_args["player_client"] = ["web_safari", "web"]

        # 3. Đảm bảo có js_runtimes cho EJS challenge solver
        if "js_runtimes" not in ydl_opts:
            ydl_opts["js_runtimes"] = {"node": {}}

        return ydl_opts

    @staticmethod
    def _apply_timeouts(ydl_opts: dict, socket_timeout: int = 60,
                          connect_timeout: int = 8) -> dict:
        """
        Inject timeout ngắn vào ydl_opts để proxy chết không block lâu.

        Args:
            socket_timeout: timeout tổng cho mỗi socket operation (giây)
            connect_timeout: timeout khi connect tới proxy (giây)
        """
        ydl_opts["socket_timeout"] = socket_timeout
        return ydl_opts

    @staticmethod
    def _short_proxy(proxy_url: Optional[str]) -> str:
        """Rút gọn proxy URL → 'ip:port' (ẩn user:pass). Trả về 'no-proxy' nếu None."""
        if not proxy_url:
            return "no-proxy"
        try:
            from urllib.parse import urlparse
            p = urlparse(proxy_url)
            return f"{p.hostname}:{p.port}"
        except Exception:
            return proxy_url[:40]

    @staticmethod
    def _safe_filename(title: str, fallback: str = "audio", max_length: int = 100) -> str:
        """
        Chuyen tieu de video thanh ten file an toan tren filesystem.
        - Bo ky tu dac biet, dau, emoji
        - Thay khoang trang bang '_'
        - Giu lai chu cai (co dau) + so + mot so ky tu phep
        - Neu title rong/sai -> dung fallback
        """
        if not title:
            return fallback

        # Normalize unicode: tach dau, lowercase
        try:
            import unicodedata
            normalized = unicodedata.normalize("NFKD", title)
            # Giu chu cai (ke ca dau) + so + space
            cleaned = "".join(
                ch for ch in normalized
                if not unicodedata.combining(ch)
            )
        except Exception:
            cleaned = title

        # Thay cac ky tu khong phai chu/so bang _
        cleaned = re.sub(r"[^\w\sÀ-ɏḀ-ỿ-]", "_", cleaned, flags=re.UNICODE)
        # Gop nhieu _ lien tiep, trim _
        cleaned = re.sub(r"\s+", "_", cleaned.strip())
        cleaned = re.sub(r"_+", "_", cleaned).strip("._-")

        if not cleaned:
            return fallback

        # Gioi han do dai
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length].rstrip("._-")

        return cleaned

    @staticmethod
    def find_transcription_json(transcription_dir, video, audio_filename: str = "") -> "Path | None":
        """
        Tim file JSON bản dịch theo nhieu pattern:
        1. {audio_stem}_transcription.json (pattern moi, ten theo audio)
        2. {safe_title}_transcription.json (pattern moi, ten theo title - khi khong download audio)
        3. {video_id}_transcription.json (pattern cu, backward-compat)
        Tra ve Path neu tim thay, None neu khong.
        """
        if not transcription_dir:
            return None
        td = Path(transcription_dir)
        candidates = []
        if audio_filename:
            stem = Path(audio_filename).stem
            candidates.append(td / f"{stem}_transcription.json")
        # Them pattern theo safe_title (dung khi pipeline khong download audio)
        if getattr(video, "title", None):
            try:
                safe_title = YouTubeResearcher._safe_filename(video.title, fallback=video.video_id)
                candidates.append(td / f"{safe_title}_transcription.json")
            except Exception:
                pass
        if getattr(video, "video_id", None):
            candidates.append(td / f"{video.video_id}_transcription.json")
        for c in candidates:
            if c.exists():
                return c
        return None

    @staticmethod
    def _build_audio_index(audio_root) -> dict:
        """
        Quet 1 LAN TAT CA subfolder audio/<timestamp>/, tra ve dict:
            {basename_no_ext: Path}

        Moi entry duoc uu tien tu subfolder moi nhat (newest first).
        Dung de lookup O(1) thay vi phai duyet subdirs cho moi video.
        """
        index: dict = {}
        if not audio_root:
            return index
        root = Path(audio_root)
        if not root.exists():
            return index

        audio_exts = {".wav", ".m4a", ".mp3", ".flac", ".opus", ".ogg", ".webm"}
        subdirs = sorted([d for d in root.iterdir() if d.is_dir()], reverse=True)
        for sub in subdirs:
            try:
                for f in sub.iterdir():
                    if not f.is_file():
                        continue
                    if f.suffix.lower() not in audio_exts:
                        continue
                    key = f.stem
                    if key and key not in index:
                        index[key] = f
            except Exception:
                continue
        return index

    @staticmethod
    def _build_json_index(transcriptions_root) -> dict:
        """
        Quet 1 LAN TAT CA subfolder transcriptions/<timestamp>/, tra ve dict:
            {basename_no_ext_no_transcription_suffix: Path}

        Key = stem cua file JSON sau khi bo hau to "_transcription.json"
        (vd: "Title_safe", "video_id", "Title_safe_video_id").
        """
        index: dict = {}
        if not transcriptions_root:
            return index
        root = Path(transcriptions_root)
        if not root.exists():
            return index

        suffix = "_transcription.json"
        subdirs = sorted([d for d in root.iterdir() if d.is_dir()], reverse=True)
        for sub in subdirs:
            try:
                for f in sub.iterdir():
                    if not f.is_file() or not f.name.endswith(suffix):
                        continue
                    stem = f.name[: -len(suffix)]
                    if stem and stem not in index:
                        index[stem] = f
            except Exception:
                continue
        return index

    def _partition_videos_for_pipeline(
        self, audio_root, transcriptions_root, skip_existing: bool,
    ) -> tuple:
        """
        Partition filtered videos thanh 3 bucket (uu tien xu ly skip truoc):

          Bucket A: co ca audio + json (matching) -> SKIP hoan toan (khong I/O)
          Bucket B: co audio, chua co json (hoac json khong khop audio) -> chi transcribe
          Bucket C: khong co audio -> full pipeline (download + transcribe + save)

        Thuc hien 1 LAN quet disk (build audio_index + json_index) thay vi
        N lan (moi video goi find_existing_audio/find_transcription_json).

        Tra ve (bucket_a, bucket_b, bucket_c):
          bucket_a: list[(video, audio_path, json_path)]
          bucket_b: list[(video, audio_path, audio_filename)]
          bucket_c: list[(video, target_name, target_filename)]
        """
        if not skip_existing:
            # Che do --force-retranscribe: download moi thu tu dau -> tat ca vao Bucket C
            bucket_c = []
            for video in self._filtered_videos:
                target_name = self._safe_filename(video.title, fallback=video.video_id)
                target_filename = f"{target_name}.wav"
                bucket_c.append((video, target_name, target_filename))
            return [], [], bucket_c

        # === Pre-build 2 index (1 disk scan moi loai) ===
        audio_index = YouTubeResearcher._build_audio_index(audio_root)
        json_index = YouTubeResearcher._build_json_index(transcriptions_root)

        bucket_a: list = []
        bucket_b: list = []
        bucket_c: list = []

        for video in self._filtered_videos:
            target_name = self._safe_filename(video.title, fallback=video.video_id)
            target_filename = f"{target_name}.wav"

            # === Lookup audio (3 pattern giong find_existing_audio) ===
            audio_path = (
                audio_index.get(target_name)
                or audio_index.get(video.video_id)
                or audio_index.get(f"{target_name}_{video.video_id}")
            )

            # === Lookup json (chi chap nhan json match voi audio_path) ===
            json_path = None
            if audio_path:
                expected_json_stem = f"{audio_path.stem}_transcription"
                for key in [audio_path.stem, video.video_id, target_name]:
                    cand = json_index.get(key)
                    if cand and cand.stem == expected_json_stem:
                        json_path = cand
                        break

            audio_filename = audio_path.name if audio_path else target_filename

            if audio_path and json_path:
                # Co ca audio + json matching -> skip hoan toan
                bucket_a.append((video, audio_path, json_path))
            elif audio_path and not json_path:
                # Co audio (o run cu) nhung chua co json -> chi transcribe
                bucket_b.append((video, audio_path, audio_filename))
            else:
                # Khong co audio -> full pipeline
                bucket_c.append((video, target_name, target_filename))

        return bucket_a, bucket_b, bucket_c

    def fetch_channel_videos_rss(
        self,
        channel_input: str,
        max_results: int = 50,
        order: str = "date",
        published_after: Optional[datetime] = None,
        rss_delay: int = 5,
        rss_page_delay: Optional[int] = None,
    ) -> list[VideoCandidate]:
        """
        Lay video tu kenh qua RSS feed (NHANH, toi da 15 moi nhat).
        Sau do extract full metadata cho tung video qua yt-dlp (1 request/video).

        Args:
            rss_delay: delay (giây) giữa các video trong loop metadata extraction.
            rss_page_delay: delay (giây) giữa các page RSS XML. Mặc định = rss_delay/2.

        Phu hop cho kenh nho (< 100 video) hoac khi can nhanh.
        """
        # Resolve channel ID (dùng proxy)
        proxy_url = self._next_proxy()
        channel_id = resolve_channel_id(self.api_key, channel_input, proxy_url=proxy_url)
        if not channel_id:
            print(f"Khong tim thay kenh: {channel_input}")
            return []

        print(f"Channel ID: {channel_id}")
        print(f"Fetching via RSS (target: {max_results} videos) + yt-dlp for full metadata...")
        # rss_page_delay: nếu không truyền thì dùng 1/2 rss_delay
        if rss_page_delay is None:
            rss_page_delay = max(1, rss_delay // 2)
        print(f"  Delays: rss_delay={rss_delay}s (per video), "
              f"rss_page_delay={rss_page_delay}s (per RSS page)")

        # Lay video tu RSS. RSS cua YouTube chi tra 15 video moi nhat,
        # nhung mot so kenh co 'next-archive' link de paginate.
        proxy_url = self._next_proxy()
        rss_entries = fetch_channel_via_rss(
            channel_id, max_results=max_results * 2,
            proxy_url=proxy_url, rss_page_delay=rss_page_delay,
        )
        if not rss_entries:
            print("RSS tra rong -> fallback sang fetch_channel_videos thuong")
            return self.fetch_channel_videos(
                channel_input=channel_input,
                max_results=max_results,
                order=order,
                published_after=published_after,
            )

        # Neu RSS khong du so luong (vi YouTube gioi han 15, hoac channel it video)
        # va max_results > 15 -> fallback sang batch mode de lay them video cu hon
        if len(rss_entries) < max_results and max_results > 15:
            print(f"  RSS chi tra {len(rss_entries)} video (can {max_results}). "
                  f"Fallback sang batch mode de lay them video cu hon...")
            # Lay oldest_date tu RSS de lam cursor cho batch
            oldest_date = None
            for e in rss_entries:
                ud = e.get("upload_date", "")
                if ud and (oldest_date is None or ud < oldest_date):
                    oldest_date = ud
            if oldest_date:
                print(f"  Cursor: before={oldest_date}")
            else:
                print(f"  [WARN] Khong co upload_date trong RSS -> khong the cursor, "
                      f"batch mode co the khong lay duoc video cu hon.")
            # Tao datetime cho cursor (UTC neu khong co published_after)
            cursor_dt = None
            if oldest_date:
                cursor_dt = datetime.strptime(oldest_date, "%Y%m%d")
                if published_after and published_after.tzinfo:
                    cursor_dt = cursor_dt.replace(tzinfo=published_after.tzinfo)
            print(f"  [DEBUG] Fallback fetch_channel_videos(cursor={cursor_dt}, "
                  f"max_batches max = 100, batch_size=200, max=20000 video)")
            return self.fetch_channel_videos(
                channel_input=channel_input,
                max_results=max_results,
                order=order,
                published_after=published_after,
                published_before_cursor=cursor_dt,
            )

        print(f"RSS: {len(rss_entries)} video")

        # Loc published_after
        if published_after:
            before_count = len(rss_entries)
            rss_entries = [
                e for e in rss_entries
                if not e.get("upload_date") or
                datetime.strptime(e["upload_date"], "%Y%m%d").replace(tzinfo=published_after.tzinfo) >= published_after
            ]
            if before_count != len(rss_entries):
                print(f"  Filter published_after: {before_count} -> {len(rss_entries)}")

        # Extract full metadata cho tung video
        detailed_videos = []
        for i, entry in enumerate(rss_entries, 1):
            # Delay giữa các video để tránh bị YouTube rate limit / chặn IP
            # (1 yt-dlp call/video qua fetch_video_info_via_ytdlp)
            if i > 1 and rss_delay > 0:
                time.sleep(rss_delay)
            video_id = entry["id"]
            print(f"  [{i}/{len(rss_entries)}] {video_id} ({entry.get('upload_date', 'N/A')}) - extracting metadata...")
            # Mỗi video 1 proxy mới
            proxy_url = self._next_proxy()
            info = fetch_video_info_via_ytdlp(video_id, proxy_url=proxy_url)
            if not info:
                # Retry 1 lần với proxy khác
                proxy_url = self._next_proxy()
                self._mark_proxy_failed(proxy_url)
                info = fetch_video_info_via_ytdlp(video_id, proxy_url=proxy_url)
            if not info:
                continue
            # Merge RSS data (title, upload_date, thumbnail) voi yt-dlp info
            # yt-dlp info se co nhieu field hon (views, likes, duration, ...)
            video = self._build_video_from_ytdlp(info)
            detailed_videos.append(video)

        # Sort neu can
        if order == "viewCount":
            detailed_videos.sort(key=lambda v: v.view_count, reverse=True)
            detailed_videos = detailed_videos[:max_results]
        else:
            # Sort by upload_date desc (RSS da sort san, nhung chac chan)
            detailed_videos.sort(key=lambda v: v.published_at, reverse=True)
            detailed_videos = detailed_videos[:max_results]

        self._videos = detailed_videos

        if not self._videos:
            print("Khong lay duoc chi tiet video nao")
            return []

        print(f"Tim thay {len(self._videos)} video tu kenh '{self._videos[0].channel if self._videos else channel_input}'")
        return self._videos

    # ================= PHASE 1 API CACHE =================
    # Cache kết quả search.list (channel → list video_id) trong 30 ngày
    # Tiết kiệm ~100 units/lần fetch lại cùng channel

    @staticmethod
    def _phase1_cache_path() -> Path:
        """Folder cache Phase 1 API."""
        return Path(__file__).parent / ".phase1_cache"

    @staticmethod
    def _load_phase1_cache(channel_id: str, ttl_days: int = 30) -> Optional[list[str]]:
        """
        Load cache cho channel: trả về list video_ids, hoặc None nếu hết hạn / không có.
        Cache file: .phase1_cache/{channel_id}.json
        """
        cache_path = YouTubeResearcher._phase1_cache_path() / f"{channel_id}.json"
        if not cache_path.exists():
            return None
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            fetched_at = data.get("fetched_at", "")
            if not fetched_at:
                return None
            from datetime import datetime as _dt
            try:
                fetched_dt = _dt.fromisoformat(fetched_at.replace("Z", "+00:00"))
            except ValueError:
                return None
            age_days = (_dt.now(fetched_dt.tzinfo) - fetched_dt).days
            if age_days > ttl_days:
                print(f"  [Cache] Phase 1 cache hết hạn ({age_days} ngày > {ttl_days} ngày)")
                return None
            video_ids = data.get("video_ids", [])
            if video_ids:
                print(f"  [Cache] Phase 1 cache HIT: {len(video_ids)} video "
                      f"(age={age_days}d, file={cache_path.name})")
            return video_ids
        except Exception as e:
            print(f"  [Cache] Load Phase 1 cache error: {e}")
            return None

    @staticmethod
    def _save_phase1_cache(channel_id: str, video_ids: list[str]):
        """Lưu cache Phase 1: {fetched_at, video_ids}."""
        cache_path = YouTubeResearcher._phase1_cache_path() / f"{channel_id}.json"
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump({
                    "fetched_at": datetime.now().isoformat(),
                    "channel_id": channel_id,
                    "video_ids": video_ids,
                }, f, ensure_ascii=False)
        except Exception as e:
            print(f"  [Cache] Save Phase 1 cache error: {e}")

    @staticmethod
    def _video_details_cache_path() -> Path:
        """Folder cache video details (videos.list)."""
        return Path(__file__).parent / ".video_details_cache"

    @staticmethod
    def _load_video_details_cache(video_ids: list[str], ttl_days: int = 30) -> dict:
        """
        Load cache cho nhiều video_id cùng lúc.
        Trả về dict {video_id: info_dict}, chỉ chứa video còn cache hợp lệ.
        """
        cache_root = YouTubeResearcher._video_details_cache_path()
        if not cache_root.exists():
            return {}
        from datetime import datetime as _dt
        result = {}
        for vid in video_ids:
            cache_file = cache_root / f"{vid}.json"
            if not cache_file.exists():
                continue
            try:
                with open(cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                fetched_at = data.get("fetched_at", "")
                if not fetched_at:
                    continue
                try:
                    fetched_dt = _dt.fromisoformat(fetched_at.replace("Z", "+00:00"))
                except ValueError:
                    continue
                age_days = (_dt.now(fetched_dt.tzinfo) - fetched_dt).days
                if age_days > ttl_days:
                    continue
                result[vid] = data.get("info", {})
            except Exception:
                continue
        if result:
            print(f"  [Cache] Video details cache HIT: {len(result)}/{len(video_ids)} video")
        return result

    @staticmethod
    def _save_video_details_cache(video_ids: list[str], info_map: dict):
        """Lưu cache video details theo từng video_id."""
        cache_root = YouTubeResearcher._video_details_cache_path()
        cache_root.mkdir(parents=True, exist_ok=True)
        now_iso = datetime.now().isoformat()
        saved = 0
        for vid in video_ids:
            if vid not in info_map:
                continue
            cache_file = cache_root / f"{vid}.json"
            try:
                with open(cache_file, "w", encoding="utf-8") as f:
                    json.dump({
                        "fetched_at": now_iso,
                        "video_id": vid,
                        "info": info_map[vid],
                    }, f, ensure_ascii=False)
                saved += 1
            except Exception:
                continue
        if saved:
            print(f"  [Cache] Saved video details cache: {saved} video")

    def _get_uploads_playlist_id(self, channel_id: str) -> Optional[str]:
        """
        Lấy uploads playlist ID (UUxxx) từ channel ID (UCxxx).
        YouTube tự động tạo playlist UUxxx chứa tất cả video của channel UCxxx.
        """
        import requests as _requests
        if not _YOUTUBE_API_KEYS:
            return None
        try:
            for api_key in _YOUTUBE_API_KEYS:
                resp = _requests.get(
                    "https://www.googleapis.com/youtube/v3/channels",
                    params={"key": api_key, "id": channel_id, "part": "contentDetails"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get("items", [])
                    if items:
                        uploads_id = items[0].get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")
                        if uploads_id:
                            return uploads_id
                    return None
                elif resp.status_code == 403:
                    # Quota hết → thử key tiếp
                    continue
                else:
                    return None
        except Exception as e:
            print(f"  [API] get_uploads_playlist_id error: {e}")
        return None

    def _phase1_via_playlist(
        self,
        channel_id: str,
        max_results: int = 20000,
        order: str = "date",
        published_after: Optional[datetime] = None,
    ) -> tuple[list[str], bool]:
        """
        Phase 1 dùng playlistItems.list (uploads playlist) - KHONG cap 500 video.
        Nhanh: ~1-2s/100 video. Quota re: 1 unit/50 video (so voi search.list 100 units/50 video).

        Quota cost (20000 video):
            - channels.list: 1 unit (1 lan)
            - playlistItems.list: 400 units (400 * 50 = 20000 video)
            - videos.list: 400 units (lay statistics)
            - TONG: ~801 units / 1 lan fetch 20k video

        Args:
            channel_id: UCxxx
            max_results: so video toi da (khong gioi han, chi gioi han boi max_results)

        Returns:
            tuple (list[video_id], quota_exhausted)
        """
        import requests as _requests
        if not _YOUTUBE_API_KEYS:
            return [], False

        phase1_start = time.time()
        api_keys = list(_YOUTUBE_API_KEYS)
        current_key_idx = 0
        api_key = api_keys[current_key_idx]

        # ====== BƯỚC 1: Lấy uploads playlist ID ======
        uploads_id = self._get_uploads_playlist_id(channel_id)
        if not uploads_id:
            print(f"  [Phase 1 PL] Khong lay duoc uploads playlist ID cho {channel_id}")
            return [], False
        print(f"  [Phase 1 PL] Uploads playlist: {uploads_id}")

        # ====== BƯỚC 2: Paginate playlistItems.list de lay video_ids ======
        all_video_ids = []
        next_page_token = None
        quota_exhausted = False
        try:
            while len(all_video_ids) < max_results:
                params = {
                    "key": api_key,
                    "playlistId": uploads_id,
                    "part": "contentDetails",
                    "maxResults": min(50, max_results - len(all_video_ids)),
                }
                if next_page_token:
                    params["pageToken"] = next_page_token

                resp = _requests.get(
                    "https://www.googleapis.com/youtube/v3/playlistItems",
                    params=params,
                    timeout=15,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get("items", [])
                    for item in items:
                        vid = item.get("contentDetails", {}).get("videoId")
                        if vid:
                            all_video_ids.append(vid)
                    next_page_token = data.get("nextPageToken")
                    if not next_page_token:
                        break
                elif resp.status_code == 403:
                    # Quota exhausted → rotate
                    current_key_idx += 1
                    if current_key_idx < len(api_keys):
                        api_key = api_keys[current_key_idx]
                        print(f"  [Phase 1 PL] Key {current_key_idx} quota het, "
                              f"chuyen sang key {current_key_idx + 1}/{len(api_keys)}")
                        continue
                    else:
                        print(f"  [Phase 1 PL] TAT CA {len(api_keys)} key da het quota!")
                        quota_exhausted = True
                        break
                else:
                    print(f"  [Phase 1 PL] playlistItems HTTP {resp.status_code}: "
                          f"{resp.text[:100]}")
                    return all_video_ids, False
        except Exception as e:
            print(f"  [Phase 1 PL] playlistItems error: {e}")
            return all_video_ids, False

        phase1_time = time.time() - phase1_start
        print(f"  [Phase 1 PL] playlistItems: {len(all_video_ids)} video IDs "
              f"(in {phase1_time:.1f}s, ~{len(all_video_ids)/max(phase1_time,0.1):.0f} video/s)")

        return all_video_ids[:max_results], quota_exhausted

    def _phase1_via_api(
        self,
        channel_id: str,
        max_results: int = 500,
        order: str = "date",
        published_after: Optional[datetime] = None,
        pre_fetched_video_ids: Optional[list[str]] = None,
    ) -> tuple[list[dict], bool]:
        """
        Phase 1 dùng YouTube Data API v3 thay cho yt-dlp flat.
        Nhanh hơn ~3-5x: 5-10s vs 20-60s (yt-dlp).

        Quota cost:
            - search.list: 100 units/lần (max 50 video/page) - CHI goi neu KHONG co pre_fetched_video_ids
            - videos.list: 1 unit/lần (max 50 video/page)
            - VD: 200 video (search.list) = 4 search + 4 videos.list = 404 units
            - VD: 200 video (pre_fetched) = 0 search + 4 videos.list = 4 units

        Args:
            channel_id: UCxxx
            max_results: số video tối đa cần lấy
            order: 'date' (search.list order=date) | 'viewCount' (sort sau khi có stats)
            pre_fetched_video_ids: nếu đã có IDs (vd từ playlistItems), truyền vào
                                   để skip search.list (tiết kiệm 100 units/page)

        Returns:
            tuple (list[dict], quota_exhausted):
                - list[dict]: giống format yt-dlp info, có thể < max_results nếu quota exhausted
                - quota_exhausted: True nếu TẤT CẢ API keys bị 403 (để caller fallback yt-dlp)
        """
        import requests as _requests

        if not _YOUTUBE_API_KEYS:
            print("  [Phase 1 API] Không có YOUTUBE_API_KEY → skip")
            return [], False

        # Cap ở 500 (giới hạn search.list pagination) - CHI áp dụng khi KHONG có pre_fetched
        api_max = 500 if pre_fetched_video_ids is None else max_results
        api_target = min(max_results, api_max)
        if max_results > api_max and pre_fetched_video_ids is None:
            print(f"  [Phase 1 API] max_results={max_results} > 500 → cap ở 500")
        max_results = api_target

        phase1_start = time.time()
        all_video_ids = []

        # Nếu có pre_fetched_video_ids → dùng luôn, skip search.list
        if pre_fetched_video_ids is not None:
            all_video_ids = list(pre_fetched_video_ids)[:max_results]
            print(f"  [Phase 1 API] Dùng pre_fetched IDs: {len(all_video_ids)} video "
                  f"(skip search.list, tiết kiệm quota)")
        else:
            # ====== BƯỚC 1: search.list để lấy video IDs (CÓ KEY ROTATION) ======
            # Khi 1 key bị 403 (quota) → rotate sang key tiếp theo
            api_keys = list(_YOUTUBE_API_KEYS)
            current_key_idx = 0
            api_key = api_keys[current_key_idx]
            quota_exhausted = False
            try:
                pages_needed = (api_target + 49) // 50  # ceil
                next_page_token = None
                for page_idx in range(pages_needed):
                    params = {
                        "key": api_key,
                        "channelId": channel_id,
                        "part": "id",
                        "type": "video",
                        "order": "date",
                        "maxResults": min(50, api_target - len(all_video_ids)),
                    }
                    if next_page_token:
                        params["pageToken"] = next_page_token

                    resp = _requests.get(
                        "https://www.googleapis.com/youtube/v3/search",
                        params=params,
                        timeout=15,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        items = data.get("items", [])
                        for item in items:
                            vid = item.get("id", {}).get("videoId")
                            if vid:
                                all_video_ids.append(vid)
                        next_page_token = data.get("nextPageToken")
                        if not next_page_token or len(all_video_ids) >= api_target:
                            break
                    elif resp.status_code == 403:
                        # Quota exhausted cho key này → rotate
                        current_key_idx += 1
                        if current_key_idx < len(api_keys):
                            api_key = api_keys[current_key_idx]
                            print(f"  [Phase 1 API] Key {current_key_idx} quota het, "
                                  f"chuyen sang key {current_key_idx + 1}/{len(api_keys)}")
                            continue  # retry với key mới
                        else:
                            print(f"  [Phase 1 API] TAT CA {len(api_keys)} key da het quota!")
                            quota_exhausted = True
                            break
                    else:
                        print(f"  [Phase 1 API] search.list HTTP {resp.status_code}: "
                              f"{resp.text[:100]}")
                        return [], False
            except Exception as e:
                print(f"  [Phase 1 API] search.list error: {e}")
                return [], False

            if not all_video_ids:
                if quota_exhausted:
                    print(f"  [Phase 1 API] Tất cả key hết quota, search.list trả rỗng")
                    return [], True
                print(f"  [Phase 1 API] search.list trả rỗng")
                return [], False

            print(f"  [Phase 1 API] search.list: {len(all_video_ids)} video IDs "
                  f"(in {time.time()-phase1_start:.1f}s)")

            # Lưu cache Phase 1
            self._save_phase1_cache(channel_id, all_video_ids)

            # Nếu quota exhausted mid-way, return luôn (không fetch videos.list)
            if quota_exhausted:
                print(f"  [Phase 1 API] Quota exhausted → skip videos.list, "
                      f"caller sẽ fallback yt-dlp")
                return [], True

        # ====== BƯỚC 2: videos.list để lấy statistics + contentDetails (CÓ KEY ROTATION) =====
        cached_details = self._load_video_details_cache(all_video_ids, ttl_days=30)
        missing_ids = [vid for vid in all_video_ids if vid not in cached_details]
        new_info_map = {}

        all_entries = []
        for vid in all_video_ids:
            if vid in cached_details:
                all_entries.append(cached_details[vid])

        if missing_ids:
            api_keys = list(_YOUTUBE_API_KEYS)
            current_key_idx = 0
            api_key = api_keys[current_key_idx]
            quota_exhausted = False
            try:
                for batch_start in range(0, len(missing_ids), 50):
                    if quota_exhausted:
                        break
                    batch_end = min(batch_start + 50, len(missing_ids))
                    batch_ids = missing_ids[batch_start:batch_end]
                    params = {
                        "key": api_key,
                        "id": ",".join(batch_ids),
                        "part": "snippet,statistics,contentDetails,status",
                    }
                    resp = _requests.get(
                        "https://www.googleapis.com/youtube/v3/videos",
                        params=params,
                        timeout=15,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        for item in data.get("items", []):
                            info = self._api_item_to_ytdlp_dict(item)
                            vid = info.get("id", "")
                            upload_date = info.get("upload_date", "")
                            if published_after and upload_date and len(upload_date) == 8:
                                try:
                                    vd = datetime.strptime(upload_date, "%Y%m%d")
                                    if published_after.tzinfo:
                                        vd = vd.replace(tzinfo=published_after.tzinfo)
                                    if vd < published_after:
                                        continue
                                except ValueError:
                                    pass
                            all_entries.append(info)
                            new_info_map[vid] = info
                    elif resp.status_code == 403:
                        # Quota exhausted → rotate
                        current_key_idx += 1
                        if current_key_idx < len(api_keys):
                            api_key = api_keys[current_key_idx]
                            print(f"  [Phase 1 API] videos.list: Key {current_key_idx} quota het, "
                                  f"chuyen sang key {current_key_idx + 1}/{len(api_keys)}")
                            batch_start -= 50  # retry batch này
                            continue
                        else:
                            print(f"  [Phase 1 API] TAT CA {len(api_keys)} key da het quota "
                                  f"(videos.list)!")
                            quota_exhausted = True
                            break
                    else:
                        print(f"  [Phase 1 API] videos.list HTTP {resp.status_code}")
                        return all_entries, False
            except Exception as e:
                print(f"  [Phase 1 API] videos.list error: {e}")
                return all_entries, False

            if new_info_map:
                self._save_video_details_cache(list(new_info_map.keys()), new_info_map)

            # Nếu quota exhausted, return entries đã có + signal True
            if quota_exhausted:
                return all_entries, True

        phase1_time = time.time() - phase1_start
        print(f"  [Phase 1 API] Done: {len(all_entries)} video trong {phase1_time:.1f}s "
              f"({len(all_entries)/max(phase1_time,0.1):.0f} video/s)")

        # Sort theo order
        if order == "viewCount":
            all_entries.sort(key=lambda v: v.get("view_count", 0), reverse=True)
        all_entries = all_entries[:max_results]

        return all_entries, False

    def _early_filter_videos(self, videos: list, min_duration: int = 600,
                              max_duration: int = None,
                              min_view: int = 0) -> list:
        """
        Lọc SỚM video dựa trên metadata NHẸ (duration, view).
        Mục đích: giảm số video phải tải audio về sau này.

        KHÔNG lọc theo transcript (cần gọi youtube-transcript-api tốn time).
        KHÔNG lọc theo keyword (sẽ làm ở phase sau).
        """
        filtered = []
        for v in videos:
            # Parse duration từ VideoCandidate (ISO 8601 format)
            dur_sec = parse_duration(v.duration) if v.duration else 0

            # Duration filter
            if dur_sec < min_duration:
                continue
            if max_duration and dur_sec > max_duration:
                continue

            # View count filter (giữ nguyên FILTER_MIN_VIEW_COUNT = 50)
            if v.view_count < min_view:
                continue

            filtered.append(v)
        return filtered

    def fetch_channel_videos(
        self,
        channel_input: str,
        max_results: int = 20000,
        order: str = "date",
        published_after: Optional[datetime] = None,
        batch_size: int = 200,
        max_batches: int = 100,
        published_before_cursor: Optional[datetime] = None,
        socket_timeout: int = 60,
        fetch_delay: int = 5,
        max_retries: int = 5,
    ) -> list[VideoCandidate]:
        """
        Lấy video từ kênh YouTube bằng yt-dlp theo 2-PHASE approach:

        Phase 1: extract_flat=True - lấy NHANH toàn bộ listing (id + duration + timestamp).
                 ~80s cho 10000 video. Dùng yt-dlp continuation token pagination (hoạt động đúng).
                 Filter sơ bộ theo duration.

        Phase 2: extract full metadata theo batch cho các video đã pass duration filter.
                 Lấy view_count, upload_date, likes, channel, subtitles...
                 Filter tiếp theo view_count, published_after, etc.

        Args:
            channel_input: URL kênh hoặc channel ID
            max_results: Số video tối đa trả về (sau khi fetch + filter)
            order: 'date' (upload_date) | 'viewCount' (view_count) | 'relevance' (default)
            published_after: Chỉ lấy video sau ngày này (format ISO)
            batch_size: Số video extract full metadata mỗi batch (Phase 2)
            max_batches: Số batch tối đa cho Phase 2
            socket_timeout: Timeout cho mỗi yt-dlp request (giây)
            fetch_delay: Delay giữa các batch (giây) để tránh YouTube rate limit
            max_retries: Số lần retry khi 1 video fail
        """
        try:
            import yt_dlp
        except ImportError:
            print("pip install yt-dlp")
            sys.exit(1)

        # Resolve channel ID (direct-first, fallback proxy)
        t0 = time.time()
        proxy_url = self._proxy_for_fallback()
        channel_id = resolve_channel_id(self.api_key, channel_input, proxy_url=proxy_url)
        if not channel_id and not proxy_url:
            self._escalate_to_proxy()
            proxy_url = self._next_proxy()
            channel_id = resolve_channel_id(self.api_key, channel_input, proxy_url=proxy_url)
        if not channel_id:
            print(f"Khong tim thay kenh: {channel_input}")
            return []
        print(f"  [TIMING] resolve_channel_id: {time.time()-t0:.1f}s, ID={channel_id}")

        print(f"Channel ID: {channel_id}")

        # ====== PHASE 1 (API): dùng YouTube Data API v3 nếu có key ======
        # Dùng playlistItems.list (uploads playlist) - KHONG cap 500 video
        # Quota: 1 unit/50 video (playlistItems) + 1 unit/50 video (videos.list)
        if _YOUTUBE_API_KEYS:
            print(f"\n  [Phase 1] Trying YouTube Data API v3 (playlistItems, target={max_results})...")

            # BƯỚC A: Lấy video_ids qua playlistItems (KHÔNG cap)
            video_ids, quota_exhausted = self._phase1_via_playlist(
                channel_id=channel_id,
                max_results=max_results,
                order=order,
                published_after=published_after,
            )

            if video_ids and not quota_exhausted:
                # BƯỚC B: Lấy statistics + contentDetails qua videos.list
                api_entries, qe_videos = self._phase1_via_api(
                    channel_id=channel_id,
                    max_results=len(video_ids),  # Lấy hết video_ids
                    order=order,
                    published_after=published_after,
                    pre_fetched_video_ids=video_ids,  # Dùng IDs đã có
                )

                if api_entries:
                    print(f"\nBuild {len(api_entries)} VideoCandidate (from API)...")
                    detailed_videos = []
                    for i, info in enumerate(api_entries, 1):
                        try:
                            video = self._build_video_from_ytdlp(info)
                            detailed_videos.append(video)
                        except Exception as e:
                            print(f"  [{i}] Build failed: {e}")
                            continue

                    # === LỌC SỚM NGAY SAU KHI CÓ METADATA NHẸ ===
                    # Lọc theo duration, view TRƯỚC khi trả về cho pipeline tải audio
                    pre_filter_count = len(detailed_videos)
                    detailed_videos = self._early_filter_videos(
                        detailed_videos,
                        min_duration=FILTER_MIN_DURATION,
                        max_duration=FILTER_MAX_DURATION,
                        min_view=FILTER_MIN_VIEW_COUNT,
                    )
                    print(f"  [Early filter] {pre_filter_count} → {len(detailed_videos)} video "
                          f"(loại {pre_filter_count - len(detailed_videos)} video ngắn/ít view, "
                          f"giữ nguyên min_view={FILTER_MIN_VIEW_COUNT})")

                    if order == "viewCount":
                        detailed_videos.sort(key=lambda v: v.view_count, reverse=True)
                    detailed_videos = detailed_videos[:max_results]

                    self._videos = detailed_videos
                    if self._videos:
                        print(f"Tim thay {len(self._videos)} video tu kenh "
                              f"'{self._videos[0].channel if self._videos else channel_input}' "
                              f"(via API playlist, KHONG qua yt-dlp listing)")
                    return self._videos

            # Nếu quota exhausted HOẶC API trả rỗng → fallback yt-dlp
            if quota_exhausted:
                print(f"  [Phase 1] API quota exhausted → fallback yt-dlp cho "
                      f"{max_results} video")
            else:
                print("  [Phase 1] API fail → fallback yt-dlp flat")
            if channel_input.startswith("UC") and len(channel_input) == 24:
                channel_url = f"https://www.youtube.com/channel/{channel_id}/videos"
            else:
                url = channel_input.strip().rstrip("/")
                channel_url = url + "/videos" if not url.endswith("/videos") else url
        else:
            # Không có API key → dùng yt-dlp luôn
            if channel_input.startswith("UC") and len(channel_input) == 24:
                channel_url = f"https://www.youtube.com/channel/{channel_id}/videos"
            else:
                url = channel_input.strip().rstrip("/")
                channel_url = url + "/videos" if not url.endswith("/videos") else url

        print(f"Fetching from: {channel_url}")

        # ====== PHASE 1: extract_flat=True - lấy nhanh toàn bộ listing ======
        # yt-dlp dùng continuation token pagination nội bộ → lấy được toàn bộ video
        # Chỉ trả về: id, title, duration, timestamp, url, thumbnails
        # Rất nhanh: ~80s cho 10000 video
        print(f"\n  [Phase 1] Fetching listing (extract_flat=True, target={max_results})...")
        phase1_start = time.time()

        ydl_listing_opts = {
            "quiet": True,
            "no_warnings": True,
            "extract_flat": True,
            "skip_download": True,
            "playlistend": max_results,
            "ignoreerrors": True,
            "js_runtimes": {"node": {}},
        }
        YouTubeResearcher._apply_auth_skip(ydl_listing_opts)
        # Cũng skip authcheck cho youtubetab (channel listing)
        ydl_listing_opts["extractor_args"].setdefault("youtubetab", {}).setdefault("skip", []).append("authcheck")
        YouTubeResearcher._apply_cookies(ydl_listing_opts)
        self._apply_timeouts(ydl_listing_opts, socket_timeout=socket_timeout)
        listing_proxy = self._proxy_for_fallback()
        if listing_proxy:
            ydl_listing_opts["proxy"] = listing_proxy

        flat_entries = []
        last_err = None
        # TOI UU: max_retries=2 thay vi 5 (mac dinh)
        # Cu: 5 retry × exponential backoff (4+8+16+32=60s sleep)
        # Moi: 2 retry × 3s sleep = 6s sleep
        # Phase 1 chi can lay LISTING (id, title, duration), fail thi fallback Phase 2 van chay duoc
        phase1_max_retries = 2
        for attempt in range(1, phase1_max_retries + 1):
            attempt_start = time.time()
            try:
                with yt_dlp.YoutubeDL(ydl_listing_opts) as ydl:
                    flat_info = ydl.extract_info(channel_url, download=False)
                attempt_time = time.time() - attempt_start
                if flat_info and "entries" in flat_info:
                    flat_entries = [e for e in flat_info["entries"] if e]
                else:
                    flat_entries = []
                if attempt > 1:
                    print(f"  [Phase 1] retry {attempt}/{phase1_max_retries} OK ({attempt_time:.1f}s)")
                else:
                    print(f"  [Phase 1] attempt 1 done in {attempt_time:.1f}s, {len(flat_entries)} entries")
                break
            except Exception as e:
                last_err = e
                err_msg = str(e).lower()
                print(f"  [Phase 1] attempt {attempt}/{phase1_max_retries} fail: "
                      f"{type(e).__name__}: {str(e)[:200]}")
                is_blocked = any(k in err_msg for k in [
                    '429', 'too many', 'rate limit', 'forbidden',
                    '403', 'blocked', 'sign in', 'bot',
                ])
                if is_blocked and not listing_proxy:
                    self._escalate_to_proxy()
                if listing_proxy:
                    # Phân loại: proxy chết thật → xóa vĩnh viễn
                    #             rate limit → chỉ cooldown
                    if is_proxy_dead_error(e):
                        self._mark_proxy_dead(listing_proxy)
                    else:
                        self._mark_proxy_failed(listing_proxy)
                if attempt < phase1_max_retries:
                    listing_proxy = self._next_proxy()
                    if listing_proxy:
                        ydl_listing_opts["proxy"] = listing_proxy
                    elif "proxy" in ydl_listing_opts:
                        del ydl_listing_opts["proxy"]
                    # Sleep cố định 3s thay vì exponential (4s, 8s, 16s, 32s)
                    time.sleep(3)

        phase1_time = time.time() - phase1_start

        if not flat_entries:
            print(f"  [Phase 1] FAIL - khong lay duoc listing ({last_err})")
            return []

        print(f"  [Phase 1] Done: {len(flat_entries)} video trong {phase1_time:.1f}s "
              f"({len(flat_entries)/max(phase1_time,0.1):.0f} video/s)")

        # Filter sơ bộ theo duration (có sẵn từ extract_flat)
        min_dur = FILTER_MIN_DURATION
        max_dur = FILTER_MAX_DURATION
        pre_filter = []
        for e in flat_entries:
            dur = e.get("duration") or 0
            if isinstance(dur, (int, float)):
                if dur < min_dur:
                    continue
                if max_dur and dur > max_dur:
                    continue
            pre_filter.append(e)

        print(f"  [Phase 1] Duration filter: {len(pre_filter)}/{len(flat_entries)} passed "
              f"(min={min_dur}s, max={max_dur}s)")

        if not pre_filter:
            print("Khong tim thay video nao pass duration filter")
            return []

        # ====== PHASE 2: YouTube Data API v3 (batch 50 video/request) ======
        # Nhanh hơn yt-dlp extract từng video: 20k video trong ~2-3 phút
        # Fallback về yt-dlp concurrent nếu không có API key
        # Key rotation: khi 1 key bị 403 quota → tự chuyển key tiếp theo
        import requests as _requests

        api_keys = list(_YOUTUBE_API_KEYS)  # copy để rotate

        if not api_keys:
            print(f"\n  [Phase 2] YOUTUBE_API_KEY not set -> fallback ve yt-dlp concurrent")
            all_entries = self._phase2_ytdlp_concurrent(
                pre_filter, published_after, max_results,
                batch_size, fetch_delay, max_retries, socket_timeout, yt_dlp,
            )
        else:
            current_key_idx = 0
            api_key = api_keys[current_key_idx]
            print(f"\n  [Phase 2] YouTube Data API v3 (batch=50, total={len(pre_filter)} video, "
                  f"{len(api_keys)} key(s) available)...")
            phase2_start = time.time()

            all_entries = []
            failed_count = 0
            api_batch_size = 50  # YouTube API max

            for batch_start in range(0, len(pre_filter), api_batch_size):
                batch_end = min(batch_start + api_batch_size, len(pre_filter))
                batch_items = pre_filter[batch_start:batch_end]
                video_ids = [e.get("id") for e in batch_items if e.get("id")]

                if not video_ids:
                    continue

                # Gọi YouTube Data API v3 với key rotation
                url = "https://www.googleapis.com/youtube/v3/videos"
                resp = None
                for attempt in range(1, max_retries + 1):
                    params = {
                        "key": api_key,
                        "id": ",".join(video_ids),
                        "part": "snippet,statistics,contentDetails,status,topicDetails",
                    }
                    try:
                        resp = _requests.get(url, params=params, timeout=15)
                        if resp.status_code == 200:
                            break
                        elif resp.status_code == 403:
                            # Quota hết → rotate sang key tiếp
                            current_key_idx += 1
                            if current_key_idx < len(api_keys):
                                api_key = api_keys[current_key_idx]
                                print(f"  [API] Key {current_key_idx} quota het, "
                                      f"chuyen sang key {current_key_idx + 1}/{len(api_keys)}")
                                resp = None  # retry với key mới
                                continue
                            else:
                                print(f"  [API] Tat ca {len(api_keys)} key da het quota!")
                                break
                        elif resp.status_code == 429:
                            wait = 5 * attempt
                            print(f"  [API] HTTP 429 - rate limited, sleep {wait}s")
                            time.sleep(wait)
                        else:
                            print(f"  [API] HTTP {resp.status_code}: {resp.text[:100]}")
                            break
                    except Exception as e:
                        if attempt < max_retries:
                            time.sleep(2 ** attempt)
                        else:
                            print(f"  [API] request failed: {e}")

                if not resp or resp.status_code != 200:
                    failed_count += len(video_ids)
                    # Nếu tất cả key hết quota → dừng
                    if current_key_idx >= len(api_keys):
                        print(f"  [Phase 2] All API keys exhausted, stopping.")
                        break
                    continue

                data = resp.json()
                items = data.get("items", [])

                for item in items:
                    info = self._api_item_to_ytdlp_dict(item)

                    # Filter published_after
                    upload_date = info.get("upload_date", "")
                    if published_after and upload_date and len(upload_date) == 8:
                        try:
                            vd = datetime.strptime(upload_date, "%Y%m%d")
                            if published_after.tzinfo:
                                vd = vd.replace(tzinfo=published_after.tzinfo)
                            if vd < published_after:
                                continue
                        except ValueError:
                            pass

                    all_entries.append(info)

                # Progress log
                processed = min(batch_end, len(pre_filter))
                elapsed = time.time() - phase2_start
                rate = processed / max(elapsed, 0.1)
                eta = (len(pre_filter) - processed) / max(rate, 0.1)
                print(f"  [Phase 2] [{processed}/{len(pre_filter)}] "
                      f"ok={len(all_entries)} fail={failed_count} "
                      f"({elapsed:.0f}s, {rate:.1f} v/s, ETA ~{eta:.0f}s)")

                if len(all_entries) >= max_results:
                    print(f"  [Phase 2] Da lay du {max_results} video -> dung.")
                    break

            phase2_time = time.time() - phase2_start
            print(f"  [Phase 2] Done: {len(all_entries)} video trong {phase2_time:.1f}s "
                  f"(failed: {failed_count}, rate: {len(all_entries)/max(phase2_time,0.1):.1f} v/s)")

        # === LỌC SỚM SAU videos.list (trước khi build VideoCandidate) ===
        if all_entries:
            pre_count = len(all_entries)
            all_entries = [
                e for e in all_entries
                if int(e.get("view_count") or 0) >= FILTER_MIN_VIEW_COUNT
                and FILTER_MIN_DURATION <= (e.get("duration") or 0) <= FILTER_MAX_DURATION
            ]
            print(f"  [Early filter] {pre_count} → {len(all_entries)} video "
                  f"(loại {pre_count - len(all_entries)} video, "
                  f"giữ nguyên min_view={FILTER_MIN_VIEW_COUNT})")

        # ====== BUILD VideoCandidate ======
        if not all_entries:
            print("Khong tim thay video nao trong kenh nay (sau early filter)")
            return []

        print(f"\nBuild {len(all_entries)} VideoCandidate...")
        detailed_videos = []
        for i, info in enumerate(all_entries, 1):
            try:
                video = self._build_video_from_ytdlp(info)
                detailed_videos.append(video)
            except Exception as e:
                print(f"  [{i}] Build failed: {e}")
                continue

        # Sort theo order
        if order == "viewCount":
            detailed_videos.sort(key=lambda v: v.view_count, reverse=True)
        detailed_videos = detailed_videos[:max_results]

        self._videos = detailed_videos

        if not self._videos:
            print("Khong lay duoc chi tiet video nao")
            return []

        print(f"Tim thay {len(self._videos)} video tu kenh "
              f"'{self._videos[0].channel if self._videos else channel_input}'")
        return self._videos

    def _api_item_to_ytdlp_dict(self, item: dict) -> dict:
        """Convert YouTube Data API v3 video item → yt-dlp compatible dict."""
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})
        status = item.get("status", {})
        topic = item.get("topicDetails", {})

        # Parse ISO 8601 duration (PT1H2M3S) → seconds
        duration_iso = content.get("duration", "PT0S")
        duration_secs = 0
        import re as _re
        m = _re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration_iso)
        if m:
            h, mn, s = (int(x) if x else 0 for x in m.groups())
            duration_secs = h * 3600 + mn * 60 + s

        # Parse publishedAt → YYYYMMDD
        pub_at = snippet.get("publishedAt", "")
        upload_date = ""
        if pub_at and len(pub_at) >= 10:
            upload_date = pub_at[:10].replace("-", "")

        # Thumbnails
        thumbs = snippet.get("thumbnails", {})
        thumbnail_url = ""
        for key in ("high", "medium", "default"):
            if key in thumbs:
                thumbnail_url = thumbs[key].get("url", "")
                break

        vid = item.get("id", "")
        return {
            "id": vid,
            "title": snippet.get("title", ""),
            "channel": snippet.get("channelTitle", ""),
            "channel_id": snippet.get("channelId", ""),
            "description": snippet.get("description", ""),
            "upload_date": upload_date,
            "duration": duration_secs,
            "view_count": int(stats.get("viewCount", 0)),
            "like_count": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
            "tags": snippet.get("tags", []),
            "categories": [snippet.get("categoryId", "")],
            "thumbnails": [{"url": thumbnail_url, "id": "high"}] if thumbnail_url else [],
            "webpage_url": f"https://www.youtube.com/watch?v={vid}",
            "subtitles": {},
            "automatic_captions": {},
            "language": snippet.get("defaultLanguage", ""),
            "audio_language": snippet.get("defaultAudioLanguage", ""),
            "height": 1080 if content.get("definition") == "hd" else 480,
            "availability": "public" if status.get("privacyStatus") == "public" else "",
            "playable_in_embed": status.get("embeddable", True),
            "is_live": snippet.get("liveBroadcastContent") == "live",
            "live_status": snippet.get("liveBroadcastContent", "none"),
            "was_live": snippet.get("liveBroadcastContent") == "completed",
            "license": content.get("licensedContent", False),
            "topic_categories": topic.get("topicCategories", []),
        }

    def _phase2_ytdlp_concurrent(
        self, pre_filter, published_after, max_results,
        batch_size, fetch_delay, max_retries, socket_timeout, yt_dlp,
    ) -> list:
        """Fallback Phase 2: yt-dlp concurrent extraction khi không có API key."""
        import concurrent.futures
        import shutil
        import tempfile

        concurrent_workers = 15
        print(f"  [Phase 2 fallback] yt-dlp concurrent (workers={concurrent_workers})...")
        phase2_start = time.time()

        _cookies_copies: list = []
        if COOKIES_FILE_STR:
            _cookies_tmp_dir = tempfile.mkdtemp(prefix="ytdlp_cookies_")
            try:
                with open(COOKIES_FILE_STR, "r", encoding="utf-8") as f:
                    cookies_content = f.read()
            except Exception:
                cookies_content = None
            if cookies_content and cookies_content.strip().startswith("# Netscape"):
                for i in range(concurrent_workers):
                    dst = os.path.join(_cookies_tmp_dir, f"cookies_{i}.txt")
                    with open(dst, "w", encoding="utf-8") as f:
                        f.write(cookies_content)
                    _cookies_copies.append(dst)
        else:
            _cookies_tmp_dir = None

        _thread_local = threading.local()
        _worker_id_lock = threading.Lock()
        _next_worker_id = [0]

        def _get_worker_cookies():
            if not _cookies_copies:
                return None
            if not hasattr(_thread_local, 'cookies_path'):
                with _worker_id_lock:
                    wid = _next_worker_id[0] % len(_cookies_copies)
                    _next_worker_id[0] += 1
                _thread_local.cookies_path = _cookies_copies[wid]
            return _thread_local.cookies_path

        def _extract_one(video_id):
            worker_cookies = _get_worker_cookies()
            for attempt in range(1, max_retries + 1):
                try:
                    # QUAN TRỌNG: wrap TOÀN BỘ request trong guard
                    # để VPN rotate KHÔNG kill tunnel giữa chừng
                    with self._proxy_guard():
                        # Gọi _next_proxy() BÊN TRONG guard (an toàn)
                        proxy = self._next_proxy()
                        ydl_opts = {
                            "quiet": True, "no_warnings": True,
                            "skip_download": True, "ignoreerrors": True,
                            "js_runtimes": {"node": {}},
                        }
                        YouTubeResearcher._apply_auth_skip(ydl_opts)
                        if worker_cookies:
                            ydl_opts["cookiefile"] = worker_cookies
                        self._apply_timeouts(ydl_opts, socket_timeout=socket_timeout)
                        if proxy:
                            ydl_opts["proxy"] = proxy
                        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                            info = ydl.extract_info(
                                f"https://www.youtube.com/watch?v={video_id}", download=False)
                        if info:
                            return info
                except Exception:
                    if attempt < max_retries:
                        time.sleep(min(2 ** attempt, 8))
            return None

        all_entries = []
        failed_count = 0
        processed_count = 0

        for batch_start in range(0, len(pre_filter), batch_size):
            batch_end = min(batch_start + batch_size, len(pre_filter))
            batch_items = pre_filter[batch_start:batch_end]
            if batch_start > 0 and fetch_delay > 0:
                time.sleep(fetch_delay)
            video_ids = [e.get("id") for e in batch_items if e.get("id")]
            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrent_workers) as executor:
                future_map = {executor.submit(_extract_one, vid): vid for vid in video_ids}
                for future in concurrent.futures.as_completed(future_map):
                    processed_count += 1
                    try:
                        info = future.result()
                    except Exception:
                        info = None
                    if not info:
                        failed_count += 1
                        continue
                    upload_date = info.get("upload_date", "")
                    if published_after and upload_date and len(upload_date) == 8:
                        try:
                            vd = datetime.strptime(upload_date, "%Y%m%d")
                            if published_after.tzinfo:
                                vd = vd.replace(tzinfo=published_after.tzinfo)
                            if vd < published_after:
                                continue
                        except ValueError:
                            pass
                    all_entries.append(info)
            elapsed = time.time() - phase2_start
            rate = processed_count / max(elapsed, 0.1)
            eta = (len(pre_filter) - processed_count) / max(rate, 0.1)
            print(f"  [Phase 2] [{processed_count}/{len(pre_filter)}] "
                  f"ok={len(all_entries)} fail={failed_count} "
                  f"({elapsed:.0f}s, {rate:.1f} v/s, ETA ~{eta:.0f}s)")
            if len(all_entries) >= max_results:
                break

        if _cookies_tmp_dir:
            try:
                shutil.rmtree(_cookies_tmp_dir, ignore_errors=True)
            except Exception:
                pass

        return all_entries

    def _build_video_from_ytdlp(self, info: dict) -> VideoCandidate:
        """Build VideoCandidate tu yt-dlp info dict - lay TOI DA thong tin."""
        # Duration: yt-dlp tra ve giay (int) -> chuyen sang ISO 8601
        duration_secs = info.get("duration") or 0
        if isinstance(duration_secs, (int, float)):
            duration_iso = f"PT{int(duration_secs)}S"
        else:
            duration_iso = ""

        # Upload date: YYYYMMDD -> ISO 8601
        upload_date = info.get("upload_date", "")  # YYYYMMDD
        if upload_date and len(upload_date) == 8:
            published_at = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}T00:00:00Z"
        else:
            published_at = ""

        # Thumbnail
        thumbs = info.get("thumbnails") or []
        thumbnail = ""
        for t in thumbs:
            if t.get("id") in ("high", "medium", "default"):
                thumbnail = t.get("url", "")
                break
        if not thumbnail and thumbs:
            thumbnail = thumbs[0].get("url", "")

        # Tags
        tags = info.get("tags") or []

        # Categories - LAY TAT CA (khong chi first)
        categories = info.get("categories") or []
        category_id = categories[0] if categories else ""

        # Caption (subtitles available?)
        subtitles = info.get("subtitles") or {}
        auto_captions = info.get("automatic_captions") or {}
        caption_available = bool(subtitles) or bool(auto_captions)

        # Like / comment / view count
        like_count = info.get("like_count") or 0
        comment_count = info.get("comment_count") or 0
        view_count = info.get("view_count") or 0

        # Release year
        release_year = 0
        if upload_date and len(upload_date) == 8:
            try:
                release_year = int(upload_date[:4])
            except ValueError:
                pass

        video = VideoCandidate(
            # Core fields
            video_id=info.get("id", ""),
            title=info.get("title", ""),
            channel=info.get("channel") or info.get("uploader") or "",
            description=info.get("description", ""),
            published_at=published_at,
            duration=duration_iso,
            duration_string=info.get("duration_string", ""),
            view_count=int(view_count),
            like_count=int(like_count),
            comment_count=int(comment_count),
            url=info.get("webpage_url") or f"https://www.youtube.com/watch?v={info.get('id', '')}",
            tags=tags,
            categories=categories,
            category_id=category_id,
            default_language=info.get("language", ""),
            default_audio_language=info.get("audio_language", ""),
            caption_available=caption_available,
            automatic_captions=auto_captions,
            definition="hd" if info.get("height", 0) >= 720 else "sd",
            dimension="2d",
            licensed_content=bool(info.get("license")),
            projection="rectangular",
            privacy_status="public" if info.get("availability") == "public" else "",
            embeddable=info.get("playable_in_embed", True),
            made_for_kids=False,  # yt-dlp khong cung cap
            live_broadcast_content="is_live" if info.get("is_live") else "none",
            live_status=info.get("live_status", ""),
            was_live=info.get("was_live", False),
            topic_categories=[],  # yt-dlp khong co
            thumbnail=thumbnail,

            # Extra yt-dlp fields
            channel_id=info.get("channel_id", ""),
            channel_url=info.get("channel_url", ""),
            channel_follower_count=int(info.get("channel_follower_count") or 0),
            uploader=info.get("uploader", ""),
            uploader_id=info.get("uploader_id", ""),
            uploader_url=info.get("uploader_url", ""),
            location=info.get("location", ""),
            width=int(info.get("width") or 0),
            height=int(info.get("height") or 0),
            fps=float(info.get("fps") or 0.0),
            vcodec=info.get("vcodec", ""),
            acodec=info.get("acodec", ""),
            tbr=float(info.get("tbr") or 0.0),
            abr=float(info.get("abr") or 0.0),
            vbr=float(info.get("vbr") or 0.0),
            filesize_approx=int(info.get("filesize_approx") or 0),
            release_year=release_year,
            release_date=upload_date,
            age_limit=int(info.get("age_limit") or 0),
            playable_in_embed=info.get("playable_in_embed", True),
            chapters=info.get("chapters") or [],
            heatmap=info.get("heatmap") or [],
            aspect_ratio=float(info.get("aspect_ratio") or 0.0),
        )

        return video

    # ================= COMMENTS =================

    def _fetch_top_comments(self, youtube, video_id, max_comments=5):
        """
        Lay top comments qua yt-dlp (khong can API key).
        """
        try:
            import yt_dlp
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "ignoreerrors": True,
                "getcomments": True,
                "extractor_args": {"youtube": {"max_comments": [str(max_comments)]}},
                "js_runtimes": {"node": {}},
            }
            YouTubeResearcher._apply_auth_skip(ydl_opts)
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(f"https://www.youtube.com/watch?v={video_id}", download=False)
            if info:
                comments = info.get("comments") or []
                return [
                    c.get("text", "") if isinstance(c, dict) else str(c)
                    for c in comments[:max_comments]
                ]
        except Exception:
            pass
        return []

    # ================= FILTER =================

    def apply_filters(self, criteria: FilterCriteria) -> list[VideoCandidate]:
        self._filtered_videos = []

        for video in self._videos:
            video.failed_filters = []
            video.passed_filters = []

            duration_secs = parse_duration(video.duration)

            if criteria.min_duration and duration_secs < criteria.min_duration:
                video.failed_filters.append("duration_too_short")

            if criteria.max_duration and duration_secs > criteria.max_duration:
                video.failed_filters.append("duration_too_long")

            if video.view_count < criteria.min_view_count:
                video.failed_filters.append("view_count_low")

            if video.like_count < criteria.min_like_count:
                video.failed_filters.append("like_count_low")

            if video.comment_count < criteria.min_comment_count:
                video.failed_filters.append("comment_count_low")

            if criteria.published_after:
                try:
                    pub_date = datetime.fromisoformat(
                        video.published_at.replace("Z", "+00:00")
                    )
                    if pub_date < criteria.published_after:
                        video.failed_filters.append("too_old")
                except ValueError:
                    pass

            if criteria.exclude_keywords:
                combined_text = (video.title + " " + video.description).lower()
                for kw in criteria.exclude_keywords:
                    if kw.lower() in combined_text:
                        video.failed_filters.append(f"excluded_keyword_{kw}")

            if video.failed_filters:
                continue

            video.passed_filters.append("passed_all_criteria")
            self._filtered_videos.append(video)

        print(f"Filter: {len(self._filtered_videos)}/{len(self._videos)} video passed")
        return self._filtered_videos

    # ================= TRANSCRIPT =================

    def fetch_transcripts(self, transcript_delay: int = 5):
        """
        Args:
            transcript_delay: Delay (giây) giữa các video trong loop, tránh
                             bị YouTube rate limit khi gọi yt-dlp fallback.
        """
        for i, video in enumerate(self._filtered_videos):
            if i > 0 and transcript_delay > 0:
                time.sleep(transcript_delay)
            if video.transcript:
                continue

            try:
                from youtube_transcript_api import YouTubeTranscriptApi
                # API mới (>=1.x): instantiate YouTubeTranscriptApi() rồi gọi .fetch()/.list()
                # API cũ: YouTubeTranscriptApi.get_transcript(video_id, languages=[...])
                try:
                    api = YouTubeTranscriptApi()
                    transcript = None
                    for lang_pref in [['vi'], ['vi', 'en'], ['en']]:
                        try:
                            transcript = api.fetch(video.video_id, languages=lang_pref)
                            video.transcript_language = lang_pref[0]
                            break
                        except Exception:
                            continue
                    if transcript is None:
                        transcript_list = api.list(video.video_id)
                        for t in transcript_list:
                            if t.language_code.startswith('vi'):
                                transcript = t.fetch()
                                video.transcript_language = t.language_code
                                video.transcript_is_auto = t.is_generated
                                break
                        if transcript is None:
                            for t in transcript_list:
                                if t.language_code.startswith('en'):
                                    transcript = t.fetch()
                                    video.transcript_language = t.language_code
                                    video.transcript_is_auto = t.is_generated
                                    break
                    if transcript is None:
                        raise Exception("No transcript found")
                    video.transcript = " ".join(x["text"] for x in transcript)
                except (TypeError, AttributeError):
                    transcript = YouTubeTranscriptApi.get_transcript(
                        video.video_id, languages=['vi', 'en']
                    )
                    video.transcript = " ".join(x["text"] for x in transcript)
                    video.transcript_language = "vi"
            except Exception:
                try:
                    import yt_dlp
                    ydl_opts = {
                        'writesubtitles': True,
                        'writeautomaticsub': True,
                        'subtitlesformat': 'json3',
                        'skip_download': True,
                        'quiet': True,
                        'js_runtimes': {'node': {}},
                    }
                    YouTubeResearcher._apply_auth_skip(ydl_opts)
                    YouTubeResearcher._apply_cookies(ydl_opts)  # FIX: cần cookies để bypass "Sign in"
                    self._apply_timeouts(ydl_opts, socket_timeout=60)
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(video.url, download=False)
                        subtitles = info.get('subtitles') or info.get('automatic_captions') or {}
                        if subtitles:
                            video.transcript = "[Subtitle Available]"
                            video.transcript_is_auto = True
                except Exception:
                    pass

    # ================= AUDIO ANALYSIS =================

    def analyze_audio_basic(self, audio_path):
        try:
            import soundfile as sf
            y, sr = sf.read(str(audio_path))
            if y.ndim > 1:
                y = y.mean(axis=1)
            duration = len(y) / sr
            # Resample to 16k for analysis (simple decimation if needed)
            if sr != 16000:
                import scipy.signal as sps
                y16 = sps.resample(y, int(len(y) * 16000 / sr))
                sr16 = 16000
            else:
                y16, sr16 = y, sr
            rms = float(np.sqrt(np.mean(y16 ** 2)))
            # frame-level RMS
            frame_len = max(1, int(0.025 * sr16))
            hop = max(1, int(0.010 * sr16))
            n_frames = max(0, (len(y16) - frame_len) // hop + 1)
            if n_frames > 0:
                frames = np.lib.stride_tricks.sliding_window_view(y16, frame_len)[::hop]
                frame_rms = np.sqrt(np.mean(frames ** 2, axis=1))
                silence_ratio = float(np.mean(frame_rms < 0.01))
            else:
                silence_ratio = 0.0
            zcr = float(np.mean(np.abs(np.diff(np.sign(y16))) > 0)) if len(y16) > 1 else 0.0
            mean_volume = float(np.mean(rms))
            return {
                "duration": duration,
                "silence_ratio": silence_ratio,
                "zero_crossing_rate": zcr,
                "mean_volume": mean_volume,
            }
        except Exception as e:
            return {"error": str(e)}

    # ================= YOUTUBE TRANSCRIPT (thay cho SONIOX) =================

    def _get_youtube_transcript(self, video_id: str, prefer_languages: list = None,
                                 proxy_url: Optional[str] = None) -> dict | None:
        """
        Lấy phụ đề SẴN CÓ của YouTube qua youtube-transcript-api.
        Ưu tiên ngôn ngữ: prefer_languages (mặc định ['vi', 'en']).

        Trả về dict:
            {
              "segments": [{"start", "duration", "text"}, ...],
              "language": "vi" | "en" | "...",
              "is_auto": bool,
              "source": "youtube-transcript-api",
            }
        hoặc None nếu không có phụ đề nào.
        """
        if prefer_languages is None:
            prefer_languages = ["vi", "en"]

        # Thử import động
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
        except ImportError:
            print("  pip install youtube-transcript-api")
            return None

        # Nếu có proxy: build session với proxy để youtube-transcript-api dùng
        # Cũng inject cookies vào session để bypass "Sign in to confirm you're not a bot"
        http_client = None
        if proxy_url or COOKIES_FILE_STR:
            try:
                import requests
                session = requests.Session()
                if proxy_url:
                    session.proxies.update({"http": proxy_url, "https": proxy_url})
                # Load cookies từ cookies.txt vào session
                if COOKIES_FILE_STR:
                    try:
                        from http.cookiejar import MozillaCookieJar
                        cj = MozillaCookieJar(COOKIES_FILE_STR)
                        cj.load(ignore_discard=True, ignore_expires=True)
                        # Convert MozillaCookieJar -> session cookies
                        for cookie in cj:
                            session.cookies.set_cookie(cookie)
                        # Verify
                        n = sum(1 for c in session.cookies if c.domain.endswith('youtube.com'))
                        if n:
                            print(f"  [transcript-api] loaded {n} youtube.com cookies")
                    except Exception as ce:
                        print(f"  [warn] Không load được cookies: {ce}")
                http_client = session
            except Exception as e:
                print(f"  [warn] Không build được proxy session: {e}")
                http_client = None

        # youtube-transcript-api >= 0.6 dùng instance method, < 0.6 dùng static.
        # Thử cả 2 cách.
        api_instance = None
        try:
            # New API: YouTubeTranscriptApi(http_client=...)
            if http_client is not None:
                try:
                    api_instance = YouTubeTranscriptApi(http_client=http_client)
                except TypeError:
                    # Old API không nhận http_client
                    api_instance = YouTubeTranscriptApi()
            else:
                api_instance = YouTubeTranscriptApi()
        except TypeError:
            api_instance = None  # old API (static methods)
        except Exception as e:
            print(f"  [transcript-api] YouTubeTranscriptApi() init error: {type(e).__name__}: {e}")
            api_instance = None

        # Helper: gọi method theo cả 2 style (instance hoặc class)
        def _call(method_name, *args, **kwargs):
            # Ưu tiên instance method
            if api_instance is not None and hasattr(api_instance, method_name):
                return getattr(api_instance, method_name)(*args, **kwargs)
            # Fallback class method (cũ)
            if hasattr(YouTubeTranscriptApi, method_name):
                return getattr(YouTubeTranscriptApi, method_name)(*args, **kwargs)
            raise AttributeError(f"No '{method_name}' on YouTubeTranscriptApi")

        # Ưu tiên 1: list transcripts để chọn manual > auto, theo thứ tự prefer_languages
        try:
            transcript_list = _call("list", video_id)
        except AttributeError:
            transcript_list = None
        except Exception as e:
            print(f"  [transcript-api] list_transcripts error: {type(e).__name__}: {e}")
            transcript_list = None

        if transcript_list is not None:
            # Thử manual trước
            for lang in prefer_languages:
                try:
                    t = transcript_list.find_manually_created_transcript([lang])
                    fetched = t.fetch()
                    return {
                        "segments": fetched,
                        "language": t.language_code,
                        "is_auto": False,
                        "source": "youtube-transcript-api-manual",
                    }
                except Exception:
                    pass
            # Thử auto-generated
            for lang in prefer_languages:
                try:
                    t = transcript_list.find_generated_transcript([lang])
                    fetched = t.fetch()
                    return {
                        "segments": fetched,
                        "language": t.language_code,
                        "is_auto": True,
                        "source": "youtube-transcript-api-auto",
                    }
                except Exception:
                    pass
            # Fallback: lấy bất kỳ transcript nào đầu tiên
            try:
                for t in transcript_list:
                    fetched = t.fetch()
                    return {
                        "segments": fetched,
                        "language": t.language_code,
                        "is_auto": t.is_generated,
                        "source": "youtube-transcript-api-any",
                    }
            except Exception:
                pass

        # Thử trực tiếp với list languages (cách cũ)
        try:
            fetched = _call("get_transcript", video_id, languages=prefer_languages)
            return {
                "segments": fetched,
                "language": prefer_languages[0],
                "is_auto": True,
                "source": "youtube-transcript-api-direct",
            }
        except AttributeError:
            pass
        except Exception as e:
            print(f"  get_transcript fallback failed: {e}")

        return None

    def _get_youtube_transcript_via_ytdlp(self, video_id: str,
                                            proxy_url: Optional[str] = None,
                                            info_cached: Optional[dict] = None) -> dict | None:
        """
        Lấy phụ đề qua yt-dlp bằng 2 bước:
          Bước 1: extract_info với listsubtitles → check video có sub không, lấy URL trực tiếp
          Bước 2: download URL sub → parse vtt/json3

        Args:
            video_id: YouTube video ID
            proxy_url: proxy URL (optional)
            info_cached: dict chứa 'subtitles' và 'automatic_captions' URLs từ lần
                         extract_info() trước đó (optional). Nếu có → dùng luôn,
                         KHÔNG gọi yt-dlp lần 2 (giảm rate limit "Sign in").

        Trả về dict hoặc None.
        """
        try:
            import yt_dlp
        except ImportError:
            return None

        # ====== BƯỚC 1: list subtitles URLs ======
        # Nếu có info_cached hợp lệ (có subtitles hoặc automatic_captions) → dùng luôn
        info = None
        if info_cached and (info_cached.get("subtitles") or info_cached.get("automatic_captions")):
            info = {
                "subtitles": info_cached.get("subtitles") or {},
                "automatic_captions": info_cached.get("automatic_captions") or {},
            }
            print(f"  [ytdlp-subs] using cached sub URLs (skip yt-dlp extract)")

        # Nếu chưa có info (hoặc cache rỗng) → gọi yt-dlp
        if info is None:
            ydl_opts = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "ignoreerrors": True,
                "js_runtimes": {"node": {}},
                "age_limit": None,
            }
            YouTubeResearcher._apply_auth_skip(ydl_opts)  # ← dùng helper (gồm player_client)
            YouTubeResearcher._apply_cookies(ydl_opts)
            self._apply_timeouts(ydl_opts, socket_timeout=60)
            if proxy_url:
                ydl_opts["proxy"] = proxy_url

            # Wrap trong thread + bound timeout 25s (kể cả khi yt-dlp internal timeout 20s fail)
            # Lý do: socket_timeout không control được proxy CONNECT timeout
            import concurrent.futures
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(
                        lambda: yt_dlp.YoutubeDL(ydl_opts).extract_info(
                            f"https://www.youtube.com/watch?v={video_id}", download=False
                        )
                    )
                    try:
                        info = future.result(timeout=25)
                    except concurrent.futures.TimeoutError:
                        print(f"  [ytdlp-subs] extract_info timeout 25s, killing")
                        if proxy_url:
                            # Timeout = proxy chết thật → xóa vĩnh viễn
                            self._mark_proxy_dead(proxy_url)
                        return None
            except Exception as e:
                err_str = str(e)
                print(f"  [ytdlp-subs] extract_info error: {type(e).__name__}: {err_str[:200]}")
                if proxy_url and is_proxy_dead_error(e):
                    # Connect timeout / SSL / proxy error → xóa vĩnh viễn
                    self._mark_proxy_dead(proxy_url)
                elif proxy_url:
                    # Rate limit (429, 403) → chỉ cooldown
                    self._mark_proxy_failed(proxy_url)
                return None

        if not info:
            print(f"  [ytdlp-subs] no info returned for {video_id}")
            return None

        # Check subtitles + automatic_captions
        subtitles = info.get("subtitles") or {}
        auto_captions = info.get("automatic_captions") or {}

        # Ưu tiên manual subtitles (chất lượng tốt hơn) > auto captions
        # Match nhiều mã Việt: vi, vi-VN, vi-VN-x-orig, vie, vietnamese
        def _find_vi_lang(d):
            """Tìm key bắt đầu bằng 'vi' trong dict subtitles."""
            if not d:
                return None
            # Exact match trước
            for k in d.keys():
                if k.lower().startswith("vi"):
                    return k
            return None

        # Thử manual trước
        vi_key = _find_vi_lang(subtitles)
        source_type = "manual"
        lang_code = vi_key or "vi"
        chosen = subtitles.get(vi_key) if vi_key else None

        # Nếu không có manual Việt → thử auto captions
        if not chosen:
            vi_key = _find_vi_lang(auto_captions)
            source_type = "auto"
            lang_code = vi_key or "vi"
            chosen = auto_captions.get(vi_key) if vi_key else None

        if not chosen:
            # Log chi tiết những gì có
            all_langs = list(subtitles.keys()) + list(auto_captions.keys())
            print(f"  [ytdlp-subs] no Vi sub. Available: {all_langs[:10]}")
            return None

        # Tìm format json3 hoặc vtt (ưu tiên json3)
        sub_url = None
        sub_format = None
        for fmt in ["json3", "vtt", "ttml", "srv3", "srv2", "srv1"]:
            for entry in chosen:
                if entry.get("ext") == fmt or fmt in (entry.get("url") or ""):
                    sub_url = entry["url"]
                    sub_format = fmt
                    break
            if sub_url:
                break

        # Fallback: lấy entry đầu tiên
        if not sub_url and chosen:
            sub_url = chosen[0].get("url")
            sub_format = chosen[0].get("ext", "vtt")

        if not sub_url:
            print(f"  [ytdlp-subs] no sub URL for {video_id}")
            return None

        print(f"  [ytdlp-subs] found {source_type} sub '{lang_code}' "
              f"format={sub_format}, downloading...")

        # ====== BƯỚC 2: download sub file ======
        try:
            import requests
            from requests.exceptions import HTTPError
            proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
            # Inject cookies
            cookies_dict = {}
            if COOKIES_FILE_STR:
                try:
                    from http.cookiejar import MozillaCookieJar
                    cj = MozillaCookieJar(COOKIES_FILE_STR)
                    cj.load(ignore_discard=True, ignore_expires=True)
                    for c in cj:
                        if c.domain.endswith("youtube.com"):
                            cookies_dict[c.name] = c.value
                except Exception as ce:
                    print(f"  [ytdlp-subs] warn: load cookies failed: {ce}")
            resp = requests.get(sub_url, proxies=proxies, cookies=cookies_dict,
                                timeout=(10, 25))  # (connect, read)
            # Detect 429 / 403 trước khi raise
            if resp.status_code == 429:
                print(f"  [ytdlp-subs] HTTP 429 (rate limited) via {self._short_proxy(proxy_url)}")
                if proxy_url:
                    # 429 = YouTube rate limit → chỉ cooldown, KHÔNG xóa proxy
                    self._mark_proxy_failed(proxy_url)
                return None  # signal retry
            if resp.status_code == 403:
                print(f"  [ytdlp-subs] HTTP 403 (forbidden) via {self._short_proxy(proxy_url)}")
                if proxy_url:
                    # 403 forbidden = YouTube rate limit → chỉ cooldown
                    self._mark_proxy_failed(proxy_url)
                return None
            resp.raise_for_status()
            content = resp.text
        except Exception as e:
            err_str = str(e)
            print(f"  [ytdlp-subs] download sub failed: {type(e).__name__}: {err_str[:200]}")
            if proxy_url and is_proxy_dead_error(e):
                # Connect timeout / SSL / 5xx → xóa vĩnh viễn
                self._mark_proxy_dead(proxy_url)
            elif proxy_url:
                # 429 / Too Many / rate limit → chỉ cooldown
                self._mark_proxy_failed(proxy_url)
            return None

        # ====== BƯỚC 3: parse theo format ======
        segs = []
        is_auto = (source_type == "auto")

        if sub_format == "json3":
            try:
                data = json.loads(content)
                segs = self._parse_json3_subtitle(data)
            except Exception as e:
                print(f"  [ytdlp-subs] json3 parse error: {e}")

        elif sub_format in ("vtt", "ttml", "srv1", "srv2", "srv3"):
            segs = self._parse_vtt_subtitle(content)

        if not segs:
            print(f"  [ytdlp-subs] parse returned 0 segments "
                  f"(format={sub_format}, len={len(content)} bytes)")
            return None

        return {
            "segments": segs,
            "language": lang_code,
            "is_auto": is_auto,
            "source": f"yt-dlp-{sub_format}-{source_type}",
        }

    @staticmethod
    def _parse_vtt_subtitle(content: str) -> list:
        """
        Parse WebVTT / SRV* subtitle thành list [{start, duration, text}].
        YouTube trả về VTT với cue identifier (number) + timestamp + text.
        """
        import re
        segs = []
        # Pattern: TIMESTAMP --> TIMESTAMP, có thể kèm cue settings
        ts_pattern = re.compile(
            r"(?P<sh>\d{2}):(?P<sm>\d{2}):(?P<ss>\d{2})[.,](?P<sms>\d{3})"
            r"\s+-->\s+"
            r"(?P<eh>\d{2}):(?P<em>\d{2}):(?P<es>\d{2})[.,](?P<ems>\d{3})"
        )

        # Tách blocks theo dòng trống
        blocks = re.split(r"\n\s*\n", content)
        for block in blocks:
            lines = block.strip().split("\n")
            if not lines:
                continue
            # Tìm dòng có timestamp
            ts_line = None
            text_start = 0
            for i, line in enumerate(lines):
                if "-->" in line:
                    ts_line = line
                    text_start = i + 1
                    break
            if not ts_line:
                continue
            m = ts_pattern.search(ts_line)
            if not m:
                continue
            sh = int(m["sh"]); sm = int(m["sm"]); ss = int(m["ss"]); sms = int(m["sms"])
            eh = int(m["eh"]); em = int(m["em"]); es = int(m["es"]); ems = int(m["ems"])
            start = sh * 3600 + sm * 60 + ss + sms / 1000
            end = eh * 3600 + em * 60 + es + ems / 1000

            # Gom text (bỏ cue identifier, bỏ tags HTML)
            text_lines = lines[text_start:]
            # Lọc bỏ dòng chỉ chứa số (cue id), tag cuesettings
            text_lines = [l for l in text_lines if l.strip() and not l.strip().isdigit()]
            text = " ".join(text_lines)
            # Strip HTML tags cơ bản (<c>, <v Speaker>)
            text = re.sub(r"<[^>]+>", "", text)
            text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
            text = text.strip()
            if not text:
                continue
            segs.append({
                "start": round(start, 3),
                "duration": round(end - start, 3),
                "text": text,
            })
        return segs

    @staticmethod
    def _parse_json3_subtitle(data: dict) -> list:
        """
        Parse JSON3 subtitle format từ YouTube.

        Format thuc te YouTube tra ve (json3):
          {
            "wireMagic": "pb3",
            "events": [
              {
                "tStartMs": 0, "dDurationMs": 1500,    <- start_ms, duration_ms
                "segs": [{"utf8": "Hello"}, {"utf8": " world"}]
              },
              ...
            ]
          }

        Mot so video cu hon co the dung "t" / "d" (alias cu).
        Parser nay ho tro ca 2 (uu tien tStartMs/dDurationMs).

        Tra ve list [{"start", "duration", "text"}] giay (float).
        """
        segs = []
        for ev in data.get("events", []):
            # Ho tro ca key cu ("t", "d") lan key moi ("tStartMs", "dDurationMs")
            start_ms = ev.get("tStartMs", ev.get("t"))
            dur_ms = ev.get("dDurationMs", ev.get("d"))
            if start_ms is None:
                continue
            start = float(start_ms) / 1000.0
            dur = float(dur_ms) / 1000.0 if dur_ms is not None else 0.0

            # Gom text từ segs[]
            parts = []
            for s in ev.get("segs", []) or []:
                # Ho tro ca "utf8" (json3) lan "text" (alias)
                txt = s.get("utf8", s.get("text", ""))
                if txt:  # giữ cả space/newline
                    parts.append(txt)
            text = "".join(parts).strip()
            if not text:
                continue

            segs.append({
                "start": round(start, 3),
                "duration": round(dur, 3),
                "text": text,
            })
        return segs

    def transcribe_with_youtube(self, video_id: str, audio_path: Path = None, lang: list = None,
                                 max_sentence_duration: float = 33.0,
                                 min_sentence_words: int = 1,
                                 info_cached: Optional[dict] = None) -> dict | None:
        """
        Lấy phụ đề sẵn có của YouTube (KHÔNG qua Soniox, KHÔNG qua youtube-transcript-api).

        Chỉ dùng yt-dlp subtitles download (đã có cookies + proxy rotation).
        Lý do bỏ youtube-transcript-api:
            - YouTube block IP cloud/datacenter (RequestBlocked)
            - Proxy pool đều là residential IP nhưng bị flag theo ASN
            - yt-dlp với cookies bypass tốt hơn, có thể download cả khi IP bị flag

        Args:
            ...
            info_cached: dict chứa 'subtitles' và 'automatic_captions' URLs từ
                         lần ydl.extract_info() trước đó (optional).
                         Nếu có sẽ tránh gọi yt-dlp extract_info() lần 2
                         → giảm rate limit "Sign in to confirm you're not a bot".

        Trả về dict cùng cấu trúc với transcribe_with_soniox:
            {
              "segments": [{"start", "end", "speaker", "text"}, ...],
              "audio_duration": float,
              "detected_languages": [str, ...],
              "transcript_language": str,
              "transcript_is_auto": bool,
              "transcript_source": str,
            }
        hoặc None nếu thất bại.
        """
        if lang is None:
            lang = ["vi"]

        print(f"  Fetching YouTube transcript via yt-dlp (langs={lang})...")

        # Chi dung IP fake (ProtonVPN), khong con IP that nen khong can retry xoay proxy.
        # Code cu retry 6 lan de xoay proxy khi direct bi block, ngioy khong con y nghia.
        # Giu 1 attempt duy nhat: that bai -> fail luon (caller xu ly tiep).
        max_attempts = 1
        for attempt in range(max_attempts):
            yt_proxy = self._proxy_for_fallback()
            print(f"  [transcript-ytdlp] attempt {attempt+1}/{max_attempts} via "
                  f"{self._short_proxy(yt_proxy) if yt_proxy else 'DIRECT'}")
            # Chỉ truyền info_cached ở attempt đầu tiên.
            # Nếu attempt đầu fail, các attempt sau sẽ gọi yt-dlp mới (với proxy mới)
            cached = info_cached if attempt == 0 else None
            result = self._get_youtube_transcript_via_ytdlp(
                video_id, proxy_url=yt_proxy, info_cached=cached
            )
            if result:
                break

        if not result:
            print("  No YouTube transcript available (no retry, IP fake only)")
            return None

        raw_segments = result["segments"]
        # Chuẩn hóa về dạng segment giống Soniox output: start, end, speaker, text
        # raw_segments có thể là list[dict] (yt-dlp) hoặc list[FetchedTranscriptSnippet] (API mới)
        raw_parsed = []
        for seg in raw_segments:
            if isinstance(seg, dict):
                start = float(seg.get("start", 0.0))
                dur = float(seg.get("duration", 0.0))
                text = (seg.get("text") or "").replace("\n", " ").strip()
            else:
                start = float(getattr(seg, "start", 0.0))
                dur = float(getattr(seg, "duration", 0.0))
                text = (getattr(seg, "text", "") or "").replace("\n", " ").strip()
            if not text:
                continue
            raw_parsed.append({
                "start": round(start, 3),
                "end": round(start + dur, 3),
                "text": text,
            })

        if not raw_parsed:
            return None

        # YouTube auto-subs trả về snippets ~3-5s overlap nhau, cắt giữa câu.
        # Gom lại thành câu hoàn chỉnh (kết thúc bằng . ? ! … hoặc đủ dài).
        segments = self._merge_youtube_segments_to_sentences(
            raw_parsed,
            max_duration=max_sentence_duration,
            min_words=min_sentence_words,
        )

        if not segments:
            return None

        # audio_duration: lấy từ file audio nếu có, fallback từ end của segment cuối
        audio_duration = 0.0
        if audio_path and Path(audio_path).exists():
            try:
                import soundfile as sf
                y, sr = sf.read(str(audio_path))
                audio_duration = round(len(y) / sr, 3)
            except Exception:
                pass
        if audio_duration <= 0:
            audio_duration = float(segments[-1]["end"])

        # Map mã ISO -> tên ngôn ngữ tiếng Việt để lưu vào JSON
        lang_name = self._iso_lang_to_vietnamese(result["language"])

        return {
            "segments": segments,
            "audio_duration": audio_duration,
            "detected_languages": [lang_name],
            "transcript_language": lang_name,
            "transcript_is_auto": result["is_auto"],
            "transcript_source": result["source"],
        }

    def _merge_youtube_segments_to_sentences(self, raw_parsed: list,
                                            max_duration: float = 60.0,
                                            min_words: int = 1) -> list:
        """
        Gom các snippet YouTube auto-subs (overlap timestamps, cắt giữa câu) thành câu hoàn chỉnh.
        YouTube chia text liên tục thành snippets ~3-5s, text nối tiếp nhau (không lặp),
        nhưng bị cắt giữa từ/câu.

        Args:
            raw_parsed: list of {start, end, text} tu parser json3/vtt
            max_duration: max giay cua 1 segment (mac dinh 120s = 2 phut)
                          Set 0 de bo cap (chi tach khi gap dau .?!…)
            min_words: so tu toi thieu de tao segment (mac dinh 1 = chap nhan cau ngan)
                       Set 3-5 de bo qua noise ngan nhu "Vang", "Uh"

        Logic:
            1. Noi text thanh 1 danh sach tu + track timestamps
            2. Gap dau . ? ! … -> ket thuc cau
            3. Qua max_duration giay -> ket thuc cau (hard cap, 0 = khong cap)
            4. Cuoi video (het tu) -> flush neu >= min_words HOAC co dau ket thuc
        """
        if not raw_parsed:
            return []

        # Dau ket thuc cau: .?!... hoac …
        SENT_END_CHARS = {".", "?", "!", "…"}

        # Bước 1: Nối tất cả text + track timestamps cho từng từ
        word_entries = []  # list of (word, start_time, end_time)
        for seg in raw_parsed:
            words = seg["text"].split()
            if not words:
                continue
            seg_dur = seg["end"] - seg["start"]
            time_per_word = seg_dur / len(words) if len(words) > 0 else 0
            for j, w in enumerate(words):
                w_start = seg["start"] + j * time_per_word
                w_end = seg["start"] + (j + 1) * time_per_word
                word_entries.append((w, w_start, w_end))

        if not word_entries:
            return []

        # Bước 2: Gom từ thành câu
        sentences = []
        current_words = []
        current_start = word_entries[0][1]

        for word, w_start, w_end in word_entries:
            current_words.append(word)
            current_end = w_end

            # Check tu cuoi co dau ket thuc khong (chi xet ky tu cuoi cung, bo qua ' " ))
            stripped_word = word.rstrip().rstrip('"').rstrip("'").rstrip(")")
            duration = current_end - current_start
            is_sentence_end = bool(stripped_word) and stripped_word[-1] in SENT_END_CHARS
            # Hard cap: neu max_duration > 0 va duration vuot -> tach
            is_duration_over = (max_duration > 0) and (duration > max_duration)

            if is_sentence_end or is_duration_over:
                text = " ".join(current_words).strip()
                if text and (len(current_words) >= min_words or text[-1] in SENT_END_CHARS):
                    sentences.append({
                        "start": round(current_start, 3),
                        "end": round(current_end, 3),
                        "speaker": "SPEAKER_00",
                        "text": text,
                    })
                current_words = []
                current_start = current_end

        # Flush phần còn lại
        if current_words:
            text = " ".join(current_words).strip()
            # Tạo segment cuối nếu: đủ từ HOAC có dấu kết thúc câu
            if text and (len(current_words) >= min_words or text[-1] in SENT_END_CHARS):
                sentences.append({
                    "start": round(current_start, 3),
                    "end": round(word_entries[-1][2], 3),
                    "speaker": "SPEAKER_00",
                    "text": text,
                })

        return sentences

    @staticmethod
    def _iso_lang_to_vietnamese(code: str) -> str:
        """
        Map mã ngôn ngữ ISO 639-1 sang tên tiếng Việt.
        vd: 'vi' -> 'Tiếng Việt', 'en' -> 'Tiếng Anh', 'zh' -> 'Tiếng Trung'
        Mặc định trả về 'Tiếng Việt' nếu không rõ (vì tool này ưu tiên tiếng Việt).
        """
        mapping = {
            "vi": "Tiếng Việt",
            "en": "Tiếng Anh",
            "zh": "Tiếng Trung",
            "zh-CN": "Tiếng Trung (Giản thể)",
            "zh-Hans": "Tiếng Trung (Giản thể)",
            "zh-Hant": "Tiếng Trung (Phồn thể)",
            "zh-TW": "Tiếng Trung (Phồn thể)",
            "ja": "Tiếng Nhật",
            "ko": "Tiếng Hàn",
            "fr": "Tiếng Pháp",
            "de": "Tiếng Đức",
            "es": "Tiếng Tây Ban Nha",
            "pt": "Tiếng Bồ Đào Nha",
            "ru": "Tiếng Nga",
            "th": "Tiếng Thái",
            "id": "Tiếng Indonesia",
            "ms": "Tiếng Mã Lai",
            "ar": "Tiếng Ả Rập",
            "hi": "Tiếng Hindi",
            "it": "Tiếng Ý",
            "nl": "Tiếng Hà Lan",
            "pl": "Tiếng Ba Lan",
            "tr": "Tiếng Thổ Nhĩ Kỳ",
            "uk": "Tiếng Ukraina",
        }
        if not code:
            return "Tiếng Việt"
        return mapping.get(code, f"Tiếng {code.upper()}")

    # ================= DATASET SCORE =================

    def compute_dataset_score(self, video):
        score = 0.0
        if video.caption_available:
            score += 0.2
        if video.definition == "hd":
            score += 0.1
        if video.avg_confidence > 0.9:
            score += 0.3
        if video.view_count > 100000:
            score += 0.1
        if not video.made_for_kids:
            score += 0.1
        if len(video.detected_languages) == 1:
            score += 0.1
        return round(score, 3)

    # ================= FIX PROPER NOUNS (MiniMax) =================

    def _get_anthropic_key(self) -> Optional[str]:
        """
        Lay ANTHROPIC_AUTH_TOKEN (key cua MiniMax qua proxy sotatek).
        Uu tien: env ANTHROPIC_AUTH_TOKEN > file .env > ANTHROPIC_API_KEY.
        """
        env_token = os.environ.get("ANTHROPIC_AUTH_TOKEN")
        if env_token:
            return env_token

        env_file = Path(__file__).parent / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if line.startswith("ANTHROPIC_AUTH_TOKEN="):
                    return line.split("=", 1)[1].strip()
                if line.startswith("ANTHROPIC_API_KEY="):
                    return line.split("=", 1)[1].strip()

        return os.environ.get("ANTHROPIC_API_KEY")

    def _get_anthropic_base_url(self) -> str:
        """
        Base URL cua Anthropic API. Mac dinh di qua proxy sotatek
        (http://127.0.0.1:3817/anthropic) nhu cau hinh trong
        /home/hientran/.config/sotatek-proxy/config.yaml.
        Co the override bang env ANTHROPIC_BASE_URL.
        """
        return os.environ.get(
            "ANTHROPIC_BASE_URL",
            "http://127.0.0.1:3817/anthropic",
        )

    def _call_minimax(self, prompt: str, max_tokens: int = 4000) -> Optional[str]:
        """
        Goi MiniMax API qua proxy sotatek (HTTP thuan, khong dung anthropic SDK).

        Returns:
            Text tra ve tu model, hoac None neu loi.
        """
        import requests  # local import de tranh loi neu thieu

        api_key = self._get_anthropic_key()
        if not api_key:
            print("  [minimax] Khong co ANTHROPIC_AUTH_TOKEN -> skip")
            return None

        base_url = self._get_anthropic_base_url().rstrip("/")
        url = f"{base_url}/v1/messages"

        payload = {
            "model": MINIMAX_MODEL,
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": prompt}],
        }

        headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        }

        for attempt in range(3):
            try:
                resp = requests.post(url, json=payload, headers=headers, timeout=60)
                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get("content") or []
                    if content and isinstance(content, list):
                        # Lay text dau tien co type='text'
                        for block in content:
                            if isinstance(block, dict) and block.get("type") == "text":
                                return block.get("text", "")
                        # Fallback: lay block dau tien co 'text'
                        for block in content:
                            if isinstance(block, dict) and "text" in block:
                                return block["text"]
                    return None
                # Retryable errors
                if resp.status_code in (408, 409, 429, 500, 502, 503, 504):
                    wait = 5 * (attempt + 1)
                    print(f"  [minimax] HTTP {resp.status_code}, retry in {wait}s ({attempt+1}/3)...")
                    time.sleep(wait)
                    continue
                # Non-retryable
                print(f"  [minimax] HTTP {resp.status_code}: {resp.text[:200]}")
                return None
            except requests.exceptions.RequestException as e:
                wait = 5 * (attempt + 1)
                print(f"  [minimax] request error ({attempt+1}/3): {e}; retry in {wait}s")
                if attempt < 2:
                    time.sleep(wait)
                    continue
                return None
        return None

    def fix_proper_nouns_minimax(self, segments: list, video_title: str = "") -> list:
        """
        Fix proper nouns (names, products, locations) su dung MiniMax
        qua proxy sotatek (HTTP thuan, KHONG dung anthropic SDK).
        """
        key = self._get_anthropic_key()
        if not key:
            print("  [minimax] Khong co key -> skip fix proper nouns")
            return segments

        full_text = "\n".join([
            f"[{s['start']:.2f}-{s['end']:.2f}] {s['speaker']}: {s['text']}"
            for s in segments
        ])

        prompt = f"""Ban la chuyen gia sua loi nhan dang giong noi (ASR) cho tieng Viet.

Nhiem vu: Sua cac loi sau trong van ban:
1. Ten rieng: nguoi, cong ty, to chuc
2. San pham: ten san pham, nhan hieu
3. Dia phuong: tinh, thanh pho, quan, huyen, xa, dia danh

Ngu canh video: {video_title}

Van ban can sua:
{full_text}

Yeu cau:
- Chi sua cac tu bi sai chinh ta hoac nham lan do ASR
- GIU NGUYEN cac tu dung chinh ta
- KHONG thay doi cau truc cau hay them bot noi dung
- Output JSON:

```json
{{
  "corrections": [
    {{"original": "...", "corrected": "...", "reason": "..."}}
  ],
  "fixed_segments": [
    {{"start": 0.0, "end": 5.5, "speaker": "SPEAKER_00", "text": "van ban da sua"}}
  ]
}}
```
"""

        response_text = self._call_minimax(prompt, max_tokens=4000)
        if not response_text:
            return segments

        json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group(1))
        else:
            json_match = re.search(r'\{[\s\S]*\}', response_text)
            if json_match:
                result = json.loads(json_match.group(0))
            else:
                print("  [minimax] khong parse duoc JSON tu response")
                return segments

        fixed_segments = result.get("fixed_segments", [])
        if fixed_segments:
            corrections = result.get("corrections", [])
            print(f"  [minimax] Fixed {len(corrections)} proper nouns (via proxy)")
            return fixed_segments

        return segments

    # ================= PIPELINE =================

    def process_videos_pipeline(
        self,
        output_dir: str = "./youtube_dataset",
        keep_videos: bool = False,
        fix_names: bool = False,  # NO-OP: MiniMax đã bỏ, transcript lấy thô từ YouTube
        audio_format: str = "m4a",
        run_timestamp: str = "",
        skip_existing_transcripts: bool = True,
        video_delay: int = 10,
        max_sentence_duration: float = 33.0,
        min_sentence_words: int = 1,
        run_logger: "RunLogger | None" = None,
        channel_idx: int = 0,
        total_channels: int = 0,
    ) -> dict:
        """
        Pipeline dùng phụ đề SẴN CÓ của YouTube (tiếng Việt).

        Args:
            fix_names: NO-OP (giữ để backward-compat). MiniMax proper-noun fix
                       đã được bỏ — transcript giữ nguyên từ YouTube.

        Args:
            skip_existing_transcripts: nếu True (mặc định), bỏ qua video đã có
                            file *_transcription.json trong transcriptions/{run_timestamp}/.
                            Set False để buộc re-transcribe tất cả.

        Note: Download audio chi de luu tru (giu tuong thich voi output cu).
        Transcript lay truc tiep tu YouTube, KHONG can file audio.
        """
        output_dir = Path(output_dir)
        # Dùng subfolder có timestamp để mỗi lần chạy tạo folder riêng
        # transcriptions/{timestamp}/
        if not run_timestamp:
            run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        transcriptions_dir = output_dir / "transcriptions" / run_timestamp
        transcriptions_dir.mkdir(parents=True, exist_ok=True)

        # Tao audio_dir (luon luu audio de giu tuong thich output cu)
        audio_dir = output_dir / "audio" / run_timestamp
        audio_dir.mkdir(parents=True, exist_ok=True)

        def _log(msg, also_print=True):
            """Helper: log vao run_logger neu co, neu khong chi print."""
            if run_logger:
                run_logger.log(msg, also_print=also_print)
            elif also_print:
                print(msg)

        results = []

        # === Pre-partition: 1 disk scan -> 3 bucket (A: skip, B: transcribe-only, C: full) ===
        # Bucket A = skip nhanh nhat (co ca audio + json, khong I/O)
        # Bucket B = co audio (o run cu), chi can transcribe
        # Bucket C = chua co audio, phai download + transcribe
        bucket_a, bucket_b, bucket_c = self._partition_videos_for_pipeline(
            audio_root=audio_dir.parent,
            transcriptions_root=transcriptions_dir.parent,
            skip_existing=skip_existing_transcripts,
        )

        total = len(self._filtered_videos)
        _log(f"Pipeline partition (total={total}):")
        _log(f"  Bucket A (audio+json da co, SKIP)         : {len(bucket_a)}")
        _log(f"  Bucket B (co audio, chua co json)         : {len(bucket_b)}")
        _log(f"  Bucket C (chua co audio, can download)   : {len(bucket_c)}")

        def _save_video_audio_filename(audio_path):
            """Load lai audio_filename tu JSON (neu co) de giu cho CSV."""
            try:
                if audio_path and audio_path.exists():
                    return audio_path.name
            except Exception:
                pass
            return None

        # ============================================================
        # BUCKET A: co ca audio + json -> SKIP nhanh (khong I/O)
        # ============================================================
        for i, (video, audio_path, json_path) in enumerate(bucket_a, 1):
            audio_filename = audio_path.name
            print(f"\n[A-{i}/{len(bucket_a)}] {video.title[:60]}")
            print(f"  [SKIP] audio + JSON đã có sẵn "
                  f"(audio: {audio_path.parent.name}/{audio_filename}, "
                  f"json: {json_path.parent.name}/{json_path.name})")
            _log(f"[A-{i}/{len(bucket_a)}] {video.video_id} | {video.title[:50]} "
                 f"-> SKIP (audio: {audio_filename}, json: {json_path.name})",
                 also_print=False)
            # Load thong tin tu JSON de giu audio_filename cho CSV
            try:
                with open(json_path, "r", encoding="utf-8") as jf:
                    existing = json.load(jf)
                video.audio_filename = existing.get("audio_path", audio_filename)
            except Exception:
                video.audio_filename = audio_filename
            results.append({
                "video_id": video.video_id,
                "title": video.title,
                "status": "skipped",
                "audio_filename": video.audio_filename,
                "transcription_filename": json_path.name,
                "transcript_language": "N/A",
                "transcript_is_auto": None,
                "transcript_source": "existing",
            })

        # ============================================================
        # BUCKET B: co audio (o run cu), chua co json -> chi transcribe
        # ============================================================
        for i, (video, audio_path, audio_filename) in enumerate(bucket_b, 1):
            print(f"\n[B-{i}/{len(bucket_b)}] {video.title[:60]}")
            print(f"  [SKIP-DOWNLOAD] audio có sẵn ở "
                  f"{audio_path.parent.name}/{audio_filename}, lấy transcript YouTube...")
            _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | {video.title[:50]} "
                 f"-> transcribe-only (audio: {audio_filename})", also_print=False)

            # Ten file JSON cung theo ten audio (de JSON + audio + CSV dong nhat)
            json_stem = Path(audio_filename).stem
            json_path = transcriptions_dir / f"{json_stem}_transcription.json"

            try:
                result = self.transcribe_with_youtube(
                    video_id=video.video_id,
                    audio_path=audio_path,
                    lang=["vi", "en"],
                    max_sentence_duration=max_sentence_duration,
                    min_sentence_words=min_sentence_words,
                )
            except Exception as e:
                _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | transcript error: {e}")
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "transcript_error",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": None,
                    "error": str(e),
                })
                continue

            if result:
                segments = result["segments"]
                video.audio_filename = audio_filename
                self._save_transcription(
                    output_path=json_path,
                    segments=segments,
                    video=video,
                    audio_duration=result["audio_duration"],
                    audio_filename=audio_filename or "",
                    extra_metadata={
                        "transcript_language": result.get("transcript_language", ""),
                        "transcript_is_auto": result.get("transcript_is_auto", False),
                        "transcript_source": result.get("transcript_source", ""),
                        "detected_languages": result.get("detected_languages", []),
                    },
                )
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "success",
                    "audio_filename": audio_filename,
                    "transcription_filename": f"{json_stem}_transcription.json",
                    "transcript_language": result.get("transcript_language", ""),
                    "transcript_is_auto": result.get("transcript_is_auto", False),
                    "transcript_source": result.get("transcript_source", ""),
                    "audio_downloaded_at": None,  # da co tu truoc
                    "transcribed_at": datetime.now().isoformat(),
                })
                print(f"  Done ({len(segments)} segments, lang={result.get('transcript_language')}, "
                      f"auto={result.get('transcript_is_auto')})")
                _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | DONE "
                     f"({len(segments)} seg, lang={result.get('transcript_language')}, "
                     f"audio: {audio_filename})", also_print=False)
            else:
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "transcript_unavailable",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": None,
                })
                print("  No YouTube transcript available")
                _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | NO TRANSCRIPT "
                     f"(audio: {audio_filename})", also_print=False)
            # Khong xoa audio vi no da co san (o run cu), khong thuoc quyen quan ly

        # ============================================================
        # BUCKET C: chua co audio -> download + transcribe + save
        # ============================================================
        for i, (video, target_name, target_filename) in enumerate(bucket_c, 1):
            print(f"\n[C-{i}/{len(bucket_c)}] {video.title[:60]}")
            _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | {video.title[:50]} "
                 f"-> download + transcribe", also_print=False)

            # Delay giữa các video để tránh YouTube rate limit (429)
            if i > 1 and video_delay > 0:
                time.sleep(video_delay)

            audio_path = None
            audio_filename = None

            # Bước 0: tải audio về (luôn giữ audio) - direct-first, fallback proxy khi 429/block
            try:
                import yt_dlp
                max_dl_retries = 3
                info = None
                # Cache subtitles URLs từ lần extract_info() đầu tiên (success)
                # để truyền sang transcribe_with_youtube() → tránh gọi yt-dlp lần 2
                info_cache: dict = {}
                # Resume support: xóa .part/.ytdl cũ nếu nhỏ (<10MB) coi như lỗi
                # File .part lớn (>10MB) → giữ để yt-dlp resume
                video_id_stem = audio_dir / video.video_id
                for stale_ext in [".ytdl"]:
                    stale_file = video_id_stem.with_suffix(stale_ext)
                    if stale_file.exists():
                        try:
                            stale_file.unlink()
                            print(f"  [audio-dl] cleaned stale {stale_ext} file")
                        except Exception:
                            pass
                for dl_attempt in range(1, max_dl_retries + 1):
                    dl_proxy = self._proxy_for_fallback()
                    ydl_opts = {
                        'format': 'bestaudio/best',
                        'merge_output_format': audio_format,
                        'outtmpl': str(audio_dir / '%(id)s.%(ext)s'),
                        'quiet': True,
                        'js_runtimes': {'node': {}},
                        'postprocessors': [{
                            'key': 'FFmpegExtractAudio',
                            'preferredcodec': 'wav',
                            'preferredquality': '192',
                        }],
                        'postprocessor_args': ['-ar', '16000', '-ac', '1'],
                    }
                    self._apply_auth_skip(ydl_opts)
                    self._apply_cookies(ydl_opts)
                    self._apply_timeouts(ydl_opts, socket_timeout=60)
                    if dl_proxy:
                        ydl_opts['proxy'] = dl_proxy
                    try:
                        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                            info = ydl.extract_info(video.url, download=True)
                            filename = ydl.prepare_filename(info)
                            # Logic tìm file audio (giống qwen3_subs version):
                            # postprocessor có thể tạo file .wav HOẶC giữ nguyên .mp4/.m4a/.webm
                            # tùy format yt-dlp chọn + có FFmpeg hay không
                            audio_path = Path(filename)
                            if not audio_path.exists() or audio_path.suffix not in (".wav", ".mp3", ".m4a", ".flac", ".opus", ".ogg", ".webm", ".mp4"):
                                # Thử tìm file .wav (postprocessor convert)
                                wav_candidate = audio_path.with_suffix(".wav")
                                if wav_candidate.exists():
                                    audio_path = wav_candidate
                                else:
                                    # Tìm stem với mọi extension
                                    stem = audio_path.with_suffix("")
                                    for ext in [".wav", ".m4a", ".mp3", ".flac", ".opus", ".ogg", ".webm", ".mp4"]:
                                        candidate = stem.with_suffix(ext)
                                        if candidate.exists():
                                            audio_path = candidate
                                            break
                            # Nếu vẫn không có file, kiểm tra file .part (yt-dlp đang download dở)
                            if not audio_path.exists():
                                stem = Path(filename).with_suffix("")
                                for ext in [".mp4.part", ".m4a.part", ".webm.part", ".mp3.part"]:
                                    candidate = stem.with_suffix(ext).with_suffix(ext)  # giữ .part
                                    # Actually: filename có thể là Gn58izhwybY.mp4 → part = Gn58izhwybY.mp4.part
                                    candidate = Path(str(stem) + ext)
                                    if candidate.exists() and candidate.stat().st_size > 1024 * 1024:  # > 1MB
                                        print(f"  [audio-dl] found .part file ({candidate.stat().st_size // 1024 // 1024}MB), "
                                              f"yt-dlp bị gián đoạn, sẽ resume lần sau")
                                        audio_path = candidate
                                        break
                            # Force extension về .wav cho đồng nhất với target_filename
                            # (KHÔNG nếu file đang là .part - cần resume)
                            if audio_path.exists() and not str(audio_path).endswith('.part'):
                                audio_path = audio_path.with_suffix(".wav") if not audio_path.suffix == ".wav" else audio_path
                            # CACHE subtitles URLs từ info dict để dùng cho transcript
                            # → tránh gọi yt-dlp extract_info() lần 2 (giảm rate limit)
                            info_cache = {
                                "subtitles": info.get("subtitles") or {},
                                "automatic_captions": info.get("automatic_captions") or {},
                            }
                            if info_cache["subtitles"] or info_cache["automatic_captions"]:
                                sub_keys = list(info_cache["subtitles"].keys())[:3]
                                auto_keys = list(info_cache["automatic_captions"].keys())[:3]
                                print(f"  [audio-dl] cached sub URLs: sub={sub_keys}, auto={auto_keys}")
                        break
                    except Exception as dl_err:
                        err_msg = str(dl_err).lower()
                        is_rate_limit = any(k in err_msg for k in [
                            '429', 'too many', 'rate limit', 'forbidden',
                            '403', 'blocked', 'sign in', 'bot',
                            'timed out', 'connect timeout', 'proxy',
                        ])
                        if is_rate_limit and dl_attempt < max_dl_retries:
                            if not dl_proxy:
                                self._escalate_to_proxy()
                            else:
                                # Phân biệt: proxy chết thật → xóa, rate limit → cooldown
                                if is_proxy_dead_error(dl_err):
                                    self._mark_proxy_dead(dl_proxy)
                                else:
                                    self._mark_proxy_failed(dl_proxy)
                            wait = 3 * dl_attempt
                            print(f"  [audio-dl] attempt {dl_attempt}/{max_dl_retries} "
                                  f"blocked/429 via {self._short_proxy(dl_proxy) if dl_proxy else 'DIRECT'}, "
                                  f"switching to proxy, retry in {wait}s...")
                            time.sleep(wait)
                            continue
                        raise
                if info is None:
                    raise RuntimeError("Download failed after all retries")

                    # Tìm file .wav
                    if not audio_path.exists() or audio_path.suffix != ".wav":
                        wav_candidate = audio_path.with_suffix(".wav")
                        if wav_candidate.exists():
                            audio_path = wav_candidate
                        else:
                            stem = audio_path.with_suffix("")
                            for ext in [".wav", ".m4a", ".mp3", ".flac", ".opus", ".ogg"]:
                                candidate = stem.with_suffix(ext)
                                if candidate.exists():
                                    audio_path = candidate
                                    break

                    # Xóa file gốc (.webm, .m4a, ...) chỉ giữ .wav
                    video_id_stem = audio_dir / video.video_id
                    for leftover_ext in [".webm", ".m4a", ".mp4", ".opus", ".ogg"]:
                        leftover = video_id_stem.with_suffix(leftover_ext)
                        if leftover.exists() and leftover != audio_path:
                            try:
                                leftover.unlink()
                            except Exception:
                                pass

                # Rename file audio theo tiêu đề video
                target_ext = audio_path.suffix if (audio_path and audio_path.exists()) else ".wav"
                target_filename = f"{target_name}{target_ext}"
                target_path = audio_dir / target_filename

                if audio_path and audio_path.exists() and audio_path != target_path:
                    if target_path.exists():
                        target_path = audio_dir / f"{target_name}_{video.video_id}{target_ext}"
                        target_filename = target_path.name
                    try:
                        audio_path.rename(target_path)
                        audio_path = target_path
                    except Exception as e:
                        print(f"  Rename failed ({e}), giu ten goc")

                audio_filename = audio_path.name if (audio_path and audio_path.exists()) else f"{target_name}.wav"
            except Exception as e:
                print(f"  Download failed: {e}")
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | DOWNLOAD FAILED: {e}")
                audio_filename = f"{target_name}.wav"
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "download_failed",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": datetime.now().isoformat(),
                    "error": str(e),
                })
                continue

            # Bước 1: lấy transcript sẵn có từ YouTube (KHÔNG qua Soniox)
            try:
                result = self.transcribe_with_youtube(
                    video_id=video.video_id,
                    audio_path=audio_path,
                    lang=["vi", "en"],
                    max_sentence_duration=max_sentence_duration,
                    min_sentence_words=min_sentence_words,
                    info_cached=info_cache if info_cache else None,
                )
            except Exception as e:
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | transcript error: {e}")
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "transcript_error",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": datetime.now().isoformat(),
                    "error": str(e),
                })
                continue

            if result:
                segments = result["segments"]
                # Tên file JSON: theo tên audio (đồng nhất với file audio trên disk)
                json_stem = Path(audio_filename).stem
                json_path = transcriptions_dir / f"{json_stem}_transcription.json"

                # Lưu kèm metadata YouTube
                video.audio_filename = audio_filename
                self._save_transcription(
                    output_path=json_path,
                    segments=segments,
                    video=video,
                    audio_duration=result["audio_duration"],
                    audio_filename=audio_filename or "",
                    extra_metadata={
                        "transcript_language": result.get("transcript_language", ""),
                        "transcript_is_auto": result.get("transcript_is_auto", False),
                        "transcript_source": result.get("transcript_source", ""),
                        "detected_languages": result.get("detected_languages", []),
                    },
                )
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "success",
                    "audio_filename": audio_filename,
                    "transcription_filename": f"{json_stem}_transcription.json",
                    "transcript_language": result.get("transcript_language", ""),
                    "transcript_is_auto": result.get("transcript_is_auto", False),
                    "transcript_source": result.get("transcript_source", ""),
                    "audio_downloaded_at": datetime.now().isoformat(),
                    "transcribed_at": datetime.now().isoformat(),
                })
                print(f"  Done ({len(segments)} segments, lang={result.get('transcript_language')}, "
                      f"auto={result.get('transcript_is_auto')})")
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | DONE "
                     f"({len(segments)} seg, lang={result.get('transcript_language')}, "
                     f"audio: {audio_filename})", also_print=False)
            else:
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "transcript_unavailable",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": datetime.now().isoformat() if audio_filename else None,
                })
                print("  No YouTube transcript available")
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | NO TRANSCRIPT "
                     f"(audio: {audio_filename})", also_print=False)

            # Trong file nay, audio LUON giu (de tuong thich output cu)
            # Nen khong xoa o day

        success = sum(1 for r in results if r.get("status") == "success")
        skipped = sum(1 for r in results if r.get("status") == "skipped")
        failed = [r for r in results if r.get("status") not in ("success", "skipped")]
        _log(f"\nPipeline channel {channel_idx}/{total_channels}: "
             f"{success} success, {skipped} skipped, {len(failed)} failed "
             f"(tong: {len(self._filtered_videos)})")
        if failed:
            _log("Cac video loi trong kenh nay:")
            for r in failed:
                _log(f"  - [{r.get('status')}] {r.get('video_id')} | "
                     f"{r.get('title', '')[:50]} | {r.get('error', '')}", also_print=False)
        print(f"\nPipeline: {success}/{len(self._filtered_videos)} thanh cong")

        return {"total": len(self._filtered_videos), "success": success, "results": results}

    # ================= SAVE =================

    def _save_transcription(
        self, output_path: Path, segments: list, video,
        audio_duration: float, audio_filename: str = "",
        extra_metadata: dict = None,
    ):
        """
        Save transcription JSON theo format giong (target):
        {
          "audio_duration": ..., "audio_path": <ten file audio>, "video_id": ...,
          "title": ..., "channel": ..., "url": ...,
          "num_speakers": ..., "speakers": [...], "source_files": [],
          "segments": [{"start":..., "end":..., "speaker":..., "text":...}]
        }

        audio_path = ten file (vd: "Gh1Sgknc6Fg.wav"), dong nhat voi file audio tren disk
        va voi cot "audio_path" trong CSV.
        """
        speakers = sorted(set(str(s["speaker"]) for s in segments))

        result = {
            "audio_duration": audio_duration,
            "audio_path": audio_filename or "",
            "video_id": video.video_id,
            "title": video.title,
            "channel": video.channel,
            "url": video.url,
            "num_speakers": len(speakers),
            "speakers": speakers,
            "source_files": [],
            "segments": segments,
        }

        # Ghi them metadata YouTube (transcript language, source, is_auto, detected_languages)
        if extra_metadata:
            result.update(extra_metadata)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    def save_research(self, filename="research_result.json"):
        output_file = self.output_dir / filename

        videos_data = []
        for v in self._filtered_videos:
            data = asdict(v)
            data["video_url"] = v.video_url
            videos_data.append(data)

        data = {
            "research_date": datetime.now().isoformat(),
            "channel": self._videos[0].channel if self._videos else "",
            "total_videos_found": len(self._videos),
            "videos_after_filter": len(self._filtered_videos),
            "videos": videos_data,
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"Saved to {output_file}")

    def save_to_csv(self, filename="research_result.csv", transcription_dir=None):
        """Save research results to CSV with all audio and transcription info"""
        import csv

        output_file = self.output_dir / filename

        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)

            headers = [
                "video_id", "title", "channel", "url", "published_at",
                "duration_formatted", "duration_seconds",
                "view_count", "like_count", "comment_count", "engagement_ratio",

                # Audio filename (dong nhat voi file audio tren disk va audio_path trong JSON)
                "audio_path",

                # Audio analysis
                "audio_total_duration", "audio_silence_ratio",
                "audio_zero_crossing_rate", "audio_mean_volume",

                # Soniox transcription
                "transcription_status", "transcription_audio_duration",
                "num_speakers", "speakers_list", "num_segments",
                "avg_confidence", "detected_languages", "dataset_score",

                # Video-level audio
                "estimated_speech_ratio", "video_avg_confidence",
                "video_dataset_score", "video_detected_languages",

                # LLM
                "niche", "llm_score", "llm_reason",

                # YouTube metadata
                "tags", "category_id", "default_language",
                "default_audio_language", "caption_available",
                "definition", "licensed_content", "projection",
                "privacy_status", "made_for_kids", "topic_categories",

                # Filter
                "passed_filters", "failed_filters", "description_preview",
            ]
            writer.writerow(headers)

            for v in self._filtered_videos:
                duration_secs = parse_duration(v.duration)
                engagement_ratio = 0
                if v.view_count > 0:
                    engagement_ratio = round((v.like_count + v.comment_count) / v.view_count * 100, 2)

                # Read audio features from transcription JSON if available
                audio_features = {}
                transcription_metadata = {}
                json_audio_path = ""  # audio_path doc tu file JSON (neu co)
                if transcription_dir:
                    json_path = YouTubeResearcher.find_transcription_json(
                        transcription_dir, v,
                        audio_filename=getattr(v, "audio_filename", ""),
                    )
                    if json_path and json_path.exists():
                        try:
                            with open(json_path, "r", encoding="utf-8") as jf:
                                tdata = json.load(jf)
                            audio_features = tdata.get("audio_features", {})
                            json_audio_path = tdata.get("audio_path", "")
                            transcription_metadata = {
                                "total_audio_duration": tdata.get("total_audio_duration") or tdata.get("audio_duration"),
                                "num_speakers": tdata.get("num_speakers"),
                                "speakers": tdata.get("speakers", []),
                                "avg_confidence": tdata.get("avg_confidence"),
                                "detected_languages": tdata.get("detected_languages", []),
                                "dataset_score": tdata.get("dataset_score"),
                                "num_segments": len(tdata.get("segments", [])),
                            }
                        except Exception:
                            pass

                # Uu tien audio_filename gan tren video (set trong pipeline),
                # fallback sang gia tri doc tu JSON, fallback sang pattern mac dinh.
                audio_filename = (
                    getattr(v, "audio_filename", "")
                    or json_audio_path
                    or f"{v.video_id}.wav"
                )

                row = [
                    v.video_id, v.title, v.channel, v.url, v.published_at,
                    format_duration(duration_secs), duration_secs,
                    v.view_count, v.like_count, v.comment_count, engagement_ratio,

                    audio_filename,

                    audio_features.get("duration", ""),
                    audio_features.get("silence_ratio", ""),
                    audio_features.get("zero_crossing_rate", ""),
                    audio_features.get("mean_volume", ""),

                    "completed" if transcription_metadata else "pending",
                    transcription_metadata.get("total_audio_duration", ""),
                    transcription_metadata.get("num_speakers", ""),
                    ", ".join(transcription_metadata.get("speakers", [])),
                    transcription_metadata.get("num_segments", ""),
                    transcription_metadata.get("avg_confidence", ""),
                    json.dumps(transcription_metadata.get("detected_languages", []), ensure_ascii=False),
                    transcription_metadata.get("dataset_score", ""),

                    v.estimated_speech_ratio,
                    v.avg_confidence,
                    v.dataset_score,
                    json.dumps(v.detected_languages, ensure_ascii=False) if v.detected_languages else "",

                    v.niche, v.llm_score, v.llm_reason,

                    json.dumps(v.tags, ensure_ascii=False),
                    v.category_id, v.default_language,
                    v.default_audio_language, v.caption_available,
                    v.definition, v.licensed_content, v.projection,
                    v.privacy_status, v.made_for_kids,
                    json.dumps(v.topic_categories, ensure_ascii=False),

                    " | ".join(v.passed_filters),
                    " | ".join(v.failed_filters),
                    v.description[:150].replace("\n", " ") if v.description else "",
                ]
                writer.writerow(row)

        print(f"CSV saved to {output_file}")
        return str(output_file)

    # ================= PRINT =================

    def print_video_table(self, videos=None):
        if videos is None:
            videos = self._filtered_videos
        if not videos:
            print("No videos")
            return

        print("\n" + "=" * 130)
        print(
            f"{'#':<3} {'Title':<45} {'Duration':<10} "
            f"{'Views':<8} {'Likes':<7} {'Caption':<8} {'Published':<12}"
        )
        print("=" * 130)

        for i, v in enumerate(videos):
            duration_secs = parse_duration(v.duration)
            title = v.title[:42] + "..." if len(v.title) > 45 else v.title
            pub_date = v.published_at[:10] if v.published_at else ""

            print(
                f"{i+1:<3} {title:<45} {format_duration(duration_secs):<10} "
                f"{format_number(v.view_count):<8} {format_number(v.like_count):<7} "
                f"{str(v.caption_available):<8} {pub_date:<12}"
            )

        print("=" * 130)
        print(f"\nVideo URLs:")
        for i, v in enumerate(videos):
            print(f"  {i+1}. {v.video_url}")


# ================= CLI =================

def load_channels_from_file(path: str) -> list[str]:
    """
    Đọc file txt, mỗi dòng là 1 URL/handle/channel_id của 1 kênh YouTube.
    - Bỏ qua dòng trống
    - Bỏ qua dòng bắt đầu bằng # (comment)
    - Trim khoảng trắng
    """
    p = Path(path)
    if not p.exists():
        print(f"Không tìm thấy file channels: {path}")
        return []

    channels = []
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        channels.append(s)

    print(f"Đọc được {len(channels)} kênh từ {path}")
    for i, c in enumerate(channels, 1):
        print(f"  {i}. {c}")
    return channels


def safe_channel_name(channel_url: str, fallback: str = "unknown") -> str:
    """
    Chuyển URL/handle/channel_id thành tên folder an toàn.
    vd: https://www.youtube.com/@vietnh1009 -> vietnh1009
        UCxxxxxxxxxxxxxxxxxxxxx -> UCxxxxxxxxxxxxxxxxxxxxx
    """
    if not channel_url:
        return fallback

    s = channel_url.strip().rstrip("/")

    # Handle: @vietnh1009
    m = re.search(r"@([^/\s?]+)", s)
    if m:
        return m.group(1)

    # /channel/UCxxx
    m = re.search(r"youtube\.com/channel/([^/\s?]+)", s)
    if m:
        return m.group(1)

    # /c/Name hoặc /user/Name
    m = re.search(r"youtube\.com/(?:c|user)/([^/\s?]+)", s)
    if m:
        return m.group(1)

    # Channel ID thuần (UCxxx 24 ký tự)
    if s.startswith("UC") and len(s) == 24 and re.match(r"^UC[\w-]+$", s):
        return s

    # Fallback: lấy phần cuối URL
    return s.split("/")[-1] or fallback


def parse_args():
    import argparse
    p = argparse.ArgumentParser(
        description="YouTube Researcher - Lấy video theo danh sách kênh YouTube (mỗi kênh 1 dòng trong file txt)"
    )

    # 2 mode: 1 kênh (--channel) HOẶC nhiều kênh (--channels-file)
    p.add_argument(
        "--channel", "-c",
        help="URL kênh YouTube đơn lẻ (vd: https://www.youtube.com/@TenKenh hoặc UCxxxxx)"
    )
    p.add_argument(
        "--channels-file", "-f",
        default="./channels_audio/channels.txt",
        help="Đường dẫn file txt chứa danh sách URL kênh, mỗi kênh 1 dòng (mặc định: ./channels_audio/channels.txt)"
    )

    p.add_argument("--output", "-o", default="./youtube_dataset",
                   help="Folder output gốc. Mỗi kênh sẽ có subfolder riêng bên trong.")
    p.add_argument("--max-results", "-m", type=int, default=20000,
                   help="Số lượng video OUTPUT thỏa mãn filter (không phải số video fetch). "
                        "Mặc định 20000 (cap bởi max_batches*batch_size).")
    p.add_argument("--max-fetch", type=int, default=20000,
                   help="Số video tối đa fetch từ YouTube (giới hạn để tránh quá nhiều API calls). "
                        "Mặc định 20000.")
    p.add_argument("--batch-size", type=int, default=20000,
                   help="Số video fetch mỗi batch (mặc định 50). "
                        "Sau mỗi batch, lấy upload_date video cũ nhất làm cursor cho batch tiếp theo. "
                        "Tăng lên 100-200 nếu muốn nhanh hơn, giảm xuống 20-30 nếu kênh rất lớn.")
    p.add_argument("--max-batches", type=int, default=8000,
                   help="Số batch tối đa (mặc định 400). Với batch_size=50, tối đa 20000 video. "
                        "Tăng lên nếu cần lấy nhiều hơn (vd 600 = 30000 video).")
    p.add_argument("--fetch-delay", type=int, default=10,
                   help="Delay giữa các batch khi fetch video list qua yt-dlp/RSS (giây, mặc định 3s). "
                        "Tăng lên 5-10s nếu bị YouTube rate limit khi fetch nhiều kênh.")
    p.add_argument("--use-rss", action="store_true",
                   help="Dùng YouTube RSS feed (NHANH nhất, tối đa 15 video mới nhất mỗi lần). "
                        "Phù hợp kênh nhỏ hoặc khi cần lấy nhanh video mới nhất. "
                        "Không dùng --use-rss nếu cần > 15 video.")
    p.add_argument("--order", default="date", help="date | viewCount | rating | relevance")
    p.add_argument("--keep-audio", action="store_true")
    p.add_argument("--audio-format", default="m4a",
                   help="Định dạng audio: m4a, wav, mp4, webm, flac (default: m4a)")
    p.add_argument("--no-transcribe", action="store_true", help="Chỉ lấy metadata, không transcribe")
    p.add_argument("--no-fix", action="store_true", help="Không fix proper nouns")
    p.add_argument("--force-retranscribe", action="store_true",
                   help="Bắt buộc transcribe lại kể cả khi đã có file JSON. "
                        "Mặc định: skip video đã có transcription.")
    p.add_argument("--skip-existing", action="store_true",
                   help="Bỏ qua kênh đã có output (file research_<channel>.json tồn tại)")
    p.add_argument("--summary-name", default=None,
                   help="Tên file summary tổng (mặc định: _multi_channel_summary_YYYYMMDD_HHMMSS.json có timestamp)")
    # ===== VPN ROTATOR (ProtonVPN) — CHỈ DÙNG CÁCH NÀY ĐỂ FAKE IP =====
    p.add_argument("--use-vpn", action="store_true",
                   help="Dùng ProtonVPN tunnel để rotate IP (cần file .ovpn trong "
                        "./proton_config/, cần sudo để start openvpn). "
                        "Mặc định KHÔNG bật → dùng IP thật của máy.")
    p.add_argument("--vpn-rotate-every", type=int, default=0,
                   help="Số request trước khi tự rotate IP qua VPN. "
                        "0 = chỉ rotate khi gặp 429/403 (mặc định: 0). "
                        "Vd: 50 = đổi IP sau mỗi 50 request.")
    p.add_argument("--vpn-strategy", choices=["random", "sequential", "least_used"],
                   default="random",
                   help="Chiến lược chọn VPN server khi rotate (mặc định: random).")
    p.add_argument("--video-delay", type=int, default=10,
                   help="Delay giữa các video (giây) để giảm YouTube rate limit. "
                        "Mặc định 10s (an toàn cho 6000+ video). "
                        "Tăng lên 15-20s nếu bị chặn, giảm xuống 5s nếu kênh nhỏ (< 100 video).")
    p.add_argument("--rss-delay", type=int, default=5,
                   help="Delay giữa các video khi fetch metadata qua RSS entries loop "
                        "(giây, mặc định 5s). Tránh bị YouTube chặn IP khi gọi "
                        "yt-dlp 1 lần/video trong fetch_channel_videos_rss.")
    p.add_argument("--rss-page-delay", type=int, default=2,
                   help="Delay giữa các trang RSS XML (giây, mặc định 2s). "
                        "RSS pages nhẹ hơn yt-dlp nên delay ngắn hơn.")
    p.add_argument("--transcript-delay", type=int, default=5,
                   help="Delay giữa các video trong fetch_transcripts loop "
                        "(giây, mặc định 5s). Áp dụng khi fallback yt-dlp subtitles.")
    p.add_argument("--socket-timeout", type=int, default=600,
                   help="Timeout cho mỗi yt-dlp request (giây, mặc định 20s). "
                        "Tăng lên 30s nếu mạng chậm/proxy chậm.")
    p.add_argument("--max-retries", type=int, default=3,
                   help="Số lần retry khi yt-dlp fail (mặc định 3). "
                        "Mỗi retry sẽ thử proxy khác.")
    p.add_argument("--max-sentence-duration", type=int, default=33.0,
                   help="Max duration (giây) của 1 segment transcript. "
                        "Mặc định 120s (câu dài tối đa 2 phút). "
                        "Tăng lên 300-600 nếu muốn gộp thành đoạn văn dài. "
                        "Set 0 để bỏ cap (gộp đến khi gặp dấu .?!… mới tách).")
    p.add_argument("--min-sentence-words", type=int, default=1,
                   help="Số từ tối thiểu để tạo 1 segment. "
                        "Mặc định 1 (câu ngắn 1-2 từ như 'Vâng', 'Đúng rồi' cũng được tạo). "
                        "Tăng lên 3-5 nếu muốn bỏ qua noise ngắn.")
    return p.parse_args()


# ================= EXPORT CSV =================

def flatten_segment_for_csv(video, segment, extra_metadata=None, audio_features=None, transcription_metadata=None):
    """Flatten one transcription segment into CSV row"""
    if audio_features is None:
        audio_features = {}
    if transcription_metadata is None:
        transcription_metadata = {}

    row = {
        "video_id": video.video_id,
        "video_title": video.title,
        "channel": video.channel,
        "video_url": video.url,
        "published_at": video.published_at,
        "duration_iso": video.duration,
        "duration_seconds": parse_duration(video.duration),
        "view_count": video.view_count,
        "like_count": video.like_count,
        "comment_count": video.comment_count,
        "thumbnail": video.thumbnail,
        "description": video.description,
        "niche": video.niche,
        "llm_score": video.llm_score,
        "llm_reason": video.llm_reason,

        # Audio filename (dong nhat voi file audio tren disk va audio_path trong JSON)
        "audio_path": getattr(video, "audio_filename", "") or (transcription_metadata.get("audio_path") if transcription_metadata else ""),

        "speaker": segment.get("speaker"),
        "segment_start": segment.get("start"),
        "segment_end": segment.get("end"),
        "segment_duration": segment.get("duration"),
        "text": segment.get("text"),

        "language": segment.get("language"),
        "language_confidence": segment.get("language_confidence"),
        "speaker_confidence": segment.get("speaker_confidence"),
        "avg_token_confidence": segment.get("avg_token_confidence"),
        "num_tokens": segment.get("num_tokens"),
        "speech_rate_wps": segment.get("speech_rate_wps"),
        "has_music": segment.get("has_music"),
        "has_noise": segment.get("has_noise"),
        "emotion": segment.get("emotion"),
        "gender": segment.get("gender"),
        "audio_energy": segment.get("audio_energy"),
        "audio_pitch": segment.get("audio_pitch"),
        "silence_before": segment.get("silence_before"),
        "silence_after": segment.get("silence_after"),

        # Audio features (from librosa analysis)
        "audio_total_duration": audio_features.get("duration"),
        "audio_silence_ratio": audio_features.get("silence_ratio"),
        "audio_zero_crossing_rate": audio_features.get("zero_crossing_rate"),
        "audio_mean_volume": audio_features.get("mean_volume"),

        # Transcription-level metadata (from Soniox)
        "total_audio_duration": transcription_metadata.get("total_audio_duration"),
        "num_speakers": transcription_metadata.get("num_speakers"),
        "speakers_list": ", ".join(transcription_metadata.get("speakers", [])) if transcription_metadata.get("speakers") else "",
        "avg_confidence": transcription_metadata.get("avg_confidence"),
        "detected_languages": json.dumps(transcription_metadata.get("detected_languages", []), ensure_ascii=False) if transcription_metadata.get("detected_languages") else "",
        "dataset_score": transcription_metadata.get("dataset_score"),

        # Video-level audio metadata
        "estimated_speech_ratio": video.estimated_speech_ratio,
        "video_avg_confidence": video.avg_confidence,
        "video_dataset_score": video.dataset_score,
        "video_detected_languages": json.dumps(video.detected_languages, ensure_ascii=False) if video.detected_languages else "",

        "tags": json.dumps(extra_metadata.get("tags", []), ensure_ascii=False) if extra_metadata else json.dumps(video.tags, ensure_ascii=False),
        "category_id": extra_metadata.get("category_id") if extra_metadata else video.category_id,
        "default_language": extra_metadata.get("default_language") if extra_metadata else video.default_language,
        "default_audio_language": extra_metadata.get("default_audio_language") if extra_metadata else video.default_audio_language,
        "caption": extra_metadata.get("caption") if extra_metadata else video.caption_available,
        "licensed_content": extra_metadata.get("licensed_content") if extra_metadata else video.licensed_content,
        "definition": extra_metadata.get("definition") if extra_metadata else video.definition,
        "projection": extra_metadata.get("projection") if extra_metadata else video.projection,
        "privacy_status": extra_metadata.get("privacy_status") if extra_metadata else video.privacy_status,
        "made_for_kids": extra_metadata.get("made_for_kids") if extra_metadata else video.made_for_kids,
        "topic_categories": json.dumps(extra_metadata.get("topic_categories", []), ensure_ascii=False) if extra_metadata else json.dumps(video.topic_categories, ensure_ascii=False),
        "top_comments": json.dumps(extra_metadata.get("top_comments", []), ensure_ascii=False) if extra_metadata else json.dumps(video.top_comments, ensure_ascii=False),
    }
    return row


def export_transcriptions_to_csv(output_csv, videos, transcription_dir):
    """Export all transcriptions + metadata into one CSV"""
    import pandas as pd

    rows = []
    for video in videos:
        json_path = YouTubeResearcher.find_transcription_json(
            transcription_dir, video,
            audio_filename=getattr(video, "audio_filename", ""),
        )
        if not json_path:
            continue

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            segments = data.get("segments", [])
            extra_metadata = data.get("youtube_metadata", {})
            audio_features = data.get("audio_features", {})

            transcription_metadata = {
                "total_audio_duration": data.get("total_audio_duration") or data.get("audio_duration"),
                "num_speakers": data.get("num_speakers"),
                "speakers": data.get("speakers", []),
                "avg_confidence": data.get("avg_confidence"),
                "detected_languages": data.get("detected_languages", []),
                "dataset_score": data.get("dataset_score"),
                "audio_path": data.get("audio_path", ""),
            }

            for seg in segments:
                row = flatten_segment_for_csv(
                    video, seg, extra_metadata,
                    audio_features=audio_features,
                    transcription_metadata=transcription_metadata,
                )
                rows.append(row)
        except Exception as e:
            print(f"Failed reading {json_path}: {e}")

    if not rows:
        print("No rows to export")
        return

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"CSV exported: {output_csv} ({len(df)} rows, {len(df.columns)} columns)")


def export_video_summary_csv(output_csv, videos, transcription_dir):
    """Export video-level summary CSV (one row per video) with all audio info"""
    import pandas as pd

    rows = []
    for video in videos:
        json_path = YouTubeResearcher.find_transcription_json(
            transcription_dir, video,
            audio_filename=getattr(video, "audio_filename", ""),
        )

        audio_features = {}
        transcription_metadata = {}

        if json_path:
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                audio_features = data.get("audio_features", {})
                transcription_metadata = {
                    "total_audio_duration": data.get("total_audio_duration") or data.get("audio_duration"),
                    "num_speakers": data.get("num_speakers"),
                    "speakers": data.get("speakers", []),
                    "avg_confidence": data.get("avg_confidence"),
                    "detected_languages": data.get("detected_languages", []),
                    "dataset_score": data.get("dataset_score"),
                    "num_segments": len(data.get("segments", [])),
                    "audio_path": data.get("audio_path", ""),
                }
            except Exception:
                pass

        duration_secs = parse_duration(video.duration)
        engagement_ratio = 0
        if video.view_count > 0:
            engagement_ratio = round((video.like_count + video.comment_count) / video.view_count * 100, 2)

        # Ten audio file: uu tien video.audio_filename, fallback sang JSON, fallback mac dinh
        audio_filename = (
            getattr(video, "audio_filename", "")
            or transcription_metadata.get("audio_path", "")
            or f"{video.video_id}.wav"
        )

        row = {
            "video_id": video.video_id,
            "title": video.title,
            "channel": video.channel,
            "url": video.url,
            "published_at": video.published_at,

            "duration_formatted": format_duration(duration_secs),
            "duration_seconds": duration_secs,
            "view_count": video.view_count,
            "like_count": video.like_count,
            "comment_count": video.comment_count,
            "engagement_ratio": engagement_ratio,

            # Audio filename (dong nhat voi file audio tren disk va audio_path trong JSON)
            "audio_path": audio_filename,

            # Audio analysis features
            "audio_total_duration": audio_features.get("duration"),
            "audio_silence_ratio": audio_features.get("silence_ratio"),
            "audio_zero_crossing_rate": audio_features.get("zero_crossing_rate"),
            "audio_mean_volume": audio_features.get("mean_volume"),

            # Soniox transcription metadata
            "transcription_status": "completed" if transcription_metadata else "pending",
            "transcription_audio_duration": transcription_metadata.get("total_audio_duration"),
            "num_speakers": transcription_metadata.get("num_speakers"),
            "speakers_list": ", ".join(transcription_metadata.get("speakers", [])),
            "num_segments": transcription_metadata.get("num_segments", 0),
            "avg_confidence": transcription_metadata.get("avg_confidence"),
            "detected_languages": json.dumps(transcription_metadata.get("detected_languages", []), ensure_ascii=False),
            "dataset_score": transcription_metadata.get("dataset_score"),

            # Video-level audio metadata
            "estimated_speech_ratio": video.estimated_speech_ratio,
            "video_avg_confidence": video.avg_confidence,
            "video_dataset_score": video.dataset_score,
            "video_detected_languages": json.dumps(video.detected_languages, ensure_ascii=False) if video.detected_languages else "",

            # LLM analysis
            "niche": video.niche,
            "llm_score": video.llm_score,
            "llm_reason": video.llm_reason,

            # YouTube metadata
            "tags": json.dumps(video.tags, ensure_ascii=False),
            "category_id": video.category_id,
            "default_language": video.default_language,
            "default_audio_language": video.default_audio_language,
            "caption_available": video.caption_available,
            "definition": video.definition,
            "licensed_content": video.licensed_content,
            "projection": video.projection,
            "privacy_status": video.privacy_status,
            "made_for_kids": video.made_for_kids,
            "topic_categories": json.dumps(video.topic_categories, ensure_ascii=False),
            "top_comments": json.dumps(video.top_comments, ensure_ascii=False),

            # Filter status
            "passed_filters": " | ".join(video.passed_filters),
            "failed_filters": " | ".join(video.failed_filters),
            "description": video.description[:200] if video.description else "",
        }
        rows.append(row)

    if not rows:
        print("No rows to export")
        return

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"Video summary CSV exported: {output_csv} ({len(df)} rows, {len(df.columns)} columns)")


# ================= RUN LOGGER =================

class RunLogger:
    """
    Ghi log TXT cho moi lan chay, gom tat ca thong tin:
      - Tong quan (command, timestamp, so kenh, duong dan script)
      - Moi kenh: bat dau / ket thuc / status / loi (neu co)
      - Moi video: trang thai (success/skipped/failed/...)
      - Tong ket: bao nhieu thanh cong, loi o dau, kenh nao chua hoan thanh

    File log duoc ghi o: <output_root>/logs/run_YYYYMMDD_HHMMSS.txt
    Co the doc nhanh de biet:
      - Con kenh nao chua xu ly xong
      - Video nao dang/da loi o phase nao
      - Toan bo batch da hoan thanh hay chua
      - Batch nay chay tu duong dan script nao, luc nao
    """

    def __init__(self, log_path: str | Path, script_path: str = ""):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # Ghi header khi tao file
        self._write_raw(self._separator("=") + "\n")
        self._write_raw(f"RUN LOG - bat dau luc {datetime.now().isoformat()}\n")
        self._write_raw(f"Log file: {self.log_path}\n")
        if script_path:
            self._write_raw(f"Script path: {script_path}\n")
        # Lay working dir va python version
        try:
            self._write_raw(f"Working dir : {os.getcwd()}\n")
        except Exception:
            pass
        self._write_raw(f"Python      : {sys.version.split()[0]}\n")
        self._write_raw(self._separator("=") + "\n\n")

    def _separator(self, ch: str = "-", length: int = 80) -> str:
        return ch * length

    def _write_raw(self, text: str):
        """Ghi raw vao file (co lock de an toan khi nhieu thread)."""
        with self._lock:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(text)

    def log(self, msg: str, also_print: bool = True):
        """Ghi 1 dong log voi timestamp HH:MM:SS."""
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        self._write_raw(line + "\n")
        if also_print:
            print(line)

    def log_section(self, title: str):
        """Ghi 1 section header (de phan biet cac phan lon)."""
        sep = self._separator("=")
        self._write_raw(f"\n{sep}\n{title}\n{sep}\n")
        print(f"\n{title}")

    def log_subsection(self, title: str):
        """Ghi 1 subsection header."""
        sep = self._separator("-")
        self._write_raw(f"\n{sep}\n{title}\n{sep}\n")
        print(title)

    def log_batch_start(self, channels_file: str, total_channels: int,
                        command: str = "", script_path: str = ""):
        """Ghi log bat dau batch (toan bo file channels.txt)."""
        sep = self._separator("=")
        self._write_raw(f"\n{sep}\nBATCH START\n{sep}\n")
        self.log(f"Timestamp      : {datetime.now().isoformat()}")
        if script_path:
            self.log(f"Script path    : {script_path}")
        self.log(f"Channels file  : {channels_file}")
        self.log(f"Total channels : {total_channels}")
        if command:
            self.log(f"Command        : {command}")
        self.log("")

    def log_channel_start(self, idx: int, total: int, channel_url: str,
                          channel_name: str, run_timestamp: str):
        """Ghi log bat dau xu ly 1 kenh."""
        sep = self._separator("=")
        self._write_raw(f"\n{sep}\nCHANNEL {idx}/{total}: {channel_url}\n{sep}\n")
        self.log(f"Channel name    : {channel_name}")
        self.log(f"Run timestamp   : {run_timestamp}")
        self.log(f"Output folder   : <output_root>/{channel_name}/")
        self.log(f"Trang thai      : BAT DAU xu ly kenh")

    def log_channel_end(self, idx: int, total: int, channel_url: str,
                        status: str, summary: dict | None = None,
                        error: str | None = None):
        """Ghi log ket thuc xu ly 1 kenh."""
        self.log("")
        self.log(f"Trang thai cuoi : {status}")
        if error:
            self.log(f"ERROR          : {error}")
        if summary:
            total_v = summary.get("total", 0)
            success_v = summary.get("success", 0)
            failed_v = total_v - success_v
            self.log(f"Video processed : {total_v} (success: {success_v}, "
                     f"failed/skipped: {failed_v})")
            # Log chi tiet tung video trong summary
            results = summary.get("results", [])
            if results:
                self.log("")
                self.log(f"Chi tiet {len(results)} video trong kenh:")
                for r in results:
                    vid = r.get("video_id", "?")
                    title = r.get("title", "?")[:50]
                    st = r.get("status", "?")
                    afn = r.get("audio_filename") or "-"
                    tfn = r.get("transcription_filename") or "-"
                    err = r.get("error", "")
                    line = f"  - [{st:<25}] {vid} | {title:<50} | audio: {afn} | json: {tfn}"
                    if err:
                        line += f" | ERROR: {err}"
                    self.log(line)
        self._write_raw(f"\n{'-' * 80}\nCHANNEL {idx}/{total} HOAN THANH (status: {status})\n{'-' * 80}\n")

    def log_batch_end(self, total_channels: int, success: int, failed: int,
                      all_results: list):
        """Ghi log ket thuc batch (toan bo file channels.txt)."""
        sep = self._separator("=")
        self._write_raw(f"\n{sep}\nBATCH END\n{sep}\n")
        self.log(f"Ket thuc luc  : {datetime.now().isoformat()}")
        self.log(f"Tong channels : {total_channels}")
        self.log(f"Thanh cong    : {success}")
        self.log(f"That bai      : {failed}")

        # Chi tiet trang thai tung kenh
        self.log("")
        self.log("CHI TIET TUNG KENH:")
        for r in all_results:
            ch = r.get("channel", "?")
            st = r.get("status", "?")
            self.log(f"  [{st:<22}] {ch}")
            if st == "error" and r.get("error"):
                self.log(f"      Error: {r['error']}")

        # Liet ke kenh chua hoan thanh (status != success/success_no_transcribe/skipped)
        incomplete = [r for r in all_results
                      if r.get("status") not in ("success", "success_no_transcribe", "skipped")]
        if incomplete:
            self.log("")
            self.log(f"CANH BAO: {len(incomplete)} kenh chua hoan thanh:")
            for r in incomplete:
                self.log(f"  - {r.get('channel')} (status: {r.get('status')})")
        else:
            self.log("")
            self.log("TAT CA KENH DA XU LY XONG!")

        self.log("")
        self.log(f"Log file: {self.log_path}")
        self._write_raw(self._separator("=") + "\n")
        self._write_raw(f"RUN LOG - ket thuc luc {datetime.now().isoformat()}\n")
        self._write_raw(self._separator("=") + "\n")


# ================= MAIN =================

def process_one_channel(
    channel_url: str,
    *,
    youtube_key: str,
    output_root: str,
    max_results: int,
    max_fetch: int,
    order: str,
    keep_audio: bool,
    audio_format: str,
    no_transcribe: bool,
    no_fix: bool,
    skip_existing: bool,
    force_retranscribe: bool = False,
    batch_size: int = 50,
    max_batches: int = 400,
    fetch_delay: int = 5,
    use_rss: bool = False,
    proxy_rotator: Optional[VPNRotator] = None,
    video_delay: int = 10,
    rss_delay: int = 5,
    rss_page_delay: int = 2,
    transcript_delay: int = 5,
    socket_timeout: int = 100,
    max_retries: int = 3,
    max_sentence_duration: float = 33.0,
    min_sentence_words: int = 1,
    run_logger: "RunLogger | None" = None,
    channel_idx: int = 0,
    total_channels: int = 0,
) -> dict:
    """
    Chạy full pipeline cho 1 kênh YouTube.
    Output: <output_root>/<channel_name>/...
    Tất cả file output đều có timestamp: research_{channel}_{YYYYMMDD_HHMMSS}.{ext}
    """
    channel_name = safe_channel_name(channel_url)
    channel_output = Path(output_root) / channel_name

    # Timestamp cho lần chày này, gắn vào tất cả file output để không bị ghi đè
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\n" + "=" * 80)
    print(f"CHANNEL: {channel_url}")
    print(f"Channel name: {channel_name}")
    print(f"Output: {channel_output}")
    print(f"Run timestamp: {run_timestamp}")
    print("=" * 80)

    # === Log bat dau kenh ===
    if run_logger:
        run_logger.log_channel_start(
            idx=channel_idx, total=total_channels,
            channel_url=channel_url, channel_name=channel_name,
            run_timestamp=run_timestamp,
        )

    # Skip nếu đã có output ĐẦY ĐỦ từ lần chạy trước.
    # Logic cũ (chỉ check research_*.json) làm skip cả khi thiếu CSV/segments/summary.
    # Logic mới: chỉ skip khi có CẢ research JSON + segments CSV + summary CSV + research CSV
    # + pipeline_summary JSON. Nếu thiếu 1 trong các file CSV thì vẫn chạy lại pipeline
    # để tái tạo đầy đủ output.
    #
    # Lưu ý: tên channel trong research_*.json có thể khác channel_name (handle).
    # vd: channel_name="Top10HuyenBi" nhưng file có tên "research_Top_10_Huyen_Bi_*.json"
    # vì safe_name = resolved_channel_name.replace(" ", "_") khi save CSV/JSON.
    # Nên check CẢ 2 pattern: channel_name và "Top_10_Huyen_Bi" (replace space→underscore).
    # Lấy tất cả pattern tên có thể: channel_name (handle) + resolved_channel_name từ JSON cũ.
    # resolved_channel_name có thể khác channel_name rất nhiều:
    # vd: channel_name="Top10HuyenBi" nhưng resolved="Top 10 Huyen Bi" → safe_name="Top_10_Huyen_Bi"
    def _get_resolved_name_from_existing(folder: Path, ch_name: str) -> Optional[str]:
        """Đọc research_*.json có sẵn để lấy resolved_channel_name đã save trước đó."""
        candidates = list(folder.glob("research_*.json"))
        if not candidates:
            return None
        # Lấy file mới nhất
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        try:
            with open(candidates[0], "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("channel")
        except Exception:
            return None

    def _all_outputs_exist(folder: Path, ch_name: str, ts: str) -> tuple[bool, list[str]]:
        """Check xem folder đã có đủ file output chưa.

        Returns:
            (all_exist, list_missing_files)
        """
        if not folder.exists():
            return False, []

        # Build list các tên có thể để check (cover cả pattern):
        name_variants = {ch_name, ch_name.replace(" ", "_")}
        resolved = _get_resolved_name_from_existing(folder, ch_name)
        if resolved:
            name_variants.add(resolved)
            name_variants.add(resolved.replace(" ", "_"))

        has_research_json = False
        for nv in name_variants:
            if list(folder.glob(f"research_{nv}_*.json")) or (folder / f"research_{nv}.json").exists():
                has_research_json = True
                break

        # Check CSV có timestamp mới nhất
        segments_csvs = []
        summary_csvs = []
        research_csvs = []
        for nv in name_variants:
            segments_csvs.extend(folder.glob(f"{nv}_segments_dataset_*.csv"))
            summary_csvs.extend(folder.glob(f"{nv}_video_summary_*.csv"))
            research_csvs.extend(folder.glob(f"research_{nv}_*.csv"))

        # Pipeline summary có timestamp mới nhất
        pipeline_summary = list(folder.glob("pipeline_summary_*.json"))

        required = {
            "research_json": has_research_json,
            "segments_csv": bool(segments_csvs),
            "summary_csv": bool(summary_csvs),
            "research_csv": bool(research_csvs),
            "pipeline_summary_json": bool(pipeline_summary),
        }
        missing = [k for k, v in required.items() if not v]
        return (len(missing) == 0), missing

    all_exist, missing_files = _all_outputs_exist(channel_output, channel_name, run_timestamp)
    if skip_existing and all_exist:
        print(f"[SKIP] Đã tồn tại đầy đủ output cho kênh này")
        # Tìm file research JSON đầu tiên (cả 2 pattern)
        existing_marker = []
        for nv in [channel_name, channel_name.replace(" ", "_")]:
            existing_marker = list(channel_output.glob(f"research_{nv}_*.json"))
            if existing_marker:
                break
            old_marker = channel_output / f"research_{nv}.json"
            if old_marker.exists():
                existing_marker = [old_marker]
                break
        if not existing_marker:
            existing_marker = [channel_output / "research_unknown.json"]  # fallback
        if run_logger:
            run_logger.log(f"[SKIP] Output day du ton tai ({existing_marker[0]}), bo qua kenh")
            run_logger.log_channel_end(
                idx=channel_idx, total=total_channels, channel_url=channel_url,
                status="skipped",
            )
        return {
            "channel": channel_url,
            "channel_folder": channel_name,
            "status": "skipped",
            "output": str(channel_output),
            "run_timestamp": run_timestamp,
            "existing_file": str(existing_marker[0]),
        }

    if skip_existing and missing_files:
        print(f"[PARTIAL] Channel đã có research JSON nhưng THIẾU: {', '.join(missing_files)}")
        print(f"         → Chạy lại pipeline để tái tạo đầy đủ output (KHÔNG skip)")
        if run_logger:
            run_logger.log(f"[PARTIAL] Channel thieu {missing_files}, chay lai pipeline")

    channel_output.mkdir(parents=True, exist_ok=True)
    researcher = YouTubeResearcher(
        api_key=youtube_key,
        output_dir=str(channel_output),
        proxy_rotator=proxy_rotator,
    )

    print(f"\nFetching videos from channel: {channel_url}")
    print(f"Target: {max_results} videos that pass filters (max fetch: {max_fetch})")
    if run_logger:
        run_logger.log(f"Fetching videos (max_results={max_results}, max_fetch={max_fetch}, "
                       f"order={order}, use_rss={use_rss}, batch_size={batch_size}, "
                       f"socket_timeout={socket_timeout}s, video_delay={video_delay}s, "
                       f"fetch_delay={fetch_delay}s, max_retries={max_retries})")

    try:
        if use_rss:
            researcher.fetch_channel_videos_rss(
                channel_input=channel_url,
                max_results=max_fetch,
                order=order,
                rss_delay=rss_delay,
                rss_page_delay=rss_page_delay,
            )
        else:
            researcher.fetch_channel_videos(
                channel_input=channel_url,
                max_results=max_fetch,
                order=order,
                batch_size=batch_size,
                max_batches=max_batches,
                socket_timeout=socket_timeout,
                fetch_delay=fetch_delay,
                max_retries=max_retries,
            )
    except Exception as e:
        if run_logger:
            run_logger.log(f"[ERROR] fetch_channel_videos that bai: {e}")
            run_logger.log_channel_end(
                idx=channel_idx, total=total_channels, channel_url=channel_url,
                status="error", error=str(e),
            )
        raise

    if not researcher._videos:
        print(f"[WARN] Không tìm được video nào từ kênh: {channel_url}")
        if run_logger:
            run_logger.log(f"[WARN] Khong tim duoc video nao tu kenh nay")
            run_logger.log_channel_end(
                idx=channel_idx, total=total_channels, channel_url=channel_url,
                status="no_videos",
            )
        return {
            "channel": channel_url,
            "channel_folder": channel_name,
            "status": "no_videos",
            "output": str(channel_output),
            "run_timestamp": run_timestamp,
        }

    if run_logger:
        run_logger.log(f"Tim thay {len(researcher._videos)} video tu kenh")

    criteria = FilterCriteria(
        min_duration=FILTER_MIN_DURATION,
        max_duration=FILTER_MAX_DURATION,
        min_view_count=FILTER_MIN_VIEW_COUNT,
        min_like_count=FILTER_MIN_LIKE_COUNT,
        min_comment_count=FILTER_MIN_COMMENT_COUNT,
    )

    researcher.apply_filters(criteria)
    if run_logger:
        run_logger.log(f"Sau filter: {len(researcher._filtered_videos)}/{len(researcher._videos)} video")

    # Limit to max_results videos that passed filter
    if len(researcher._filtered_videos) > max_results:
        researcher._filtered_videos = researcher._filtered_videos[:max_results]
        print(f"Limiting to {max_results} filtered videos (as requested)")
        if run_logger:
            run_logger.log(f"Limit xuong {max_results} video (theo --max-results)")

    researcher.fetch_transcripts(transcript_delay=transcript_delay)
    researcher.print_video_table()

    resolved_channel_name = researcher._videos[0].channel if researcher._videos else channel_name
    # Đặt tên file theo tên kênh đã resolve (có dấu, space...) + timestamp
    safe_name = resolved_channel_name.replace(" ", "_")
    researcher.save_research(f"research_{safe_name}_{run_timestamp}.json")

    if not no_transcribe:
        print("\nRunning pipeline (YouTube transcript via yt-dlp - khong fix names)...")
        if run_logger:
            run_logger.log("Bat dau pipeline (YouTube transcript)")
        summary = researcher.process_videos_pipeline(
            output_dir=str(channel_output),
            keep_videos=keep_audio,
            fix_names=not no_fix,  # NO-OP, chi de backward-compat
            audio_format=audio_format,
            run_timestamp=run_timestamp,
            skip_existing_transcripts=not force_retranscribe,
            video_delay=video_delay,
            run_logger=run_logger,
            channel_idx=channel_idx,
            total_channels=total_channels,
        )

        summary_path = channel_output / f"pipeline_summary_{run_timestamp}.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        # Export CSV với audio features (có timestamp)
        # Lưu ý: transcriptions giờ ở subfolder transcriptions/{timestamp}/
        transcription_dir = str(channel_output / "transcriptions" / run_timestamp)

        # Segment-level CSV (mỗi segment 1 row, kèm audio features)
        csv_segments_path = channel_output / f"{safe_name}_segments_dataset_{run_timestamp}.csv"
        export_transcriptions_to_csv(
            output_csv=str(csv_segments_path),
            videos=researcher._filtered_videos,
            transcription_dir=transcription_dir,
        )

        # Video-summary CSV (mỗi video 1 row, đầy đủ audio info)
        csv_summary_path = channel_output / f"{safe_name}_video_summary_{run_timestamp}.csv"
        export_video_summary_csv(
            output_csv=str(csv_summary_path),
            videos=researcher._filtered_videos,
            transcription_dir=transcription_dir,
        )

        # CSV thứ 3: dùng class method (kèm transcription_dir để lấy audio features)
        researcher.save_to_csv(
            filename=f"research_{safe_name}_{run_timestamp}.csv",
            transcription_dir=transcription_dir,
        )

        print(f"\n[DONE] {channel_url} -> {channel_output}")
        if run_logger:
            run_logger.log_channel_end(
                idx=channel_idx, total=total_channels, channel_url=channel_url,
                status="success", summary=summary,
            )
        return {
            "channel": channel_url,
            "channel_name": resolved_channel_name,
            "channel_folder": channel_name,
            "status": "success",
            "output": str(channel_output),
            "run_timestamp": run_timestamp,
            "audio_dir": f"audio/{run_timestamp}/",
            "transcriptions_dir": f"transcriptions/{run_timestamp}/",
            "output_files": {
                "research_json": f"research_{safe_name}_{run_timestamp}.json",
                "research_csv": f"research_{safe_name}_{run_timestamp}.csv",
                "segments_dataset_csv": f"{safe_name}_segments_dataset_{run_timestamp}.csv",
                "video_summary_csv": f"{safe_name}_video_summary_{run_timestamp}.csv",
                "pipeline_summary_json": f"pipeline_summary_{run_timestamp}.json",
            },
            "audio_files": [
                r.get("audio_filename") for r in summary.get("results", []) if r.get("audio_filename")
            ],
            "videos": summary.get("results", []),
            "summary": summary,
        }
    else:
        # Không transcribe: vẫn export metadata CSV (có timestamp)
        researcher.save_to_csv(filename=f"research_{safe_name}_{run_timestamp}.csv")
        print(f"\n[DONE] {channel_url} -> {channel_output} (metadata only)")
        if run_logger:
            run_logger.log(f"[DONE] Da xuat metadata CSV (khong transcribe)")
            run_logger.log_channel_end(
                idx=channel_idx, total=total_channels, channel_url=channel_url,
                status="success_no_transcribe",
            )
        return {
            "channel": channel_url,
            "channel_name": resolved_channel_name,
            "channel_folder": channel_name,
            "status": "success_no_transcribe",
            "output": str(channel_output),
            "run_timestamp": run_timestamp,
            "output_files": {
                "research_json": f"research_{safe_name}_{run_timestamp}.json",
                "research_csv": f"research_{safe_name}_{run_timestamp}.csv",
            },
        }


def main():
    args = parse_args()

    # === yt-dlp version: KHONG CAN YOUTUBE_API_KEY ===
    # API key chi can cho Soniox + Anthropic (transcribe + fix names)
    # Lay video: yt-dlp (mien phi, khong can key)
    print("Mode: yt-dlp (khong can YOUTUBE_API_KEY)")
    youtube_key = "ytdlp"  # placeholder, khong su dung

    # === VPN rotator (chỉ dùng ProtonVPN để fake IP) ===
    proxy_rotator = None
    if args.use_vpn:
        # VPN rotator (ProtonVPN) qua file .ovpn trong ./proton_config/
        try:
            proxy_rotator = get_vpn_rotator_from_config(
                rotate_every=args.vpn_rotate_every,
                strategy=args.vpn_strategy,
            )
            if proxy_rotator:
                print(f"VPN: {len(proxy_rotator)} ProtonVPN servers, "
                      f"rotate_every={args.vpn_rotate_every}, "
                      f"strategy={args.vpn_strategy}")
                print(f"  (cần sudo để start openvpn — sẽ prompt khi connect)")
            else:
                print("VPN: KHONG co file .ovpn trong ./proton_config/ — fallback IP that.")
        except ImportError as e:
            print(f"VPN: Khong import duoc vpn_rotator ({e}) — fallback IP that.")
        except Exception as e:
            print(f"VPN: Loi khoi tao ({e}) — fallback IP that.")
    else:
        print("VPN: TAT (dung IP that cua may). Truyen --use-vpn de fake IP qua ProtonVPN.")

    # === Xác định danh sách kênh ===
    if args.channel:
        # Mode 1 kênh (backward-compat)
        channels = [args.channel]
        print(f"Mode: 1 channel\n  {args.channel}")
    else:
        # Mode nhiều kênh từ file
        channels = load_channels_from_file(args.channels_file)
        if not channels:
            print(f"\n[HELP] File {args.channels_file} đang rỗng hoặc không có URL hợp lệ.")
            print("Thêm URL kênh vào file, mỗi kênh 1 dòng, ví dụ:")
            print("  https://www.youtube.com/@vietnh1009")
            print("  https://www.youtube.com/@channelA")
            print("  UCxxxxxxxxxxxxxxxxxxxxx")
            sys.exit(1)

    # === Tạo output root ===
    output_root = Path(args.output)
    output_root.mkdir(parents=True, exist_ok=True)

    print(f"\nOutput root: {output_root}")
    print(f"Tổng số kênh sẽ xử lý: {len(channels)}\n")

    # === Tao RunLogger de ghi log TXT cho ca batch ===
    batch_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = output_root / "logs"
    log_path = log_dir / f"run_{batch_timestamp}.txt"
    # Xac dinh duong dan script dang chay
    script_path = str(Path(__file__).resolve())
    run_logger = RunLogger(log_path, script_path=script_path)
    cmdline = " ".join(sys.argv) if sys.argv else "python youtube_researcher_youtube_subs.py"
    run_logger.log_batch_start(
        channels_file=args.channels_file,
        total_channels=len(channels),
        command=cmdline,
        script_path=script_path,
    )

    # === Loop qua từng kênh ===
    all_results = []
    for idx, ch_url in enumerate(channels, 1):
        print(f"\n>>> [{idx}/{len(channels)}] {ch_url}")
        try:
            result = process_one_channel(
                ch_url,
                youtube_key=youtube_key,
                output_root=str(output_root),
                max_results=args.max_results,
                max_fetch=args.max_fetch,
                order=args.order,
                keep_audio=args.keep_audio,
                audio_format=args.audio_format,
                no_transcribe=args.no_transcribe,
                no_fix=args.no_fix,
                skip_existing=args.skip_existing,
                force_retranscribe=args.force_retranscribe,
                batch_size=args.batch_size,
                max_batches=args.max_batches,
                fetch_delay=args.fetch_delay,
                use_rss=args.use_rss,
                proxy_rotator=proxy_rotator,
                video_delay=args.video_delay,
                rss_delay=args.rss_delay,
                rss_page_delay=args.rss_page_delay,
                transcript_delay=args.transcript_delay,
                socket_timeout=args.socket_timeout,
                max_retries=args.max_retries,
                max_sentence_duration=args.max_sentence_duration,
                min_sentence_words=args.min_sentence_words,
                run_logger=run_logger,
                channel_idx=idx,
                total_channels=len(channels),
            )
            all_results.append(result)
        except Exception as e:
            err_msg = str(e)
            print(f"[ERROR] Kênh {ch_url} gặp lỗi: {e}")
            import traceback
            traceback.print_exc()
            run_logger.log(f"[ERROR] Kenh {ch_url} gap loi: {e}")
            run_logger.log_channel_end(
                idx=idx, total=len(channels), channel_url=ch_url,
                status="error", error=err_msg,
            )
            all_results.append({"channel": ch_url, "status": "error", "error": err_msg})
            continue

    # === Tổng kết ===
    print("\n" + "=" * 80)
    print("TỔNG KẾT")
    print("=" * 80)
    success = sum(1 for r in all_results if r.get("status") in ("success", "success_no_transcribe", "skipped"))
    failed = sum(1 for r in all_results if r.get("status") in ("error", "no_videos"))
    for r in all_results:
        print(f"  {r.get('status'):<22}  {r.get('channel')}  ->  {r.get('output', '')}")
    print(f"\nTổng: {len(all_results)} | OK: {success} | Lỗi: {failed}")

    # === Log batch end (ghi vao file log TXT) ===
    run_logger.log_batch_end(
        total_channels=len(channels),
        success=success,
        failed=failed,
        all_results=all_results,
    )
    # In duong dan log de user biet
    print(f"\nLog file: {log_path}")

    # Lưu summary tổng (có timestamp để không bị ghi đè giữa các lần chạy)
    # Mặc định: _multi_channel_summary_YYYYMMDD_HHMMSS.json
    # Có thể tùy chỉnh bằng --summary-name
    summary_filename = args.summary_name or f"_multi_channel_summary_{batch_timestamp}.json"
    summary_path = output_root / summary_filename
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "run_timestamp": datetime.now().isoformat(),
                "channels_file": args.channels_file,
                "total_channels": len(channels),
                "success": success,
                "failed": failed,
                "results": all_results,
            },
            f, ensure_ascii=False, indent=2,
        )
    print(f"Summary: {summary_path}")

    # In proxy stats cuối cùng
    if proxy_rotator:
        print("\n" + "=" * 80)
        print("PROXY STATS (final)")
        print("=" * 80)
        proxy_rotator.print_stats()


if __name__ == "__main__":
    main()



# === Mode nhiều kênh (file txt) ===
# === yt-dlp version: KHONG CAN YOUTUBE_API_KEY ===
# === Dung phu de SAN CO cua YouTube (tieng Viet) thay cho Soniox ===
# Mỗi kênh 1 dòng trong file channels.txt:
#   https://www.youtube.com/@vietnh1009
#   https://www.youtube.com/@channelA
#   UCxxxxxxxxxxxxxxxxxxxxx
#
# Chạy loop qua tất cả kênh trong file (mặc định ./channels_audio/channels.txt)
# python youtube_researcher_youtube_subs.py

# Hoặc chỉ định file khác
# python youtube_researcher_youtube_subs.py --channels-file ./my_channels.txt

# Output tách riêng theo từng kênh: <output_root>/<ten_kenh>/{transcriptions, *.csv, *.json}
# python youtube_researcher_youtube_subs.py -o ./datasets --max-results 50

# Bỏ qua kênh đã xử lý
# python youtube_researcher_youtube_subs.py --skip-existing

# KHONG fix proper nouns (bo LLM)
# python youtube_researcher_youtube_subs.py --no-fix

# === Mode 1 kênh (backward-compat) ===
# python youtube_researcher_youtube_subs.py --channel "https://www.youtube.com/@vietnh1009" --max-results 1 --order viewCount --no-fix


# python youtube_researcher_youtube_subs.py --max-results 6000 --order viewCount --no-fix --channels-file /home/hientran/sythetic_crawl_data/channels_audio/channels_thoi_su_0.txt --use-rss 

# source /home/hientran/miniconda3/etc/profile.d/conda.sh &&   conda activate crawl &&   python /home/hientran/sythetic_crawl_data/youtube_researcher_youtube_subs.py   --max-results 1000 --order viewCount --no-fix   --channels-file /home/hientran/sythetic_crawl_data/channels_audio/channels_khoa_hoc_6.txt   --video-delay 5   --max-fetch 5000   --batch-size 50   --skip-existing --use-vpn --vpn-rotate-every 5

# cd /home/hientran/sythetic_crawl_data && /home/hientran/miniconda3/envs/crawl/bin/python3 \
#   youtube_researcher_youtube_subs_multi.py \
#   --channels-file /home/hientran/sythetic_crawl_data/channels_audio/channels_khoa_hoc_6.txt \
#   --output /home/hientran/datasets/khoa_hoc \
#   --max-results 1000 \
#   --max-fetch 5000 \
#   --video-delay 5 \
#   --skip-existing \
