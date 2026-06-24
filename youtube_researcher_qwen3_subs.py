#!/usr/bin/env python3
"""
YouTube Researcher - Qwen3-ASR version (NO API KEY needed)
Bản sao của youtube_researcher_youtube_subs.py, thay YouTube transcript bằng Qwen3-ASR local.

- Không cần YOUTUBE_API_KEY, SONIOX_API_KEY, ANTHROPIC_API_KEY
- Lấy video từ kênh qua yt-dlp (flat-playlist + extract_info)
- **Transcript dùng Qwen3-ASR local** (model ở /home/hientran/Qwen3-ASR)
    + Download audio về .wav 16kHz mono
    + Chạy Qwen3-ASR local (transformers + torch)
    + Không cần API key, không bị YouTube rate-limit transcript
- Có proxy rotation + cookies.txt (dùng cho metadata fetch, không cho ASR)

Usage:
    # Mode nhiều kênh
    python youtube_researcher_qwen3_subs.py

    # Mode 1 kênh
    python youtube_researcher_qwen3_subs.py --channel "https://www.youtube.com/@vietnh1009"

    # Đổi model path (mặc định /home/hientran/Qwen3-ASR)
    QWEN3_MODEL_PATH=/path/to/model python youtube_researcher_qwen3_subs.py ...
"""

import json
import os
import re
import sys
import time
import numpy as np
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

# ================= PROXY HELPER =================
try:
    from proxy_helper import (
        get_rotator_from_file,
        ProxyRotator,
    )
except ImportError:
    # Fallback nếu proxy_helper.py không ở cùng folder
    sys.path.insert(0, str(Path(__file__).parent))
    from proxy_helper import (  # type: ignore
        get_rotator_from_file,
        ProxyRotator,
    )

# ================= COOKIES =================
# Tự động tìm cookies.txt cùng folder script để bypass "Sign in to confirm you're not a bot"
COOKIES_FILE = Path(__file__).parent / "cookies.txt"
COOKIES_FILE_STR = str(COOKIES_FILE) if COOKIES_FILE.exists() else None

# ================= CONFIG =================
YOUTUBE_API_KEY = ""
SONIOX_API_KEY = ""
ANTHROPIC_API_KEY = ""

MINIMAX_MODEL = os.environ.get(
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "MiniMax/MiniMax-M2.7"
)

# ================= FILTER CONFIG =================
FILTER_PUBLISHED_DAYS = 36500
FILTER_MIN_DURATION = 100
FILTER_MAX_DURATION = 100000
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


def api_call_with_retry(func, max_retries=3, delay=5):
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
        "extractor_args": {"youtubetab": {"skip": ["authcheck"]}},
    }
    YouTubeResearcher._apply_cookies(ydl_opts)
    YouTubeResearcher._apply_timeouts(ydl_opts, socket_timeout=12)
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
                          proxy_url: Optional[str] = None) -> list[dict]:
    """
    Lay video tu kenh qua YouTube RSS feed.
    RSS feed cua YouTube tra 15 video moi nhat, co the lap lai nhieu lan
    bang cach dung <link rel='next-archive' href='...start-index=N'/> de lay
    15 video cu hon moi lan.

    Args:
        channel_id: channel ID (UCxxxxx)
        max_results: so video toi da. Neu > 15, script se loop nhieu trang RSS.

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
    max_pages = (max_results // 15) + 2  # 15 video/page, +2 buffer

    for page_idx in range(max_pages):
        if len(all_entries) >= max_results:
            break

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
    YouTubeResearcher._apply_cookies(ydl_opts)
    YouTubeResearcher._apply_timeouts(ydl_opts, socket_timeout=12)
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
                 proxy_rotator: Optional[ProxyRotator] = None,
                 seg_max_words: int = 75,
                 seg_max_duration: float = 30.0,
                 seg_soft_pause: float = 1.0,
                 seg_min_pause_words: int = 10,
                 seg_min_soft_break_words: int = 20):
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._videos: list[VideoCandidate] = []
        self._filtered_videos: list[VideoCandidate] = []
        # Proxy rotator: mỗi request gọi self._next_proxy() để lấy IP mới
        self._rotator = proxy_rotator
        # ===== Segment chunking config (cho _build_segments_from_timestamps) =====
        # Có thể override từ CLI arg --seg-*
        self.seg_max_words = seg_max_words
        self.seg_max_duration = seg_max_duration
        self.seg_soft_pause = seg_soft_pause
        self.seg_min_pause_words = seg_min_pause_words
        self.seg_min_soft_break_words = seg_min_soft_break_words

    def _next_proxy(self, test_before_return: bool = False,
                    max_test_attempts: int = 5) -> Optional[str]:
        """
        Lấy proxy URL tiếp theo từ rotator.

        Args:
            test_before_return: nếu True, test TCP connect trước khi trả.
                               Mặc định False — vì test có thể false-positive
                               (TCP open nhưng HTTP CONNECT fail, hoặc ngược lại).
                               Cứ trả proxy rồi để yt-dlp tự handle + mark_failed khi fail.
            max_test_attempts: số lần thử tối đa (chỉ dùng khi test_before_return=True).
        """
        if not self._rotator:
            return None

        # Nếu không test: chỉ 1 lần next()
        if not test_before_return:
            url = self._rotator.next()
            if url:
                try:
                    from urllib.parse import urlparse
                    p = urlparse(url)
                    short = f"{p.hostname}:{p.port}"
                except Exception:
                    short = url[:40]
                print(f"    [proxy] → {short}")
            return url

        # Có test: lặp cho đến khi tìm proxy OK
        for attempt in range(max_test_attempts):
            url = self._rotator.next()
            if not url:
                return None

            if self._test_proxy_fast(url, timeout=6.0):
                try:
                    from urllib.parse import urlparse
                    p = urlparse(url)
                    short = f"{p.hostname}:{p.port}"
                except Exception:
                    short = url[:40]
                print(f"    [proxy] → {short}")
                return url
            else:
                # Test fail → mark + thử proxy tiếp
                self._mark_proxy_failed(url)
                if attempt < max_test_attempts - 1:
                    continue
                return None

        return None

    def _mark_proxy_failed(self, proxy_url: Optional[str]):
        """Đánh dấu proxy fail (khi gặp 429/timeout/SSL)."""
        if self._rotator and proxy_url:
            self._rotator.mark_failed(proxy_url)

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
    def _apply_timeouts(ydl_opts: dict, socket_timeout: int = 15,
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

    def fetch_channel_videos_rss(
        self,
        channel_input: str,
        max_results: int = 50,
        order: str = "date",
        published_after: Optional[datetime] = None,
    ) -> list[VideoCandidate]:
        """
        Lay video tu kenh qua RSS feed (NHANH, toi da 15 moi nhat).
        Sau do extract full metadata cho tung video qua yt-dlp (1 request/video).

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

        # Lay video tu RSS. RSS cua YouTube chi tra 15 video moi nhat,
        # nhung mot so kenh co 'next-archive' link de paginate.
        proxy_url = self._next_proxy()
        rss_entries = fetch_channel_via_rss(channel_id, max_results=max_results * 2, proxy_url=proxy_url)
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
            # Tao datetime cho cursor (UTC neu khong co published_after)
            cursor_dt = None
            if oldest_date:
                cursor_dt = datetime.strptime(oldest_date, "%Y%m%d")
                if published_after and published_after.tzinfo:
                    cursor_dt = cursor_dt.replace(tzinfo=published_after.tzinfo)
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

    def fetch_channel_videos(
        self,
        channel_input: str,
        max_results: int = 50,
        order: str = "date",
        published_after: Optional[datetime] = None,
        batch_size: int = 5,
        max_batches: int = 10,
        published_before_cursor: Optional[datetime] = None,
    ) -> list[VideoCandidate]:
        """
        Lấy video từ kênh YouTube bằng yt-dlp theo CƠ CHẾ BATCH + CURSOR (nhanh, k stuck kenh lon).

        Args:
            channel_input: URL kênh hoặc channel ID
            max_results: Số video tối đa trả về (sau khi fetch + filter)
            order: 'date' (upload_date) | 'viewCount' (view_count) | 'relevance' (default)
            published_after: Chỉ lấy video sau ngày này (format ISO)
            batch_size: Số video fetch mỗi batch (mặc định 50).
                        Sau mỗi batch, lấy upload_date của video cũ nhất làm cursor
                        cho batch tiếp theo -> tránh phải enumerate toàn bộ playlist.
            max_batches: Số batch tối đa (mặc định 20 = 1000 video tối đa nếu batch_size=50).
                        Tránh loop vô tận nếu kênh có quá nhiều video.
        """
        try:
            import yt_dlp
        except ImportError:
            print("pip install yt-dlp")
            sys.exit(1)

        # Resolve channel ID bang yt-dlp (dùng proxy)
        proxy_url = self._next_proxy()
        channel_id = resolve_channel_id(self.api_key, channel_input, proxy_url=proxy_url)
        if not channel_id:
            print(f"Khong tim thay kenh: {channel_input}")
            return []

        print(f"Channel ID: {channel_id}")

        # Lay danh sach video bang yt-dlp (flat-playlist, nhanh, khong can API key)
        # Tu channel_id, tao URL channel
        if channel_input.startswith("UC") and len(channel_input) == 24:
            channel_url = f"https://www.youtube.com/channel/{channel_id}/videos"
        else:
            # Dam bao URL co /videos
            url = channel_input.strip().rstrip("/")
            if not url.endswith("/videos"):
                channel_url = url + "/videos"
            else:
                channel_url = url

        print(f"Fetching from: {channel_url}")

        # Map order -> yt-dlp sort
        sort_map = {
            "date": "upload_date",
            "viewCount": "view_count",
            "rating": "rating",
            "relevance": "relevance",
        }
        yt_sort = sort_map.get(order, "upload_date")

        need_full_extract = (order == "viewCount")

        # ====== BATCH LOOP: fetch theo batch nho de tranh stuck kenh lon ======
        # Voi moi batch, tao URL rieng co query dateafter/datbefore de lay dung
        # khoang thoi gian cua batch do. Sau batch, lay upload_date cua video
        # cu nhat lam cursor cho batch tiep theo.
        all_entries = []      # List[dict] (yt-dlp info) - da extract full
        seen_ids = set()      # Tranh trung lap giua cac batch
        # Cursor ban dau (neu co tu published_before_cursor)
        last_upload_date = None
        if published_before_cursor:
            last_upload_date = published_before_cursor.strftime("%Y%m%d")
            print(f"  Bat dau tu cursor: before={last_upload_date}")

        for batch_idx in range(1, max_batches + 1):
            # Build URL cho batch nay
            from urllib.parse import urlencode
            params = {"sort": yt_sort, "flow": "grid", "view": 0}
            if last_upload_date:
                # Batch tiep theo: chi lay video CŨ HƠN last_upload_date
                # YouTube playlist dung query 'before' voi YYYYMMDD
                params["before"] = last_upload_date
            batch_url = f"{channel_url}?{urlencode(params)}"

            print(f"\n  [Batch {batch_idx}/{max_batches}] fetch {batch_size} video (cursor={last_upload_date or 'now'})")

            # Luon extract_full (extract_flat=False) de lay upload_date lam cursor
            # Trade-off: cham hon extract_flat=true nhung co cursor incremental
            ydl_flat_opts = {
                "quiet": True,
                "no_warnings": True,
                "extract_flat": False,
                "skip_download": True,
                "playlistend": batch_size,
                "ignoreerrors": True,
                "js_runtimes": {"node": {}},
                # Skip auth check cho playlist/tab (cookies có thể không đủ quyền,
                # nhưng public channel VTV24 vẫn xem được)
                "extractor_args": {"youtubetab": {"skip": ["authcheck"]}},
            }
            YouTubeResearcher._apply_cookies(ydl_flat_opts)
            self._apply_timeouts(ydl_flat_opts, socket_timeout=12)

            # Retry tối đa 3 lần với proxy khác nếu batch fail
            batch_entries = []
            for batch_retry in range(3):
                # Mỗi batch (hoặc retry) 1 proxy mới
                batch_proxy = self._next_proxy()
                if batch_proxy:
                    ydl_flat_opts["proxy"] = batch_proxy

                # Wrap trong thread để bound timeout 30s (kể cả khi socket_timeout fail)
                import concurrent.futures
                try:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(
                            lambda: yt_dlp.YoutubeDL(ydl_flat_opts).extract_info(
                                batch_url, download=False
                            )
                        )
                        try:
                            flat_info = future.result(timeout=30)
                        except concurrent.futures.TimeoutError:
                            print(f"  [yt-dlp] batch {batch_idx} timeout 30s")
                            flat_info = None
                except Exception as e:
                    err_str = str(e)
                    print(f"  [yt-dlp] batch {batch_idx} error: "
                          f"{type(e).__name__}: {err_str[:200]}")
                    flat_info = None

                if flat_info and "entries" in flat_info:
                    batch_entries = [e for e in flat_info["entries"] if e]
                    break  # success
                else:
                    # Batch này fail → mark proxy + retry với proxy khác
                    is_proxy_error = flat_info is None  # timeout/error thường do proxy
                    if self._rotator and batch_proxy:
                        self._rotator.mark_failed(batch_proxy)
                        if is_proxy_error:
                            self._rotator.mark_failed(batch_proxy)  # double cooldown
                    if batch_retry < 2:
                        print(f"  [yt-dlp] batch {batch_idx} retry {batch_retry+2}/3...")
                        time.sleep(3)
                        continue
                    # Hết retry → batch_entries = []
                    batch_entries = []

            if not batch_entries:
                print(f"  Batch {batch_idx} rong -> het video, dung.")
                break

            # Loc bo video trung lap va lay upload_date
            new_entries = []
            batch_oldest_date = None
            for e in batch_entries:
                vid = e.get("id")
                if not vid or vid in seen_ids:
                    continue
                seen_ids.add(vid)
                new_entries.append(e)
                upload_date = e.get("upload_date", "")  # YYYYMMDD
                if upload_date and len(upload_date) == 8:
                    if batch_oldest_date is None or upload_date < batch_oldest_date:
                        batch_oldest_date = upload_date

            if not new_entries:
                print(f"  Batch {batch_idx}: toan video trung lap -> het, dung.")
                break

            print(f"  Batch {batch_idx}: {len(new_entries)} video moi (oldest={batch_oldest_date})")

            # Filter published_after + accumulate
            for entry in new_entries:
                upload_date = entry.get("upload_date", "")

                # Filter published_after
                if published_after and upload_date:
                    try:
                        vd = datetime.strptime(upload_date, "%Y%m%d").replace(tzinfo=published_after.tzinfo)
                        if vd < published_after:
                            continue
                    except ValueError:
                        pass

                all_entries.append(entry)

                if len(all_entries) >= max_results:
                    break

            # Cursor cho batch tiep theo
            if batch_oldest_date:
                last_upload_date = batch_oldest_date
            else:
                # Khong co upload_date -> khong the cursor
                print(f"  Batch {batch_idx}: khong co upload_date -> dung (khong the tiep tuc).")
                break

            # Check da du max_results chua
            if len(all_entries) >= max_results:
                print(f"  Da lay du {len(all_entries)} video -> dung.")
                break

        # ====== END BATCH LOOP ======

        if not all_entries:
            print("Khong tim thay video nao trong kenh nay")
            return []

        # Build VideoCandidate tu info dict (da extract full trong batch loop)
        print(f"\nBuild {len(all_entries)} VideoCandidate tu batch loop...")
        detailed_videos = []
        for i, info in enumerate(all_entries, 1):
            try:
                video = self._build_video_from_ytdlp(info)
                detailed_videos.append(video)
                if i % 10 == 0:
                    print(f"  [{i}/{len(all_entries)}]")
            except Exception as e:
                print(f"  [{i}] Build failed: {e}")
                continue

        # Neu order=viewCount, sort detailed_videos theo view_count
        if order == "viewCount":
            detailed_videos.sort(key=lambda v: v.view_count, reverse=True)
            detailed_videos = detailed_videos[:max_results]

        self._videos = detailed_videos

        if not self._videos:
            print("Khong lay duoc chi tiet video nao")
            return []

        print(f"Tim thay {len(self._videos)} video tu kenh '{self._videos[0].channel if self._videos else channel_input}'")
        return self._videos

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

    def fetch_transcripts(self):
        for video in self._filtered_videos:
            if video.transcript:
                continue

            try:
                from youtube_transcript_api import YouTubeTranscriptApi
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

    # ================= QWEN3-ASR TRANSCRIPT =================
    # Dùng qwen_asr package (chính thức từ Alibaba Qwen team)
    # - Qwen3ASRModel: transcribe audio → text
    # - Qwen3ForcedAligner: align text với audio → start/end time chính xác
    #
    # 2 model weights (sẽ tự download lần đầu):
    # - Qwen/Qwen3-ASR-1.7B (~3.4GB)
    # - Qwen/Qwen3-ForcedAligner-0.6B (~1.2GB)
    #
    # API: asr.transcribe(audio, language, return_time_stamps=True)
    # → results[0].text + results[0].time_stamps (list[(start, end)])

    _qwen3_asr = None  # cache Qwen3ASRModel instance (load 1 lần)

    @staticmethod
    def _get_qwen3_asr_path() -> str:
        """Path model Qwen3-ASR-1.7B."""
        env = os.environ.get("QWEN3_ASR_PATH")
        if env and Path(env).exists():
            return env
        # Default: HuggingFace (sẽ tự download lần đầu)
        return os.environ.get("QWEN3_ASR_REPO", "Qwen/Qwen3-ASR-1.7B")

    @staticmethod
    def _get_qwen3_aligner_path() -> str:
        """Path model Qwen3-ForcedAligner-0.6B."""
        env = os.environ.get("QWEN3_ALIGNER_PATH")
        if env and Path(env).exists():
            return env
        return os.environ.get("QWEN3_ALIGNER_REPO", "Qwen/Qwen3-ForcedAligner-0.6B")

    def _load_qwen3_model(self):
        """
        Lazy-load Qwen3ASRModel (kèm forced aligner).
        Load 1 lần, cache trong self._qwen3_asr.
        """
        if self._qwen3_asr is not None:
            return self._qwen3_asr

        asr_path = self._get_qwen3_asr_path()
        aligner_path = self._get_qwen3_aligner_path()
        print(f"  [qwen3-asr] Loading ASR model: {asr_path}")
        print(f"  [qwen3-asr] Loading forced aligner: {aligner_path}")

        try:
            import torch
        except ImportError:
            print("  [qwen3-asr] pip install torch")
            return None

        try:
            # Import từ local Qwen3-ASR repo (đã có ở /home/hientran/Qwen3-ASR)
            qwen_asr_root = "/home/hientran/Qwen3-ASR"
            if Path(qwen_asr_root).exists() and qwen_asr_root not in sys.path:
                sys.path.insert(0, qwen_asr_root)

            from qwen_asr import Qwen3ASRModel
        except ImportError as e:
            print(f"  [qwen3-asr] Không import được qwen_asr: {e}")
            print(f"  [qwen3-asr] Cài: pip install -e /home/hientran/Qwen3-ASR")
            return None

        try:
            device = "cuda" if torch.cuda.is_available() else "cpu"
            dtype = torch.bfloat16 if device == "cuda" else torch.float32

            self._qwen3_asr = Qwen3ASRModel.from_pretrained(
                asr_path,
                dtype=dtype,
                device_map=device,
                forced_aligner=aligner_path,
                forced_aligner_kwargs=dict(
                    dtype=dtype,
                    device_map=device,
                ),
                max_inference_batch_size=8,  # tuỳ VRAM
                max_new_tokens=2048,
            )
            print(f"  [qwen3-asr] Loaded on {device}, dtype={dtype}")
            return self._qwen3_asr
        except Exception as e:
            print(f"  [qwen3-asr] Failed to load: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            self._qwen3_asr = None
            return None

    def _transcribe_with_qwen3(self, audio_path: Path, lang: list = None) -> dict | None:
        """
        Chạy Qwen3-ASR + ForcedAligner trên file audio đã download.

        Args:
            audio_path: path file audio (wav/mp3/opus, bất kỳ format soundfile support)
            lang: list ngôn ngữ ưu tiên (vd: ['vi', 'en'])
                  None = auto-detect (chậm hơn ~20%)

        Returns:
            dict {segments, audio_duration, transcript_language, transcript_is_auto, source}
            hoặc None nếu fail.
        """
        if not audio_path or not Path(audio_path).exists():
            print(f"  [qwen3-asr] audio not found: {audio_path}")
            return None

        asr = self._load_qwen3_model()
        if asr is None:
            return None

        try:
            import soundfile as sf

            # Lấy audio duration
            try:
                audio_data, sr = sf.read(str(audio_path))
                if audio_data.ndim > 1:
                    audio_data = audio_data.mean(axis=1)
                audio_duration = len(audio_data) / sr
            except Exception:
                audio_duration = 0.0

            # Map mã ngôn ngữ ISO → tên đầy đủ (Qwen3-ASR dùng tên tiếng Anh)
            # Xem SUPPORTED_LANGUAGES trong qwen_asr/inference/qwen3_asr.py
            # ví dụ: "vi" → "Vietnamese", "en" → "English", "zh" → "Chinese"
            lang_map = {
                "vi": "Vietnamese", "en": "English", "zh": "Chinese",
                "ja": "Japanese", "ko": "Korean", "th": "Thai",
                "fr": "French", "de": "German", "es": "Spanish",
                "ru": "Russian", "ar": "Arabic", "hi": "Hindi",
                "id": "Indonesian", "ms": "Malay", "pt": "Portuguese",
                "it": "Italian", "nl": "Dutch", "pl": "Polish",
            }
            qwen_lang = None
            if lang:
                iso = lang[0].lower().split("-")[0]  # "vi-VN" -> "vi"
                qwen_lang = lang_map.get(iso, iso.capitalize())

            print(f"  [qwen3-asr] Transcribing {audio_duration:.1f}s audio, "
                  f"lang={qwen_lang}...")

            # Gọi API chính thức với timestamps
            results = asr.transcribe(
                audio=str(audio_path),
                language=qwen_lang,
                return_time_stamps=True,
            )

            if not results:
                print(f"  [qwen3-asr] empty results")
                return None

            result = results[0]  # single sample
            text = result.text or ""
            timestamps = result.time_stamps or []

            if not text.strip():
                print(f"  [qwen3-asr] empty text")
                return None

            # timestamps = list of (start_sec, end_sec)
            # text = full transcription string
            # Convert thành segments: mỗi segment = 1 timestamp span
            segments = []
            if timestamps:
                # Có timestamps: chia text theo timestamps
                # (qwen3_forced_aligner trả timestamp per-token, ta nhóm theo dấu câu)
                segments = self._build_segments_from_timestamps(
                    text, timestamps,
                    max_words=self.seg_max_words,
                    max_duration=self.seg_max_duration,
                    soft_pause=self.seg_soft_pause,
                    min_pause_words=self.seg_min_pause_words,
                    min_soft_break_words=self.seg_min_soft_break_words,
                )
            else:
                # Fallback: không có timestamps (aligner fail) → chia đều
                print(f"  [qwen3-asr] no timestamps from aligner, fallback proportional")
                segments = self._split_text_to_segments(text, audio_duration)

            print(f"  [qwen3-asr] OK: {len(segments)} segments, "
                  f"{len(text)} chars, {len(timestamps)} timestamp tokens")

            # Language
            lang_name = (self._iso_lang_to_vietnamese(lang[0]) if lang
                          else "Tiếng Việt")

            return {
                "segments": segments,
                "audio_duration": round(audio_duration, 3),
                "detected_languages": [lang_name],
                "transcript_language": lang_name,
                "transcript_is_auto": False,
                "transcript_source": "qwen3-asr-forced-aligner",
            }
        except Exception as e:
            print(f"  [qwen3-asr] transcription failed: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return None

    @staticmethod
    def _build_segments_from_timestamps(text: str, timestamps: list,
                                          max_words: int = 75,
                                          max_duration: float = 30.0,
                                          soft_pause: float = 1.0,
                                          min_pause_words: int = 10,
                                          min_soft_break_words: int = 20,
                                          hard_break_chars: str = ".?!",
                                          soft_break_chars: str = ",;:—–-") -> list:
        """
        Build segments từ timestamps per-token. Tất cả thresholds là tham số.

        Một segment được đóng khi gặp 1 trong các điều kiện:
          1. HARD BREAK  : token kết thúc bằng ký tự trong `hard_break_chars` (mặc định .?!)
          2. SOFT BREAK  : token kết thúc bằng ký tự trong `soft_break_chars` (mặc định ,;:) VÀ
                          segment hiện tại đã có >= `min_soft_break_words` từ
          3. PAUSE       : khoảng lặng giữa 2 token > `soft_pause` giây VÀ
                          segment hiện tại đã có >= `min_pause_words` từ
          4. MAX LENGTH  : segment hiện tại >= `max_words` từ HOẶC >= `max_duration` giây
                          (force break để tránh segment quá dài)

        Mặc định phù hợp với văn nói tiếng Việt dài (~75 từ / 30s / đoạn):
          - Câu ngắn: tách tại dấu .?!
          - Câu dài không dấu: chờ pause > 1s (sau khi đủ 10 từ)
          - Câu rất dài: force break tại 75 từ / 30s

        `timestamps` có thể là:
          - list[tuple/list] (start, end)
          - list[ForcedAlignItem] với attrs .start_time, .end_time
        """
        if not timestamps:
            return []

        # Helper: lấy start/end từ item bất kể là tuple hay ForcedAlignItem
        def _ts_start(item):
            if isinstance(item, (list, tuple)):
                return float(item[0])
            return float(getattr(item, "start_time", 0.0))

        def _ts_end(item):
            if isinstance(item, (list, tuple)):
                return float(item[1])
            return float(getattr(item, "end_time", 0.0))

        # Tách text thành tokens (theo space)
        words = text.split()
        if len(words) != len(timestamps):
            # Misalign: dùng proportional split với total_duration = end của item cuối
            total_duration = _ts_end(timestamps[-1]) if timestamps else 0.0
            return YouTubeResearcher._split_text_to_segments(text, total_duration)

        segments = []
        current_words = []
        current_start = _ts_start(timestamps[0])
        last_end_t = current_start  # track để tính pause giữa các token

        for word, ts_item in zip(words, timestamps):
            start_t = _ts_start(ts_item)
            end_t = _ts_end(ts_item)
            stripped = word.rstrip()
            last_char = stripped[-1] if stripped else ""

            current_words.append(word)
            cur_duration = end_t - current_start
            cur_word_count = len(current_words)
            pause_here = start_t - last_end_t

            is_hard_break = last_char in hard_break_chars
            is_soft_break = (last_char in soft_break_chars
                             and cur_word_count >= min_soft_break_words)
            is_long_pause = (pause_here > soft_pause
                             and cur_word_count >= min_pause_words)
            is_too_long = (cur_word_count >= max_words
                            or cur_duration >= max_duration)

            if is_hard_break or is_too_long or is_soft_break or is_long_pause:
                segments.append({
                    "start": round(current_start, 3),
                    "end": round(end_t, 3),
                    "speaker": "SPEAKER_00",
                    "text": " ".join(current_words).strip(),
                })
                current_words = []
                current_start = end_t

            last_end_t = end_t

        # Flush phần còn lại
        if current_words:
            last_end = _ts_end(timestamps[-1]) if timestamps else current_start
            segments.append({
                "start": round(current_start, 3),
                "end": round(last_end, 3),
                "speaker": "SPEAKER_00",
                "text": " ".join(current_words).strip(),
            })

        return [s for s in segments if s["text"]]

    @staticmethod
    def _split_text_to_segments(text: str, total_duration: float) -> list:
        """
        Chia text dài thành segments theo dấu câu (. ? !).
        Phân bố thời gian đều trong total_duration.
        """
        import re
        # Tách câu theo dấu kết thúc, giữ lại dấu
        sentences = re.split(r'(?<=[.?!])\s+', text.strip())
        sentences = [s.strip() for s in sentences if s.strip()]

        if not sentences:
            return [{
                "start": 0.0,
                "end": round(total_duration, 3),
                "speaker": "SPEAKER_00",
                "text": text.strip(),
            }]

        # Tính thời gian mỗi câu theo tỉ lệ độ dài
        total_chars = sum(len(s) for s in sentences)
        if total_chars == 0:
            total_chars = 1

        segments = []
        current_t = 0.0
        for s in sentences:
            ratio = len(s) / total_chars
            seg_duration = total_duration * ratio
            segments.append({
                "start": round(current_t, 3),
                "end": round(current_t + seg_duration, 3),
                "speaker": "SPEAKER_00",
                "text": s,
            })
            current_t += seg_duration

        # Adjust: đảm bảo segment cuối = total_duration
        if segments:
            segments[-1]["end"] = round(total_duration, 3)

        return segments

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
                                            proxy_url: Optional[str] = None) -> dict | None:
        """
        Lấy phụ đề qua yt-dlp bằng 2 bước:
          Bước 1: extract_info với listsubtitles → check video có sub không, lấy URL trực tiếp
          Bước 2: download URL sub → parse vtt/json3

        Trả về dict hoặc None.
        """
        try:
            import yt_dlp
        except ImportError:
            return None

        # ====== BƯỚC 1: list subtitles URLs ======
        ydl_opts = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "ignoreerrors": True,
            "js_runtimes": {"node": {}},
            "extractor_args": {"youtubetab": {"skip": ["authcheck"]}},
        }
        YouTubeResearcher._apply_cookies(ydl_opts)
        self._apply_timeouts(ydl_opts, socket_timeout=12)
        if proxy_url:
            ydl_opts["proxy"] = proxy_url

        # Wrap trong thread + bound timeout 25s (kể cả khi yt-dlp internal timeout 20s fail)
        # Lý do: socket_timeout không control được proxy CONNECT timeout
        import concurrent.futures
        info = None
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
                        self._mark_proxy_failed(proxy_url)
                    return None
        except Exception as e:
            err_str = str(e)
            print(f"  [ytdlp-subs] extract_info error: {type(e).__name__}: {err_str[:200]}")
            if proxy_url and ('connect timeout' in err_str.lower()
                              or 'timed out' in err_str.lower()
                              or 'proxy' in err_str.lower()):
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
                    self._mark_proxy_failed(proxy_url)
                return None  # signal retry
            if resp.status_code == 403:
                print(f"  [ytdlp-subs] HTTP 403 (forbidden) via {self._short_proxy(proxy_url)}")
                if proxy_url:
                    self._mark_proxy_failed(proxy_url)
                return None
            resp.raise_for_status()
            content = resp.text
        except Exception as e:
            err_str = str(e)
            print(f"  [ytdlp-subs] download sub failed: {type(e).__name__}: {err_str[:200]}")
            # Nếu lỗi có chữ "429" hoặc "Too Many" → mark proxy fail
            if '429' in err_str or 'Too Many' in err_str or 'connect timeout' in err_str.lower():
                if proxy_url:
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
        Format:
          {
            "events": [
              {
                "t": 0, "d": 1500,           <- start_ms, duration_ms của cả event
                "segs": [{"utf8": "Hello"}, {"utf8": " world"}]
              },
              ...
            ]
          }
        Mỗi event = 1 câu có timing, segs chỉ chứa text fragments.

        Trả về list [{"start", "duration", "text"}] giây (float).
        """
        segs = []
        for ev in data.get("events", []):
            start_ms = ev.get("t")
            dur_ms = ev.get("d")
            if start_ms is None:
                continue
            start = float(start_ms) / 1000.0
            dur = float(dur_ms) / 1000.0 if dur_ms is not None else 0.0

            # Gom text từ segs[]
            parts = []
            for s in ev.get("segs", []) or []:
                txt = s.get("utf8") or s.get("text") or ""
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

    def transcribe_with_youtube(self, video_id: str, audio_path: Path = None, lang: list = None) -> dict | None:
        """
        Transcribe bằng Qwen3-ASR local (audio đã download ở Bước 0).

        Không dùng:
            - Soniox API
            - youtube-transcript-api (YouTube block IP cloud)
            - yt-dlp subtitles (bị 429 rate limit)

        Args:
            video_id: chỉ để log, không dùng
            audio_path: path file .wav 16kHz mono (BẮT BUỘC có)
            lang: list ngôn ngữ ưu tiên (vd: ['vi', 'en'])

        Returns:
            dict {segments, audio_duration, transcript_language, ...}
            hoặc None nếu fail.
        """
        if lang is None:
            lang = ["vi"]

        if not audio_path or not Path(audio_path).exists():
            print(f"  [qwen3-asr] No audio file: {audio_path}")
            return None

        print(f"  Transcribing with Qwen3-ASR (langs={lang})...")

        # Gọi Qwen3-ASR (model đã lazy-load ở lần đầu)
        result = self._transcribe_with_qwen3(audio_path, lang=lang)

        if not result:
            print("  Qwen3-ASR returned no result")
            return None

        # Đã có segments sẵn từ _transcribe_with_qwen3 (đã split sentences)
        return result

        # ===== CODE CŨ (yt-dlp) - GIỮ LẠI ĐỂ BACKUP, KHÔNG GỌI =====
        if False:  # noqa
            pass

    def _merge_youtube_segments_to_sentences(self, raw_parsed: list) -> list:
        """
        Gom các snippet YouTube auto-subs (overlap timestamps, cắt giữa câu) thành câu hoàn chỉnh.
        YouTube chia text liên tục thành snippets ~3-5s, text nối tiếp nhau (không lặp),
        nhưng bị cắt giữa từ/câu. Gom lại cho đến khi gặp dấu kết thúc câu (. ? !).
        """
        if not raw_parsed:
            return []

        # Bước 1: Nối tất cả text + track timestamps cho từng từ
        # Mỗi raw segment ~3-5s chứa vài từ, nối liên tục
        word_entries = []  # list of (word, start_time, end_time)
        for seg in raw_parsed:
            words = seg["text"].split()
            if not words:
                continue
            seg_dur = seg["end"] - seg["start"]
            # Phân bố thời gian đều cho các từ trong segment
            time_per_word = seg_dur / len(words) if len(words) > 0 else 0
            for j, w in enumerate(words):
                w_start = seg["start"] + j * time_per_word
                w_end = seg["start"] + (j + 1) * time_per_word
                word_entries.append((w, w_start, w_end))

        if not word_entries:
            return []

        # Bước 2: Gom từ thành câu theo dấu kết thúc (. ? !)
        sentences = []
        current_words = []
        current_start = word_entries[0][1]

        for word, w_start, w_end in word_entries:
            current_words.append(word)
            current_end = w_end

            # Kiểm tra từ cuối có kết thúc câu không
            stripped_word = word.rstrip()
            duration = current_end - current_start
            is_sentence_end = stripped_word and stripped_word[-1] in ".?!"

            if is_sentence_end or duration > 30.0:
                text = " ".join(current_words).strip()
                if text:
                    sentences.append({
                        "start": round(current_start, 3),
                        "end": round(current_end, 3),
                        "speaker": "SPEAKER_00",
                        "text": text,
                    })
                current_words = []
                current_start = current_end

        # Flush phần còn lại (bỏ noise ngắn < 3 từ không có dấu kết thúc)
        if current_words:
            text = " ".join(current_words).strip()
            if text and (len(current_words) >= 3 or (text[-1] in ".?!")):
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
                resp = requests.post(url, json=payload, headers=headers, timeout=120)
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
        video_delay: int = 5,
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

        results = []

        for i, video in enumerate(self._filtered_videos, 1):
            print(f"\n[{i}/{len(self._filtered_videos)}] {video.title[:60]}")

            # Delay giữa các video để tránh YouTube rate limit (429)
            if i > 1 and video_delay > 0:
                time.sleep(video_delay)

            audio_path = None
            audio_filename = None

            # Bước -1: skip nếu đã có transcription JSON cho video này
            # (tìm trong TẤT CẢ timestamp folders dưới transcriptions/,
            #  không chỉ folder hiện tại - để re-run không re-transcribe)
            if skip_existing_transcripts:
                target_name = self._safe_filename(video.title, fallback=video.video_id)
                target_json_name = f"{target_name}_transcription.json"

                existing_json = None
                # 1) Tìm trong folder timestamp hiện tại
                expected_json = transcriptions_dir / target_json_name
                if expected_json.exists():
                    existing_json = expected_json
                else:
                    # 2) Tìm trong TẤT CẢ timestamp folders khác
                    trans_root = output_dir / "transcriptions"
                    if trans_root.exists():
                        # Tìm theo glob (vd: */target_name_transcription.json)
                        for candidate in trans_root.glob(f"*/{target_json_name}"):
                            if candidate.exists():
                                existing_json = candidate
                                break

                if existing_json is not None:
                    print(f"  [SKIP] Đã có {existing_json.relative_to(output_dir)}, bỏ qua (dùng --force-retranscribe để ép chạy lại)")
                    # Load lại thông tin từ JSON để giữ video.audio_filename cho CSV
                    try:
                        with open(existing_json, "r", encoding="utf-8") as jf:
                            existing = json.load(jf)
                        video.audio_filename = existing.get("audio_path", f"{target_name}.wav")
                    except Exception:
                        video.audio_filename = f"{target_name}.wav"
                    results.append({
                        "video_id": video.video_id,
                        "title": video.title,
                        "status": "skipped",
                        "audio_filename": video.audio_filename,
                        "transcription_filename": existing_json.name,
                        "transcript_language": "N/A",
                        "transcript_is_auto": None,
                        "transcript_source": "existing",
                    })
                    continue

            # Bước 0: tải audio về (luôn giữ audio)
            target_name = self._safe_filename(video.title, fallback=video.video_id)
            try:
                import yt_dlp
                # Mỗi video 1 proxy mới cho audio download
                dl_proxy = self._next_proxy()
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
                self._apply_cookies(ydl_opts)
                self._apply_timeouts(ydl_opts, socket_timeout=20)  # audio download: cho phép lâu hơn
                if dl_proxy:
                    ydl_opts['proxy'] = dl_proxy
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(video.url, download=True)
                    filename = ydl.prepare_filename(info)
                    audio_path = Path(filename)

                    # Tìm file .wav (postprocessor convert từ .webm/.m4a sang .wav)
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
                audio_filename = f"{target_name}.wav"

            # Bước 1: lấy transcript sẵn có từ YouTube (KHÔNG qua Soniox)
            result = self.transcribe_with_youtube(
                video_id=video.video_id,
                audio_path=audio_path,
                lang=["vi", "en"],
            )

            if result:
                segments = result["segments"]

                # KHONG fix proper nouns nua - giu nguyen transcript tu YouTube.
                # (MiniMax integration da bi loai bo.)

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
            else:
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "transcript_unavailable",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": datetime.now().isoformat() if audio_filename else None,
                })
                print("  No YouTube transcript available")


        success = sum(1 for r in results if r.get("status") == "success")
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
    p.add_argument("--max-results", "-m", type=int, default=8000,
                   help="Số lượng video OUTPUT thỏa mãn filter (không phải số video fetch)")
    p.add_argument("--max-fetch", type=int, default=8000,
                   help="Số video tối đa fetch từ YouTube API (giới hạn để tránh quá nhiều API calls)")
    p.add_argument("--batch-size", type=int, default=50,
                   help="Số video fetch mỗi batch (mặc định 50). "
                        "Sau mỗi batch, lấy upload_date video cũ nhất làm cursor cho batch tiếp theo. "
                        "Tăng lên 100-200 nếu muốn nhanh hơn, giảm xuống 20-30 nếu kênh rất lớn.")
    p.add_argument("--max-batches", type=int, default=160,
                   help="Số batch tối đa (mặc định 20). Với batch_size=50, tối đa 1000 video.")
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
    p.add_argument("--proxy-file", default="./proxies.txt",
                   help="File TXT chứa proxy pool, mỗi dòng 'ip:port:user:pass' "
                        "(mặc định: ./proxies.txt)")
    p.add_argument("--proxy-rotate", choices=["sequential", "random", "least_used"],
                   default="sequential",
                   help="Chiến lược rotate IP: sequential | random | least_used")
    p.add_argument("--no-proxy", action="store_true",
                   help="Tắt proxy hoàn toàn (dùng IP thật của máy)")
    p.add_argument("--proxy-cooldown", type=int, default=180,
                   help="Số giây skip proxy sau khi fail (mặc định 180s = 3 phút)")
    p.add_argument("--video-delay", type=int, default=5,
                   help="Delay giữa các video (giây) để giảm YouTube rate limit (mặc định 5s)")

    # ===== Segment chunking config (cho _build_segments_from_timestamps) =====
    # Mặc định: 75 từ / 30s / segment — phù hợp với văn nói tiếng Việt dài
    p.add_argument("--seg-max-words", type=int, default=75,
                   help="Số từ tối đa mỗi segment (force break khi đạt). Mặc định: 75")
    p.add_argument("--seg-max-duration", type=float, default=30.0,
                   help="Thời lượng tối đa mỗi segment, giây. Mặc định: 30.0")
    p.add_argument("--seg-soft-pause", type=float, default=1.0,
                   help="Tách segment khi im lặng > N giây (giữa 2 token). Mặc định: 1.0s")
    p.add_argument("--seg-min-pause-words", type=int, default=10,
                   help="Chỉ tách tại pause nếu segment hiện tại đã có >= N từ. Mặc định: 10")
    p.add_argument("--seg-min-soft-break-words", type=int, default=20,
                   help="Chỉ tách tại dấu ,;:— nếu segment hiện tại đã có >= N từ. Mặc định: 20")
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
    max_batches: int = 160,
    use_rss: bool = False,
    proxy_rotator: Optional[ProxyRotator] = None,
    video_delay: int = 5,
) -> dict:
    """
    Chạy full pipeline cho 1 kênh YouTube.
    Output: <output_root>/<channel_name>/...
    Tất cả file output đều có timestamp: research_{channel}_{YYYYMMDD_HHMMSS}.{ext}
    """
    channel_name = safe_channel_name(channel_url)
    channel_output = Path(output_root) / channel_name

    # Timestamp cho lần chạy này, gắn vào tất cả file output để không bị ghi đè
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\n" + "=" * 80)
    print(f"CHANNEL: {channel_url}")
    print(f"Channel name: {channel_name}")
    print(f"Output: {channel_output}")
    print(f"Run timestamp: {run_timestamp}")
    print("=" * 80)

    # Skip nếu đã có output từ lần chạy trước
    # Pattern file có timestamp: research_{channel}_{timestamp}.json
    # Cũng chấp nhận pattern cũ không timestamp: research_{channel}.json (backward-compat)
    existing_marker = list(channel_output.glob(f"research_{channel_name}_*.json")) if channel_output.exists() else []
    if not existing_marker:
        # Check pattern cũ (backward-compat)
        old_marker = channel_output / f"research_{channel_name}.json"
        if old_marker.exists():
            existing_marker = [old_marker]
    if skip_existing and existing_marker:
        print(f"[SKIP] Đã tồn tại {existing_marker[0]}, bỏ qua (dùng --no-skip-existing để ép chạy lại)")
        return {
            "channel": channel_url,
            "channel_folder": channel_name,
            "status": "skipped",
            "output": str(channel_output),
            "run_timestamp": run_timestamp,
            "existing_file": str(existing_marker[0]),
        }

    channel_output.mkdir(parents=True, exist_ok=True)
    researcher = YouTubeResearcher(
        api_key=youtube_key,
        output_dir=str(channel_output),
        proxy_rotator=proxy_rotator,
        seg_max_words=args.seg_max_words,
        seg_max_duration=args.seg_max_duration,
        seg_soft_pause=args.seg_soft_pause,
        seg_min_pause_words=args.seg_min_pause_words,
        seg_min_soft_break_words=args.seg_min_soft_break_words,
    )

    print(f"\nFetching videos from channel: {channel_url}")
    print(f"Target: {max_results} videos that pass filters (max fetch: {max_fetch})")

    if use_rss:
        researcher.fetch_channel_videos_rss(
            channel_input=channel_url,
            max_results=max_fetch,
            order=order,
        )
    else:
        researcher.fetch_channel_videos(
            channel_input=channel_url,
            max_results=max_fetch,
            order=order,
            batch_size=batch_size,
            max_batches=max_batches,
        )

    if not researcher._videos:
        print(f"[WARN] Không tìm được video nào từ kênh: {channel_url}")
        return {
            "channel": channel_url,
            "channel_folder": channel_name,
            "status": "no_videos",
            "output": str(channel_output),
            "run_timestamp": run_timestamp,
        }

    criteria = FilterCriteria(
        min_duration=FILTER_MIN_DURATION,
        max_duration=FILTER_MAX_DURATION,
        min_view_count=FILTER_MIN_VIEW_COUNT,
        min_like_count=FILTER_MIN_LIKE_COUNT,
        min_comment_count=FILTER_MIN_COMMENT_COUNT,
    )

    researcher.apply_filters(criteria)

    # Limit to max_results videos that passed filter
    if len(researcher._filtered_videos) > max_results:
        researcher._filtered_videos = researcher._filtered_videos[:max_results]
        print(f"Limiting to {max_results} filtered videos (as requested)")

    researcher.fetch_transcripts()
    researcher.print_video_table()

    resolved_channel_name = researcher._videos[0].channel if researcher._videos else channel_name
    # Đặt tên file theo tên kênh đã resolve (có dấu, space...) + timestamp
    safe_name = resolved_channel_name.replace(" ", "_")
    researcher.save_research(f"research_{safe_name}_{run_timestamp}.json")

    if not no_transcribe:
        print("\nRunning pipeline (YouTube transcript via yt-dlp - khong fix names)...")
        summary = researcher.process_videos_pipeline(
            output_dir=str(channel_output),
            keep_videos=keep_audio,
            fix_names=not no_fix,  # NO-OP, chi de backward-compat
            audio_format=audio_format,
            run_timestamp=run_timestamp,
            skip_existing_transcripts=not force_retranscribe,
            video_delay=video_delay,
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

    # === yt-dlp + Qwen3-ASR version: KHONG CAN YOUTUBE_API_KEY, SONIOX, ANTHROPIC ===
    # Lay video: yt-dlp (mien phi, khong can key)
    # Transcript: Qwen3-ASR local (khong can API key, khong bi YouTube rate-limit)
    asr_path = YouTubeResearcher._get_qwen3_asr_path()
    aligner_path = YouTubeResearcher._get_qwen3_aligner_path()
    print("Mode: yt-dlp + Qwen3-ASR (khong can YOUTUBE_API_KEY)")
    print(f"Qwen3 ASR: {asr_path} "
          f"({'local' if Path(asr_path).exists() else 'will download'})")
    print(f"Qwen3 ForcedAligner: {aligner_path} "
          f"({'local' if Path(aligner_path).exists() else 'will download'})")
    youtube_key = "ytdlp"  # placeholder, khong su dung

    # === Proxy rotator ===
    proxy_rotator = None
    if args.no_proxy:
        print("Proxy: TAT (dung IP that cua may)")
    else:
        proxy_rotator = get_rotator_from_file(
            file_path=args.proxy_file,
            strategy=args.proxy_rotate,
            fail_cooldown=args.proxy_cooldown,
        )
        if proxy_rotator:
            print(f"Proxy: {len(proxy_rotator)} proxies loaded, "
                  f"strategy={args.proxy_rotate}, cooldown={args.proxy_cooldown}s")
        else:
            print("Proxy: KHONG CO (se dung IP that cua may)")

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
                use_rss=args.use_rss,
                proxy_rotator=proxy_rotator,
                video_delay=args.video_delay,
            )
            all_results.append(result)
        except Exception as e:
            print(f"[ERROR] Kênh {ch_url} gặp lỗi: {e}")
            import traceback
            traceback.print_exc()
            all_results.append({"channel": ch_url, "status": "error", "error": str(e)})
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

    # Lưu summary tổng (có timestamp để không bị ghi đè giữa các lần chạy)
    # Mặc định: _multi_channel_summary_YYYYMMDD_HHMMSS.json
    # Có thể tùy chỉnh bằng --summary-name
    batch_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
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



# python3 youtube_researcher_youtube_subs.py \
#   --channels-file /home/hientran/sythetic_crawl_data/channels_audio/channels_thoi_su_0.txt \
#   --output /home/hientran/sythetic_crawl_data/youtube_dataset \
#   --max-results 15 \
#   --use-rss \
#   --no-fix