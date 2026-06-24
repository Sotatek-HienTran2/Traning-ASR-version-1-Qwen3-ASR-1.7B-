#!/usr/bin/env python3
"""
YouTube Researcher — AUDIO + YOUTUBE SUBS (MULTI-ROTATOR) — v3 METADATA-ENRICHED
==================================================================================

v3 = youtube_researcher_audio_subs_multi_rotator_v2.py + YouTube API metadata nâng cao.

So với v2, bổ sung:
  - VideoCandidate: +12 field (dimension, projection, licensed_content,
    privacy_status, embeddable, made_for_kids, live_broadcast_content,
    live_status, was_live, topic_categories, recording_location, availability)
  - _api_item_to_ytdlp_dict(): +5 keys (height, availability, playable_in_embed,
    is_live, live_status, was_live, license)
  - _save_transcription(): JSON lưu đủ 12 field mới
  - export_video_summary_csv(): video-level CSV 40+ cột (bổ sung bên cạnh
    segment CSV 11 cột)

Giữ nguyên từ v2:
  - 3 VPN rotator TÁCH BIỆT cho 3 nhóm việc:
    1. METADATA rotator  : channel listing + Data API + yt-dlp info
    2. AUDIO rotator     : yt-dlp audio download + ffmpeg → .wav
    3. TRANSCRIPT rotator: yt-dlp subtitles (NO fallback)

  Tại sao TÁCH RIÊNG?
  - 3 nhóm có **đặc thù traffic khác nhau**:
    * Metadata: ít request, cần IP ổn định
    * Audio: NHIỀU request + traffic lớn, dễ bị rate-limit (low-speed)
    * Transcript: cần IP riêng để bypass "Sign in to confirm" challenge
      mà KHÔNG ảnh hưởng audio đang download
  - 3 rotator dùng CHUNG 1 VPNRotator sẽ:
    * Chia sẻ state (_request_count, _current_idx, _current_pid)
    * Trigger rotate theo nhau → pattern IP không ổn định
    * Khi 1 nhóm rate-limit → 2 nhóm kia cũng bị ảnh hưởng

Cấu hình mặc định:
  - metadata_rotator  : rotate_every=10, real_ip_cycle=0 (TẮT cycle)
  - audio_rotator     : rotate_every=0,  real_ip_cycle=11 (cứ 10 fake + 1 real)
  - transcript_rotator: rotate_every=10, real_ip_cycle=0 (TẮT cycle)

Mỗi rotator có:
  - Tunnel openvpn RIÊNG (PID riêng, log file riêng)
  - State riêng (_current_idx, _current_pid, _request_count, _usage_count)
  - File log: /tmp/openvpn-proton-<instance_id>_<role>.log

Đặc điểm:
  - **LUÔN dùng ProtonVPN OpenVPN tunnel** để fake IP (./proton_config/*.ovpn).
    KHÔNG có flag tắt. Nếu không có file .ovpn → sys.exit(1).
  - **YouTube Data API key vẫn dùng** (videos.list) để có metadata đầy đủ
    (view/like/comment/description/tags/...).
  - **Multi-instance safe**: mỗi instance có 3 tunnel + cache riêng.
  - **Resumable**: scan folder audio/ + transcriptions/ để biết video nào đã xong.
  - **Multi-channel**: đọc file txt, mỗi dòng 1 URL kênh.
  - **KHÔNG Soniox, KHÔNG LLM fix names, KHÔNG audio features analysis, KHÔNG diarization.**

OUTPUT:
  <output>/
    audio/<run_ts>/*.wav                       : audio (BẮT BUỘC)
    transcriptions/<run_ts>/*_transcription.json : {URL + metadata + segments}
    pipeline_summary_<run_ts>.json
    <channel>_segments_minimal_<run_ts>.csv
    research_<channel>_<run_ts>.json
    _multi_channel_summary_<run_ts>.json
    logs/crawl_<instance_id>.log

Cấu trúc file <safe_title>_transcription.json:
    {
      "video_id": "...", "url": "...", "title": "...", "channel": "...",
      "channel_id": "...", "channel_url": "...",
      "published_at": "...", "duration": "PT123S", "duration_seconds": 123,
      "view_count": 12345, "like_count": 100, "comment_count": 50,
      "description": "...", "tags": [...], "category_id": "...",
      "thumbnail": "...", "caption_available": true, "definition": "hd",
      "audio_filename": "Tieu_De.wav", "audio_path": "Tieu_De.wav",
      "audio_duration": 123.0, "audio_downloaded_at": "...",
      "transcript_language": "vi", "transcript_is_auto": false,
      "transcript_source": "yt-dlp-json3-manual",
      "num_speakers": 1, "speakers": ["SPEAKER_00"],
      "segments": [{"start": 3.0, "end": 10.787, "speaker": "SPEAKER_00", "text": "..."}]
    }

CÁCH DÙNG:
    python youtube_researcher_audio_subs_multi_rotator.py \\
        --channels-file ./channels_audio/channels_khoa_hoc_2.txt \\
        --output ./youtube_dataset_resumable \\
        --use-vpn --vpn-isolated \\
        --instance-id inst1 \\
        --video-delay 5 --skip-existing

    # Rebuild CSV/summary từ JSON có sẵn (không gọi API, không tải audio)
    python youtube_researcher_audio_subs_multi_rotator.py \\
        --rebuild-from-transcripts

    # Chỉ fetch metadata (không audio, không transcript)
    python youtube_researcher_audio_subs_multi_rotator.py \\
        --metadata-only --channels-file ./channels_audio/channels.txt
"""

import json
import os
import re
import sys
import time
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ================= VPN ROTATOR (BẮT BUỘC - OpenVPN) =================
# Chỉ dùng ProtonVPN OpenVPN tunnel để fake IP.
# - 5 server free (CA/MX/NL/SG/US/JP), rotate random theo --vpn-strategy
# - Auth: ./proton_config/auth.txt (chmod 600)
# - Cần: sudo setcap cap_net_admin+ep /usr/sbin/openvpn (chạy 1 lần)
try:
    from vpn_rotator import (
        get_vpn_rotator_from_config,
        VPNRotator,
        is_proxy_dead_error,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from vpn_rotator import (  # type: ignore
        get_vpn_rotator_from_config,
        VPNRotator,
        is_proxy_dead_error,
    )


# ================= ISOLATED VPN ROTATOR (multi-instance safe) =================
class IsolatedVPNRotator:
    """Wrapper cho VPNRotator để an toàn khi chạy song song nhiều instance.

    Fix 3 vấn đề của VPNRotator gốc khi chạy nhiều process cùng lúc:
      1. OPENVPN_LOG constant → 2 instance ghi đè log của nhau.
      2. pkill fallback trong _disconnect() → kill nhầm tunnel instance khác.
      3. pgrep trong _is_connected() → thấy tunnel instance khác → tưởng mình đã connected.
    """

    def __init__(self, instance_id: str, **vpn_kwargs):
        self.instance_id = instance_id
        self._instance_log = f"/tmp/openvpn-proton-{instance_id}.log"
        self._instance_pid_prefix = f"/tmp/openvpn-proton-{instance_id}.pid"

        import vpn_rotator as _vr_mod
        _vr_mod.OPENVPN_LOG = self._instance_log
        self._vr_mod = _vr_mod

        self._inner = VPNRotator(**vpn_kwargs)
        self._patch_connect_server_pid()

    def _patch_connect_server_pid(self):
        instance_pid_prefix = self._instance_pid_prefix
        original_connect = self._inner._connect_server

        def _patched(idx: int, retry: int = 0) -> bool:
            import subprocess as _sp
            import time as _t
            import logging as _log

            ovpn = self._inner._ovpn_files[idx]
            _log.getLogger("vpn_rotator").info(
                "VPN[isolated=%s]: connecting to %s (attempt %d)",
                self.instance_id, ovpn.name, retry + 1,
            )
            self._inner._disconnect()
            prepared_config = self._inner._prepare_config(ovpn)
            old_pid = getattr(self._inner, "_current_pid", None)
            new_pid = None
            try:
                log_path = f"{self._instance_log}.{idx}.{retry}"
                pid_file = Path(f"{instance_pid_prefix}.{idx}.{retry}")
                try:
                    pid_file.unlink(missing_ok=True)
                except Exception:
                    pass
                proc = _sp.Popen(
                    [
                        "/usr/sbin/openvpn", "--config", str(prepared_config),
                        "--auth-user-pass", str(self._inner._auth_file),
                        "--auth-retry", "nointeract", "--auth-nocache",
                        "--daemon", "--log", log_path, "--writepid", str(pid_file),
                        "--script-security", "2", "--up", "/bin/true", "--down", "/bin/true",
                    ],
                    stdout=_sp.DEVNULL, stderr=_sp.DEVNULL, start_new_session=True,
                )
                try:
                    proc.wait(timeout=10)
                except _sp.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                    return False
                for _ in range(20):
                    _t.sleep(0.1)
                    if pid_file.exists():
                        try:
                            new_pid = int(pid_file.read_text().strip())
                            break
                        except Exception:
                            pass
                if new_pid is None:
                    return False
                if old_pid is not None and new_pid == old_pid:
                    try:
                        os.kill(new_pid, 9)
                    except Exception:
                        pass
                    return False
                self._inner._current_pid = new_pid
            except Exception as e:
                _log.getLogger("vpn_rotator").error("VPN[isolated=%s]: %s", self.instance_id, e)
                return False

            for i in range(self._vr_mod.CONNECT_TIMEOUT):
                _t.sleep(1)
                if self._inner._has_tun0():
                    ip = self._inner._get_current_ip()
                    real_ip = self._inner._last_known_real_ip
                    if ip and ip != real_ip:
                        self._inner._current_ip = ip
                        self._inner._current_idx = idx
                        self._inner._usage_count[idx] = self._inner._usage_count.get(idx, 0) + 1
                        self._inner._request_count = 0
                        self._inner._last_connect_time = _t.time()
                        return True
                elif i >= 3:
                    ip = self._inner._get_current_ip()
                    real_ip = self._inner._last_known_real_ip
                    if ip and real_ip and ip != real_ip:
                        self._inner._current_ip = ip
                        self._inner._current_idx = idx
                        self._inner._usage_count[idx] = self._inner._usage_count.get(idx, 0) + 1
                        self._inner._request_count = 0
                        self._inner._last_connect_time = _t.time()
                        return True
            self._inner._disconnect()
            return False

        self._inner._connect_server = _patched

    def _is_connected(self) -> bool:
        pid = getattr(self._inner, "_current_pid", None)
        if pid is None:
            return False
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def _disconnect(self):
        pid = getattr(self._inner, "_current_pid", None)
        if pid is None:
            return
        try:
            os.kill(pid, 15)
        except ProcessLookupError:
            self._inner._current_pid = None
            return
        except PermissionError:
            return
        import time as _t
        deadline = _t.time() + 10
        while _t.time() < deadline:
            try:
                os.kill(pid, 0)
                _t.sleep(0.5)
            except ProcessLookupError:
                self._inner._current_pid = None
                _t.sleep(2)
                return
            except PermissionError:
                pass
        try:
            os.kill(pid, 9)
            _t.sleep(0.5)
        except (ProcessLookupError, PermissionError):
            pass
        self._inner._current_pid = None
        _t.sleep(2)

    def __getattr__(self, name):
        if name in ("_inner", "instance_id", "_instance_log", "_instance_pid_prefix"):
            raise AttributeError(name)
        return getattr(self._inner, name)

    def __len__(self) -> int:
        return len(self._inner)

    def __bool__(self) -> bool:
        return bool(self._inner)

    def disconnect(self):
        self._disconnect()
        self._inner._current_idx = None
        self._inner._current_ip = None


def get_isolated_vpn_rotator_from_config(
    instance_id: str,
    config_dir: Optional[str] = None,
    rotate_every: int = 0,
    strategy: str = "random",
    real_ip_cycle: int = 0,
) -> Optional["IsolatedVPNRotator"]:
    try:
        return IsolatedVPNRotator(
            instance_id=instance_id,
            config_dir=Path(config_dir) if config_dir else None,
            rotate_every=rotate_every,
            strategy=strategy,
            real_ip_cycle=real_ip_cycle,
        )
    except FileNotFoundError as e:
        import logging as _log
        _log.getLogger("vpn_rotator").warning("IsolatedVPN rotator không khả dụng: %s", e)
        return None


# ================= COOKIES =================
COOKIES_FILE = Path(__file__).parent / "cookies.txt"
COOKIES_FILE_STR = str(COOKIES_FILE) if COOKIES_FILE.exists() else None

# ================= CONFIG =================
_YOUTUBE_API_KEYS: list = []
for _k in [
    "YOUTUBE_API_KEY",
    "YOUTUBE_API_KEY_1", "YOUTUBE_API_KEY_2", "YOUTUBE_API_KEY_3",
    "YOUTUBE_API_KEY_4", "YOUTUBE_API_KEY_5", "YOUTUBE_API_KEY_6",
    "YOUTUBE_API_KEY_7",
]:
    _v = os.environ.get(_k, "")
    if _v and _v not in _YOUTUBE_API_KEYS:
        _YOUTUBE_API_KEYS.append(_v)

# ================= FILTER CONFIG =================
FILTER_MIN_DURATION = 3600       # 30 phút
FILTER_MAX_DURATION = 1000000   # không cap
FILTER_MIN_VIEW_COUNT = 0


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
    categories: list = field(default_factory=list)
    default_language: str = ""
    default_audio_language: str = ""
    caption_available: bool = False
    definition: str = ""
    channel_id: str = ""
    channel_url: str = ""
    channel_follower_count: int = 0
    uploader: str = ""
    uploader_id: str = ""
    uploader_url: str = ""
    duration_string: str = ""
    audio_filename: str = ""
    # === Subs URLs cache (Phase 2 yt-dlp extract_info) ===
    # Dùng cho Bucket B (TRANSCRIBE-ONLY) để skip gọi yt-dlp lần 2.
    subtitles: dict = field(default_factory=dict)          # Manual subtitles URLs
    automatic_captions: dict = field(default_factory=dict) # Auto-caption URLs
    passed_filters: list = field(default_factory=list)
    failed_filters: list = field(default_factory=list)

    # === v3: YouTube API metadata bổ sung (12 field) ===
    dimension: str = "2d"                    # "2d" / "3d"
    licensed_content: bool = False         # Video có license YouTube
    projection: str = "rectangular"        # "rectangular" / "360"
    privacy_status: str = ""               # public/unlisted/private
    embeddable: bool = True                # Có embed được không
    made_for_kids: bool = False            # Designed for kids
    live_broadcast_content: str = "none"   # none/live/upcoming
    topic_categories: list = field(default_factory=list)  # Wikipedia topics
    recording_location: str = ""           # Vị trí địa lý (nếu có)
    live_status: str = "not_live"          # not_live/is_live/was_live
    was_live: bool = False                 # Đã live trước đó
    availability: str = ""                 # public/unlisted/private/...

    @property
    def video_url(self) -> str:
        return f"https://www.youtube.com/watch?v={self.video_id}"


@dataclass
class FilterCriteria:
    published_after: Optional[datetime] = None
    min_duration: Optional[int] = None
    max_duration: Optional[int] = None
    min_view_count: int = 0


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


# ================= CHANNEL ID RESOLUTION =================
def _channel_id_cache_path() -> Path:
    return Path(__file__).parent / ".cache_shared" / "channel_id_cache.json"


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
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  [warn] khong luu duoc channel_id cache: {e}")


def resolve_channel_id(api_key: str, channel_input: str,
                        proxy_url: Optional[str] = None) -> Optional[str]:
    if channel_input.startswith("UC") and len(channel_input) == 24:
        return channel_input
    url = channel_input.strip().rstrip("/")
    channel_match = re.search(r'youtube\.com/channel/([^/\s?]+)', url)
    if channel_match:
        cid = channel_match.group(1)
        if cid.startswith("UC") and len(cid) == 24:
            return cid
    cache = _load_channel_id_cache()
    if channel_input in cache:
        return cache[channel_input]
    if _YOUTUBE_API_KEYS:
        import requests as _req
        handle_match = re.search(r"youtube\.com/@([^/\s?]+)", url)
        custom_match = re.search(r"youtube\.com/c/([^/\s?]+)", url)
        user_match = re.search(r"youtube\.com/user/([^/\s?]+)", url)
        bare_handle_match = None
        if not handle_match and not custom_match and not user_match:
            if url.startswith("@"):
                bare_handle_match = re.match(r"@([^/\s?]+)", url)
            elif re.match(r"^[\w.-]+$", url) and not url.startswith("UC"):
                bare_handle_match = re.match(r"([\w.-]+)", url)
        try:
            for api_key in _YOUTUBE_API_KEYS:
                if handle_match:
                    resp = _req.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"key": api_key, "part": "id",
                                "forHandle": "@" + handle_match.group(1)},
                        timeout=10,
                    )
                elif custom_match or user_match:
                    m = custom_match or user_match
                    resp = _req.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"key": api_key, "part": "id",
                                "forUsername": m.group(1)},
                        timeout=10,
                    )
                elif bare_handle_match:
                    resp = _req.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"key": api_key, "part": "id",
                                "forHandle": "@" + bare_handle_match.group(1)},
                        timeout=10,
                    )
                else:
                    return None
                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get("items", [])
                    if items:
                        cid = items[0].get("id", "")
                        if cid:
                            cache[channel_input] = cid
                            _save_channel_id_cache(cache)
                            return cid
                elif resp.status_code == 403:
                    continue
        except Exception as e:
            print(f"  [API] resolve_channel_id error: {e}")
    # Fallback yt-dlp
    try:
        import yt_dlp
        ydl_opts = {
            "quiet": True, "no_warnings": True, "skip_download": True,
            "ignoreerrors": True, "playlistend": 1,
        }
        if proxy_url:
            ydl_opts["proxy"] = proxy_url
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
        if info:
            cid = info.get("channel_id") or info.get("id")
            if cid and cid.startswith("UC"):
                cache[channel_input] = cid
                _save_channel_id_cache(cache)
                return cid
    except Exception as e:
        print(f"  [yt-dlp] resolve_channel_id error: {e}")
    return None


# ================= YT-DLP INFO FETCH =================
def fetch_video_info_via_ytdlp(video_id: str,
                               proxy_url: Optional[str] = None) -> dict | None:
    try:
        import yt_dlp
    except ImportError:
        return None
    ydl_opts = {
        "quiet": True, "no_warnings": True, "skip_download": True,
        "ignoreerrors": True, "js_runtimes": {"node": {}},
    }
    YouTubeResearcher._apply_auth_skip(ydl_opts)
    if COOKIES_FILE_STR:
        ydl_opts["cookiefile"] = COOKIES_FILE_STR
    if proxy_url:
        ydl_opts["proxy"] = proxy_url
    target_url = f"https://www.youtube.com/watch?v={video_id}"
    last_err = None
    for attempt in range(1, 3):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(target_url, download=False)
            return info
        except Exception as e:
            last_err = e
            err_msg = str(e).lower()
            is_blocked = any(k in err_msg for k in [
                'captcha', 'challenge', 'sign in', 'bot', '429',
                'too many', 'rate limit', 'forbidden', '403', 'blocked',
            ])
            if is_blocked and attempt < 2:
                time.sleep(5)
                continue
            return None
    return None


# ================= YouTubeResearcher =================
class YouTubeResearcher:
    """Lấy video theo kênh (channel URL) → tải audio + YouTube subs.

    3 rotator TÁCH BIỆT (optional, default fallback về proxy_rotator):
      - proxy_rotator           : cho metadata (channel listing, Data API)
      - audio_proxy_rotator     : cho audio download
      - transcript_proxy_rotator: cho transcript fetch (yt-dlp subs + API fallback)
    """

    def __init__(self, api_key: str, output_dir: str = "./researched_videos",
                 proxy_rotator: Optional[VPNRotator] = None,
                 audio_proxy_rotator: Optional[VPNRotator] = None,
                 transcript_proxy_rotator: Optional[VPNRotator] = None,
                 proxy_mode: str = "auto"):
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._videos: list[VideoCandidate] = []
        self._filtered_videos: list[VideoCandidate] = []
        # === METADATA rotator (proxy_rotator) ===
        self._rotator = proxy_rotator
        # === AUDIO rotator (riêng biệt) ===
        # Nếu không truyền → fallback dùng proxy_rotator (backward-compat)
        self._audio_rotator = (
            audio_proxy_rotator if audio_proxy_rotator is not None else proxy_rotator
        )
        # === TRANSCRIPT rotator (riêng biệt) ===
        # Nếu không truyền → fallback dùng proxy_rotator (backward-compat)
        self._transcript_rotator = (
            transcript_proxy_rotator
            if transcript_proxy_rotator is not None
            else proxy_rotator
        )
        self._proxy_mode = proxy_mode
        self._use_vpn_tunnel = True  # BẮT BUỘC
        self._direct_blocked = False
        self._audio_escalated = False
        self._transcript_escalated = False

    def _next_proxy(self) -> Optional[str]:
        if not self._rotator:
            return None
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

    def _next_proxy_for_transcript(self) -> Optional[str]:
        """Lấy proxy URL tiếp theo từ transcript_rotator (riêng biệt)."""
        if not self._transcript_rotator:
            return None
        url = self._transcript_rotator.next()
        if url:
            try:
                from urllib.parse import urlparse
                p = urlparse(url)
                short = f"{p.hostname}:{p.port}"
            except Exception:
                short = url[:40]
            print(f"    [transcript-proxy] → {short}")
        return url

    def _next_proxy_for_audio(self) -> Optional[str]:
        """Lấy proxy URL tiếp theo từ audio_rotator (riêng biệt)."""
        if not self._audio_rotator:
            return None
        url = self._audio_rotator.next()
        if url:
            try:
                from urllib.parse import urlparse
                p = urlparse(url)
                short = f"{p.hostname}:{p.port}"
            except Exception:
                short = url[:40]
            print(f"    [audio-proxy] → {short}")
        return url

    def _proxy_guard(self):
        if not self._rotator:
            class _NoOp:
                def __enter__(self): return self
                def __exit__(self, *a): return False
            return _NoOp()
        return self._rotator.acquire()

    def _proxy_guard_for_transcript(self):
        """Context manager bảo vệ tunnel transcript_rotator."""
        if not self._transcript_rotator:
            class _NoOp:
                def __enter__(self): return self
                def __exit__(self, *a): return False
            return _NoOp()
        return self._transcript_rotator.acquire()

    def _proxy_guard_for_audio(self):
        """Context manager bảo vệ tunnel audio_rotator."""
        if not self._audio_rotator:
            class _NoOp:
                def __enter__(self): return self
                def __exit__(self, *a): return False
            return _NoOp()
        return self._audio_rotator.acquire()

    def _mark_proxy_failed(self, proxy_url: Optional[str]):
        if self._rotator and proxy_url:
            self._rotator.mark_failed(proxy_url)

    def _mark_proxy_dead(self, proxy_url: Optional[str]):
        if self._rotator and proxy_url:
            self._rotator.remove_proxy(proxy_url)

    def _mark_audio_proxy_failed(self, proxy_url: Optional[str]):
        """Mark failed trên audio_rotator (không ảnh hưởng metadata/transcript)."""
        if self._audio_rotator and proxy_url:
            try:
                self._audio_rotator.mark_failed(proxy_url)
            except Exception:
                pass

    def _mark_transcript_proxy_failed(self, proxy_url: Optional[str]):
        """Mark failed trên transcript_rotator (không ảnh hưởng metadata/audio)."""
        if self._transcript_rotator and proxy_url:
            try:
                self._transcript_rotator.mark_failed(proxy_url)
            except Exception:
                pass

    def _mark_audio_proxy_dead(self, proxy_url: Optional[str]):
        """Mark dead trên audio_rotator."""
        if self._audio_rotator and proxy_url:
            try:
                self._audio_rotator.remove_proxy(proxy_url)
            except Exception:
                pass

    def _mark_transcript_proxy_dead(self, proxy_url: Optional[str]):
        """Mark dead trên transcript_rotator."""
        if self._transcript_rotator and proxy_url:
            try:
                self._transcript_rotator.remove_proxy(proxy_url)
            except Exception:
                pass

    def _proxy_for_fallback(self) -> Optional[str]:
        if self._direct_blocked and self._rotator and len(self._rotator) > 0:
            return self._next_proxy()
        return None

    def _proxy_for_transcript_fallback(self) -> Optional[str]:
        """Direct-first cho transcript: trả None nếu chưa bị block,
        trả proxy từ transcript_rotator nếu đã bị block.
        """
        if self._direct_blocked and self._transcript_rotator and len(self._transcript_rotator) > 0:
            return self._next_proxy_for_transcript()
        return None

    def _proxy_for_audio_fallback(self) -> Optional[str]:
        """Direct-first cho audio: trả None nếu chưa bị block,
        trả proxy từ audio_rotator nếu đã bị block.
        """
        if self._direct_blocked and self._audio_rotator and len(self._audio_rotator) > 0:
            return self._next_proxy_for_audio()
        return None

    def _is_youtube_blocked_error(self, err) -> bool:
        err_str = str(err).lower()
        keys = ('429', 'too many requests', 'rate limit', 'quota exceeded',
                '403', 'forbidden', 'blocked', 'access denied',
                'sign in to confirm', 'not a bot', 'bot check',
                'captcha', 'challenge',
                'timed out', 'connect timeout', 'read timeout',
                'connection reset', 'broken pipe', 'ssl')
        return any(k in err_str for k in keys)

    def _on_youtube_blocked(self, err, proxy_url, context: str):
        if not proxy_url:
            self._direct_blocked = True
            return
        if is_proxy_dead_error(err):
            self._mark_proxy_dead(proxy_url)
        else:
            self._mark_proxy_failed(proxy_url)

    def _on_youtube_blocked_transcript(self, err, proxy_url, context: str):
        """Handler riêng cho transcript rotator."""
        if not proxy_url:
            # Đang dùng IP thật → escalate dùng transcript_rotator
            self._direct_blocked = True
            return
        if is_proxy_dead_error(err):
            self._mark_transcript_proxy_dead(proxy_url)
        else:
            self._mark_transcript_proxy_failed(proxy_url)

    def _on_youtube_blocked_audio(self, err, proxy_url, context: str):
        """Handler riêng cho audio rotator."""
        if not proxy_url:
            # Đang dùng IP thật → escalate dùng audio_rotator
            self._direct_blocked = True
            return
        if is_proxy_dead_error(err):
            self._mark_audio_proxy_dead(proxy_url)
        else:
            self._mark_audio_proxy_failed(proxy_url)

    @staticmethod
    def _apply_cookies(ydl_opts: dict) -> dict:
        if COOKIES_FILE_STR:
            ydl_opts["cookiefile"] = COOKIES_FILE_STR
        return ydl_opts

    @staticmethod
    def _apply_auth_skip(ydl_opts: dict) -> dict:
        ydl_opts.setdefault("extractor_args", {})
        if "youtube" not in ydl_opts["extractor_args"]:
            ydl_opts["extractor_args"]["youtube"] = {}
        yt_args = ydl_opts["extractor_args"]["youtube"]
        if "skip" not in yt_args:
            yt_args["skip"] = []
        if "authcheck" not in yt_args["skip"]:
            yt_args["skip"].append("authcheck")
        if "player_client" not in yt_args:
            yt_args["player_client"] = ["web_safari", "web"]
        if "js_runtimes" not in ydl_opts:
            node_path = "/home/hientran/.nvm/versions/node/v24.15.0/bin/node"
            if not Path(node_path).exists():
                import shutil
                node_path = shutil.which("node") or "node"
            ydl_opts["js_runtimes"] = {"node": {"path": node_path}}
        ydl_opts["extractor_args"].setdefault("youtubepot-bgutilhttp", {})
        if "base_url" not in ydl_opts["extractor_args"]["youtubepot-bgutilhttp"]:
            ydl_opts["extractor_args"]["youtubepot-bgutilhttp"]["base_url"] = [
                "http://127.0.0.1:4416"]
        return ydl_opts

    @staticmethod
    def _apply_timeouts(ydl_opts: dict, socket_timeout: int = 60) -> dict:
        ydl_opts["socket_timeout"] = socket_timeout
        return ydl_opts

    @staticmethod
    def _short_proxy(proxy_url: Optional[str]) -> str:
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
        if not title:
            return fallback
        try:
            import unicodedata
            normalized = unicodedata.normalize("NFKD", title)
            cleaned = "".join(ch for ch in normalized
                              if not unicodedata.combining(ch))
        except Exception:
            cleaned = title
        cleaned = re.sub(r"[^\w\sÀ-ɏḀ-ỿ-]", "_", cleaned, flags=re.UNICODE)
        cleaned = re.sub(r"\s+", "_", cleaned.strip())
        cleaned = re.sub(r"_+", "_", cleaned).strip("._-")
        if not cleaned:
            return fallback
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length].rstrip("._-")
        return cleaned

    # ================== AUDIO + JSON LOOKUP ==================
    @staticmethod
    def find_transcription_json(transcription_dir, video, audio_filename: str = "",
                                search_all_runs: bool = False) -> "Path | None":
        if not transcription_dir:
            return None
        td = Path(transcription_dir)
        candidates = []
        if audio_filename:
            stem = Path(audio_filename).stem
            candidates.append(td / f"{stem}_transcription.json")
        if getattr(video, "title", None):
            try:
                safe_title = YouTubeResearcher._safe_filename(
                    video.title, fallback=video.video_id)
                candidates.append(td / f"{safe_title}_transcription.json")
            except Exception:
                pass
        if getattr(video, "video_id", None):
            candidates.append(td / f"{video.video_id}_transcription.json")
        for c in candidates:
            if c.exists():
                return c
        if not search_all_runs:
            return None
        parent = td.parent if td.name else td
        if not parent.exists():
            return None
        for sub in sorted(parent.iterdir(), reverse=True):
            if not sub.is_dir() or sub == td:
                continue
            for c in candidates:
                alt = sub / c.name
                if alt.exists():
                    return alt
        return None

    @staticmethod
    def find_existing_audio(audio_root, video, target_filename: str = "",
                            min_size_bytes: int = 50 * 1024) -> "Path | None":
        """Tìm file audio đã có cho video. Skip file < min_size_bytes (corrupt)."""
        if not audio_root:
            return None
        root = Path(audio_root)
        if not root.exists():
            return None
        candidates = []
        if target_filename:
            candidates.append(target_filename)
        if getattr(video, "video_id", None):
            candidates.append(f"{video.video_id}.wav")
            if target_filename:
                stem = Path(target_filename).stem
                candidates.append(f"{stem}_{video.video_id}.wav")
        subdirs = sorted([d for d in root.iterdir() if d.is_dir()], reverse=True)
        for sub in subdirs:
            for name in candidates:
                p = sub / name
                if p.exists():
                    try:
                        if p.stat().st_size >= min_size_bytes:
                            return p
                    except OSError:
                        continue
        return None

    @staticmethod
    def _build_audio_index(audio_root, min_size_bytes: int = 50 * 1024) -> dict:
        """Build index {stem: Path} cho TẤT CẢ subfolder audio/, lấy file mới nhất.

        Skip file audio có size < min_size_bytes (file rỗng/corrupt chỉ có header).
        Mặc định 50KB — file WAV thật ≥ vài trăm KB.
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
                    if not f.is_file() or f.suffix.lower() not in audio_exts:
                        continue
                    try:
                        if f.stat().st_size < min_size_bytes:
                            continue
                    except OSError:
                        continue
                    key = f.stem
                    if key and key not in index:
                        index[key] = f
            except Exception:
                continue
        return index

    @staticmethod
    def _build_json_index(transcriptions_root, min_size_bytes: int = 100) -> dict:
        """Build index {stem: Path} cho TẤT CẢ subfolder transcriptions/, lấy file mới nhất.

        Skip file JSON: size < min_size_bytes, parse fail, hoặc thiếu video_id/segments.
        Mặc định 100 bytes — JSON transcription thật > 1KB.
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
                    try:
                        if f.stat().st_size < min_size_bytes:
                            continue
                    except OSError:
                        continue
                    stem = f.name[: -len(suffix)]
                    if not stem or stem in index:
                        continue
                    if not YouTubeResearcher._is_valid_transcription_json(f):
                        continue
                    index[stem] = f
            except Exception:
                continue
        return index

    @staticmethod
    def _is_valid_transcription_json(path: Path) -> bool:
        """Check file JSON có phải transcription hợp lệ không.

        Tiêu chí:
          - Parse OK (không phải text rác / JSON broken)
          - Là dict
          - Có 'video_id' (string không rỗng)
          - Có 'segments' là list
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return False
        if not isinstance(data, dict):
            return False
        if not data.get("video_id"):
            return False
        if not isinstance(data.get("segments"), list):
            return False
        return True

    def _cleanup_orphan_part_files(self, audio_dir: Path, min_size_mb: int = 100) -> int:
        if not audio_dir.exists():
            return 0
        min_size_bytes = min_size_mb * 1024 * 1024
        deleted = 0
        wav_stems = {p.stem for p in audio_dir.glob("*.wav")}
        for part_file in (list(audio_dir.glob("*.part")) +
                          list(audio_dir.glob("*.ytdl"))):
            try:
                size = part_file.stat().st_size
            except OSError:
                continue
            if size < min_size_bytes:
                continue
            original_stem = part_file.name
            for suffix in (".part-Frag", ".part", ".ytdl"):
                if suffix in original_stem:
                    original_stem = original_stem.split(suffix)[0]
                    break
            original_stem = original_stem.rsplit(".", 1)[0]
            if original_stem in wav_stems:
                continue
            try:
                size_mb = size / (1024 * 1024)
                print(f"  [CLEANUP] Xóa orphan {part_file.name} "
                      f"({size_mb:.1f}MB) - không có .wav tương ứng")
                part_file.unlink()
                deleted += 1
            except Exception:
                pass
        return deleted

    # ============= FIX v2 Skip #2: No-transcript marker =============
    # Khi 1 video được xác nhận là KHÔNG có transcript YouTube (transcript_unavailable),
    # ghi marker file rỗng `{video_id}.no_transcript` vào transcriptions_dir.
    # Ở các run sau, video có marker sẽ được skip ngay → không tốn thời gian
    # gọi yt-dlp extract_info() / download sub URL nữa.

    @staticmethod
    def _has_no_transcript_marker(video_id: str, transcriptions_dir: Path) -> bool:
        """Check video có marker .no_transcript không (đã thử fail ở run trước)."""
        marker = transcriptions_dir / f"{video_id}.no_transcript"
        return marker.exists()

    @staticmethod
    def _mark_no_transcript(video_id: str, transcriptions_dir: Path) -> None:
        """Ghi marker .no_transcript để các run sau skip video này."""
        try:
            transcriptions_dir.mkdir(parents=True, exist_ok=True)
            marker = transcriptions_dir / f"{video_id}.no_transcript"
            marker.touch(exist_ok=True)
        except Exception:
            pass

    # ================== FETCH VIDEOS ==================
    def fetch_channel_videos(self, channel_input: str, max_results: int = 20000,
                              batch_size: int = 200, max_batches: int = 100,
                              socket_timeout: int = 60, fetch_delay: int = 5,
                              max_retries: int = 5,
                              published_after: Optional[datetime] = None,
                              order: str = "date",
                              ) -> list[VideoCandidate]:
        try:
            import yt_dlp
        except ImportError:
            print("pip install yt-dlp")
            sys.exit(1)
        t0 = time.time()
        proxy_url = self._proxy_for_fallback()
        channel_id = resolve_channel_id(self.api_key, channel_input,
                                        proxy_url=proxy_url)
        if not channel_id and not proxy_url:
            self._direct_blocked = True
            proxy_url = self._next_proxy()
            channel_id = resolve_channel_id(self.api_key, channel_input,
                                            proxy_url=proxy_url)
        if not channel_id:
            print(f"Khong tim thay kenh: {channel_input}")
            return []
        print(f"  [TIMING] resolve_channel_id: {time.time()-t0:.1f}s, ID={channel_id}")

        if channel_input.startswith("UC") and len(channel_input) == 24:
            channel_url = f"https://www.youtube.com/channel/{channel_id}/videos"
        else:
            url = channel_input.strip().rstrip("/")
            channel_url = url + "/videos" if not url.endswith("/videos") else url

        # === Phase 1: yt-dlp flat ===
        print(f"\n  [Phase 1] Fetching listing (extract_flat=True, target={max_results})...")
        phase1_start = time.time()
        ydl_listing_opts = {
            "quiet": True, "no_warnings": True,
            "extract_flat": True, "skip_download": True,
            "playlistend": max_results, "ignoreerrors": True,
            "js_runtimes": {"node": {}},
        }
        YouTubeResearcher._apply_auth_skip(ydl_listing_opts)
        ydl_listing_opts["extractor_args"].setdefault("youtubetab", {}).setdefault("skip", []).append("authcheck")
        YouTubeResearcher._apply_cookies(ydl_listing_opts)
        self._apply_timeouts(ydl_listing_opts, socket_timeout=socket_timeout)
        listing_proxy = self._proxy_for_fallback()
        if listing_proxy:
            ydl_listing_opts["proxy"] = listing_proxy

        flat_entries = []
        last_err = None
        for attempt in range(1, 3):
            try:
                with yt_dlp.YoutubeDL(ydl_listing_opts) as ydl:
                    flat_info = ydl.extract_info(channel_url, download=False)
                if flat_info and "entries" in flat_info:
                    flat_entries = [e for e in flat_info["entries"] if e]
                break
            except Exception as e:
                last_err = e
                if self._is_youtube_blocked_error(e):
                    self._on_youtube_blocked(e, listing_proxy, "phase1")
                if attempt < 2:
                    listing_proxy = self._next_proxy()
                    if listing_proxy:
                        ydl_listing_opts["proxy"] = listing_proxy
                    time.sleep(3)

        if not flat_entries:
            print(f"  [Phase 1] FAIL ({last_err})")
            return []

        phase1_time = time.time() - phase1_start
        print(f"  [Phase 1] Done: {len(flat_entries)} video trong {phase1_time:.1f}s")

        pre_filter = []
        for e in flat_entries:
            dur = e.get("duration") or 0
            if isinstance(dur, (int, float)):
                if dur < FILTER_MIN_DURATION or dur > FILTER_MAX_DURATION:
                    continue
            pre_filter.append(e)
        print(f"  [Phase 1] Duration filter: {len(pre_filter)}/{len(flat_entries)} passed")

        if not pre_filter:
            return []

        # === Phase 2: YouTube Data API v3 (videos.list batch=50) ===
        import requests as _req
        all_entries = []
        failed_count = 0
        api_batch_size = 50

        if not _YOUTUBE_API_KEYS:
            print("  [Phase 2] YOUTUBE_API_KEY not set → fallback yt-dlp concurrent")
            for e in pre_filter:
                vid = e.get("id")
                if not vid:
                    continue
                proxy_url = self._next_proxy()
                info = fetch_video_info_via_ytdlp(vid, proxy_url=proxy_url)
                if info:
                    all_entries.append(self._api_item_to_ytdlp_dict(info))
        else:
            current_key_idx = 0
            api_key = _YOUTUBE_API_KEYS[current_key_idx]
            print(f"\n  [Phase 2] YouTube Data API v3 (batch=50, {len(_YOUTUBE_API_KEYS)} key(s))...")
            phase2_start = time.time()
            for batch_start in range(0, len(pre_filter), api_batch_size):
                batch_end = min(batch_start + api_batch_size, len(pre_filter))
                batch_items = pre_filter[batch_start:batch_end]
                video_ids = [e.get("id") for e in batch_items if e.get("id")]
                if not video_ids:
                    continue
                url = "https://www.googleapis.com/youtube/v3/videos"
                resp = None
                for attempt in range(1, max_retries + 1):
                    params = {
                        "key": api_key,
                        "id": ",".join(video_ids),
                        "part": "snippet,statistics,contentDetails,status,topicDetails",
                    }
                    try:
                        resp = _req.get(url, params=params, timeout=15)
                        if resp.status_code == 200:
                            break
                        elif resp.status_code == 403:
                            current_key_idx += 1
                            if current_key_idx < len(_YOUTUBE_API_KEYS):
                                api_key = _YOUTUBE_API_KEYS[current_key_idx]
                                print(f"  [API] Key {current_key_idx} quota het, chuyển key {current_key_idx + 1}")
                                resp = None
                                continue
                            else:
                                print(f"  [API] Tất cả {len(_YOUTUBE_API_KEYS)} key đã hết quota!")
                                break
                        elif resp.status_code == 429:
                            time.sleep(5 * attempt)
                        else:
                            break
                    except Exception as e:
                        if attempt < max_retries:
                            time.sleep(2 ** attempt)
                if not resp or resp.status_code != 200:
                    failed_count += len(video_ids)
                    if current_key_idx >= len(_YOUTUBE_API_KEYS):
                        break
                    continue
                data = resp.json()
                for item in data.get("items", []):
                    info = self._api_item_to_ytdlp_dict(item)
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
                processed = min(batch_end, len(pre_filter))
                elapsed = time.time() - phase2_start
                rate = processed / max(elapsed, 0.1)
                eta = (len(pre_filter) - processed) / max(rate, 0.1)
                print(f"  [Phase 2] [{processed}/{len(pre_filter)}] "
                      f"ok={len(all_entries)} fail={failed_count} "
                      f"({elapsed:.0f}s, {rate:.1f} v/s, ETA ~{eta:.0f}s)")
                if len(all_entries) >= max_results:
                    break
            phase2_time = time.time() - phase2_start
            print(f"  [Phase 2] Done: {len(all_entries)} video trong {phase2_time:.1f}s")

        # Early filter
        if all_entries:
            pre_count = len(all_entries)
            all_entries = [
                e for e in all_entries
                if int(e.get("view_count") or 0) >= FILTER_MIN_VIEW_COUNT
                and FILTER_MIN_DURATION <= (e.get("duration") or 0) <= FILTER_MAX_DURATION
            ]
            print(f"  [Early filter] {pre_count} → {len(all_entries)}")

        if not all_entries:
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
        detailed_videos = detailed_videos[:max_results]
        self._videos = detailed_videos
        if not self._videos:
            return []
        print(f"Tim thay {len(self._videos)} video từ kênh "
              f"'{self._videos[0].channel if self._videos else channel_input}'")
        return self._videos

    def _api_item_to_ytdlp_dict(self, item: dict) -> dict:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})
        status = item.get("status", {})
        topic = item.get("topicDetails", {})
        duration_iso = content.get("duration", "PT0S")
        duration_secs = 0
        m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration_iso)
        if m:
            h, mn, s = (int(x) if x else 0 for x in m.groups())
            duration_secs = h * 3600 + mn * 60 + s
        pub_at = snippet.get("publishedAt", "")
        upload_date = pub_at[:10].replace("-", "") if pub_at and len(pub_at) >= 10 else ""
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
            "channel_url": f"https://www.youtube.com/channel/{snippet.get('channelId', '')}",
            "default_language": snippet.get("defaultLanguage", ""),
            "default_audio_language": snippet.get("defaultAudioLanguage", ""),
            "caption_available": "caption" in content and content["caption"] == "true",
            "definition": "hd",
            "privacy_status": status.get("privacyStatus", "public"),
            "made_for_kids": status.get("madeForKids", False),
            # v3: giữ NGUYÊN full URL (không split) để khớp _vpn_v2.py
            "topic_categories": list(topic.get("topicCategories", []) or []),
            "embeddable": status.get("embeddable", True),
            "licensed_content": content.get("licensedContent", False),

            # === v3: 8 key mới ===
            # dimension/projection: contentDetails có thể có nhưng thường thiếu → default "2d"/"rectangular"
            "dimension": content.get("dimension", "2d"),
            "projection": content.get("projection", "rectangular"),
            # availability: từ status.privacyStatus (sync với privacy_status)
            "availability": status.get("privacyStatus", "public"),
            # playable_in_embed: alias cho embeddable
            "playable_in_embed": status.get("embeddable", True),
            # live_broadcast_content + derived fields
            "live_broadcast_content": snippet.get("liveBroadcastContent", "none"),
            "is_live": snippet.get("liveBroadcastContent") == "live",
            "live_status": snippet.get("liveBroadcastContent", "not_live"),
            "was_live": snippet.get("liveBroadcastContent") == "completed",
            # license: alias cho licensed_content
            "license": content.get("licensedContent", False),
            # height: từ definition (hd=1080, sd=480)
            "height": 1080 if (content.get("definition", "hd") == "hd") else 480,
        }

    def _build_video_from_ytdlp(self, info: dict) -> VideoCandidate:
        duration_secs = info.get("duration") or 0
        if isinstance(duration_secs, (int, float)):
            duration_iso = f"PT{int(duration_secs)}S"
        else:
            duration_iso = ""
        upload_date = info.get("upload_date", "")
        if upload_date and len(upload_date) == 8:
            published_at = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}T00:00:00Z"
        else:
            published_at = ""
        thumbs = info.get("thumbnails") or []
        thumbnail = ""
        for t in thumbs:
            if isinstance(t, dict) and t.get("id") in ("high", "medium", "default"):
                thumbnail = t.get("url", "")
                break
        if not thumbnail and thumbs:
            thumbnail = (thumbs[0].get("url", "") if isinstance(thumbs[0], dict) else "")
        tags = info.get("tags") or []
        categories = info.get("categories") or []
        category_id = categories[0] if categories else ""
        subtitles = info.get("subtitles") or {}
        auto_captions = info.get("automatic_captions") or {}
        caption_available = bool(subtitles) or bool(auto_captions)

        return VideoCandidate(
            video_id=info.get("id", ""),
            title=info.get("title", ""),
            channel=info.get("channel") or info.get("uploader") or "",
            description=info.get("description", ""),
            published_at=published_at,
            duration=duration_iso,
            duration_string=info.get("duration_string", ""),
            view_count=int(info.get("view_count") or 0),
            like_count=int(info.get("like_count") or 0),
            comment_count=int(info.get("comment_count") or 0),
            url=info.get("webpage_url") or f"https://www.youtube.com/watch?v={info.get('id', '')}",
            tags=tags,
            categories=categories,
            category_id=category_id,
            default_language=info.get("language", "") or info.get("default_language", ""),
            default_audio_language=info.get("audio_language", "") or info.get("default_audio_language", ""),
            caption_available=caption_available,
            definition="hd" if (info.get("height") or 0) >= 720 else "sd",
            channel_id=info.get("channel_id", ""),
            channel_url=info.get("channel_url", ""),
            channel_follower_count=int(info.get("channel_follower_count") or 0),
            uploader=info.get("uploader", ""),
            uploader_id=info.get("uploader_id", ""),
            uploader_url=info.get("uploader_url", ""),
            thumbnail=thumbnail,
            subtitles=subtitles,
            automatic_captions=auto_captions,
            # === v3: 12 field metadata bổ sung ===
            dimension=info.get("dimension", "2d"),
            licensed_content=bool(info.get("licensed_content") or info.get("license")),
            projection=info.get("projection", "rectangular"),
            privacy_status=info.get("privacy_status") or info.get("availability", ""),
            embeddable=bool(info.get("embeddable", info.get("playable_in_embed", True))),
            made_for_kids=bool(info.get("made_for_kids", False)),
            live_broadcast_content=info.get("live_broadcast_content", "none"),
            topic_categories=info.get("topic_categories", []) or [],
            live_status=info.get("live_status", "not_live"),
            was_live=bool(info.get("was_live", False)),
            availability=info.get("availability", info.get("privacy_status", "")),
        )

    # ================== FILTERS ==================
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
            if criteria.published_after and video.published_at:
                try:
                    pub_date = datetime.fromisoformat(
                        video.published_at.replace("Z", "+00:00"))
                    if pub_date < criteria.published_after:
                        video.failed_filters.append("too_old")
                except ValueError:
                    pass
            if video.view_count < criteria.min_view_count:
                video.failed_filters.append("view_count_low")
            if video.failed_filters:
                continue
            video.passed_filters.append("passed_all_criteria")
            self._filtered_videos.append(video)
        print(f"Filter: {len(self._filtered_videos)}/{len(self._videos)} passed")
        return self._filtered_videos

    def print_video_table(self, videos=None):
        videos = videos or self._filtered_videos
        if not videos:
            print("No videos")
            return
        print("\n" + "=" * 130)
        print(f"{'#':<3} {'Title':<45} {'Duration':<10} "
              f"{'Views':<8} {'Likes':<7} {'Caption':<8} {'Published':<12}")
        print("=" * 130)
        for i, v in enumerate(videos):
            duration_secs = parse_duration(v.duration)
            title = v.title[:42] + "..." if len(v.title) > 45 else v.title
            pub_date = v.published_at[:10] if v.published_at else ""
            print(f"{i+1:<3} {title:<45} {format_duration(duration_secs):<10} "
                  f"{format_number(v.view_count):<8} {format_number(v.like_count):<7} "
                  f"{str(v.caption_available):<8} {pub_date:<12}")
        print("=" * 130)
        print("\nVideo URLs:")
        for i, v in enumerate(videos):
            print(f"  {i+1}. {v.video_url}")

    # ================== TRANSCRIPT ==================
    # NOTE: _get_youtube_transcript() (dùng youtube-transcript-api) đã bị XOÁ.
    # Bây giờ chỉ dùng _get_youtube_transcript_via_ytdlp() duy nhất.
    # Xem transcribe_with_youtube() để biết retry logic (2 attempts qua transcript_rotator).

    def _merge_youtube_segments_to_sentences(self, raw_parsed, max_duration=31.0,
                                             min_words=1):
        if not raw_parsed:
            return []
        SENT_END = {".", "?", "!", "…"}
        words = []
        for seg in raw_parsed:
            ws = seg["text"].split()
            if not ws:
                continue
            dur = seg["end"] - seg["start"]
            tpw = dur / len(ws) if ws else 0
            for j, w in enumerate(ws):
                ws_start = seg["start"] + j * tpw
                ws_end = seg["start"] + (j + 1) * tpw
                words.append((w, ws_start, ws_end))
        if not words:
            return []
        sents = []
        cur = []
        cur_start = words[0][1]
        for word, ws, we in words:
            cur.append(word)
            cur_end = we
            stripped = word.rstrip().rstrip('"').rstrip("'").rstrip(")")
            duration = cur_end - cur_start
            is_end = bool(stripped) and stripped[-1] in SENT_END
            is_over = (max_duration > 0) and (duration > max_duration)
            if is_end or is_over:
                text = " ".join(cur).strip()
                if text and (len(cur) >= min_words or text[-1] in SENT_END):
                    sents.append({
                        "start": round(cur_start, 3),
                        "end": round(cur_end, 3),
                        "speaker": "SPEAKER_00",
                        "text": text,
                    })
                cur = []
                cur_start = cur_end
        if cur:
            text = " ".join(cur).strip()
            if text and (len(cur) >= min_words or text[-1] in SENT_END):
                sents.append({
                    "start": round(cur_start, 3),
                    "end": round(cur_end if 'cur_end' in dir() else cur_start, 3),
                    "speaker": "SPEAKER_00",
                    "text": text,
                })
        return sents

    def _iso_lang_to_vietnamese(self, code: str) -> str:
        m = {
            "vi": "Tiếng Việt", "en": "Tiếng Anh",
            "zh": "Tiếng Trung", "zh-CN": "Tiếng Trung (Giản thể)",
            "zh-Hans": "Tiếng Trung (Giản thể)",
            "zh-Hant": "Tiếng Trung (Phồn thể)",
            "zh-TW": "Tiếng Trung (Phồn thể)",
            "ja": "Tiếng Nhật", "ko": "Tiếng Hàn",
            "fr": "Tiếng Pháp", "de": "Tiếng Đức",
            "es": "Tiếng Tây Ban Nha", "pt": "Tiếng Bồ Đào Nha",
            "ru": "Tiếng Nga", "th": "Tiếng Thái",
            "id": "Tiếng Indonesia", "ms": "Tiếng Mã Lai",
            "ar": "Tiếng Ả Rập", "hi": "Tiếng Hindi",
            "it": "Tiếng Ý", "nl": "Tiếng Hà Lan",
            "pl": "Tiếng Ba Lan", "tr": "Tiếng Thổ Nhĩ Kỳ",
            "uk": "Tiếng Ukraina",
        }
        if not code:
            return "Tiếng Việt"
        return m.get(code, f"Tiếng {code.upper()}")

    # ================== _safe_extract_info (yt-dlp wrapper) ==================
    def _safe_extract_info(self, url: str, ydl_opts: dict, max_attempts: int = 3,
                            context: str = "extract"):
        """
        Wrapper an toàn cho yt-dlp extract_info() có retry + escalate proxy.
        """
        import yt_dlp
        opts_base = dict(ydl_opts)
        last_err = None
        for attempt in range(1, max_attempts + 1):
            proxy_url = self._next_proxy() if self._rotator else None
            opts = dict(opts_base)
            if proxy_url:
                opts["proxy"] = proxy_url
            elif "proxy" in opts:
                del opts["proxy"]
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                if proxy_url and self._rotator:
                    try:
                        self._rotator.mark_success(proxy_url)
                    except Exception:
                        pass
                return info
            except Exception as e:
                last_err = e
                err_short = f"{type(e).__name__}: {str(e)[:120]}"
                is_blocked = self._is_youtube_blocked_error(e)
                if is_blocked and attempt < max_attempts:
                    if not self._rotator:
                        print(f"    [{context}] attempt {attempt}/{max_attempts} blocked "
                              f"(no rotator): {err_short}")
                    else:
                        self._on_youtube_blocked(e, proxy_url, context=context)
                        print(f"    [{context}] attempt {attempt}/{max_attempts} blocked, "
                              f"retry in {3 * attempt}s...")
                    time.sleep(3 * attempt)
                    continue
                if attempt == max_attempts:
                    print(f"    [{context}] attempt {attempt}/{max_attempts} fail: {err_short}")
                break
        return None

    # ================== yt-dlp subtitles downloader ==================
    def _get_youtube_transcript_via_ytdlp(self, video_id: str,
                                            proxy_url: Optional[str] = None,
                                            info_cached: Optional[dict] = None) -> dict | None:
        """
        Lấy phụ đề qua yt-dlp bằng 2 bước:
          Bước 1: extract_info lấy sub URLs
          Bước 2: download sub file → parse json3/vtt

        Engine DUY NHẤT để lấy transcript. Dùng cookies + yt-dlp player_client
        để bypass IP-block của YouTube.
        """
        try:
            import yt_dlp
        except ImportError:
            return None

        # === Bước 1: lấy info (sub URLs) ===
        info = None
        # FIX v2 Skip #1: Nếu info_cached là dict rỗng → Phase 2 đã xác nhận
        # video KHÔNG có subs → skip luôn, không gọi yt-dlp extract_info() lại.
        # An toàn vì Phase 2 chỉ set dict rỗng khi extract_info() thật sự thành
        # công (không phải do cookies/VPN fail).
        if info_cached is not None:
            has_subs = bool(info_cached.get("subtitles")) or bool(info_cached.get("automatic_captions"))
            if not has_subs:
                print(f"  [ytdlp-subs] SKIP: Phase 2 confirmed no subs for {video_id}")
                return None
            info = {
                "subtitles": info_cached.get("subtitles") or {},
                "automatic_captions": info_cached.get("automatic_captions") or {},
            }
            print(f"  [ytdlp-subs] using cached sub URLs (skip yt-dlp extract)")

        if info is None:
            ydl_opts = {
                "quiet": True, "no_warnings": True,
                "skip_download": True, "ignoreerrors": True,
                "js_runtimes": {"node": {}}, "age_limit": None,
            }
            self._apply_auth_skip(ydl_opts)
            self._apply_cookies(ydl_opts)
            self._apply_timeouts(ydl_opts, socket_timeout=60)
            if proxy_url:
                ydl_opts["proxy"] = proxy_url

            # Bound timeout 25s (kể cả khi socket_timeout 60s fail)
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
                            # Dùng transcript_rotator riêng để mark dead
                            self._mark_transcript_proxy_dead(proxy_url)
                        return None
            except Exception as e:
                err_str = str(e)
                print(f"  [ytdlp-subs] extract_info error: {type(e).__name__}: {err_str[:200]}")
                if proxy_url and is_proxy_dead_error(e):
                    self._mark_transcript_proxy_dead(proxy_url)
                elif proxy_url:
                    self._mark_transcript_proxy_failed(proxy_url)
                return None

        if not info:
            print(f"  [ytdlp-subs] no info returned for {video_id}")
            return None

        subtitles = info.get("subtitles") or {}
        auto_captions = info.get("automatic_captions") or {}

        # Tìm key bắt đầu bằng 'vi' trong dict subtitles
        def _find_vi_lang(d):
            if not d:
                return None
            for k in d.keys():
                if k.lower().startswith("vi"):
                    return k
            return None

        # Thử manual trước, rồi auto
        vi_key = _find_vi_lang(subtitles)
        source_type = "manual"
        lang_code = vi_key or "vi"
        chosen = subtitles.get(vi_key) if vi_key else None

        if not chosen:
            vi_key = _find_vi_lang(auto_captions)
            source_type = "auto"
            lang_code = vi_key or "vi"
            chosen = auto_captions.get(vi_key) if vi_key else None

        if not chosen:
            all_langs = list(subtitles.keys()) + list(auto_captions.keys())
            print(f"  [ytdlp-subs] no Vi sub. Available: {all_langs[:10]}")
            return None

        # Tìm format json3 hoặc vtt
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

        if not sub_url and chosen:
            sub_url = chosen[0].get("url")
            sub_format = chosen[0].get("ext", "vtt")

        if not sub_url:
            print(f"  [ytdlp-subs] no sub URL for {video_id}")
            return None

        print(f"  [ytdlp-subs] found {source_type} sub '{lang_code}' "
              f"format={sub_format}, downloading...")

        # === Bước 2: download sub file ===
        try:
            import requests
            proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
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
                                timeout=(10, 25))
            if resp.status_code == 429:
                print(f"  [ytdlp-subs] HTTP 429 (rate limited)")
                if proxy_url:
                    self._mark_transcript_proxy_failed(proxy_url)
                return None
            if resp.status_code == 403:
                print(f"  [ytdlp-subs] HTTP 403 (forbidden)")
                if proxy_url:
                    self._mark_transcript_proxy_failed(proxy_url)
                return None
            resp.raise_for_status()
            content = resp.text
        except Exception as e:
            err_str = str(e)
            print(f"  [ytdlp-subs] download sub failed: {type(e).__name__}: {err_str[:200]}")
            if proxy_url and is_proxy_dead_error(e):
                self._mark_transcript_proxy_dead(proxy_url)
            elif proxy_url:
                self._mark_transcript_proxy_failed(proxy_url)
            return None

        # === Bước 3: parse ===
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
        """Parse WebVTT / SRV* subtitle → list [{start, duration, text}]."""
        ts_pattern = re.compile(
            r"(?P<sh>\d{2}):(?P<sm>\d{2}):(?P<ss>\d{2})[.,](?P<sms>\d{3})"
            r"\s+-->\s+"
            r"(?P<eh>\d{2}):(?P<em>\d{2}):(?P<es>\d{2})[.,](?P<ems>\d{3})"
        )
        segs = []
        blocks = re.split(r"\n\s*\n", content)
        for block in blocks:
            lines = block.strip().split("\n")
            if not lines:
                continue
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
            text_lines = lines[text_start:]
            text_lines = [l for l in text_lines if l.strip() and not l.strip().isdigit()]
            text = " ".join(text_lines)
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
        """Parse JSON3 subtitle format từ YouTube.

        Format thuc te YouTube tra ve (json3):
          {
            "wireMagic": "pb3",
            "events": [
              {
                "tStartMs": 0, "dDurationMs": 1500,
                "segs": [{"utf8": "Hello"}, {"utf8": " world"}]
              },
              ...
            ]
          }
        """
        segs = []
        for ev in data.get("events", []):
            start_ms = ev.get("tStartMs", ev.get("t"))
            dur_ms = ev.get("dDurationMs", ev.get("d"))
            if start_ms is None:
                continue
            start = float(start_ms) / 1000.0
            dur = float(dur_ms) / 1000.0 if dur_ms is not None else 0.0
            parts = []
            for s in ev.get("segs", []) or []:
                txt = s.get("utf8", s.get("text", ""))
                if txt:
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

    def transcribe_with_youtube(self, video_id: str, audio_path: Path = None,
                                 lang: list = None, max_sentence_duration: float = 33.0,
                                 min_sentence_words: int = 1,
                                 info_cached: Optional[dict] = None,
                                 attempt: int = 1) -> dict | None:
        """
        Lấy phụ đề sẵn có của YouTube theo cách của youtube_researcher_youtube_subs_multi_only_vpn.py.

        Priority:
          1. yt-dlp subtitles download (retry 2 lần với proxy khác từ transcript_rotator).
             KHÔNG còn fallback nào khác.

        Args:
            attempt: KHÔNG còn dùng (giữ để tương thích signature cũ).
                Retry logic đã được tích hợp bên trong (range(2)).
        """
        if lang is None:
            lang = ["vi"]

        print(f"  Fetching YouTube transcript via yt-dlp (langs={lang})...")

        # === ONLY: yt-dlp subtitles download ===
        # KHÔNG còn fallback youtube-transcript-api.
        # Retry tối đa 2 lần với proxy khác từ transcript_rotator.
        result = None
        for a in range(2):
            yt_proxy = self._proxy_for_transcript_fallback()
            print(f"  [transcript-ytdlp] attempt {a+1}/2 via "
                  f"{self._short_proxy(yt_proxy) if yt_proxy else 'DIRECT'} "
                  f"(transcript_rotator={'ON' if self._transcript_rotator and self._transcript_rotator is not self._rotator else 'shared'})")
            cached = info_cached if a == 0 else None
            result = self._get_youtube_transcript_via_ytdlp(
                video_id, proxy_url=yt_proxy, info_cached=cached)
            if result:
                break

        if not result:
            print("  No YouTube transcript available (yt-dlp fail sau 2 attempts)")
            return None

        raw_segments = result["segments"]
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
        segments = self._merge_youtube_segments_to_sentences(
            raw_parsed, max_duration=max_sentence_duration,
            min_words=min_sentence_words)
        if not segments:
            return None
        audio_duration = 0.0
        if audio_path and Path(audio_path).exists():
            try:
                audio_path_str = str(audio_path)
                # Cách 1: WAV thuần → đọc header instant (vài KB)
                if audio_path_str.lower().endswith(".wav"):
                    try:
                        import wave as _wave_mod
                        with _wave_mod.open(audio_path_str, "rb") as _wf:
                            _frames = _wf.getnframes()
                            _rate = _wf.getframerate()
                            if _rate > 0:
                                audio_duration = round(_frames / _rate, 3)
                    except Exception:
                        pass
                # Cách 2: format khác (m4a, webm, mp4, ...) → ffprobe (subprocess, ~50-200ms)
                if audio_duration <= 0:
                    try:
                        import subprocess
                        _ff = subprocess.run(
                            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                             "-of", "default=noprint_wrappers=1:nokey=1", audio_path_str],
                            capture_output=True, text=True, timeout=10,
                        )
                        if _ff.returncode == 0 and _ff.stdout.strip():
                            audio_duration = round(float(_ff.stdout.strip()), 3)
                    except Exception:
                        pass
                # Cách 3: fallback cuối → sf.SoundFile (chỉ metadata, không load sample)
                if audio_duration <= 0:
                    try:
                        import soundfile as _sf
                        with _sf.SoundFile(audio_path_str) as _sf_f:
                            _sr = _sf_f.samplerate
                            _frames = len(_sf_f)
                            if _sr > 0:
                                audio_duration = round(_frames / _sr, 3)
                    except Exception:
                        pass
            except Exception:
                pass
        if audio_duration <= 0:
            audio_duration = float(segments[-1]["end"])
        lang_name = self._iso_lang_to_vietnamese(result["language"])
        return {
            "segments": segments,
            "audio_duration": audio_duration,
            "detected_languages": [lang_name],
            "transcript_language": lang_name,
            "transcript_is_auto": result["is_auto"],
            "transcript_source": result["source"],
        }

    def has_youtube_subs(self, video_id: str, info_cached: Optional[dict] = None,
                          video_obj: Optional[VideoCandidate] = None) -> bool:
        """Check video có YouTube subs không. Ưu tiên O(1) từ info_cached / video_obj,
        fallback gọi yt-dlp extract_info() (KHÔNG dùng youtube-transcript-api nữa)."""
        if video_obj and video_obj.caption_available:
            return True
        if info_cached:
            if info_cached.get("subtitles") or info_cached.get("automatic_captions"):
                return True
            return False   # đã có info_cached rỗng → biết chắc không có sub
        # Không có info_cached → gọi yt-dlp 1 lần để check
        info = fetch_video_info_via_ytdlp(video_id)
        if not info:
            return False
        return bool(info.get("subtitles")) or bool(info.get("automatic_captions"))

    # ================== SAVE (MỞ RỘNG) ==================
    def _save_transcription(self, output_path: Path, segments: list, video,
                            audio_duration: float, audio_filename: str = "",
                            audio_downloaded_at: Optional[str] = None,
                            extra_metadata: dict = None):
        """Lưu transcription JSON chứa ĐẦY ĐỦ URL + metadata + segments.

        File này là NGUỒN DỮ LIỆU DUY NHẤT để extract lại metadata mà không
        cần gọi API.
        """
        speakers = sorted(set(str(s.get("speaker", "SPEAKER_00")) for s in segments))
        em = extra_metadata or {}

        result = {
            # === Video metadata (extract lại được từ đây) ===
            "video_id": video.video_id,
            "url": video.video_url,
            "title": video.title,
            "channel": video.channel,
            "channel_id": getattr(video, "channel_id", ""),
            "channel_url": getattr(video, "channel_url", ""),
            "published_at": video.published_at,
            "duration": video.duration,
            "duration_seconds": parse_duration(video.duration),
            "duration_string": getattr(video, "duration_string", ""),
            "view_count": int(getattr(video, "view_count", 0)),
            "like_count": int(getattr(video, "like_count", 0)),
            "comment_count": int(getattr(video, "comment_count", 0)),
            "description": getattr(video, "description", ""),
            "tags": list(getattr(video, "tags", []) or []),
            "category_id": getattr(video, "category_id", ""),
            "categories": list(getattr(video, "categories", []) or []),
            "default_language": getattr(video, "default_language", ""),
            "default_audio_language": getattr(video, "default_audio_language", ""),
            "thumbnail": getattr(video, "thumbnail", ""),
            "caption_available": bool(getattr(video, "caption_available", False)),
            "definition": getattr(video, "definition", ""),
            "channel_follower_count": int(getattr(video, "channel_follower_count", 0)),
            "uploader": getattr(video, "uploader", ""),
            "uploader_id": getattr(video, "uploader_id", ""),
            "uploader_url": getattr(video, "uploader_url", ""),

            # === v3: YouTube API metadata bổ sung (12 field) ===
            "dimension": getattr(video, "dimension", "2d"),
            "licensed_content": bool(getattr(video, "licensed_content", False)),
            "projection": getattr(video, "projection", "rectangular"),
            "privacy_status": getattr(video, "privacy_status", ""),
            "embeddable": bool(getattr(video, "embeddable", True)),
            "made_for_kids": bool(getattr(video, "made_for_kids", False)),
            "live_broadcast_content": getattr(video, "live_broadcast_content", "none"),
            "topic_categories": list(getattr(video, "topic_categories", []) or []),
            "recording_location": getattr(video, "recording_location", ""),
            "live_status": getattr(video, "live_status", "not_live"),
            "was_live": bool(getattr(video, "was_live", False)),
            "availability": getattr(video, "availability", getattr(video, "privacy_status", "")),

            # === Audio ===
            "audio_filename": audio_filename or "",
            "audio_path": audio_filename or "",
            "audio_duration": float(audio_duration or 0.0),
            "audio_downloaded_at": audio_downloaded_at,

            # === Transcript ===
            "transcript_language": em.get("transcript_language", ""),
            "transcript_is_auto": em.get("transcript_is_auto", False),
            "transcript_source": em.get("transcript_source", ""),
            "detected_languages": em.get("detected_languages", []),

            # === Soniox-compatible (cho CSV/JSON cũ) ===
            "num_speakers": len(speakers),
            "speakers": speakers,
            "source_files": [],
            "segments": segments,
        }
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
        data_out = {
            "research_date": datetime.now().isoformat(),
            "channel": self._videos[0].channel if self._videos else "",
            "total_videos_found": len(self._videos),
            "videos_after_filter": len(self._filtered_videos),
            "videos": videos_data,
        }
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data_out, f, ensure_ascii=False, indent=2)
        print(f"Saved to {output_file}")


# ================= CSV EXPORT =================
def export_segments_minimal_csv(output_csv, videos, transcription_dir):
    """CSV 11 cột: video_id, video_title, channel, video_url,
    segment_start, segment_end, segment_duration, text, language,
    audio_path, audio_duration_seconds.
    """
    import csv
    headers = [
        "video_id", "video_title", "channel", "video_url",
        "segment_start", "segment_end", "segment_duration",
        "text", "language", "audio_path", "audio_duration_seconds",
    ]
    rows = []
    for video in videos:
        json_path = YouTubeResearcher.find_transcription_json(
            transcription_dir, video,
            audio_filename=getattr(video, "audio_filename", ""),
            search_all_runs=True,
        )
        if not json_path:
            continue
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        segments = data.get("segments", [])
        audio_path = data.get("audio_path", "") or data.get("audio_filename", "")
        audio_dur = data.get("audio_duration", 0.0)
        language = data.get("transcript_language", "")
        for seg in segments:
            rows.append([
                video.video_id, video.title, video.channel, video.video_url,
                seg.get("start"), seg.get("end"),
                round((seg.get("end") or 0) - (seg.get("start") or 0), 3),
                seg.get("text"), language, audio_path, audio_dur,
            ])
    if not rows:
        print("No rows to export")
        return
    with open(output_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    print(f"CSV exported: {output_csv} ({len(rows)} rows)")


def export_video_summary_csv(output_csv, videos, transcription_dir):
    """v3: Export video-level summary CSV (1 row/video) với 40+ cột YouTube metadata.

    Cột (40+):
      - Core (10):    video_id, title, channel, url, published_at,
                      duration_formatted, duration_seconds,
                      view_count, like_count, comment_count
      - Engagement (2): engagement_ratio, audio_path
      - Audio (4):    audio_duration_seconds, num_segments,
                      num_speakers, speakers_list
      - YouTube v2 (12): tags, category_id, default_language, default_audio_language,
                         caption_available, definition, channel_id, channel_url,
                         channel_follower_count, uploader, uploader_id, uploader_url
      - YouTube v3 (12): dimension, licensed_content, projection, privacy_status,
                         embeddable, made_for_kids, live_broadcast_content,
                         live_status, was_live, availability, recording_location,
                         topic_categories
      - Filter (3):   passed_filters, failed_filters, description_short

    Dùng csv.writer (không pandas) để giữ nhẹ.
    """
    import csv
    headers = [
        # Core
        "video_id", "title", "channel", "url", "published_at",
        "duration_formatted", "duration_seconds",
        "view_count", "like_count", "comment_count",
        # Engagement
        "engagement_ratio", "audio_path",
        # Audio (từ JSON nếu có)
        "audio_duration_seconds", "num_segments", "num_speakers", "speakers_list",
        # YouTube v2
        "tags", "category_id", "default_language", "default_audio_language",
        "caption_available", "definition", "channel_id", "channel_url",
        "channel_follower_count", "uploader", "uploader_id", "uploader_url",
        # YouTube v3 (12 field mới)
        "dimension", "licensed_content", "projection", "privacy_status",
        "embeddable", "made_for_kids", "live_broadcast_content",
        "live_status", "was_live", "availability", "recording_location",
        "topic_categories",
        # Filter
        "passed_filters", "failed_filters", "description_short",
    ]
    rows = []
    for video in videos:
        # Lookup JSON để lấy audio stats
        json_path = YouTubeResearcher.find_transcription_json(
            transcription_dir, video,
            audio_filename=getattr(video, "audio_filename", ""),
            search_all_runs=True,
        )
        audio_dur = 0.0
        num_segments = 0
        num_speakers = 0
        speakers_list = ""
        if json_path:
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                audio_dur = data.get("audio_duration", 0.0)
                segments = data.get("segments", [])
                num_segments = len(segments)
                speakers_set = sorted(set(
                    str(s.get("speaker", "SPEAKER_00")) for s in segments))
                num_speakers = len(speakers_set)
                speakers_list = ", ".join(speakers_set)
            except Exception:
                pass

        duration_secs = parse_duration(video.duration)
        engagement_ratio = 0.0
        view_count = int(getattr(video, "view_count", 0) or 0)
        like_count = int(getattr(video, "like_count", 0) or 0)
        comment_count = int(getattr(video, "comment_count", 0) or 0)
        if view_count > 0:
            engagement_ratio = round(
                (like_count + comment_count) / view_count * 100, 2)

        rows.append([
            # Core
            video.video_id, video.title, video.channel, video.url,
            video.published_at,
            format_duration(duration_secs), duration_secs,
            view_count, like_count, comment_count,
            # Engagement
            engagement_ratio,
            getattr(video, "audio_filename", ""),
            # Audio
            audio_dur, num_segments, num_speakers, speakers_list,
            # YouTube v2
            json.dumps(getattr(video, "tags", []) or [], ensure_ascii=False),
            getattr(video, "category_id", ""),
            getattr(video, "default_language", ""),
            getattr(video, "default_audio_language", ""),
            bool(getattr(video, "caption_available", False)),
            getattr(video, "definition", ""),
            getattr(video, "channel_id", ""),
            getattr(video, "channel_url", ""),
            int(getattr(video, "channel_follower_count", 0) or 0),
            getattr(video, "uploader", ""),
            getattr(video, "uploader_id", ""),
            getattr(video, "uploader_url", ""),
            # YouTube v3
            getattr(video, "dimension", "2d"),
            bool(getattr(video, "licensed_content", False)),
            getattr(video, "projection", "rectangular"),
            getattr(video, "privacy_status", ""),
            bool(getattr(video, "embeddable", True)),
            bool(getattr(video, "made_for_kids", False)),
            getattr(video, "live_broadcast_content", "none"),
            getattr(video, "live_status", "not_live"),
            bool(getattr(video, "was_live", False)),
            getattr(video, "availability", getattr(video, "privacy_status", "")),
            getattr(video, "recording_location", ""),
            json.dumps(getattr(video, "topic_categories", []) or [],
                       ensure_ascii=False),
            # Filter
            " | ".join(getattr(video, "passed_filters", []) or []),
            " | ".join(getattr(video, "failed_filters", []) or []),
            (getattr(video, "description", "") or "")[:200],
        ])
    if not rows:
        print("No rows to export")
        return
    with open(output_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    print(f"Video summary CSV exported: {output_csv} ({len(rows)} rows, "
          f"{len(headers)} columns)")


# ================= CHANNEL LOADERS =================
def load_channels_from_file(path: str) -> list[str]:
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
    if not channel_url:
        return fallback
    s = channel_url.strip().rstrip("/")
    m = re.search(r"@([^/\s?]+)", s)
    if m:
        return m.group(1)
    m = re.search(r"youtube\.com/channel/([^/\s?]+)", s)
    if m:
        return m.group(1)
    m = re.search(r"youtube\.com/(?:c|user)/([^/\s?]+)", s)
    if m:
        return m.group(1)
    if s.startswith("UC") and len(s) == 24:
        return s
    return s.split("/")[-1] or fallback


# ================= ARGS =================
def parse_args():
    import argparse
    p = argparse.ArgumentParser(
        description="YouTube Researcher - Audio + YouTube Subs (Multi-Rotator: 3 VPN tunnels độc lập)"
    )
    p.add_argument("--channel", "-c", help="URL kênh YouTube đơn lẻ")
    p.add_argument("--channels-file", "-f",
                   default="./channels_audio/channels.txt",
                   help="File txt chứa danh sách URL kênh")
    p.add_argument("--output", "-o", default="./youtube_dataset",
                   help="Folder output gốc (giống youtube_researcher_youtube_subs_multi_vpn_v2.py)")
    p.add_argument("--max-results", "-m", type=int, default=20000)
    p.add_argument("--max-fetch", type=int, default=20000)
    p.add_argument("--max-batches", type=int, default=8000)
    p.add_argument("--fetch-delay", type=int, default=5)
    p.add_argument("--order", default="date")
    p.add_argument("--audio-format", default="m4a")
    p.add_argument("--force-retranscribe", action="store_true")
    p.add_argument("--force-redownload", action="store_true",
                   help="Ép tải lại audio kể cả khi đã có file")
    p.add_argument("--skip-existing", action="store_true",
                   help="Skip channel đã có output đầy đủ")
    p.add_argument("--rebuild-from-transcripts", action="store_true",
                   help="CHỈ đọc JSON có sẵn, tạo CSV/summary. "
                        "Không gọi API, không tải audio, không lấy transcript.")
    p.add_argument("--metadata-only", action="store_true",
                   help="Chỉ fetch metadata (không audio, không transcript)")
    p.add_argument("--use-vpn", action="store_true", default=True,
                   help="(BẮT BUỘC trong bản này)")
    p.add_argument("--vpn-rotate-every", type=int, default=10,
                   help="Áp dụng cho metadata_rotator + transcript_rotator. "
                        "Số request trước khi tự rotate IP qua VPN. "
                        "0 = chỉ rotate khi gặp 429/403 (mặc định: 10).")
    p.add_argument("--vpn-real-ip-cycle", type=int, default=6,
                   help="Áp dụng CHO audio_rotator (CHỈ rotator này cycle). "
                        "Cycle 'N fake VPN → 1 IP thật' (mặc định: 11). "
                        "Sau N request fake VPN thì request kế tiếp sẽ disconnect VPN "
                        "và dùng IP thật (default route), rồi lại reconnect VPN. "
                        "Vd: 11 → cứ 10 fake VPN thì 1 IP thật. "
                        "0 = TẮT cycle.")
    p.add_argument("--vpn-strategy", choices=["random", "sequential", "least_used"],
                   default="random")
    p.add_argument("--video-delay", type=int, default=10,
                   help="Delay giữa các video (giây)")
    p.add_argument("--socket-timeout", type=int, default=600)
    p.add_argument("--max-retries", type=int, default=3)
    p.add_argument("--max-sentence-duration", type=int, default=33.0)
    p.add_argument("--min-sentence-words", type=int, default=1)
    p.add_argument("--instance-id", default=None)
    p.add_argument("--cache-dir", default=None)
    p.add_argument("--vpn-isolated", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--proxy-mode", choices=["auto", "always", "split", "never"],
                   default="auto")
    return p.parse_args()


# ================= RUN LOGGER =================
class RunLogger:
    def __init__(self, log_path, script_path=""):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"{'='*80}\nRUN LOG {datetime.now().isoformat()}\n")
            if script_path:
                f.write(f"Script: {script_path}\n")
            f.write(f"{'='*80}\n\n")

    def log(self, msg, also_print=True):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        with self._lock:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        if also_print:
            print(line)

    def log_channel_start(self, idx, total, channel_url, channel_name, run_ts):
        self.log(f"CHANNEL {idx}/{total}: {channel_url} (name={channel_name}, ts={run_ts})")

    def log_channel_end(self, idx, total, channel_url, status, summary=None, error=None):
        self.log(f"CHANNEL {idx}/{total} DONE: status={status}")
        if error:
            self.log(f"  ERROR: {error}")

    def log_batch_start(self, channels_file, total_channels, command="", script_path=""):
        self.log(f"BATCH START: {total_channels} channels from {channels_file}")
        if command:
            self.log(f"  command: {command}")

    def log_batch_end(self, total_channels, success, failed, all_results):
        self.log(f"BATCH END: {total_channels} channels, "
                 f"{success} success, {failed} failed")


# ================= process_one_channel =================
def process_one_channel(
    channel_url: str, *, youtube_key: str, output_root: str,
    max_results: int, max_fetch: int, order: str, audio_format: str,
    skip_existing: bool, force_retranscribe: bool = False,
    force_redownload: bool = False,
    max_batches: int = 400, fetch_delay: int = 5,
    proxy_rotator=None, audio_proxy_rotator=None,
    transcript_proxy_rotator=None,
    video_delay: int = 10,
    socket_timeout: int = 100, max_retries: int = 3,
    max_sentence_duration: float = 33.0, min_sentence_words: int = 1,
    run_logger=None, channel_idx: int = 0, total_channels: int = 0,
    metadata_only: bool = False,
    rebuild_from_transcripts: bool = False,
) -> dict:
    channel_name = safe_channel_name(channel_url)
    channel_output = Path(output_root) / channel_name
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\n" + "=" * 80)
    print(f"CHANNEL: {channel_url}")
    print(f"Output: {channel_output}")
    print("=" * 80)
    if run_logger:
        run_logger.log_channel_start(channel_idx, total_channels, channel_url,
                                     channel_name, run_timestamp)

    # === REBUILD MODE: chỉ đọc JSON có sẵn ===
    if rebuild_from_transcripts:
        return _rebuild_from_transcripts(
            channel_url, channel_name, Path(output_root),
            run_timestamp, run_logger, channel_idx, total_channels,
        )

    # === METADATA-ONLY MODE: chỉ fetch metadata ===
    if metadata_only:
        return _metadata_only_mode(
            channel_url, channel_name, channel_output, run_timestamp,
            youtube_key, max_results, max_fetch, order,
            max_batches, fetch_delay, socket_timeout, max_retries,
            proxy_rotator, run_logger, channel_idx, total_channels,
        )

    # === KHÔNG check skip-existing ở CHANNEL level nữa ===
    # Lý do: run_timestamp là timestamp mới của lần chạy hiện tại,
    # file pipeline_summary_<NEW_TIMESTAMP>.json chưa tồn tại → check vô dụng.
    # Logic skip chính xác nằm ở _process_videos_pipeline (per-video) —
    # build index từ TẤT CẢ subfolders audio/ + transcriptions/ cũ,
    # video có đủ audio + JSON hợp lệ → skip, thiếu → tải phần thiếu.
    channel_output.mkdir(parents=True, exist_ok=True)
    researcher = YouTubeResearcher(
        api_key=youtube_key, output_dir=str(channel_output),
        proxy_rotator=proxy_rotator,
        audio_proxy_rotator=audio_proxy_rotator,
        transcript_proxy_rotator=transcript_proxy_rotator,
    )
    try:
        researcher.fetch_channel_videos(
            channel_input=channel_url, max_results=max_fetch, order=order,
            batch_size=200, max_batches=max_batches,
            socket_timeout=socket_timeout, fetch_delay=fetch_delay,
            max_retries=max_retries,
        )
    except Exception as e:
        if run_logger:
            run_logger.log_channel_end(channel_idx, total_channels, channel_url,
                                       "error", error=str(e))
        raise
    if not researcher._videos:
        return {"channel": channel_url, "status": "no_videos", "output": str(channel_output)}

    criteria = FilterCriteria(
        min_duration=FILTER_MIN_DURATION,
        max_duration=FILTER_MAX_DURATION,
        min_view_count=FILTER_MIN_VIEW_COUNT,
    )
    researcher.apply_filters(criteria)
    if len(researcher._filtered_videos) > max_results:
        researcher._filtered_videos = researcher._filtered_videos[:max_results]
    researcher.print_video_table()

    resolved_channel_name = researcher._videos[0].channel if researcher._videos else channel_name
    safe_name = resolved_channel_name.replace(" ", "_")
    researcher.save_research(f"research_{safe_name}_{run_timestamp}.json")

    print("\nRunning pipeline (audio download + YouTube transcript)...")
    summary = researcher.process_videos_pipeline(
        output_dir=str(channel_output),
        run_timestamp=run_timestamp,
        skip_existing_transcripts=not force_retranscribe,
        force_redownload=force_redownload,
        video_delay=video_delay,
        audio_format=audio_format,
        run_logger=run_logger,
    )
    summary_path = channel_output / f"pipeline_summary_{run_timestamp}.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    transcription_dir = str(channel_output / "transcriptions" / run_timestamp)
    csv_path = channel_output / f"{safe_name}_segments_minimal_{run_timestamp}.csv"
    export_segments_minimal_csv(
        output_csv=str(csv_path),
        videos=researcher._filtered_videos,
        transcription_dir=transcription_dir,
    )
    # v3: thêm video-level summary CSV với 40+ cột YouTube metadata
    summary_csv_path = channel_output / f"{safe_name}_video_summary_{run_timestamp}.csv"
    export_video_summary_csv(
        output_csv=str(summary_csv_path),
        videos=researcher._filtered_videos,
        transcription_dir=transcription_dir,
    )
    print(f"\n[DONE] {channel_url} -> {channel_output}")
    if run_logger:
        run_logger.log_channel_end(channel_idx, total_channels, channel_url, "success",
                                   summary=summary)
    return {
        "channel": channel_url, "channel_name": resolved_channel_name,
        "status": "success", "output": str(channel_output),
        "run_timestamp": run_timestamp,
        "summary": summary,
    }


# ================= REBUILD FROM TRANSCRIPTS =================
def _resolve_channel_folder(output_root: Path, channel_url: str,
                            channel_name: str) -> Optional[Path]:
    """Tìm folder kênh trong output_root bằng nhiều cách:
      1. exact match channel_name
      2. normalized match (bỏ diacritics, lowercase, bỏ space)
      3. handle @xxx extracted từ URL làm substring
    """
    if not output_root.exists():
        return None
    # 1. exact match
    exact = output_root / channel_name
    if exact.exists():
        return exact
    # 2. normalize
    def _norm(s):
        import unicodedata
        n = unicodedata.normalize("NFKD", s)
        n = "".join(ch for ch in n if not unicodedata.combining(ch))
        return n.lower().replace(" ", "")
    target = _norm(channel_name)
    # 3. Lấy handle gốc từ URL
    handle = None
    m = re.search(r"@([^/\s?]+)", channel_url or "")
    if m:
        handle = m.group(1)
    for sub in output_root.iterdir():
        if not sub.is_dir():
            continue
        sub_norm = _norm(sub.name)
        if sub_norm == target:
            return sub
        # 4. Match bằng handle gốc (lowercase, no diacritics)
        if handle and _norm(handle) in sub_norm:
            return sub
    return None


def _rebuild_from_transcripts(channel_url, channel_name, output_root, run_ts,
                               run_logger, channel_idx, total_channels):
    """Đọc JSON có sẵn trong transcriptions/ để tạo CSV + summary.

    KHÔNG gọi API, KHÔNG tải audio, KHÔNG lấy transcript.
    Dùng khi user muốn extract lại metadata từ JSON bất kỳ lúc nào.
    """
    print(f"\n[REBUILD MODE] Đọc JSON có sẵn...")
    # Tìm folder kênh linh hoạt (vì handle URL != tên folder thật)
    channel_output = _resolve_channel_folder(output_root, channel_url, channel_name)
    if channel_output is None:
        print(f"  [WARN] Không tìm thấy folder kênh trong {output_root}. Bỏ qua.")
        return {"channel": channel_url, "status": "no_channel_folder", "rebuild": True}
    print(f"  Channel folder: {channel_output}")
    transcriptions_dir = channel_output / "transcriptions"
    if not transcriptions_dir.exists():
        print(f"  [WARN] Không có folder transcriptions/. Bỏ qua.")
        return {"channel": channel_url, "status": "no_transcripts", "rebuild": True}

    # Lấy tất cả JSON từ tất cả subfolder timestamp
    all_jsons = []
    for sub in sorted(transcriptions_dir.iterdir(), reverse=True):
        if sub.is_dir():
            for f in sub.glob("*_transcription.json"):
                all_jsons.append(f)
    if not all_jsons:
        print(f"  [WARN] Không có file JSON. Bỏ qua.")
        return {"channel": channel_url, "status": "no_json", "rebuild": True}

    print(f"  Tìm thấy {len(all_jsons)} file JSON")

    # Build danh sách VideoCandidate từ JSON
    videos = []
    for jp in all_jsons:
        try:
            with open(jp, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        v = VideoCandidate(
            video_id=data.get("video_id", ""),
            title=data.get("title", ""),
            channel=data.get("channel", ""),
            description=data.get("description", ""),
            published_at=data.get("published_at", ""),
            duration=data.get("duration", ""),
            view_count=int(data.get("view_count", 0)),
            like_count=int(data.get("like_count", 0)),
            comment_count=int(data.get("comment_count", 0)),
            url=data.get("url", ""),
            tags=data.get("tags", []),
            category_id=data.get("category_id", ""),
            categories=data.get("categories", []),
            default_language=data.get("default_language", ""),
            default_audio_language=data.get("default_audio_language", ""),
            caption_available=bool(data.get("caption_available", False)),
            definition=data.get("definition", ""),
            channel_id=data.get("channel_id", ""),
            channel_url=data.get("channel_url", ""),
            duration_string=data.get("duration_string", ""),
            audio_filename=data.get("audio_filename", ""),
        )
        videos.append(v)
    resolved_channel_name = videos[0].channel if videos else channel_name
    safe_name = resolved_channel_name.replace(" ", "_")

    # Save research JSON
    channel_output.mkdir(parents=True, exist_ok=True)
    research_data = {
        "research_date": datetime.now().isoformat(),
        "channel": resolved_channel_name,
        "total_videos_found": len(videos),
        "videos_after_filter": len(videos),
        "videos": [asdict(v) | {"video_url": v.video_url} for v in videos],
        "rebuild_mode": True,
    }
    research_path = channel_output / f"research_{safe_name}_{run_ts}.json"
    with open(research_path, "w", encoding="utf-8") as f:
        json.dump(research_data, f, ensure_ascii=False, indent=2)
    print(f"  Saved research: {research_path}")

    # CSV từ JSON mới nhất
    transcription_dir = str(transcriptions_dir / max(
        d.name for d in transcriptions_dir.iterdir() if d.is_dir()))
    csv_path = channel_output / f"{safe_name}_segments_minimal_{run_ts}.csv"
    export_segments_minimal_csv(
        output_csv=str(csv_path), videos=videos, transcription_dir=transcription_dir)
    # v3: video-level summary CSV
    summary_csv_path = channel_output / f"{safe_name}_video_summary_{run_ts}.csv"
    export_video_summary_csv(
        output_csv=str(summary_csv_path), videos=videos,
        transcription_dir=transcription_dir)

    summary = {
        "rebuild": True, "total": len(videos), "success": len(videos),
        "results": [{"video_id": v.video_id, "title": v.title, "status": "rebuilt"}
                    for v in videos],
    }
    summary_path = channel_output / f"pipeline_summary_{run_ts}.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    if run_logger:
        run_logger.log_channel_end(channel_idx, total_channels, channel_url,
                                   "rebuild_success", summary=summary)
    print(f"\n[REBUILD DONE] {channel_url}")
    return {"channel": channel_url, "status": "rebuild_success",
            "output": str(channel_output), "summary": summary}


# ================= METADATA ONLY =================
def _metadata_only_mode(channel_url, channel_name, channel_output, run_ts,
                          youtube_key, max_results, max_fetch, order,
                          max_batches, fetch_delay, socket_timeout, max_retries,
                          proxy_rotator, run_logger, channel_idx, total_channels):
    """Chỉ fetch metadata, KHÔNG tải audio, KHÔNG lấy transcript."""
    print(f"\n[METADATA-ONLY MODE]")
    channel_output.mkdir(parents=True, exist_ok=True)
    researcher = YouTubeResearcher(
        api_key=youtube_key, output_dir=str(channel_output),
        proxy_rotator=proxy_rotator,
    )
    try:
        researcher.fetch_channel_videos(
            channel_input=channel_url, max_results=max_fetch, order=order,
            batch_size=200, max_batches=max_batches,
            socket_timeout=socket_timeout, fetch_delay=fetch_delay,
            max_retries=max_retries,
        )
    except Exception as e:
        if run_logger:
            run_logger.log_channel_end(channel_idx, total_channels, channel_url,
                                       "error", error=str(e))
        raise
    if not researcher._videos:
        return {"channel": channel_url, "status": "no_videos"}
    criteria = FilterCriteria(
        min_duration=FILTER_MIN_DURATION, max_duration=FILTER_MAX_DURATION,
        min_view_count=FILTER_MIN_VIEW_COUNT,
    )
    researcher.apply_filters(criteria)
    if len(researcher._filtered_videos) > max_results:
        researcher._filtered_videos = researcher._filtered_videos[:max_results]
    researcher.print_video_table()
    resolved_channel_name = researcher._videos[0].channel if researcher._videos else channel_name
    safe_name = resolved_channel_name.replace(" ", "_")
    researcher.save_research(f"research_{safe_name}_{run_ts}.json")
    if run_logger:
        run_logger.log_channel_end(channel_idx, total_channels, channel_url,
                                   "metadata_only_success")
    return {"channel": channel_url, "status": "metadata_only_success",
            "output": str(channel_output)}


# ================= PIPELINE =================
def _process_videos_pipeline(self, output_dir, run_timestamp="",
                              skip_existing_transcripts=True,
                              force_redownload=False,
                              video_delay=10,
                              max_sentence_duration=33.0,
                              min_sentence_words=1,
                              audio_format="m4a",
                              run_logger=None) -> dict:
    """Pipeline: tải audio + lấy YouTube subs transcript.

    Logic 4 case (resumable):
      - Có audio + JSON → SKIP (load audio_filename từ JSON cho CSV)
      - Có audio, thiếu JSON → chỉ transcribe
      - Có JSON, thiếu audio → download audio
      - Thiếu cả 2 → full pipeline
    """
    output_dir = Path(output_dir)
    if not run_timestamp:
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    transcriptions_dir = output_dir / "transcriptions" / run_timestamp
    transcriptions_dir.mkdir(parents=True, exist_ok=True)
    audio_dir = output_dir / "audio" / run_timestamp
    audio_dir.mkdir(parents=True, exist_ok=True)

    def _log(msg, also_print=True):
        if run_logger:
            run_logger.log(msg, also_print=also_print)
        elif also_print:
            print(msg)

    results = []
    if skip_existing_transcripts:
        self._cleanup_orphan_part_files(audio_dir, min_size_mb=100)

    audio_index = YouTubeResearcher._build_audio_index(audio_dir.parent)
    json_index = YouTubeResearcher._build_json_index(transcriptions_dir.parent)

    total = len(self._filtered_videos)
    _log(f"Pipeline (total={total}):")

    for i, video in enumerate(self._filtered_videos, 1):
        target_name = self._safe_filename(video.title, fallback=video.video_id)
        target_filename = f"{target_name}.wav"

        audio_path = (
            audio_index.get(target_name)
            or audio_index.get(video.video_id)
            or audio_index.get(f"{target_name}_{video.video_id}")
        )
        json_path = None
        if audio_path:
            expected_stem = f"{audio_path.stem}_transcription"
            for key in [audio_path.stem, video.video_id, target_name]:
                cand = json_index.get(key)
                if cand and cand.stem == expected_stem:
                    json_path = cand
                    break

        audio_filename = audio_path.name if audio_path else target_filename
        video.audio_filename = audio_filename

        # === Case 1: CÓ CẢ AUDIO + JSON → SKIP ===
        if audio_path and json_path and not force_redownload and skip_existing_transcripts:
            print(f"\n[{i}/{total}] {video.title[:60]}")
            try:
                audio_size_kb = audio_path.stat().st_size // 1024
            except OSError:
                audio_size_kb = 0
            try:
                # audio_dir = output_dir / "audio" / run_timestamp
                audio_rel = audio_path.relative_to(audio_dir.parent)
                json_rel = json_path.relative_to(transcriptions_dir.parent)
            except ValueError:
                audio_rel = Path(audio_path.name)
                json_rel = Path(json_path.name)
            print(f"  [SKIP] audio + JSON đã có sẵn")
            print(f"    audio: {audio_rel} ({audio_size_kb} KB)")
            print(f"    json:  {json_rel}")
            try:
                with open(json_path, "r", encoding="utf-8") as jf:
                    existing = json.load(jf)
                video.audio_filename = existing.get("audio_path", audio_filename)
            except Exception:
                pass
            results.append({
                "video_id": video.video_id, "title": video.title,
                "status": "skipped",
                "audio_filename": video.audio_filename,
                "transcription_filename": json_path.name,
            })
            continue

        # === Case 2: CÓ AUDIO, THIẾU JSON → chỉ transcribe ===
        json_stem = Path(audio_filename).stem
        new_json_path = transcriptions_dir / f"{json_stem}_transcription.json"
        if audio_path and not json_path:
            # FIX v2 Skip #2: nếu video đã được đánh dấu no_transcript ở run trước
            # → skip luôn, không tốn thời gian gọi yt-dlp.
            if YouTubeResearcher._has_no_transcript_marker(video.video_id, transcriptions_dir):
                print(f"\n[{i}/{total}] {video.title[:60]}")
                print(f"  [SKIP-NO-TRANSCRIPT] marker exists, skip yt-dlp "
                      f"(video={video.video_id}, audio: {audio_filename})")
                results.append({
                    "video_id": video.video_id, "title": video.title,
                    "status": "transcript_unavailable",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": None,
                })
                continue

            print(f"\n[{i}/{total}] {video.title[:60]}")
            print(f"  [TRANSCRIBE-ONLY] audio có sẵn: {audio_filename}")
            # FIX v2: truyền info_cached từ Phase 2 (nếu có) để skip gọi yt-dlp lần 2
            # → giảm rate limit "Sign in to confirm you're not a bot"
            info_cached_b: dict = {}
            try:
                if getattr(video, "subtitles", None):
                    info_cached_b["subtitles"] = video.subtitles
                if getattr(video, "automatic_captions", None):
                    info_cached_b["automatic_captions"] = video.automatic_captions
            except Exception:
                pass
            try:
                result = self.transcribe_with_youtube(
                    video_id=video.video_id, audio_path=audio_path,
                    lang=["vi", "en"],
                    max_sentence_duration=max_sentence_duration,
                    min_sentence_words=min_sentence_words,
                    info_cached=info_cached_b if info_cached_b else None,
                    attempt=1,  # transcript_rotator riêng
                )
                if result:
                    self._save_transcription(
                        output_path=new_json_path, segments=result["segments"],
                        video=video, audio_duration=result["audio_duration"],
                        audio_filename=audio_filename,
                        audio_downloaded_at=None,
                        extra_metadata={
                            "transcript_language": result.get("transcript_language", ""),
                            "transcript_is_auto": result.get("transcript_is_auto", False),
                            "transcript_source": result.get("transcript_source", ""),
                            "detected_languages": result.get("detected_languages", []),
                        },
                    )
                    results.append({
                        "video_id": video.video_id, "title": video.title,
                        "status": "success", "audio_filename": audio_filename,
                        "transcription_filename": new_json_path.name,
                        "transcript_language": result.get("transcript_language", ""),
                    })
                    print(f"  Done ({len(result['segments'])} segments)")
                    continue
            except Exception as e:
                _log(f"[{i}/{total}] {video.video_id} transcript error: {e}")
                results.append({
                    "video_id": video.video_id, "title": video.title,
                    "status": "transcript_error",
                    "audio_filename": audio_filename, "error": str(e),
                })
                continue

        # === Case 3 & 4: cần download audio ===
        if i > 1 and video_delay > 0:
            time.sleep(video_delay)
        # FIX v2 Skip #2: nếu video đã được đánh dấu no_transcript ở run trước
        # → skip download audio luôn, tiết kiệm bandwidth.
        if YouTubeResearcher._has_no_transcript_marker(video.video_id, transcriptions_dir):
            print(f"\n[{i}/{total}] {video.title[:60]}")
            print(f"  [SKIP-NO-TRANSCRIPT] marker exists, skip download audio + yt-dlp "
                  f"(video={video.video_id})")
            results.append({
                "video_id": video.video_id, "title": video.title,
                "status": "transcript_unavailable",
                "audio_filename": None,
                "audio_downloaded_at": None,
            })
            continue
        print(f"\n[{i}/{total}] {video.title[:60]}")
        try:
            import yt_dlp
            dl_retries = 3
            info = None
            info_cache = {}
            video_id_stem = audio_dir / video.video_id
            for stale in [".ytdl"]:
                stale_file = video_id_stem.with_suffix(stale)
                if stale_file.exists():
                    try:
                        stale_file.unlink()
                    except Exception:
                        pass
            for dl_attempt in range(1, dl_retries + 1):
                dl_proxy = None  # VPN tunnel dùng default route
                use_vpn = True
                ydl_opts = {
                    'format': 'bestaudio/best',
                    'merge_output_format': audio_format,
                    'outtmpl': str(audio_dir / '%(id)s.%(ext)s'),
                    'quiet': True, 'js_runtimes': {'node': {}},
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
                try:
                    # === MULTI-STAGE PROXY: bảo vệ audio_rotator riêng ===
                    # Khi dùng audio_rotator (riêng biệt), wrap trong _proxy_guard_for_audio()
                    # để VPN rotate KHÔNG kill tunnel của metadata/transcript giữa request.
                    if self._audio_rotator is not None and self._audio_rotator is not self._rotator:
                        with self._proxy_guard_for_audio():
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                info = ydl.extract_info(video.url, download=True)
                                filename = ydl.prepare_filename(info)
                    else:
                        with self._proxy_guard():
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                info = ydl.extract_info(video.url, download=True)
                                filename = ydl.prepare_filename(info)
                    audio_path = Path(filename)
                    if not audio_path.exists() or audio_path.suffix not in (
                            ".wav", ".mp3", ".m4a", ".flac", ".opus", ".ogg",
                            ".webm", ".mp4"):
                        wav_candidate = audio_path.with_suffix(".wav")
                        if wav_candidate.exists():
                            audio_path = wav_candidate
                        else:
                            stem = audio_path.with_suffix("")
                            for ext in [".wav", ".m4a", ".mp3", ".flac",
                                        ".opus", ".ogg", ".webm", ".mp4"]:
                                cand = stem.with_suffix(ext)
                                if cand.exists():
                                    audio_path = cand
                                    break
                    if audio_path.exists() and not str(audio_path).endswith('.part'):
                        if audio_path.suffix != ".wav":
                            audio_path = audio_path.with_suffix(".wav")
                    info_cache = {
                        "subtitles": info.get("subtitles") or {},
                        "automatic_captions": info.get("automatic_captions") or {},
                    }
                    break
                except Exception as dl_err:
                    if self._is_youtube_blocked_error(dl_err) and dl_attempt < dl_retries:
                        # Dùng handler riêng cho audio_rotator
                        if self._audio_rotator is not None and self._audio_rotator is not self._rotator:
                            self._on_youtube_blocked_audio(dl_err, None, "audio-dl")
                        else:
                            self._on_youtube_blocked(dl_err, None, "audio-dl")
                        time.sleep(3 * dl_attempt)
                        continue
                    raise
            if info is None:
                raise RuntimeError("Download failed after all retries")

            # Rename theo title
            target_ext = audio_path.suffix if audio_path and audio_path.exists() else ".wav"
            target_filename_new = f"{target_name}{target_ext}"
            target_path = audio_dir / target_filename_new
            if audio_path and audio_path.exists() and audio_path != target_path:
                if target_path.exists():
                    target_path = audio_dir / f"{target_name}_{video.video_id}{target_ext}"
                try:
                    audio_path.rename(target_path)
                    audio_path = target_path
                except Exception:
                    pass
            audio_filename = audio_path.name if audio_path and audio_path.exists() else f"{target_name}.wav"
            video.audio_filename = audio_filename
            audio_downloaded_at = datetime.now().isoformat()

            # Xóa file gốc (.webm/.m4a/...) chỉ giữ .wav
            for leftover_ext in [".webm", ".m4a", ".mp4", ".opus", ".ogg"]:
                leftover = (audio_dir / video.video_id).with_suffix(leftover_ext)
                if leftover.exists() and leftover != audio_path:
                    try:
                        leftover.unlink()
                    except Exception:
                        pass

            # === Case 3: đã có JSON từ run cũ → KHÔNG transcribe lại ===
            if json_path and skip_existing_transcripts and not force_retranscribe:
                print(f"  [AUDIO-ONLY] downloaded, JSON cũ có sẵn: {json_path.name}")
                results.append({
                    "video_id": video.video_id, "title": video.title,
                    "status": "audio_added",
                    "audio_filename": audio_filename,
                    "transcription_filename": json_path.name,
                })
                continue

            # === Case 4: full pipeline ===
            result = self.transcribe_with_youtube(
                video_id=video.video_id, audio_path=audio_path,
                lang=["vi", "en"],
                max_sentence_duration=max_sentence_duration,
                min_sentence_words=min_sentence_words,
                info_cached=info_cache if info_cache else None,
                attempt=1,  # transcript_rotator riêng
            )
            if result:
                self._save_transcription(
                    output_path=new_json_path, segments=result["segments"],
                    video=video, audio_duration=result["audio_duration"],
                    audio_filename=audio_filename,
                    audio_downloaded_at=audio_downloaded_at,
                    extra_metadata={
                        "transcript_language": result.get("transcript_language", ""),
                        "transcript_is_auto": result.get("transcript_is_auto", False),
                        "transcript_source": result.get("transcript_source", ""),
                        "detected_languages": result.get("detected_languages", []),
                    },
                )
                results.append({
                    "video_id": video.video_id, "title": video.title,
                    "status": "success", "audio_filename": audio_filename,
                    "transcription_filename": new_json_path.name,
                    "transcript_language": result.get("transcript_language", ""),
                })
                print(f"  Done ({len(result['segments'])} segments)")
            else:
                results.append({
                    "video_id": video.video_id, "title": video.title,
                    "status": "transcript_unavailable",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": audio_downloaded_at,
                })
                # FIX v2 Skip #2: ghi marker để lần sau skip luôn (cả download lẫn yt-dlp)
                YouTubeResearcher._mark_no_transcript(video.video_id, transcriptions_dir)
        except Exception as e:
            print(f"  Download failed: {e}")
            results.append({
                "video_id": video.video_id, "title": video.title,
                "status": "download_failed",
                "audio_filename": f"{target_name}.wav", "error": str(e),
            })

    success = sum(1 for r in results if r.get("status") in ("success", "skipped"))
    return {"total": total, "success": success, "results": results}


# Bind pipeline method to class
YouTubeResearcher.process_videos_pipeline = _process_videos_pipeline


# ================= MAIN =================
def main():
    args = parse_args()
    print("=" * 80)
    print("YOUTUBE AUDIO + SUBS RESUMABLE — VPN BẮT BUỘC")
    print("=" * 80)

    # Instance ID
    global INSTANCE_ID
    INSTANCE_ID = args.instance_id or f"pid{os.getpid()}_t{int(time.time())}"
    print(f"[Multi-instance] Instance ID: {INSTANCE_ID}")

    cache_root = Path(args.cache_dir) if args.cache_dir else (
        Path(__file__).parent / f".cache_{INSTANCE_ID}")
    cache_root.mkdir(parents=True, exist_ok=True)

    if not _YOUTUBE_API_KEYS:
        print("WARN: YOUTUBE_API_KEY không có trong .env → sẽ fallback yt-dlp "
              "cho metadata (mất view/like/comment count).", file=sys.stderr)
    youtube_key = _YOUTUBE_API_KEYS[0] if _YOUTUBE_API_KEYS else "ytdlp"

    # === 3 VPN rotator TÁCH BIỆT cho 3 nhóm việc ===
    # - metadata_rotator  : rotate theo --vpn-rotate-every, KHÔNG cycle (real_ip_cycle=0)
    # - audio_rotator     : cycle theo --vpn-real-ip-cycle, rotate_every=0
    # - transcript_rotator: rotate theo --vpn-rotate-every, KHÔNG cycle (real_ip_cycle=0)
    # Mỗi rotator có instance_id riêng → log file, PID file, openvpn process
    # hoàn toàn độc lập → 3 nhóm chạy SONG SONG không xung đột.
    metadata_rotator = None
    audio_rotator = None
    transcript_rotator = None

    try:
        metadata_rotator = get_isolated_vpn_rotator_from_config(
            instance_id=f"{INSTANCE_ID}_meta",
            rotate_every=args.vpn_rotate_every,
            strategy=args.vpn_strategy,
            real_ip_cycle=0,  # TẮT cycle cho metadata
        )
        audio_rotator = get_isolated_vpn_rotator_from_config(
            instance_id=f"{INSTANCE_ID}_audio",
            rotate_every=0,  # để cycle điều khiển
            strategy=args.vpn_strategy,
            real_ip_cycle=args.vpn_real_ip_cycle,
        )
        transcript_rotator = get_isolated_vpn_rotator_from_config(
            instance_id=f"{INSTANCE_ID}_subs",
            rotate_every=args.vpn_rotate_every,
            strategy=args.vpn_strategy,
            real_ip_cycle=0,  # TẮT cycle cho transcript
        )
    except (ImportError, Exception) as e:
        print(f"FATAL: Lỗi khởi tạo IsolatedVPNRotator ({e}). "
              f"Không fallback về IP thật.", file=sys.stderr)
        sys.exit(1)

    if metadata_rotator is None or audio_rotator is None or transcript_rotator is None:
        print("FATAL: Không tìm thấy file .ovpn trong ./proton_config/.\n"
              "       Bản này BẮT BUỘC phải có ProtonVPN config.\n"
              "       Không fallback về IP thật.", file=sys.stderr)
        sys.exit(1)

    print(f"\n=== 3 VPN ROTATOR ĐỘC LẬP ===")
    print(f"  • metadata_rotator  : instance '{INSTANCE_ID}_meta', "
          f"rotate_every={args.vpn_rotate_every}, real_ip_cycle=0 (TẮT cycle)")
    print(f"    → log: /tmp/openvpn-proton-{INSTANCE_ID}_meta.log")
    print(f"  • audio_rotator     : instance '{INSTANCE_ID}_audio', "
          f"rotate_every=0, real_ip_cycle={args.vpn_real_ip_cycle} "
          f"({'TẮT cycle' if args.vpn_real_ip_cycle <= 0 else f'BẬT {args.vpn_real_ip_cycle-1} fake + 1 real'})")
    print(f"    → log: /tmp/openvpn-proton-{INSTANCE_ID}_audio.log")
    print(f"  • transcript_rotator: instance '{INSTANCE_ID}_subs', "
          f"rotate_every={args.vpn_rotate_every}, real_ip_cycle=0 (TẮT cycle)")
    print(f"    → log: /tmp/openvpn-proton-{INSTANCE_ID}_subs.log")
    print(f"  • KHÔNG dùng pkill → an toàn chạy song song nhiều instance")

    # Run logger
    output_root = Path(args.output)
    log_dir = output_root / "logs"
    log_path = log_dir / f"crawl_{INSTANCE_ID}.log"
    run_logger = RunLogger(log_path, script_path=str(Path(__file__).absolute()))

    # Load channels
    if args.channel:
        channels = [args.channel]
    else:
        channels = load_channels_from_file(args.channels_file)
    if not channels:
        print("Không có kênh nào để xử lý.")
        sys.exit(1)

    run_logger.log_batch_start(args.channels_file, len(channels),
                                command=" ".join(sys.argv))
    all_results = []
    for i, ch_url in enumerate(channels, 1):
        try:
            res = process_one_channel(
                ch_url, youtube_key=youtube_key, output_root=args.output,
                max_results=args.max_results, max_fetch=args.max_fetch,
                order=args.order, audio_format=args.audio_format,
                skip_existing=args.skip_existing,
                force_retranscribe=args.force_retranscribe,
                force_redownload=args.force_redownload,
                max_batches=args.max_batches, fetch_delay=args.fetch_delay,
                proxy_rotator=metadata_rotator,
                audio_proxy_rotator=audio_rotator,
                transcript_proxy_rotator=transcript_rotator,
                video_delay=args.video_delay,
                socket_timeout=args.socket_timeout, max_retries=args.max_retries,
                max_sentence_duration=args.max_sentence_duration,
                min_sentence_words=args.min_sentence_words,
                run_logger=run_logger, channel_idx=i, total_channels=len(channels),
                metadata_only=args.metadata_only,
                rebuild_from_transcripts=args.rebuild_from_transcripts,
            )
        except Exception as e:
            print(f"  ERROR processing {ch_url}: {e}")
            run_logger.log(f"ERROR: {ch_url}: {e}")
            res = {"channel": ch_url, "status": "error", "error": str(e)}
        all_results.append(res)
        # Ghi summary giữa các kênh (để crash giữa chừng vẫn còn dữ liệu)
        summary_path = output_root / f"_multi_channel_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_path, "w", encoding="utf_8") as f:
            json.dump({"results": all_results}, f, ensure_ascii=False, indent=2)

    success_n = sum(1 for r in all_results if r.get("status") in (
        "success", "rebuild_success", "metadata_only_success"))
    failed_n = len(all_results) - success_n
    run_logger.log_batch_end(len(channels), success_n, failed_n, all_results)

    # === Cleanup 3 VPN tunnels (độc lập, không ảnh hưởng nhau) ===
    print(f"\n[Cleanup] Disconnect 3 VPN tunnels...")
    for r, name in [(metadata_rotator, "meta"),
                    (audio_rotator, "audio"),
                    (transcript_rotator, "subs")]:
        if r is None:
            continue
        try:
            r.disconnect()
            print(f"  ✓ Tunnel '{INSTANCE_ID}_{name}' đã đóng")
        except Exception as e:
            print(f"  ⚠️  Lỗi disconnect {name}: {e}")


if __name__ == "__main__":
    main()
