#!/usr/bin/env python3
"""
YouTube Researcher - Channel-based (multi-channel loop)
Nhập file txt chứa danh sách URL kênh YouTube (mỗi kênh 1 dòng)
-> lap qua tung kenh -> lay video -> loc -> pipeline (download/transcribe/fix)
-> output tach rieng theo tung kenh

Usage:
    # Loop qua tất cả kênh trong file channels.txt (mặc định)
    python youtube_researcher_sub.py --channels-file ./channels_audio/channels.txt

    # Tuỳ chỉnh
    python youtube_researcher_sub.py --channels-file ./my_channels.txt --output ./datasets --max-results 50
"""

import json
import os
import re
import sys
import time
import random
import threading
import numpy as np
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

# ================= CONFIG =================
YOUTUBE_API_KEY = ""
SONIOX_API_KEY = ""
ANTHROPIC_API_KEY = ""

MINIMAX_MODEL = os.environ.get(
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "MiniMax/MiniMax-M2.7"
)

# Soniox HTTP timeout (giay). Upload audio lon + wait transcription co the can nhieu thoi gian.
# Mac dinh 30s qua ngan -> SSL handshake timeout tren mang cham / nhieu request dong thoi.
SONIOX_TIMEOUT_SEC = float(os.environ.get("SONIOX_TIMEOUT_SEC", "300"))

# So request toi da gui DONG THOI toi Soniox trong pipeline.
# Neu qua nhieu, server se drop SSL handshake -> handshake timeout.
# Dat bang so worker thread cua pipeline (mac dinh 1 -> khong can semaphore).
SONIOX_MAX_CONCURRENCY = int(os.environ.get("SONIOX_MAX_CONCURRENCY", "2"))

# So lan retry khi gap SSL/timeout/connection error
SONIOX_MAX_RETRIES = int(os.environ.get("SONIOX_MAX_RETRIES", "5"))

# ================= FILTER CONFIG =================
FILTER_PUBLISHED_DAYS = 36500
FILTER_MIN_DURATION = 50
FILTER_MAX_DURATION = 10000
FILTER_MIN_VIEW_COUNT = 50
FILTER_MIN_LIKE_COUNT = 0
FILTER_MIN_COMMENT_COUNT = 0

# Global semaphore de gioi han so request DONG THOI toi Soniox.
# SSL handshake timeout thuong xay ra khi gui qua nhieu request song song.
_SONIOX_SEMAPHORE = threading.BoundedSemaphore(value=max(1, SONIOX_MAX_CONCURRENCY))


def _run_in_sem(sem, fn):
    """Chay fn trong semaphore (block neu vuot gioi han). Tra ve ket qua cua fn."""
    with sem:
        return fn()


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


def api_call_with_retry(func, max_retries=3, delay=5, rotator=None):
    """
    Retry API calls on SSL/connection errors.
    Neu co `rotator` (YouTubeKeyRotator), se xoay vong key khi gap quotaExceeded
    (HTTP 403 "quotaExceeded" / "rateLimitExceeded" / "dailyLimitExceeded").
    """
    last_err = None
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            last_err = e
            error_str = str(e).lower()
            is_ssl = any(kw in error_str for kw in [
                'ssl', 'timeout', 'connection', 'reset', 'broken pipe', 'handshake',
            ])
            is_quota = False
            if rotator is not None:
                is_quota = _is_youtube_quota_error(e)
                if is_quota and not rotator.is_exhausted():
                    new_key = rotator.rotate()
                    print(f"  [YouTube] Quota exceeded -> switch to key #{rotator.current_index + 1} "
                          f"({new_key[:8]}...{new_key[-4:]})")
                    continue  # thu lai ngay voi key moi
            if is_ssl and attempt < max_retries - 1:
                wait = delay * (attempt + 1)
                print(f"  Connection error, retrying in {wait}s... (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
            elif not (is_ssl or is_quota):
                raise
            elif attempt >= max_retries - 1:
                raise
    raise last_err


def _is_youtube_quota_error(e: Exception) -> bool:
    """Nhan dien loi YouTube quotaExceeded / rateLimitExceeded / dailyLimitExceeded."""
    msg = str(e).lower()
    if any(kw in msg for kw in [
        "quotaexceeded", "ratelimitexceeded", "dailylimitexceeded",
        "userexceeded", "forbidden", "quota exceeded", "rate limit",
        "daily limit", "quota_limit", "quotalimit",
    ]):
        return True
    # googleapiclient errors co .resp.status == 403
    resp = getattr(e, "resp", None)
    status = getattr(resp, "status", None)
    if status == 403 and ("quota" in msg or "limit" in msg):
        return True
    return False


class YouTubeKeyRotator:
    """
    Xoay vong nhieu YouTube API key khi bi quotaExceeded.

    Thu tu uu tien key:
      1) YOUTUBE_API_KEY
      2) YOUTUBE_API_KEY_1, YOUTUBE_API_KEY_2, ...

    Moi key duoc danh dau "exhausted" khi gap quotaExceeded, se khong thu lai
    trong cung 1 lan chay process (tranh retry nhieu lan tren cung key da hong).
    """

    def __init__(self, keys: list[str]):
        self.keys = [k for k in (keys or []) if k]
        self.exhausted: set[str] = set()
        self.current_index = 0
        self._lock = threading.Lock()

    @classmethod
    def from_env_file(cls, env_path: Path | str) -> "YouTubeKeyRotator":
        """
        Doc .env, lay YOUTUBE_API_KEY + YOUTUBE_API_KEY_1..N.
        Thứ tự ưu tiên: YOUTUBE_API_KEY -> _1 -> _2 -> _3 -> ...
        """
        keys: list[str] = []
        path = Path(env_path)
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Match YOUTUBE_API_KEY= hoặc YOUTUBE_API_KEY_N=
                m = re.match(r"^YOUTUBE_API_KEY(?:_(\d+))?=(.+)$", line)
                if m:
                    suffix, val = m.group(1), m.group(2).strip()
                    val = val.strip('"').strip("'")
                    if val:
                        if suffix is None:
                            keys.insert(0, val)  # YOUTUBE_API_KEY luon dau tien
                        else:
                            keys.append(val)
        # Fallback: env vars
        if not keys:
            base = os.environ.get("YOUTUBE_API_KEY")
            if base:
                keys.append(base)
            i = 1
            while True:
                k = os.environ.get(f"YOUTUBE_API_KEY_{i}")
                if not k:
                    break
                keys.append(k)
                i += 1
        return cls(keys)

    def __len__(self) -> int:
        return len(self.keys)

    def is_empty(self) -> bool:
        return len(self.keys) == 0

    def is_exhausted(self) -> bool:
        """True neu TAT CA key da bi exhausted."""
        return len(self.exhausted) >= len(self.keys)

    def current_key(self) -> Optional[str]:
        if not self.keys:
            return None
        if self.current_index >= len(self.keys):
            self.current_index = 0
        key = self.keys[self.current_index]
        if key in self.exhausted:
            return None
        return key

    def build(self):
        """Tao googleapiclient YouTube client voi key hien tai."""
        from googleapiclient.discovery import build
        k = self.current_key()
        if not k:
            raise RuntimeError("YouTube: khong con key nao kha dung (tat ca da exhausted)")
        return build("youtube", "v3", developerKey=k)

    def mark_exhausted(self, key: str):
        with self._lock:
            self.exhausted.add(key)
        print(f"  [YouTube] Key {key[:8]}...{key[-4:]} da bi danh dau exhausted "
              f"({len(self.exhausted)}/{len(self.keys)} keys)")

    def rotate(self) -> Optional[str]:
        """
        Chuyen sang key tiep theo chua bi exhausted.
        Tra ve key moi, hoac None neu khong con.
        """
        with self._lock:
            if self.is_exhausted():
                return None
            n = len(self.keys)
            for _ in range(n):
                self.current_index = (self.current_index + 1) % n
                cand = self.keys[self.current_index]
                if cand not in self.exhausted:
                    return cand
            return None

    def execute_with_retry(self, request_factory, label: str = ""):
        """
        Thuc thi 1 googleapiclient request voi key hien tai.
        Khi gap quotaExceeded -> mark exhausted, rotate, retry voi key moi.

        Args:
            request_factory: callable nhan (youtube_client) -> request object
            label: ten de log (vd: "playlistItems.list")
        """
        if self.is_empty():
            raise RuntimeError("YouTube: chua co API key nao")

        last_err = None
        for attempt in range(len(self.keys) + 1):  # +1 de con luot cho moi key
            try:
                youtube = self.build()
                req = request_factory(youtube)
                return req.execute()
            except Exception as e:
                last_err = e
                if not _is_youtube_quota_error(e):
                    raise
                cur = self.current_key()
                if cur:
                    self.mark_exhausted(cur)
                if self.is_exhausted():
                    print(f"  [YouTube] TAT CA {len(self.keys)} key da exhausted -> dung")
                    raise
                new_key = self.rotate()
                if not new_key:
                    raise
                tag = f" [{label}]" if label else ""
                print(f"  [YouTube]{tag} Quota exceeded -> switch to key "
                      f"#{self.current_index + 1} ({new_key[:8]}...{new_key[-4:]})")
        raise last_err


def resolve_channel_id(rotator, channel_input: str) -> Optional[str]:
    """
    Resolve channel URL/handle/ID to channel ID.

    Supports:
        - https://www.youtube.com/@ChannelHandle
        - https://www.youtube.com/channel/UCxxxxx
        - https://www.youtube.com/c/ChannelName
        - UCxxxxx (direct channel ID)

    Args:
        rotator: YouTubeKeyRotator (hoac 1 api_key string) - cung cap youtube client
                 va tu xoay vong key khi bi quotaExceeded.
    """
    from googleapiclient.discovery import build

    # Backward-compat: neu truyen string -> dung nhu 1 key co dinh
    if isinstance(rotator, str):
        api_key = rotator
        def get_youtube(k=api_key):
            return build("youtube", "v3", developerKey=k)
    else:
        def get_youtube():
            return rotator.build()

    # Direct channel ID
    if channel_input.startswith("UC") and len(channel_input) == 24:
        return channel_input

    # Extract from URL
    url = channel_input.strip().rstrip("/")

    # Handle: @ChannelName
    handle_match = re.search(r'youtube\.com/@([^/\s?]+)', url)
    if handle_match:
        handle = handle_match.group(1)
        youtube = get_youtube()
        response = api_call_with_retry(
            lambda: youtube.channels().list(part="id", forHandle=handle).execute(),
            rotator=rotator if not isinstance(rotator, str) else None,
        )
        items = response.get("items", [])
        if items:
            return items[0]["id"]

    # /channel/UCxxxxx
    channel_match = re.search(r'youtube\.com/channel/([^/\s?]+)', url)
    if channel_match:
        return channel_match.group(1)

    # /c/ChannelName or /user/Username
    name_match = re.search(r'youtube\.com/(?:c|user)/([^/\s?]+)', url)
    if name_match:
        name = name_match.group(1)
        youtube = get_youtube()
        response = api_call_with_retry(
            lambda: youtube.channels().list(part="id", forUsername=name).execute(),
            rotator=rotator if not isinstance(rotator, str) else None,
        )
        items = response.get("items", [])
        if items:
            return items[0]["id"]

    # Last resort: treat as search query for channel (uses search - 100 units)
    youtube = get_youtube()
    response = api_call_with_retry(
        lambda: youtube.search().list(
            part="snippet", q=channel_input, type="channel", maxResults=1,
        ).execute(),
        rotator=rotator if not isinstance(rotator, str) else None,
    )
    items = response.get("items", [])
    if items:
        return items[0]["snippet"]["channelId"]

    return None

    # Direct channel ID
    if channel_input.startswith("UC") and len(channel_input) == 24:
        return channel_input

    # Extract from URL
    url = channel_input.strip().rstrip("/")

    # Handle: @ChannelName
    handle_match = re.search(r'youtube\.com/@([^/\s?]+)', url)
    if handle_match:
        handle = handle_match.group(1)
        response = youtube.channels().list(
            part="id",
            forHandle=handle,
        ).execute()
        items = response.get("items", [])
        if items:
            return items[0]["id"]

    # /channel/UCxxxxx
    channel_match = re.search(r'youtube\.com/channel/([^/\s?]+)', url)
    if channel_match:
        return channel_match.group(1)

    # /c/ChannelName or /user/Username
    name_match = re.search(r'youtube\.com/(?:c|user)/([^/\s?]+)', url)
    if name_match:
        name = name_match.group(1)
        response = youtube.channels().list(
            part="id",
            forUsername=name,
        ).execute()
        items = response.get("items", [])
        if items:
            return items[0]["id"]

    # Last resort: treat as search query for channel (uses search - 100 units)
    response = youtube.search().list(
        part="snippet",
        q=channel_input,
        type="channel",
        maxResults=1,
    ).execute()
    items = response.get("items", [])
    if items:
        return items[0]["snippet"]["channelId"]

    return None


class YouTubeResearcher:
    """
    YouTube Researcher - Lấy video theo kênh (channel URL)
    Thay vì search keyword, nhập URL kênh YouTube -> lấy video -> lọc -> pipeline
    """

    def __init__(self, api_key=None, output_dir: str = "./researched_videos", key_rotator=None):
        """
        Args:
            api_key: 1 key don le (backward-compat) - se duoc boc thanh 1-key rotator
            output_dir: thu muc output
            key_rotator: YouTubeKeyRotator - uu tien dung de xoay vong khi quota
        """
        if key_rotator is not None:
            self.rotator = key_rotator
            self.api_key = key_rotator.current_key() or ""
        elif api_key:
            # Backward-compat: 1 key -> 1-key rotator
            self.rotator = YouTubeKeyRotator([api_key])
            self.api_key = api_key
        else:
            self.rotator = YouTubeKeyRotator([])
            self.api_key = ""

        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._videos: list[VideoCandidate] = []
        self._filtered_videos: list[VideoCandidate] = []

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
    def find_transcription_json(transcription_dir, video, audio_filename: str = "",
                                  search_all_runs: bool = False) -> "Path | None":
        """
        Tim file JSON bản dịch theo nhieu pattern:
        1. {audio_stem}_transcription.json (pattern moi, ten theo audio/title)
        2. {video_id}_transcription.json (pattern cu, backward-compat)

        Args:
            transcription_dir: folder transcriptions (uu tien folder run_timestamp cu the).
            search_all_runs: neu True va khong tim thay trong transcription_dir, quet them
                cac subfolder transcriptions/<other_timestamp>/ trong parent directory
                (de tim JSON cu khi run hien tai chua co).
        Tra ve Path neu tim thay, None neu khong.
        """
        if not transcription_dir:
            return None
        td = Path(transcription_dir)
        candidates = []
        if audio_filename:
            stem = Path(audio_filename).stem
            candidates.append(td / f"{stem}_transcription.json")
        if getattr(video, "video_id", None):
            candidates.append(td / f"{video.video_id}_transcription.json")
        for c in candidates:
            if c.exists():
                return c

        if not search_all_runs:
            return None

        # Quet cac run_timestamp khac trong parent (transcriptions/)
        parent = td.parent if td.name else td
        if not parent.exists():
            return None
        for sub in sorted(parent.iterdir(), reverse=True):  # moi nhat truoc
            if not sub.is_dir() or sub == td:
                continue
            for c in candidates:
                alt = sub / c.name
                if alt.exists():
                    return alt
        return None

    @staticmethod
    def find_existing_audio(audio_root, video, target_filename: str = "") -> "Path | None":
        """
        Quet TAT CA subfolder audio/<timestamp>/ trong audio_root, tim file audio
        tuong ung voi video. Tra ve Path dau tien tim duoc (uu tien subfolder moi nhat).

        Pattern duoc check (theo thu tu):
        1. {target_filename} (vd: "Tieu_De.wav")
        2. {stem_target}_{video_id}.wav  (truong hop 2 video trung ten)
        3. {video_id}.wav                 (pattern cu, backward-compat)
        """
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

        # Quet subfolder moi nhat truoc (uu tien dung lai file gan nhat)
        subdirs = sorted([d for d in root.iterdir() if d.is_dir()], reverse=True)
        for sub in subdirs:
            for name in candidates:
                p = sub / name
                if p.exists():
                    return p
        return None

    @staticmethod
    def _build_audio_index(audio_root) -> dict:
        """
        Quet 1 LAN TAT CA subfolder audio/<timestamp>/, tra ve dict:
            {basename_no_ext: Path}

        Moi entry duoc uu tien tu subfolder moi nhat (newest first).
        Dung de lookup O(1) thay vi phai duyet subdirs cho moi video.

        Key = stem cua file (vd: "Title_safe", "video_id", "Title_safe_video_id").
        Chi index cac file audio co extension hop le.
        """
        index: dict = {}
        if not audio_root:
            return index
        root = Path(audio_root)
        if not root.exists():
            return index

        audio_exts = {".wav", ".m4a", ".mp3", ".flac", ".opus", ".ogg", ".webm"}
        # Moi nhat truoc: skip se uu tien file o run hien tai hon run cu
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
        self, audio_root, transcriptions_root, skip_downloaded: bool,
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
        if not skip_downloaded:
            # Che do --no-skip-downloaded: download moi thu tu dau -> tat ca vao Bucket C
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
            # Logic giong find_transcription_json + check stem
            # (json.stem phai bang f"{audio_path.stem}_transcription" de dam bao
            #  JSON nay sinh ra tu chinh file audio nay, tranh sai khi title bi doi ten)
            json_path = None
            if audio_path:
                expected_json_stem = f"{audio_path.stem}_transcription"
                for key in [audio_path.stem, video.video_id]:
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

    def fetch_channel_videos(
        self,
        channel_input: str,
        max_results: int = 800,
        order: str = "date",
        published_after: Optional[datetime] = None,
    ) -> list[VideoCandidate]:
        """
        Lấy video từ kênh YouTube.

        Args:
            channel_input: URL kênh hoặc channel ID
                - https://www.youtube.com/@TenKenh
                - https://www.youtube.com/channel/UCxxxxx
                - UCxxxxx
            max_results: Số video tối đa
            order: date | viewCount | rating | relevance
            published_after: Chỉ lấy video sau ngày này
        """
        try:
            from googleapiclient.discovery import build
        except ImportError:
            print("pip install google-api-python-client")
            sys.exit(1)

        # Resolve channel ID (rotator co the xoay vong key neu quota)
        channel_id = resolve_channel_id(self.rotator, channel_input)
        if not channel_id:
            print(f"Khong tim thay kenh: {channel_input}")
            return []

        print(f"Channel ID: {channel_id}")

        # Fetch videos from channel using playlistItems (1 unit/request vs 100 for search)
        uploads_playlist_id = "UU" + channel_id[2:]
        print(f"Uploads playlist: {uploads_playlist_id}")

        # Paginate playlistItems
        video_ids = []
        page_token = None
        fetched = 0

        rotator = self.rotator

        while fetched < max_results:
            pl_params = {
                "playlistId": uploads_playlist_id,
                "part": "contentDetails,snippet",
                "maxResults": min(max_results - fetched, 50),
            }
            if page_token:
                pl_params["pageToken"] = page_token

            try:
                response = rotator.execute_with_retry(
                    lambda y, p=pl_params: y.playlistItems().list(**p),
                    label="playlistItems.list",
                )
            except Exception as e:
                err = str(e).lower()
                if "ssl" in err or "timeout" in err or "connection" in err or "handshake" in err:
                    # Loi SSL/connection -> retry voi cung key 3 lan
                    response = api_call_with_retry(
                        lambda p=pl_params: rotator.build().playlistItems().list(**p).execute(),
                        max_retries=3, delay=3, rotator=rotator,
                    )
                else:
                    raise

            for item in response.get("items", []):
                vid_id = item["contentDetails"]["videoId"]
                pub_date = item["contentDetails"].get("videoPublishedAt") or item["snippet"].get("publishedAt", "")

                if published_after and pub_date:
                    from datetime import timezone
                    video_pub = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                    cutoff = published_after.replace(tzinfo=timezone.utc) if published_after.tzinfo is None else published_after
                    if video_pub < cutoff:
                        continue

                video_ids.append(vid_id)
                fetched += 1
                if fetched >= max_results:
                    break

            page_token = response.get("nextPageToken")
            if not page_token:
                break

        if not video_ids:
            print("Khong tim thay video nao trong kenh nay")
            return []

        # Get detailed info in batches of 50
        self._videos = []

        for i in range(0, len(video_ids), 50):
            batch = video_ids[i:i+50]

            try:
                detail_response = rotator.execute_with_retry(
                    lambda y, b=batch: y.videos().list(
                        part="snippet,contentDetails,statistics,status,topicDetails",
                        id=",".join(b),
                    ),
                    label="videos.list",
                )
            except Exception as e:
                err = str(e).lower()
                if "ssl" in err or "timeout" in err or "connection" in err or "handshake" in err:
                    detail_response = api_call_with_retry(
                        lambda b=batch: rotator.build().videos().list(
                            part="snippet,contentDetails,statistics,status,topicDetails",
                            id=",".join(b),
                        ).execute(),
                        max_retries=3, delay=3, rotator=rotator,
                    )
                else:
                    raise

            for item in detail_response.get("items", []):
                stats = item.get("statistics", {})
                content = item.get("contentDetails", {})
                snippet = item.get("snippet", {})
                status = item.get("status", {})
                topic = item.get("topicDetails", {})

                video = VideoCandidate(
                    video_id=item["id"],
                    title=snippet.get("title", ""),
                    channel=snippet.get("channelTitle", ""),
                    description=snippet.get("description", ""),
                    published_at=snippet.get("publishedAt", ""),
                    duration=content.get("duration", ""),
                    view_count=int(stats.get("viewCount", 0)),
                    like_count=int(stats.get("likeCount", 0)),
                    comment_count=int(stats.get("commentCount", 0)),
                    url=f"https://www.youtube.com/watch?v={item['id']}",
                    tags=snippet.get("tags", []),
                    category_id=snippet.get("categoryId", ""),
                    default_language=snippet.get("defaultLanguage", ""),
                    default_audio_language=snippet.get("defaultAudioLanguage", ""),
                    caption_available=content.get("caption", "false") == "true",
                    definition=content.get("definition", ""),
                    dimension=content.get("dimension", ""),
                    licensed_content=content.get("licensedContent", False),
                    projection=content.get("projection", ""),
                    privacy_status=status.get("privacyStatus", ""),
                    embeddable=status.get("embeddable", True),
                    made_for_kids=status.get("madeForKids", False),
                    live_broadcast_content=snippet.get("liveBroadcastContent", ""),
                    topic_categories=topic.get("topicCategories", []),
                )

                thumbs = snippet.get("thumbnails", {})
                if "high" in thumbs:
                    video.thumbnail = thumbs["high"].get("url", "")
                elif "medium" in thumbs:
                    video.thumbnail = thumbs["medium"].get("url", "")

                try:
                    items = self._fetch_top_comments(video.video_id, max_comments=5)
                    video.top_comments = [
                        it["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                        for it in items
                    ]
                except Exception:
                    pass

                self._videos.append(video)

        print(f"Tim thay {len(self._videos)} video tu kenh '{self._videos[0].channel if self._videos else channel_input}'")
        return self._videos

    # ================= COMMENTS =================

    def _fetch_top_comments(self, video_id, max_comments=5):
        try:
            rotator = self.rotator
            return rotator.execute_with_retry(
                lambda y: y.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=max_comments,
                    textFormat="plainText",
                ),
                label="commentThreads.list",
            ).get("items", [])
        except Exception:
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

    # ================= SONIOX =================

    def _get_soniox_key(self):
        global SONIOX_API_KEY
        if SONIOX_API_KEY:
            return SONIOX_API_KEY

        # Try .env next to this script first
        for path in [Path(__file__).parent / ".env"]:
            if path.exists():
                for line in path.read_text().splitlines():
                    if line.startswith("SONIOX_API_KEY="):
                        SONIOX_API_KEY = line.split("=", 1)[1].strip()
                        return SONIOX_API_KEY

        import os
        return os.environ.get("SONIOX_API_KEY")

    def transcribe_with_soniox(self, audio_path: Path, lang: list = None):
        """
        Transcribe audio with Soniox.
        Tra ve cac utterance (cau thoai) cua nguoi noi, KHONG luu token-level.
        Moi segment = mot utterance ket thuc bang dau cau (., ?, !) hoac theo speaker.

        Tu dong xu ly khi Soniox bao loi quota/file tran (vd: "file limit reached",
        "too many files", "quota", "storage"). Trong truong hop do:
          1) Chay soniox_cleanup.py de xoa het transcriptions + files cu.
          2) Retry 1 lan cho transcription nay.
        """
        if lang is None:
            lang = ["vi"]

        key = self._get_soniox_key()
        if not key:
            print("SONIOX_API_KEY not found")
            return None

        try:
            from soniox.client import SonioxClient
            from soniox.types import CreateTranscriptionConfig
            import soundfile as sf

            # Tang timeout (mac dinh 30s qua ngan cho upload file lon)
            client = SonioxClient(api_key=key, timeout_sec=SONIOX_TIMEOUT_SEC)
            y, sr = sf.read(str(audio_path))
            audio_duration = round(len(y) / sr, 3)

            # Wrapper co retry + jitter + SSL/timeout handling.
            def _do_with_retry(label, fn):
                last_err = None
                for attempt in range(1, SONIOX_MAX_RETRIES + 1):
                    try:
                        return fn()
                    except Exception as e:
                        last_err = e
                        msg = str(e).lower()
                        is_net = any(kw in msg for kw in [
                            "ssl", "timeout", "handshake", "connection",
                            "reset", "broken pipe", "eof", "read",
                        ])
                        if not is_net or attempt >= SONIOX_MAX_RETRIES:
                            raise
                        # Exponential backoff co jitter: 2,4,8,16,32s + 0-2s
                        backoff = (2 ** (attempt - 1)) + random.uniform(0, 2)
                        print(f"  [Soniox] {label} fail (attempt {attempt}/{SONIOX_MAX_RETRIES}): {e}")
                        print(f"  [Soniox] retry in {backoff:.1f}s ...")
                        time.sleep(backoff)
                raise last_err

            # Heuristic: nhung loi lien quan toi quota/storage/file limit
            QUOTA_KEYWORDS = (
                "quota", "limit", "too many", "file limit", "storage",
                "max files", "max file", "exceeded", "capacity",
                "file count", "too many files", "no space",
            )

            def _is_quota_error(e: Exception) -> bool:
                msg = str(e).lower()
                # Ket hop ca response body (neu co) + message
                extra = ""
                resp = getattr(e, "response", None)
                if resp is not None:
                    try:
                        extra = (resp.text or "").lower()
                    except Exception:
                        pass
                haystack = msg + " " + extra
                return any(kw in haystack for kw in QUOTA_KEYWORDS)

            def _run_soniox_cleanup():
                """Chay soniox_cleanup.py de xoa transcriptions + files cu."""
                cleanup_script = Path(__file__).parent / "soniox_cleanup.py"
                if not cleanup_script.exists():
                    print(f"  [Soniox] Khong tim thay {cleanup_script}, skip cleanup")
                    return False
                print(f"  [Soniox] Chay {cleanup_script.name} de giai phong quota...")
                try:
                    import subprocess
                    env = os.environ.copy()
                    # Truyen SONIOX_API_KEY cho subprocess neu co
                    if SONIOX_API_KEY and "SONIOX_API_KEY" not in env:
                        env["SONIOX_API_KEY"] = SONIOX_API_KEY
                    res = subprocess.run(
                        [sys.executable, str(cleanup_script)],
                        env=env,
                        capture_output=True,
                        text=True,
                        timeout=300,
                    )
                    last_lines = "\n".join(
                        (res.stdout or "").strip().splitlines()[-5:]
                    )
                    print(f"  [Soniox] cleanup exit={res.returncode}; tail:\n{last_lines}")
                    return res.returncode == 0
                except Exception as ce:
                    print(f"  [Soniox] cleanup loi: {ce}")
                    return False

            # Gioi han so request dong thoi (nhieu qua se gay SSL handshake timeout)
            sem = _SONIOX_SEMAPHORE

            quota_retried = False  # da cleanup + retry 1 lan hay chua

            def _do_transcription():
                nonlocal quota_retried
                uploaded = _do_with_retry(
                    "files.upload",
                    lambda: _run_in_sem(sem, lambda: client.files.upload(str(audio_path))),
                )
                try:
                    config = CreateTranscriptionConfig(
                        model="stt-async-v4",
                        language_hints=lang,
                        enable_language_identification=True,
                        enable_speaker_diarization=True,
                    )
                    job = _do_with_retry(
                        "stt.create",
                        lambda: _run_in_sem(sem, lambda: client.stt.create(config=config, file_id=uploaded.id)),
                    )
                    _do_with_retry(
                        "stt.wait",
                        lambda: _run_in_sem(sem, lambda: client.stt.wait(job.id)),
                    )
                    result = _do_with_retry(
                        "stt.get_transcript",
                        lambda: _run_in_sem(sem, lambda: client.stt.get_transcript(job.id)),
                    )
                finally:
                    # Xóa file trên Soniox sau khi transcribe xong để tránh vượt quota
                    try:
                        _run_in_sem(sem, lambda: client.files.delete_if_exists(uploaded.id))
                    except Exception:
                        pass
                return result

            try:
                result = _do_transcription()
            except Exception as quota_err:
                # Neu loi quota va chua retry -> cleanup + thu lai 1 lan
                if _is_quota_error(quota_err) and not quota_retried:
                    quota_retried = True
                    print(f"  [Soniox] Loi quota/file: {quota_err}")
                    cleaned = _run_soniox_cleanup()
                    if cleaned:
                        # Doi them vai giay de server dong nhat
                        time.sleep(2)
                        print("  [Soniox] Retry transcription sau cleanup...")
                        result = _do_transcription()
                    else:
                        raise
                else:
                    raise

            # Gom token theo (speaker, dau ket thuc cau) -> tao utterance
            sentences_by_speaker = []  # list of {speaker, start, end, text, tokens}
            current = None  # dict dang xay dung

            for t in result.tokens:
                token_text = getattr(t, "text", "")
                speaker = getattr(t, "speaker", "SPEAKER_00")
                # Soniox Token dung start_ms / end_ms (millisecond, KHONG phai start_time/end_time)
                start = float(t.start_ms) / 1000.0 if t.start_ms is not None else 0.0
                end = float(t.end_ms) / 1000.0 if t.end_ms is not None else start

                # Khoi tao utterance moi khi doi speaker
                if current is None or current["speaker"] != speaker:
                    if current is not None:
                        sentences_by_speaker.append(current)
                    current = {
                        "speaker": speaker,
                        "start": start,
                        "end": end,
                        "text": token_text,
                        "first_token": t,
                        "has_sentence_end": False,
                    }
                else:
                    current["end"] = end
                    current["text"] += token_text
                    current["last_token"] = t

                # Kiem tra token nay co dau ket thuc cau khong
                # Soniox tokens bao gom ca khoang trang/dau cau rieng le
                stripped = token_text.strip()
                if stripped and stripped[-1] in ".?!":
                    current["has_sentence_end"] = True
                    sentences_by_speaker.append(current)
                    current = None
                # Token chi la dau ket thuc (khoang trang truoc do da flush)
                elif not stripped and current is not None and current.get("has_sentence_end"):
                    # Bo qua, da duoc flush o tren
                    pass

            if current is not None:
                sentences_by_speaker.append(current)

            # Build segments tu cac utterance
            segments = []
            for u in sentences_by_speaker:
                seg = self._build_utterance(u)
                if seg and seg.get("text"):
                    segments.append(seg)

            # Detect languages neu co
            detected_languages = set()
            for u in sentences_by_speaker:
                if "first_token" in u and hasattr(u["first_token"], "language"):
                    lang_token = u["first_token"].language
                    if lang_token:
                        detected_languages.add(lang_token)

            return {
                "segments": segments,
                "audio_duration": audio_duration,
                "detected_languages": sorted(detected_languages),
            }
        except Exception as e:
            msg = str(e).lower()
            if any(kw in msg for kw in ["ssl", "handshake", "timeout"]):
                print(f"Soniox failed: {e}")
                print(f"  -> Goi y: tang SONIOX_TIMEOUT_SEC (hien tai {SONIOX_TIMEOUT_SEC}s) "
                      f"hoac giam SONIOX_MAX_CONCURRENCY (hien tai {SONIOX_MAX_CONCURRENCY}).")
            else:
                print(f"Soniox failed: {e}")
            return None

    def _build_utterance(self, utterance: dict) -> dict:
        """Build utterance segment dict voi cac truong toi gian."""
        text = (utterance.get("text") or "").strip()
        if not text:
            return None
        return {
            "start": round(float(utterance["start"]), 3),
            "end": round(float(utterance["end"]), 3),
            "speaker": utterance["speaker"],
            "text": text,
        }

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
        global ANTHROPIC_API_KEY
        if ANTHROPIC_API_KEY:
            return ANTHROPIC_API_KEY

        env_file = Path(__file__).parent / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("ANTHROPIC_API_KEY="):
                    ANTHROPIC_API_KEY = line.split("=", 1)[1].strip()
                    return ANTHROPIC_API_KEY

        return os.environ.get("ANTHROPIC_API_KEY")

    def fix_proper_nouns_minimax(self, segments: list, video_title: str = "") -> list:
        """Fix proper nouns (names, products, locations) using MiniMax"""
        key = self._get_anthropic_key()
        if not key:
            return segments

        try:
            from anthropic import Anthropic
        except ImportError:
            return segments

        client = Anthropic(api_key=key)

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

        try:
            response = client.messages.create(
                model=MINIMAX_MODEL,
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = response.content[0].text

            json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group(1))
            else:
                json_match = re.search(r'\{[\s\S]*\}', response_text)
                if json_match:
                    result = json.loads(json_match.group(0))
                else:
                    return segments

            fixed_segments = result.get("fixed_segments", [])
            if fixed_segments:
                corrections = result.get("corrections", [])
                print(f"  Fixed {len(corrections)} proper nouns")
                return fixed_segments

            return segments

        except Exception as e:
            print(f"  MiniMax fix failed: {e}")
            return segments

    # ================= PIPELINE =================

    def process_videos_pipeline(
        self,
        output_dir: str = "./youtube_dataset",
        keep_videos: bool = False,
        fix_names: bool = True,
        audio_format: str = "m4a",
        run_timestamp: str = "",
        skip_downloaded: bool = True,
        run_logger: "RunLogger | None" = None,
        channel_idx: int = 0,
        total_channels: int = 0,
    ) -> dict:
        output_dir = Path(output_dir)
        # Dùng subfolder có timestamp để mỗi lần chạy tạo folder riêng
        # audio/{timestamp}/ và transcriptions/{timestamp}/
        if not run_timestamp:
            run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        audio_dir = output_dir / "audio" / run_timestamp
        transcriptions_dir = output_dir / "transcriptions" / run_timestamp
        audio_dir.mkdir(parents=True, exist_ok=True)
        transcriptions_dir.mkdir(parents=True, exist_ok=True)

        def _log(msg, also_print=True):
            """Helper: log vao run_logger neu co, neu khong chi print."""
            if run_logger:
                run_logger.log(msg, also_print=also_print)
            elif also_print:
                print(msg)

        results = []

        try:
            import yt_dlp
        except ImportError:
            print("pip install yt-dlp")
            return {"total": 0, "results": []}

        # === Pre-partition: 1 disk scan -> 3 bucket (A: skip, B: audio-only, C: full) ===
        # Bucket A = skip nhanh nhat (khong I/O), xu ly dau tien
        # Bucket B = da co audio, chi can transcribe
        # Bucket C = chua co audio, phai download + transcribe
        bucket_a, bucket_b, bucket_c = self._partition_videos_for_pipeline(
            audio_root=audio_dir.parent,
            transcriptions_root=transcriptions_dir.parent,
            skip_downloaded=skip_downloaded,
        )

        total = len(self._filtered_videos)
        _log(f"Pipeline partition (total={total}):")
        _log(f"  Bucket A (audio+json da co, SKIP)         : {len(bucket_a)}")
        _log(f"  Bucket B (co audio, chua co json)         : {len(bucket_b)}")
        _log(f"  Bucket C (chua co audio, can download)   : {len(bucket_c)}")

        # ============================================================
        # BUCKET A: co ca audio + json -> SKIP nhanh (khong I/O)
        # ============================================================
        # Xu ly truoc de khong bi lan voi download/soniox
        for i, (video, audio_path, json_path) in enumerate(bucket_a, 1):
            audio_filename = audio_path.name
            print(f"\n[A-{i}/{len(bucket_a)}] {video.title[:60]}")
            print(f"  [SKIP] audio + JSON đã có sẵn "
                  f"(audio: {audio_path.parent.name}/{audio_filename}, "
                  f"json: {json_path.parent.name}/{json_path.name})")
            _log(f"[A-{i}/{len(bucket_a)}] {video.video_id} | {video.title[:50]} "
                 f"-> SKIP (audio: {audio_filename}, json: {json_path.name})",
                 also_print=False)
            video.audio_filename = audio_filename
            results.append({
                "video_id": video.video_id,
                "title": video.title,
                "status": "skipped_already_done",
                "audio_filename": audio_filename,
                "transcription_filename": json_path.name,
                "audio_downloaded_at": None,
                "transcribed_at": None,
            })

        # ============================================================
        # BUCKET B: co audio, chua co json -> chi transcribe
        # ============================================================
        for i, (video, audio_path, audio_filename) in enumerate(bucket_b, 1):
            print(f"\n[B-{i}/{len(bucket_b)}] {video.title[:60]}")
            print(f"  [SKIP-DOWNLOAD] audio có sẵn ở "
                  f"{audio_path.parent.name}/{audio_filename}, chạy Soniox + LLM fix...")
            _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | {video.title[:50]} "
                 f"-> transcribe-only (audio: {audio_filename})",
                 also_print=False)

            # Ten file JSON cung theo ten audio (de JSON + audio + CSV dong nhat)
            json_stem = Path(audio_filename).stem  # bo .wav
            json_path = transcriptions_dir / f"{json_stem}_transcription.json"

            try:
                result = self.transcribe_with_soniox(audio_path, lang=["vi"])
            except Exception as e:
                _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | Soniox loi: {e}")
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "transcription_failed",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": None,
                    "error": str(e),
                })
                continue

            if result:
                segments = result["segments"]

                # Fix proper nouns with MiniMax
                if fix_names and segments:
                    print("  Fixing proper nouns...")
                    segments = self.fix_proper_nouns_minimax(segments, video_title=video.title)

                self._save_transcription(
                    output_path=json_path,
                    segments=segments,
                    video=video,
                    audio_duration=result["audio_duration"],
                    audio_filename=audio_filename,
                )
                # Gan ten audio vao video de CSV dung chung
                video.audio_filename = audio_filename
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "success",
                    "audio_filename": audio_filename,
                    "transcription_filename": f"{json_stem}_transcription.json",
                    "audio_downloaded_at": None,  # da co tu truoc, khong phai download hom nay
                    "transcribed_at": datetime.now().isoformat(),
                })
                print("  Done (transcribe-only)")
                _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | DONE transcribe",
                     also_print=False)
            else:
                _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | transcription FAILED",
                     also_print=False)
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "transcription_failed",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": None,
                })
            # Khong xoa audio vi no da co san (o run cu), khong thuoc quyen quan ly

        # ============================================================
        # BUCKET C: chua co audio -> download + transcribe + fix
        # ============================================================
        for i, (video, target_name, target_filename) in enumerate(bucket_c, 1):
            print(f"\n[C-{i}/{len(bucket_c)}] {video.title[:60]}")
            _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | {video.title[:50]} "
                 f"-> download + transcribe", also_print=False)

            audio_path = None
            audio_filename = None
            download_skipped = False

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
                'postprocessor_args': [
                    '-ar', '16000',
                    '-ac', '1',
                ],
            }

            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(video.url, download=True)
                    filename = ydl.prepare_filename(info)
                    audio_path = Path(filename)
                    # If postprocessor changed extension (e.g. .webm -> .wav), use the new file
                    if not audio_path.exists():
                        stem = audio_path.with_suffix("")
                        for ext in [".wav", ".m4a", ".mp3", ".flac", ".opus", ".ogg"]:
                            candidate = stem.with_suffix(ext)
                            if candidate.exists():
                                audio_path = candidate
                                break

                # Sau khi download xong, doi ten file theo TIEU DE video
                # de JSON / CSV / file tren disk cung 1 ten (la tieu de)
                target_ext = audio_path.suffix if audio_path else ".wav"
                target_filename = f"{target_name}{target_ext}"
                target_path = audio_dir / target_filename

                if audio_path and audio_path.exists() and audio_path != target_path:
                    # Tranh ghi de file dang ton tai (2 video trung ten)
                    if target_path.exists():
                        target_path = audio_dir / f"{target_name}_{video.video_id}{target_ext}"
                        target_filename = target_path.name
                    try:
                        audio_path.rename(target_path)
                        audio_path = target_path
                    except Exception as e:
                        print(f"  Rename failed ({e}), giu ten goc")

                # Ten file audio canonical (la tieu de, dong nhat giua disk / json / csv)
                audio_filename = audio_path.name if audio_path else f"{target_name}{target_ext}"
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | download OK: {audio_filename}",
                     also_print=False)
            except Exception as e:
                print(f"  Download failed: {e}")
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | DOWNLOAD FAILED: {e}")
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "download_failed",
                    "audio_filename": None,
                    "audio_downloaded_at": datetime.now().isoformat(),
                    "error": str(e),
                })
                continue

            try:
                result = self.transcribe_with_soniox(audio_path, lang=["vi"])
            except Exception as e:
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | Soniox loi: {e}")
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "transcription_failed",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": datetime.now().isoformat(),
                    "error": str(e),
                })
                continue

            if result:
                segments = result["segments"]

                # Fix proper nouns with MiniMax
                if fix_names and segments:
                    print("  Fixing proper nouns...")
                    try:
                        segments = self.fix_proper_nouns_minimax(segments, video_title=video.title)
                    except Exception as e:
                        _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | LLM fix loi (tiep tuc voi segments goc): {e}",
                             also_print=False)

                # Ten file JSON cung theo ten audio (de JSON + audio + CSV dong nhat)
                json_stem = Path(audio_filename).stem  # bo .wav
                json_path = transcriptions_dir / f"{json_stem}_transcription.json"
                self._save_transcription(
                    output_path=json_path,
                    segments=segments,
                    video=video,
                    audio_duration=result["audio_duration"],
                    audio_filename=audio_filename,
                )
                # Gan ten audio vao video de CSV dung chung
                video.audio_filename = audio_filename
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "success",
                    "audio_filename": audio_filename,
                    "transcription_filename": f"{json_stem}_transcription.json",
                    "audio_downloaded_at": datetime.now().isoformat(),
                    "transcribed_at": datetime.now().isoformat(),
                })
                print("  Done")
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | DONE (audio: {audio_filename})",
                     also_print=False)
            else:
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | transcription FAILED "
                     f"(audio: {audio_filename})", also_print=False)
                results.append({
                    "video_id": video.video_id,
                    "title": video.title,
                    "status": "transcription_failed",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": datetime.now().isoformat(),
                })

            if not keep_videos and audio_path and audio_path.exists():
                try:
                    audio_path.unlink()
                except Exception:
                    pass

        success = sum(1 for r in results if r.get("status") in ("success", "skipped_already_done"))
        failed = [r for r in results if r.get("status") not in ("success", "skipped_already_done")]
        _log(f"\nPipeline channel {channel_idx}/{total_channels}: "
             f"{success}/{len(self._filtered_videos)} thanh cong, "
             f"{len(failed)} loi")
        if failed:
            _log(f"Cac video loi trong kenh nay:")
            for r in failed:
                _log(f"  - [{r.get('status')}] {r.get('video_id')} | {r.get('title', '')[:50]} "
                     f"| {r.get('error', '')}", also_print=False)

        return {"total": len(self._filtered_videos), "success": success, "results": results}

    # ================= SAVE =================

    def _save_transcription(
        self, output_path: Path, segments: list, video,
        audio_duration: float, audio_filename: str = "",
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
    p.add_argument("--max-results", "-m", type=int, default=600,
                   help="Số lượng video OUTPUT thỏa mãn filter (không phải số video fetch)")
    p.add_argument("--max-fetch", type=int, default=600,
                   help="Số video tối đa fetch từ YouTube API (giới hạn để tránh quá nhiều API calls)")
    p.add_argument("--order", default="date", help="date | viewCount | rating | relevance")
    p.add_argument("--keep-audio", action="store_true")
    p.add_argument("--audio-format", default="m4a",
                   help="Định dạng audio: m4a, wav, mp4, webm, flac (default: m4a)")
    p.add_argument("--no-transcribe", action="store_true", help="Chỉ lấy metadata, không transcribe")
    p.add_argument("--no-fix", action="store_true", help="Không fix proper nouns")
    p.add_argument("--skip-existing", action="store_true",
                   help="Bỏ qua kênh đã có output (file research_<channel>.json tồn tại)")
    p.add_argument("--no-skip-downloaded", action="store_true",
                   help="Tắt skip khi audio đã tải (mặc định: bật — sẽ skip download, "
                        "và bỏ qua cả Soniox nếu JSON đã có sẵn trong run hiện tại)")
    p.add_argument("--summary-name", default=None,
                   help="Tên file summary tổng (mặc định: _multi_channel_summary_YYYYMMDD_HHMMSS.json có timestamp)")
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
      - Tong quan (command, timestamp, so kenh)
      - Moi kenh: bat dau / ket thuc / status / loi (neu co)
      - Moi video: bucket A/B/C, status (success/skipped/failed)
      - Tong ket: bao nhieu thanh cong, loi o dau, kenh nao chua hoan thanh

    File log duoc ghi o: <output_root>/logs/run_YYYYMMDD_HHMMSS.txt
    Co the doc nhanh de biet:
      - Con kenh nao chua xu ly xong
      - Video nao dang/da loi o phase nao
      - Toan bo batch da hoan thanh hay chua
    """

    def __init__(self, log_path: str | Path):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        # Ghi header khi tao file
        self._write_raw(self._separator("=") + "\n")
        self._write_raw(f"RUN LOG - bat dau luc {datetime.now().isoformat()}\n")
        self._write_raw(f"Log file: {self.log_path}\n")
        self._write_raw(self._separator("=") + "\n\n")

    def _separator(self, ch: str = "-", length: int = 80) -> str:
        return ch * length

    def _write_raw(self, text: str):
        """Ghi raw vao file (co lock de an toan khi nhieu thread)."""
        with self._lock:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(text)

    def log(self, msg: str, also_print: bool = True):
        """Ghi 1 dong log voi timestamp."""
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

    def log_batch_start(self, channels_file: str, total_channels: int, command: str = ""):
        """Ghi log bat dau batch (toan bo file channels.txt)."""
        sep = self._separator("=")
        self._write_raw(f"\n{sep}\nBATCH START\n{sep}\n")
        self.log(f"Timestamp      : {datetime.now().isoformat()}")
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
    youtube_rotator,
    output_root: str,
    max_results: int,
    max_fetch: int,
    order: str,
    keep_audio: bool,
    audio_format: str,
    no_transcribe: bool,
    no_fix: bool,
    skip_existing: bool,
    no_skip_downloaded: bool,
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

    # Timestamp cho lần chạy này, gắn vào tất cả file output để không bị ghi đè
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
        if run_logger:
            run_logger.log(f"[SKIP] Da ton tai {existing_marker[0]}, bo qua kenh")
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

    channel_output.mkdir(parents=True, exist_ok=True)
    researcher = YouTubeResearcher(
        key_rotator=youtube_rotator,
        output_dir=str(channel_output),
    )

    print(f"\nFetching videos from channel: {channel_url}")
    print(f"Target: {max_results} videos that pass filters (max fetch: {max_fetch})")
    if run_logger:
        run_logger.log(f"Fetching videos (max_results={max_results}, max_fetch={max_fetch}, order={order})")

    try:
        researcher.fetch_channel_videos(
            channel_input=channel_url,
            max_results=max_fetch,
            order=order,
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

    researcher.fetch_transcripts()
    researcher.print_video_table()

    resolved_channel_name = researcher._videos[0].channel if researcher._videos else channel_name
    # Đặt tên file theo tên kênh đã resolve (có dấu, space...) + timestamp
    safe_name = resolved_channel_name.replace(" ", "_")
    researcher.save_research(f"research_{safe_name}_{run_timestamp}.json")

    if not no_transcribe:
        print("\nRunning pipeline (download -> transcribe -> fix)...")
        if run_logger:
            run_logger.log("Bat dau pipeline (download -> transcribe -> fix)")
        summary = researcher.process_videos_pipeline(
            output_dir=str(channel_output),
            keep_videos=keep_audio,
            fix_names=not no_fix,
            audio_format=audio_format,
            run_timestamp=run_timestamp,
            skip_downloaded=not no_skip_downloaded,
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

    # === Load YouTube API keys (rotator) ===
    # Ho tro nhieu key: YOUTUBE_API_KEY, YOUTUBE_API_KEY_1, _2, _3, ...
    # Tu dong xoay vong khi gap quotaExceeded.
    env_file = Path(__file__).parent / ".env"
    youtube_rotator = YouTubeKeyRotator.from_env_file(env_file)

    if youtube_rotator.is_empty():
        print("Missing YOUTUBE_API_KEY (set YOUTUBE_API_KEY hoặc YOUTUBE_API_KEY_1, _2, ... "
              "trong .env hoặc environment)")
        sys.exit(1)

    print(f"[YouTube] Loaded {len(youtube_rotator)} key(s):")
    for i, k in enumerate(youtube_rotator.keys, 1):
        print(f"  #{i}: {k[:8]}...{k[-4:]}")

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
    run_logger = RunLogger(log_path)
    cmdline = " ".join(sys.argv) if sys.argv else "python youtube_researcher_sub_playlist.py"
    run_logger.log_batch_start(
        channels_file=args.channels_file,
        total_channels=len(channels),
        command=cmdline,
    )

    # === Loop qua từng kênh ===
    all_results = []
    for idx, ch_url in enumerate(channels, 1):
        print(f"\n>>> [{idx}/{len(channels)}] {ch_url}")
        try:
            result = process_one_channel(
                ch_url,
                youtube_rotator=youtube_rotator,
                output_root=str(output_root),
                max_results=args.max_results,
                max_fetch=args.max_fetch,
                order=args.order,
                keep_audio=args.keep_audio,
                audio_format=args.audio_format,
                no_transcribe=args.no_transcribe,
                no_fix=args.no_fix,
                skip_existing=args.skip_existing,
                no_skip_downloaded=args.no_skip_downloaded,
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


if __name__ == "__main__":
    main()



# === Mode nhiều kênh (file txt) ===
# Mỗi kênh 1 dòng trong file channels.txt:
#   https://www.youtube.com/@vietnh1009
#   https://www.youtube.com/@channelA
#   UCxxxxxxxxxxxxxxxxxxxxx
#
# Chạy loop qua tất cả kênh trong file (mặc định ./channels_audio/channels.txt)
# python youtube_researcher_sub.py

# Hoặc chỉ định file khác
# python youtube_researcher_sub.py --channels-file ./my_channels.txt

# Output tách riêng theo từng kênh: <output_root>/<ten_kenh>/{audio, transcriptions, *.csv, *.json}
# python youtube_researcher_sub.py -o ./datasets --max-results 50

# Bỏ qua kênh đã xử lý (có research_<channel>.json trong output)
# python youtube_researcher_sub.py --skip-existing

# Giữ audio sau khi transcribe
# python youtube_researcher_sub.py --keep-audio

# Không fix proper nouns (bỏ LLM)
# python youtube_researcher_sub.py --no-fix

# === Mode 1 kênh (backward-compat) ===
# python youtube_researcher_sub.py --channel "https://www.youtube.com/@vietnh1009" --max-results 50
# python youtube_researcher_sub.py --channel "https://www.youtube.com/@vietnh1009" --max-results 1 --order viewCount --keep-audio --no-fix --channels-file /path/to/my_channels.txt
# python youtube_researcher_sub.py --channel "UCxxxxxxxxxxxxxxxxxxxxx" --max-results 30
# python youtube_researcher_sub.py --channel "https://www.youtube.com/@TenKenh" --no-transcribe
# python youtube_researcher_sub.py --max-results 1 --order viewCount --keep-audio --no-fix --channels-file /home/hientran/sythetic_crawl_data/channels_audio/channels.
# python youtube_researcher_sub_playlist.py --max-results 600 --order viewCount --keep-audio --no-fix --channels-file /home/hientran/sythetic_crawl_data/channels_audio/channels_AI_2.txt