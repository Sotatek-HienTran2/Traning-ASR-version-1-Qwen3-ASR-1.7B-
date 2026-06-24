#!/usr/bin/env python3
"""
YouTube Researcher - Tìm kiếm, chọn video, tải về, transcribe và sửa lỗi
Dùng cho tạo dataset fine-tune Speech-to-Text

Luồng:
1. Search YouTube theo keyword (chủ đề khoa học)
2. Thu thập metadata (view, like, duration, description, transcript)
3. Filter theo criteria (time, duration, view count, etc.)
4. LLM phân tích và chọn video theo ngách khác nhau
5. Tải video/audio về server
6. Transcribe bằng Soniox API
7. Sửa lỗi tên riêng, sản phẩm, địa phương bằng MiniMax/MiniMax-M2.7
"""

import json
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# ============== CONFIG ==============
YOUTUBE_API_KEY = ""  # Sẽ load từ .env hoặc config
SONIOX_API_KEY = ""   # Soniox API key
ANTHROPIC_API_KEY = ""  # Anthropic/MiniMax API key

# MiniMax Model (from .bashrc: ANTHROPIC_DEFAULT_OPUS_MODEL)
MINIMAX_MODEL = os.environ.get("ANTHROPIC_DEFAULT_OPUS_MODEL", "MiniMax/MiniMax-M2.7")

# ============== SEARCH CONFIG ==============
SEARCH_MAX_RESULTS = 5          # Số video tối đa (max 50)
SEARCH_ORDER = "relevance"       # relevance | date | viewCount | rating
SEARCH_DURATION = "medium"      # any | short (<4m) | medium (4-20m) | long (>20m)

# ============== FILTER CONFIG ==============
FILTER_PUBLISHED_DAYS = 3650      # Video trong bao nhiêu ngày (0 = không giới hạn)
FILTER_MIN_DURATION = 60        # Giây - tối thiểu
FILTER_MAX_DURATION = 600     # Giây - tối đa
FILTER_MIN_VIEW_COUNT = 1      # Lượt xem tối thiểu
FILTER_MIN_LIKE_COUNT = 0       # Like tối thiểu
FILTER_MIN_COMMENT_COUNT = 0     # Comment tối thiểu
# FILTER_EXCLUDE_KEYWORDS = ["trailer", "teaser"]  # Loại bỏ video chứa keyword

@dataclass
class VideoCandidate:
    """Thông tin video candidate"""
    video_id: str
    title: str
    channel: str
    description: str
    published_at: str
    duration: str  # ISO 8601 duration (PT1H2M3S)
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    transcript: Optional[str] = None
    thumbnail: str = ""
    url: str = ""
    detected_language: str = ""  # Ngôn ngữ detect từ YouTube

    # LLM analysis results
    niche: str = ""  # Ngách: "giáo dục", "nghiên cứu", "tin tức", etc.
    llm_score: float = 0.0  # Điểm phù hợp cho dataset STT
    llm_reason: str = ""  # Lý do chọn

    # Soniox transcription data
    transcription_data: dict = field(default_factory=dict)  # {segments, audio_duration, num_speakers, speakers}
    soniox_full_json: Optional[str] = None  # Full JSON response from Soniox
    audio_file_path: Optional[str] = None  # Path to downloaded audio file
    transcription_json_original_path: Optional[str] = None  # Path to original transcription JSON (before fix)
    transcription_json_fixed_path: Optional[str] = None  # Path to fixed transcription JSON (after LLM)

    # Corrections tracking
    corrections: list = field(default_factory=list)  # [(original, corrected, reason), ...]

    # Filter status
    passed_filters: list = field(default_factory=list)
    failed_filters: list = field(default_factory=list)

    @property
    def video_url(self) -> str:
        return f"https://www.youtube.com/watch?v={self.video_id}"

@dataclass
class FilterCriteria:
    """Criteria để lọc video"""
    # Time filters
    published_after: Optional[datetime] = None
    published_before: Optional[datetime] = None

    # Duration filters (seconds)
    min_duration: Optional[int] = None  # seconds
    max_duration: Optional[int] = None

    # Engagement filters
    min_view_count: int = 0
    max_view_count: Optional[int] = None
    min_like_count: int = 0
    min_comment_count: int = 0

    # Content filters
    require_transcript: bool = False
    exclude_keywords: list = field(default_factory=list)  # Loại video chứa keyword này
    include_keywords: list = field(default_factory=list)  # Ưu tiên video chứa keyword này

    # Language filter
    required_languages: list = field(default_factory=lambda: ["vi"])  # Chỉ lấy video tiếng Việt

    # Language hint (for LLM analysis)
    language_hint: str = "vi"  # Vietnamese


def parse_duration(duration_str: str) -> int:
    """Parse ISO 8601 duration to seconds"""
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
    """Format seconds to human readable"""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


def format_number(n: int) -> str:
    """Format large numbers: 1200 -> 1.2K, 1500000 -> 1.5M"""
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def detect_language(text: str) -> str:
    """Detect language from text using textblob or langdetect"""
    if not text:
        return "unknown"

    try:
        from langdetect import detect
        return detect(text[:500])  # Use first 500 chars
    except:
        pass

    try:
        from textblob import TextBlob
        return TextBlob(text[:500]).detect_language()
    except:
        pass

    # Fallback: simple heuristic for Vietnamese
    vietnamese_chars = "àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ"
    if any(c in text.lower() for c in vietnamese_chars):
        return "vi"

    return "unknown"


class YouTubeResearcher:
    """
    YouTube Researcher - Tìm kiếm và chọn video thông minh
    """

    def __init__(self, api_key: str, output_dir: str = "./researched_videos"):
        self.api_key = api_key
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._videos: list[VideoCandidate] = []
        self._filtered_videos: list[VideoCandidate] = []

    def search(
        self,
        query: str,
        max_results: int = 50,
        order: str = "relevance",
        video_duration: Optional[str] = None,
        published_after: Optional[datetime] = None,
    ) -> list[VideoCandidate]:
        """
        Search videos on YouTube

        Args:
            query: Search query (chủ đề)
            max_results: Số lượng video tối đa (max 50 per request)
            order: relevance | date | rating | viewCount
            video_duration: any | short (<4m) | medium (4-20m) | long (>20m)
            published_after: Lọc video đăng sau ngày này

        Returns:
            List of VideoCandidate
        """
        try:
            from googleapiclient.discovery import build
        except ImportError:
            print("❌ Cần cài google-api-python-client: pip install google-api-python-client")
            sys.exit(1)

        youtube = build("youtube", "v3", developerKey=self.api_key)

        published_after_str = None
        if published_after:
            published_after_str = published_after.strftime("%Y-%m-%dT%H:%M:%SZ")

        search_params = {
            "q": query,
            "type": "video",
            "part": "id,snippet",
            "maxResults": min(max_results, 50),
            "order": order,
        }

        if published_after_str:
            search_params["publishedAfter"] = published_after_str
        if video_duration:
            search_params["videoDuration"] = video_duration

        # Search
        search_response = youtube.search().list(**search_params).execute()

        video_ids = []
        snippet_map = {}

        for item in search_response.get("items", []):
            if item["id"]["kind"] == "youtube#video":
                vid = item["id"]["videoId"]
                video_ids.append(vid)
                snippet_map[vid] = item["snippet"]

        if not video_ids:
            print("⚠️  Không tìm thấy video nào")
            return []

        # Get detailed info (video details)
        detail_response = youtube.videos().list(
            part="contentDetails,statistics,snippet",
            id=",".join(video_ids)
        ).execute()

        self._videos = []

        for item in detail_response.get("items", []):
            stats = item.get("statistics", {})
            content = item.get("contentDetails", {})
            snippet = item.get("snippet", {})

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
            )

            # Detect language from title + description
            text_to_detect = f"{video.title} {video.description}"
            video.detected_language = detect_language(text_to_detect)

            # Thumbnail
            thumbs = snippet.get("thumbnails", {})
            if "high" in thumbs:
                video.thumbnail = thumbs["high"].get("url", "")
            elif "medium" in thumbs:
                video.thumbnail = thumbs["medium"].get("url", "")

            self._videos.append(video)

        print(f"🔍 Tìm thấy {len(self._videos)} video cho '{query}'")
        return self._videos

    def apply_filters(self, criteria: FilterCriteria) -> list[VideoCandidate]:
        """Apply filter criteria to videos"""
        self._filtered_videos = []

        for video in self._videos:
            video.passed_filters = []
            video.failed_filters = []

            # Time filter
            if criteria.published_after:
                pub_date = datetime.fromisoformat(video.published_at.replace("Z", "+00:00"))
                if pub_date < criteria.published_after:
                    video.failed_filters.append(f"published_before_{criteria.published_after.date()}")

            if criteria.published_before:
                pub_date = datetime.fromisoformat(video.published_at.replace("Z", "+00:00"))
                if pub_date > criteria.published_before:
                    video.failed_filters.append(f"published_after_{criteria.published_before.date()}")

            # Duration filter
            duration_secs = parse_duration(video.duration)
            if criteria.min_duration and duration_secs < criteria.min_duration:
                video.failed_filters.append(f"duration_too_short_{format_duration(duration_secs)}_min_{format_duration(criteria.min_duration)}")
            if criteria.max_duration and duration_secs > criteria.max_duration:
                video.failed_filters.append(f"duration_too_long_{format_duration(duration_secs)}_max_{format_duration(criteria.max_duration)}")

            # View count filter
            if video.view_count < criteria.min_view_count:
                video.failed_filters.append(f"view_count_low_{format_number(video.view_count)}_min_{format_number(criteria.min_view_count)}")
            if criteria.max_view_count and video.view_count > criteria.max_view_count:
                video.failed_filters.append(f"view_count_high_{format_number(video.view_count)}_max_{format_number(criteria.max_view_count)}")

            # Like count filter
            if video.like_count < criteria.min_like_count:
                video.failed_filters.append(f"like_count_low_{format_number(video.like_count)}_min_{format_number(criteria.min_like_count)}")

            # Comment count filter
            if video.comment_count < criteria.min_comment_count:
                video.failed_filters.append(f"comment_count_low_{format_number(video.comment_count)}_min_{format_number(criteria.min_comment_count)}")

            # Exclude keywords filter
            if criteria.exclude_keywords:
                combined_text = (video.title + " " + video.description).lower()
                for kw in criteria.exclude_keywords:
                    if kw.lower() in combined_text:
                        video.failed_filters.append(f"excluded_keyword_{kw}")

            # Language filter
            if criteria.required_languages:
                if video.detected_language not in criteria.required_languages:
                    video.failed_filters.append(f"language_{video.detected_language}_not_in_{','.join(criteria.required_languages)}")

            if video.failed_filters:
                continue

            video.passed_filters.append("passed_all_criteria")
            self._filtered_videos.append(video)

        print(f"📊 Filter: {len(self._filtered_videos)}/{len(self._videos)} video passed")
        return self._filtered_videos

    def fetch_transcripts(self, video_ids: list[str] = None) -> None:
        """
        Fetch captions/transcripts cho các video
        Dùng yt-dlp hoặc YouTube API captions
        """
        try:
            import yt_dlp
        except ImportError:
            print("⚠️  yt-dlp not installed, skipping transcripts")
            return

        videos_to_fetch = video_ids or [v.video_id for v in self._filtered_videos]

        print(f"📝 Fetching transcripts for {len(videos_to_fetch)} videos...")

        for video in self._filtered_videos:
            if video.transcript:
                continue

            try:
                # Use yt-dlp to get transcript
                ydl_opts = {
                    'writesubtitles': True,
                    'writeautomaticsub': True,
                    'subtitlesformat': 'json3',
                    'skip_download': True,
                    'quiet': True,
                    'no_warnings': True,
                    'extract_flat': False,
                }

                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(video.url, download=False)

                    # Get subtitles
                    subtitles = info.get('subtitles') or info.get('automatic_captions') or {}

                    if subtitles:
                        # Prefer Vietnamese, then English
                        sub_lang = 'vi' if 'vi' in subtitles else list(subtitles.keys())[0]
                        # Note: actual transcript extraction needs more work
                        video.transcript = f"[Auto-generated {sub_lang} subtitles available]"

            except Exception as e:
                print(f"⚠️  Failed to fetch transcript for {video.video_id}: {e}")

    def select_with_llm(
        self,
        topic: str,
        diversity_weight: float = 0.4,
        quality_weight: float = 0.3,
        transcript_availability_weight: float = 0.3,
    ) -> list[VideoCandidate]:
        """
        Dùng LLM để chọn video phù hợp cho dataset STT
        Ưu tiên:
        - Diversity: Video có ngách/niche khác nhau
        - Quality: View, like, comment count cao
        - Transcript: Có sẵn transcript

        Args:
            topic: Chủ đề chính
            diversity_weight: Trọng số cho đa dạng ngách
            quality_weight: Trọng số cho chất lượng (engagement)
            transcript_availability_weight: Trọng số cho việc có transcript

        Returns:
            List of selected VideoCandidate, sorted by final score
        """
        try:
            from anthropic import Anthropic
        except ImportError:
            print("❌ Cần cài anthropic: pip install anthropic")
            sys.exit(1)

        # Get API key
        api_key = self._get_anthropic_key()
        if not api_key:
            print("❌ Không tìm thấy ANTHROPIC_API_KEY")
            sys.exit(1)

        client = Anthropic(api_key=api_key)

        # Build prompt
        videos_info = []
        for i, v in enumerate(self._filtered_videos[:20]):  # Limit to 20 for LLM
            duration_secs = parse_duration(v.duration)
            videos_info.append(f"""Video {i+1}:
  ID: {v.video_id}
  Title: {v.title}
  Channel: {v.channel}
  Duration: {format_duration(duration_secs)}
  Views: {format_number(v.view_count)}
  Likes: {format_number(v.like_count)}
  Comments: {format_number(v.comment_count)}
  Published: {v.published_at[:10]}
  Description (first 500 chars): {v.description[:500]}...
  Has Transcript: {"Yes" if v.transcript else "No"}
  Niche: {v.niche or "Chưa phân ngách"}
""")

        prompt = f"""Bạn là chuyên gia chọn video để tạo dataset fine-tune Speech-to-Text.

## Nhiệm vụ
Chọn video phù hợp cho dataset STT về chủ đề: "{topic}"

## Yêu cầu chọn lọc:
1. **Đa dạng ngách (Niche Diversity)**: Chọn video có nội dung khác nhau về góc độ, phong cách trình bày
   - Ví dụ ngách: bài giảng, phỏng vấn, tin tức, nghiên cứu, giải trí nhẹ, tutorial, debate...
   - Ưu tiên video có giọng nói rõ ràng, tốc độ vừa phải

2. **Chất lượng sản xuất**: Video có engagement tốt (view, like, comment)

3. **Transcript**: Ưu tiên video có sẵn phụ đề/transcript

## Danh sách video cần đánh giá:
{chr(10).join(videos_info)}

## Output format (JSON):
```json
{{
  "selected_videos": [
    {{
      "video_id": "xxx",
      "niche": "mô tả ngách ngắn gọn",
      "reason": "tại sao video này phù hợp cho dataset STT"
    }}
  ],
  "niche_analysis": "phân tích ngắn về các ngách đã chọn",
  "dataset_coverage": "đánh giá coverage của dataset"
}}
```

Hãy chọn từ 5-10 video đa dạng nhất, phù hợp cho fine-tune STT.
"""

        print("🤖 Đang phân tích với LLM...")

        try:
            response = client.messages.create(
                model="MiniMax/MiniMax-M2.5",
                max_tokens=6000,
                messages=[{"role": "user", "content": prompt}]
            )

            # Parse response
            response_text = response.content[0].text

            # Extract JSON from response
            import re
            json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group(1))
            else:
                # Try to find JSON object
                json_match = re.search(r'\{[\s\S]*\}', response_text)
                if json_match:
                    result = json.loads(json_match.group(0))
                else:
                    print("❌ Không parse được JSON từ LLM response")
                    return self._filtered_videos[:10]

            # Match selected videos back
            selected_ids = {v["video_id"] for v in result.get("selected_videos", [])}

            for video in self._filtered_videos:
                if video.video_id in selected_ids:
                    for sel in result["selected_videos"]:
                        if sel["video_id"] == video.video_id:
                            video.niche = sel.get("niche", "unknown")
                            video.llm_reason = sel.get("reason", "")
                            video.llm_score = 0.8  # Base score for being selected

            # Sort by niche diversity + quality
            selected = [v for v in self._filtered_videos if v.video_id in selected_ids]

            print(f"✅ LLM chọn được {len(selected)} video đa dạng")
            print(f"\n📋 Niche Analysis: {result.get('niche_analysis', 'N/A')}")

            return selected

        except Exception as e:
            print(f"❌ LLM Error: {e}")
            return self._filtered_videos[:10]

    def _get_anthropic_key(self) -> Optional[str]:
        """Get Anthropic API key from .env or env var"""
        global ANTHROPIC_API_KEY
        if ANTHROPIC_API_KEY:
            return ANTHROPIC_API_KEY

        env_file = Path(__file__).parent / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                if line.startswith("ANTHROPIC_API_KEY="):
                    ANTHROPIC_API_KEY = line.split("=", 1)[1].strip()
                    return ANTHROPIC_API_KEY

        # Try sythetic_crawl_data .env
        for path in [Path("/home/hientran/sythetic_crawl_data/.env")]:
            if path.exists():
                for line in path.read_text().splitlines():
                    if line.startswith("ANTHROPIC_API_KEY="):
                        ANTHROPIC_API_KEY = line.split("=", 1)[1].strip()
                        return ANTHROPIC_API_KEY

        import os
        return os.environ.get("ANTHROPIC_API_KEY")

    def _get_soniox_key(self) -> Optional[str]:
        """Get Soniox API key from .env or env var"""
        global SONIOX_API_KEY
        if SONIOX_API_KEY:
            return SONIOX_API_KEY

        # Try sythetic_crawl_data .env first
        for path in [Path("/home/hientran/sythetic_crawl_data/.env"), Path("/home/hientran/Sonix/.env")]:
            if path.exists():
                for line in path.read_text().splitlines():
                    if line.startswith("SONIOX_API_KEY="):
                        SONIOX_API_KEY = line.split("=", 1)[1].strip()
                        return SONIOX_API_KEY

        import os
        return os.environ.get("SONIOX_API_KEY")

    def download_videos(
        self,
        video_ids: list[str],
        output_folder: str,
        format: str = "bestaudio/best",
        merge_format: str = "m4a",
    ) -> list[str]:
        """
        Download videos sử dụng yt-dlp

        Args:
            video_ids: List of YouTube video IDs
            output_folder: Thư mục lưu video
            format: yt-dlp format string
            merge_format: Output format

        Returns:
            List of downloaded file paths
        """
        try:
            import yt_dlp
        except ImportError:
            print("❌ Cần cài yt-dlp: pip install yt-dlp")
            sys.exit(1)

        output_path = Path(output_folder)
        output_path.mkdir(parents=True, exist_ok=True)

        # Get URLs
        urls = [f"https://www.youtube.com/watch?v={vid}" for vid in video_ids]

        ydl_opts = {
            'format': format,
            'merge_output_format': merge_format,
            'output': str(output_path / '%(title)s-%(id)s.%(ext)s'),
            'quiet': False,
            'no_warnings': False,
            'extract_flat': False,
        }

        print(f"📥 Downloading {len(urls)} videos to {output_path}")

        downloaded = []

        for url in urls:
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    filename = ydl.prepare_filename(info)
                    downloaded.append(str(output_path / filename))
                    print(f"  ✓ {info.get('title', 'Unknown')[:50]}...")
            except Exception as e:
                print(f"  ❌ Failed: {url} - {e}")

        print(f"\n✅ Downloaded {len(downloaded)}/{len(urls)} videos")
        return downloaded

    # ============== TRANSCRIBE WITH SONIOX ==============
    def transcribe_with_soniox(self, audio_path: Path, lang: list = ["vi"]) -> Optional[dict]:
        """Transcribe audio file using Soniox API"""
        key = self._get_soniox_key()
        if not key:
            print("  ⚠️  SONIOX_API_KEY not found, skipping transcription")
            return None

        try:
            from soniox.client import SonioxClient
            from soniox.types import CreateTranscriptionConfig
            import librosa

            client = SonioxClient(api_key=key)

            # Get audio duration
            try:
                y, sr = librosa.load(str(audio_path), sr=None)
                audio_duration = round(librosa.get_duration(y=y, sr=sr), 3)
            except:
                audio_duration = 0.0

            # Upload and transcribe
            uploaded = client.files.upload(str(audio_path))
            config = CreateTranscriptionConfig(
                model="stt-async-v4",
                language_hints=lang,
                enable_language_identification=True,
                enable_speaker_diarization=True,
            )
            job = client.stt.create(config=config, file_id=uploaded.id)
            client.stt.wait(job.id)
            result = client.stt.get_transcript(job.id)

            # Build segments
            segments = []
            current_speaker = None
            current_tokens = []

            for t in result.tokens:
                speaker = getattr(t, "speaker", "SPEAKER_00")
                if speaker != current_speaker:
                    if current_tokens:
                        segments.append(self._build_segment(current_speaker, current_tokens))
                    current_speaker = speaker
                    current_tokens = []
                current_tokens.append(t)

            if current_tokens:
                segments.append(self._build_segment(current_speaker, current_tokens))

            # Cleanup
            client.stt.delete(job.id)
            client.files.delete(uploaded.id)

            return {
                "segments": segments,
                "audio_duration": audio_duration,
            }

        except Exception as e:
            print(f"  ⚠️  Soniox transcription failed: {e}")
            return None

    def _build_segment(self, speaker, tokens):
        """Build segment from tokens"""
        text = "".join(getattr(t, "text", "") for t in tokens).strip()

        start = getattr(tokens[0], "start_time", None) or getattr(tokens[0], "start_ms", None) or getattr(tokens[0], "start", None) or 0.0
        if isinstance(start, (int, float)) and start > 10000:
            start = start / 1000.0

        end = getattr(tokens[-1], "end_time", None) or getattr(tokens[-1], "end_ms", None) or getattr(tokens[-1], "end", None) or start
        if isinstance(end, (int, float)) and end > 10000:
            end = end / 1000.0

        return {
            "speaker": speaker,
            "start": round(float(start), 3),
            "end": round(float(end), 3),
            "duration": round(float(end) - float(start), 3),
            "text": text
        }

    # ============== FIX PROPER NOUNS WITH MINIMAX ==============
    def fix_proper_nouns_minimax(self, segments: list, video: VideoCandidate = None) -> list:
        """Fix proper nouns (names, products, locations) using MiniMax/MiniMax-M2.7"""
        key = self._get_anthropic_key()
        if not key:
            print("  ⚠️  MiniMax API key not found, skipping fix")
            return segments

        try:
            from anthropic import Anthropic
        except ImportError:
            try:
                subprocess.run([sys.executable, "-m", "pip", "install", "anthropic"], check=True)
                from anthropic import Anthropic
            except:
                print("  ⚠️  Failed to import anthropic, skipping fix")
                return segments

        client = Anthropic(api_key=key)

        # Prepare text for correction
        full_text = "\n".join([
            f"[{s['start']:.2f}-{s['end']:.2f}] {s['speaker']}: {s['text']}"
            for s in segments
        ])

        video_title = video.title if video else ""

        prompt = f"""Bạn là chuyên gia sửa lỗi nhận dạng giọng nói (ASR) cho tiếng Việt.

## Nhiệm vụ
Sửa các lỗi sau trong văn bản:
1. **Tên riêng**: người, công ty, tổ chức (VD: "Hà Nội" thay vì "Hà Nộ", "Nguyễn" thay vì "Nguyễn")
2. **Sản phẩm**: tên sản phẩm, nhãn hiệu
3. **Địa phương**: tỉnh, thành phố, quận, huyện, xã, địa danh

## Ngữ cảnh
Video: {video_title}

## Văn bản cần sửa (có timestamp và speaker):
{full_text}

## Yêu cầu
- Chỉ sửa các từ bị sai chính tả hoặc nhầm lẫn do ASR
- GIỮ NGUYÊN các từ đúng chính tả
- KHÔNG thay đổi cấu trúc câu hay thêm bớt nội dung
- Output JSON format như sau:

```json
{{
  "corrections": [
    {{"original": "Hà Nộ", "corrected": "Hà Nội", "reason": "lỗi ASR thiếu chữ"}},
    ...
  ],
  "fixed_segments": [
    {{"start": 0.0, "end": 5.5, "speaker": "SPEAKER_00", "text": "văn bản đã sửa"}},
    ...
  ]
}}
```

Hãy sửa lỗi và trả về JSON.
"""

        try:
            response = client.messages.create(
                model=MINIMAX_MODEL,
                max_tokens=4000,
                messages=[{"role": "user", "content": prompt}]
            )

            response_text = response.content[0].text

            # Parse JSON response
            json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group(1))
            else:
                json_match = re.search(r'\{[\s\S]*\}', response_text)
                if json_match:
                    result = json.loads(json_match.group(0))
                else:
                    print("  ⚠️  Could not parse MiniMax response, keeping original")
                    return segments

            # Track corrections in video object
            if video:
                for corr in result.get("corrections", []):
                    video.corrections.append((
                        corr.get("original", ""),
                        corr.get("corrected", ""),
                        corr.get("reason", "")
                    ))

            # Apply corrections to segments
            fixed_segments = result.get("fixed_segments", [])
            if fixed_segments:
                print(f"  ✅ Applied {len(result.get('corrections', []))} corrections")
                return fixed_segments

            return segments

        except Exception as e:
            print(f"  ⚠️  MiniMax fix failed: {e}")
            return segments

    # ============== FULL PIPELINE ==============
    def process_videos_pipeline(
        self,
        output_dir: str = "./youtube_dataset",
        download_audio: bool = True,
        transcribe: bool = True,
        fix_names: bool = True,
        keep_videos: bool = False,
    ) -> dict:
        """
        Run full pipeline: download -> transcribe -> fix proper nouns

        Args:
            output_dir: Output directory for audio and transcriptions
            download_audio: Download audio from videos
            transcribe: Transcribe using Soniox
            fix_names: Fix proper nouns using MiniMax
            keep_videos: Keep downloaded audio files after processing

        Returns:
            Summary dict with results
        """
        output_dir = Path(output_dir)
        audio_dir = output_dir / "audio"
        transcriptions_dir = output_dir / "transcriptions"
        transcriptions_json_original_dir = output_dir / "transcriptions_json_original"
        transcriptions_json_fixed_dir = output_dir / "transcriptions_json_fixed"

        audio_dir.mkdir(parents=True, exist_ok=True)
        transcriptions_dir.mkdir(parents=True, exist_ok=True)
        transcriptions_json_original_dir.mkdir(parents=True, exist_ok=True)
        transcriptions_json_fixed_dir.mkdir(parents=True, exist_ok=True)

        results = []

        for i, video in enumerate(self._filtered_videos, 1):
            video_id = video.video_id
            title = video.title
            url = video.url

            print(f"\n[{i}/{len(self._filtered_videos)}] {title[:60]}...")

            audio_path = None

            # Step 1: Download audio
            if download_audio:
                print(f"  📥 Downloading audio...")
                # Use video_id as filename to avoid issues with special chars
                ydl_opts = {
                    'format': 'bestaudio/best',
                    'merge_output_format': 'm4a',
                    'output': str(audio_dir / f'%(id)s.%(ext)s'),
                    'quiet': True,
                    'no_warnings': True,
                }

                try:
                    import yt_dlp
                    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                        info = ydl.extract_info(url, download=True)
                        filename = ydl.prepare_filename(info)
                        audio_path = Path(filename)
                        print(f"  ✅ Downloaded: {audio_path.name}")
                except Exception as e:
                    print(f"  ❌ Download failed: {e}")
                    results.append({"video_id": video_id, "title": title, "status": "download_failed"})
                    continue
            else:
                # Look for existing audio file
                for ext in ['.m4a', '.mp3', '.wav']:
                    possible = audio_dir / f"{video_id}{ext}"
                    if possible.exists():
                        audio_path = possible
                        break

                if not audio_path:
                    print(f"  ⚠️  No audio found, skipping")
                    results.append({"video_id": video_id, "title": title, "status": "no_audio"})
                    continue

            # Step 2: Transcribe
            segments = []
            audio_duration = 0.0

            if transcribe and audio_path:
                print(f"  🎤 Transcribing with Soniox...")
                result = self.transcribe_with_soniox(audio_path, lang=["vi"])
                if result:
                    segments = result.get("segments", [])
                    audio_duration = result.get("audio_duration", 0.0)
                    print(f"  ✅ Transcribed: {len(segments)} segments, {audio_duration:.1f}s")
                else:
                    print(f"  ⚠️  Transcription failed")

            # Save original transcription (before LLM fix)
            if segments:
                json_path_original = transcriptions_json_original_dir / f"{video_id}_transcription.json"
                self._save_transcription(json_path_original, segments, video, audio_duration)
                video.transcription_json_original_path = str(json_path_original)
                print(f"  💾 Saved original JSON: {json_path_original.name}")

            # Step 3: Fix proper nouns with LLM
            if fix_names and segments:
                print(f"  🔧 Fixing proper nouns with MiniMax/MiniMax-M2.7...")
                segments = self.fix_proper_nouns_minimax(segments, video=video)
                print(f"  ✅ Segments fixed and ready to save")

            # Save fixed transcription (after LLM fix)
            if segments:
                json_path_fixed = transcriptions_json_fixed_dir / f"{video_id}_transcription.json"

                # Track paths in video object
                video.audio_file_path = str(audio_path) if audio_path else ""
                video.transcription_json_fixed_path = str(json_path_fixed)

                # Save JSON with fixed segments (LLM corrections applied)
                self._save_transcription(json_path_fixed, segments, video, audio_duration)
                print(f"  💾 Saved fixed JSON: {json_path_fixed.name}")

                results.append({
                    "video_id": video_id,
                    "title": title,
                    "status": "success",
                    "segments": len(segments),
                    "audio_duration": audio_duration,
                    "audio_path": str(audio_path) if audio_path else "",
                    "json_original_path": video.transcription_json_original_path,
                    "json_fixed_path": video.transcription_json_fixed_path,
                })
            else:
                results.append({"video_id": video_id, "title": title, "status": "no_transcription"})

            # Cleanup audio file if not keeping
            if not keep_videos and audio_path and audio_path.exists():
                try:
                    audio_path.unlink()
                    print(f"  🗑️  Cleaned up audio file")
                except:
                    pass

        # Print summary
        success = sum(1 for r in results if r.get("status") == "success")
        print("\n" + "=" * 60)
        print("PIPELINE SUMMARY")
        print("=" * 60)
        print(f"  Total videos: {len(self._filtered_videos)}")
        print(f"  Success: {success}")
        print(f"  Output: {output_dir}")
        print("=" * 60)

        return {
            "total": len(self._filtered_videos),
            "success": success,
            "results": results,
        }

    def _save_transcription(self, output_path: Path, segments: list, video, audio_duration: float):
        """Save transcription to JSON and store in video object"""
        speakers = sorted(set(str(s["speaker"]) for s in segments))

        # Format matching Soniox output structure
        result = {
            "audio_duration": round(audio_duration, 1),
            "audio_path": video.audio_file_path if video.audio_file_path else "",
            "video_id": video.video_id,
            "title": video.title,
            "channel": video.channel,
            "url": video.url,
            "num_speakers": len(speakers),
            "speakers": speakers,
            "source_files": [],
            "segments": segments,
        }

        # Store transcription data in video object
        video.transcription_data = {
            "total_audio_duration": audio_duration,
            "num_speakers": len(speakers),
            "speakers": speakers,
            "num_segments": len(segments),
        }

        # Store full JSON as string
        video.soniox_full_json = json.dumps(result, ensure_ascii=False)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    def save_research(self, filename: str = "research_result.json") -> str:
        """Save research results to JSON"""
        output_file = self.output_dir / filename

        videos_data = []
        for v in self._filtered_videos:
            data = asdict(v)
            data["video_url"] = v.video_url  # Include computed URL
            videos_data.append(data)

        data = {
            "research_date": datetime.now().isoformat(),
            "total_videos_found": len(self._videos),
            "videos_after_filter": len(self._filtered_videos),
            "videos": videos_data,
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        print(f"💾 Research saved to {output_file}")
        return str(output_file)

    def save_to_csv(self, filename: str = "research_result.csv") -> str:
        """Save research results to CSV with all detailed information"""
        import csv

        output_file = self.output_dir / filename

        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)

            # Header - YouTube Search Info + Soniox + LLM + Corrections
            headers = [
                # YouTube Search Info
                "video_id",
                "title",
                "channel",
                "url",
                "published_at",
                "detected_language",

                # Duration & Engagement (YouTube)
                "duration_formatted",
                "duration_seconds",
                "view_count",
                "like_count",
                "comment_count",
                "engagement_ratio",

                # Soniox Transcription Info
                "transcription_status",
                "audio_duration_seconds",
                "num_speakers",
                "speakers_list",
                "num_segments",

                # Soniox Full JSON (complete transcription data)
                "soniox_transcription_json",

                # LLM Analysis
                "niche",
                "llm_score",
                "llm_reason",

                # Corrections (từ sai -> từ sửa)
                "corrections_count",
                "corrections_original_words",
                "corrections_fixed_words",
                "corrections_reasons",

                # Filter Status
                "passed_filters",
                "failed_filters",
                "description_preview",
            ]
            writer.writerow(headers)

            # Data rows
            for v in self._filtered_videos:
                duration_secs = parse_duration(v.duration)

                # Calculate engagement ratio
                engagement_ratio = 0
                if v.view_count > 0:
                    engagement_ratio = round((v.like_count + v.comment_count) / v.view_count * 100, 2)

                # Transcription status
                transcription_status = "completed" if v.transcription_data else "pending"
                audio_duration = v.transcription_data.get("total_audio_duration", 0) if v.transcription_data else 0
                num_speakers = v.transcription_data.get("num_speakers", 0) if v.transcription_data else 0
                speakers_list = ", ".join(v.transcription_data.get("speakers", [])) if v.transcription_data else ""
                num_segments = v.transcription_data.get("num_segments", 0) if v.transcription_data else 0

                # Soniox full JSON
                soniox_json = v.soniox_full_json if v.soniox_full_json else ""

                # Corrections - separate original and fixed words
                corrections_original = " | ".join([c[0] for c in v.corrections]) if v.corrections else ""
                corrections_fixed = " | ".join([c[1] for c in v.corrections]) if v.corrections else ""
                corrections_reasons = " | ".join([c[2] for c in v.corrections]) if v.corrections else ""

                # Description preview (first 100 chars)
                desc_preview = v.description[:100].replace("\n", " ") if v.description else ""

                row = [
                    # YouTube Search Info
                    v.video_id,
                    v.title,
                    v.channel,
                    v.url,
                    v.published_at,
                    v.detected_language,

                    # Duration & Engagement
                    format_duration(duration_secs),
                    duration_secs,
                    v.view_count,
                    v.like_count,
                    v.comment_count,
                    engagement_ratio,

                    # Soniox Transcription
                    transcription_status,
                    round(audio_duration, 2),
                    num_speakers,
                    speakers_list,
                    num_segments,

                    # Soniox Full JSON
                    soniox_json,

                    # LLM Analysis
                    v.niche,
                    f"{v.llm_score:.2f}",
                    v.llm_reason,

                    # Corrections
                    len(v.corrections),
                    corrections_original,
                    corrections_fixed,
                    corrections_reasons,

                    # Filter Status
                    " | ".join(v.passed_filters),
                    " | ".join(v.failed_filters),
                    desc_preview,
                ]
                writer.writerow(row)

        print(f"💾 CSV saved to {output_file}")
        return str(output_file)

    def save_corrections_csv(self, filename: str = "corrections_detail.csv") -> str:
        """Save detailed corrections to separate CSV (one row per correction)"""
        import csv

        output_file = self.output_dir / filename

        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)

            # Header
            headers = [
                "video_id",
                "title",
                "channel",
                "original_word",
                "corrected_word",
                "correction_reason",
            ]
            writer.writerow(headers)

            # Data rows - one row per correction
            for v in self._filtered_videos:
                if v.corrections:
                    for original, corrected, reason in v.corrections:
                        row = [
                            v.video_id,
                            v.title,
                            v.channel,
                            original,
                            corrected,
                            reason,
                        ]
                        writer.writerow(row)

        print(f"💾 Corrections CSV saved to {output_file}")
        return str(output_file)

    def save_audio_dataset_csv(self, filename: str = "audio_dataset.csv") -> str:
        """Save comprehensive CSV with audio paths and all metadata (one row per audio file)"""
        import csv

        output_file = self.output_dir / filename

        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)

            # Header - Audio + YouTube + Soniox + LLM + Corrections
            headers = [
                # Audio File Info
                "audio_file_path",
                "transcription_json_path",
                "video_id",

                # YouTube Search Info
                "title",
                "channel",
                "url",
                "published_at",
                "detected_language",

                # Duration & Engagement
                "duration_formatted",
                "duration_seconds",
                "view_count",
                "like_count",
                "comment_count",
                "engagement_ratio",

                # Soniox Transcription Info
                "transcription_status",
                "audio_duration_seconds",
                "num_speakers",
                "speakers_list",
                "num_segments",

                # LLM Analysis
                "niche",
                "llm_score",
                "llm_reason",

                # Corrections Summary
                "corrections_count",
                "corrections_original_words",
                "corrections_fixed_words",

                # Filter Status
                "passed_filters",
                "failed_filters",
            ]
            writer.writerow(headers)

            # Data rows - one row per video with audio
            for v in self._filtered_videos:
                # Only include videos with audio files
                if not v.audio_file_path:
                    continue

                duration_secs = parse_duration(v.duration)

                # Calculate engagement ratio
                engagement_ratio = 0
                if v.view_count > 0:
                    engagement_ratio = round((v.like_count + v.comment_count) / v.view_count * 100, 2)

                # Transcription status
                transcription_status = "completed" if v.transcription_data else "pending"
                audio_duration = v.transcription_data.get("total_audio_duration", 0) if v.transcription_data else 0
                num_speakers = v.transcription_data.get("num_speakers", 0) if v.transcription_data else 0
                speakers_list = ", ".join(v.transcription_data.get("speakers", [])) if v.transcription_data else ""
                num_segments = v.transcription_data.get("num_segments", 0) if v.transcription_data else 0

                # Corrections
                corrections_original = " | ".join([c[0] for c in v.corrections]) if v.corrections else ""
                corrections_fixed = " | ".join([c[1] for c in v.corrections]) if v.corrections else ""

                row = [
                    # Audio File Info
                    v.audio_file_path,
                    v.transcription_json_path,
                    v.video_id,

                    # YouTube Search Info
                    v.title,
                    v.channel,
                    v.url,
                    v.published_at,
                    v.detected_language,

                    # Duration & Engagement
                    format_duration(duration_secs),
                    duration_secs,
                    v.view_count,
                    v.like_count,
                    v.comment_count,
                    engagement_ratio,

                    # Soniox Transcription
                    transcription_status,
                    round(audio_duration, 2),
                    num_speakers,
                    speakers_list,
                    num_segments,

                    # LLM Analysis
                    v.niche,
                    f"{v.llm_score:.2f}",
                    v.llm_reason,

                    # Corrections
                    len(v.corrections),
                    corrections_original,
                    corrections_fixed,

                    # Filter Status
                    " | ".join(v.passed_filters),
                    " | ".join(v.failed_filters),
                ]
                writer.writerow(row)

        print(f"💾 Audio dataset CSV saved to {output_file}")
        return str(output_file)

    def save_unified_csv(self, filename: str = "unified_dataset.csv") -> str:
        """Save unified CSV with all data merged - one row per audio file with corrections in separate columns"""
        import csv

        output_file = self.output_dir / filename

        with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)

            # Build headers
            headers = [
                # Audio File Info
                "audio_file_path",
                "transcription_json_original_path",
                "transcription_json_fixed_path",
                "video_id",

                # YouTube Search Info
                "title",
                "channel",
                "url",
                "published_at",
                "detected_language",

                # Duration & Engagement
                "duration_formatted",
                "duration_seconds",
                "view_count",
                "like_count",
                "comment_count",
                "engagement_ratio",

                # Soniox Transcription Info
                "transcription_status",
                "audio_duration_seconds",
                "num_speakers",
                "speakers_list",
                "num_segments",

                # LLM Analysis
                "niche",
                "llm_score",
                "llm_reason",

                # Corrections (simplified)
                "corrections_count",
                "corrections_original",  # a, b, c (separated by comma)
                "corrections_fixed",     # a', b', c' (separated by comma)
                "corrections_reason",    # reason1, reason2, reason3 (separated by comma)

                # Filter Status
                "passed_filters",
                "failed_filters",
            ]

            writer.writerow(headers)

            # Data rows - one row per video with audio
            for v in self._filtered_videos:
                # Only include videos with audio files
                if not v.audio_file_path:
                    continue

                duration_secs = parse_duration(v.duration)

                # Calculate engagement ratio
                engagement_ratio = 0
                if v.view_count > 0:
                    engagement_ratio = round((v.like_count + v.comment_count) / v.view_count * 100, 2)

                # Transcription status
                transcription_status = "completed" if v.transcription_data else "pending"
                audio_duration = v.transcription_data.get("total_audio_duration", 0) if v.transcription_data else 0
                num_speakers = v.transcription_data.get("num_speakers", 0) if v.transcription_data else 0
                speakers_list = ", ".join(v.transcription_data.get("speakers", [])) if v.transcription_data else ""
                num_segments = v.transcription_data.get("num_segments", 0) if v.transcription_data else 0

                # Corrections - gộp tất cả vào 2 cột
                corrections_original = ", ".join([c[0] for c in v.corrections]) if v.corrections else ""
                corrections_fixed = ", ".join([c[1] for c in v.corrections]) if v.corrections else ""
                corrections_reason = ", ".join([c[2] for c in v.corrections]) if v.corrections else ""

                row = [
                    # Audio File Info
                    v.audio_file_path,
                    v.transcription_json_original_path,
                    v.transcription_json_fixed_path,
                    v.video_id,

                    # YouTube Search Info
                    v.title,
                    v.channel,
                    v.url,
                    v.published_at,
                    v.detected_language,

                    # Duration & Engagement
                    format_duration(duration_secs),
                    duration_secs,
                    v.view_count,
                    v.like_count,
                    v.comment_count,
                    engagement_ratio,

                    # Soniox Transcription
                    transcription_status,
                    round(audio_duration, 2),
                    num_speakers,
                    speakers_list,
                    num_segments,

                    # LLM Analysis
                    v.niche,
                    f"{v.llm_score:.2f}",
                    v.llm_reason,

                    # Corrections
                    len(v.corrections),
                    corrections_original,
                    corrections_fixed,
                    corrections_reason,

                    # Filter Status
                    " | ".join(v.passed_filters),
                    " | ".join(v.failed_filters),
                ]

                writer.writerow(row)

        print(f"💾 Unified CSV saved to {output_file}")
        return str(output_file)

    def print_video_table(self, videos: list[VideoCandidate] = None):
        """Print nice table of videos"""
        if videos is None:
            videos = self._filtered_videos

        if not videos:
            print("No videos to display")
            return

        print(f"\n{'='*120}")
        print(f"{'#':<3} {'Title':<40} {'Channel':<15} {'Duration':<10} {'Views':<8} {'Likes':<6} {'Niche':<15}")
        print(f"{'='*120}")

        for i, v in enumerate(videos):
            duration_secs = parse_duration(v.duration)
            title = v.title[:37] + "..." if len(v.title) > 40 else v.title
            niche = (v.niche or "-")[:13]

            print(f"{i+1:<3} {title:<40} {v.channel[:13]:<15} {format_duration(duration_secs):<10} {format_number(v.view_count):<8} {format_number(v.like_count):<6} {niche:<15}")

        print(f"{'='*120}")
        print(f"\n📎 Video URLs:")
        for i, v in enumerate(videos):
            print(f"  {i+1}. {v.video_url}")


def parse_args():
    """Parse command line arguments for full automation"""
    import argparse
    p = argparse.ArgumentParser(description="YouTube Researcher - Full Automated Pipeline")
    p.add_argument("--topic", "-t", required=True, help="Search topic/keyword")
    p.add_argument("--output", "-o", default="./youtube_dataset", help="Output directory")
    p.add_argument("--max-results", "-m", type=int, default=5, help="Max videos to search")
    p.add_argument("--no-transcribe", action="store_true", help="Skip transcription")
    p.add_argument("--no-fix", action="store_true", help="Skip fixing proper nouns")
    p.add_argument("--keep-audio", action="store_true", help="Keep audio files after processing")
    p.add_argument("--order", default="relevance", help="Search order: relevance/date/viewCount")
    p.add_argument("--duration", default="medium", help="Video duration: any/short/medium/long")
    return p.parse_args()


def main():
    """Main entry point - fully automated pipeline"""
    args = parse_args()

    import os
    from pathlib import Path

    # Load API keys
    env_file = Path(__file__).parent / ".env"
    youtube_key = None
    anthropic_key = None

    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("YOUTUBE_API_KEY="):
                youtube_key = line.split("=", 1)[1].strip()
            if line.startswith("ANTHROPIC_API_KEY="):
                anthropic_key = line.split("=", 1)[1].strip()

    if not youtube_key:
        print("❌ Cần YOUTUBE_API_KEY trong file .env")
        sys.exit(1)

    # Initialize
    researcher = YouTubeResearcher(
        api_key=youtube_key,
        output_dir="./researched_videos"
    )

    topic = args.topic

    print(f"\n🔍 Searching for: '{topic}'")
    videos = researcher.search(
        query=topic,
        max_results=args.max_results,
        order=args.order,
        video_duration=args.duration,
    )

    if not videos:
        print("❌ Không tìm thấy video nào")
        sys.exit(1)

    # Apply filters
    published_after = datetime.now().astimezone() - timedelta(days=FILTER_PUBLISHED_DAYS)
    criteria = FilterCriteria(
        published_after=published_after,
        min_duration=FILTER_MIN_DURATION,
        max_duration=FILTER_MAX_DURATION,
        min_view_count=FILTER_MIN_VIEW_COUNT,
        min_like_count=FILTER_MIN_LIKE_COUNT,
        min_comment_count=FILTER_MIN_COMMENT_COUNT,
    )

    researcher.apply_filters(criteria)
    researcher.print_video_table(researcher._filtered_videos)
    researcher.save_research(f"research_{topic.replace(' ', '_')}.json")

    print(f"\n🚀 Running full pipeline (download->transcribe->fix)...")
    print(f"   Output: {args.output}")
    print(f"   Transcribe: {not args.no_transcribe}")
    print(f"   Fix names: {not args.no_fix}")
    print(f"   Keep audio: {args.keep_audio}")

    summary = researcher.process_videos_pipeline(
        output_dir=args.output,
        download_audio=True,
        transcribe=not args.no_transcribe,
        fix_names=not args.no_fix,
        keep_videos=args.keep_audio,
    )

    # Save summary
    summary_path = Path(args.output) / "pipeline_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    # Save unified CSV with all metadata and corrections
    researcher.save_unified_csv(f"unified_dataset_{topic.replace(' ', '_')}.csv")

    print(f"\n✅ DONE!")
    print(f"📊 Summary: {summary_path}")
    print(f"📁 Output dir: {args.output}")


if __name__ == "__main__":
    main()
    
    
    
# emotion
# gender
# laughter
# applause
# music
# pitch
# energy
# SNR
# speech/music ratio

# subtitle human vs auto-generated
# overlap speech detection
# speech/music/noise classifier
# applause/laughter detector
# emotion classifier
# gender estimation
# MOS audio quality
# SNR signal/noise ratio
# WER estimation
# semantic density
# toxicity
# NSFW
# topic embedding
# CLAP embedding
# speaker embedding
# wav2vec embedding
# whisper embedding