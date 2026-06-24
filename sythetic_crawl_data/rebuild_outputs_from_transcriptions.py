#!/usr/bin/env python3
"""
Rebuild CSV/JSON output từ folder transcriptions/ có sẵn.
Dùng khi script bị SKIP do --skip-existing nhưng cần regenerate
các file output (CSV, pipeline_summary, segments_dataset, video_summary).

Usage:
    python rebuild_outputs_from_transcriptions.py \\
        --channel-folder youtube_dataset/Top10HuyenBi
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("pip install pandas")
    sys.exit(1)


# ================= HELPERS (copy từ youtube_researcher_youtube_subs.py) =================

def parse_duration(duration_str):
    """Parse ISO 8601 duration (PT1H2M3S) -> seconds"""
    if not duration_str:
        return 0
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_str)
    if not match:
        return 0
    h = int(match.group(1) or 0)
    m = int(match.group(2) or 0)
    s = int(match.group(3) or 0)
    return h * 3600 + m * 60 + s


def format_duration(seconds):
    if not seconds:
        return ""
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


def find_video_in_research(research_videos, video_id):
    """Tìm video trong research JSON theo video_id"""
    for v in research_videos:
        if v.get("video_id") == video_id:
            return v
    return None


# ================= CSV EXPORTERS (logic giống script gốc) =================

def rebuild_segments_csv(transcription_dir, research_videos, output_csv):
    """
    Tạo segments_dataset CSV: mỗi segment 1 row.
    Tương đương export_transcriptions_to_csv() trong script gốc.
    """
    rows = []
    matched = 0
    unmatched = []

    for json_file in sorted(transcription_dir.glob("*_transcription.json")):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  [WARN] Cannot read {json_file.name}: {e}")
            continue

        video_id = data.get("video_id", "")
        # Match với research video
        video_meta = find_video_in_research(research_videos, video_id) or {}

        segments = data.get("segments", [])
        if not segments:
            continue
        matched += 1

        audio_features = data.get("audio_features", {})
        extra_metadata = data.get("youtube_metadata", {})
        audio_path = data.get("audio_path", "")

        transcription_metadata = {
            "total_audio_duration": data.get("total_audio_duration") or data.get("audio_duration"),
            "num_speakers": data.get("num_speakers"),
            "speakers": data.get("speakers", []),
            "avg_confidence": data.get("avg_confidence"),
            "detected_languages": data.get("detected_languages", []),
            "dataset_score": data.get("dataset_score"),
            "audio_path": audio_path,
        }

        for seg in segments:
            row = {
                "video_id": video_id,
                "video_title": data.get("title", video_meta.get("title", "")),
                "channel": data.get("channel", video_meta.get("channel", "")),
                "video_url": f"https://www.youtube.com/watch?v={video_id}",
                "published_at": video_meta.get("published_at", ""),
                "duration_iso": video_meta.get("duration", ""),
                "duration_seconds": parse_duration(video_meta.get("duration", "")),
                "view_count": video_meta.get("view_count", 0),
                "like_count": video_meta.get("like_count", 0),
                "comment_count": video_meta.get("comment_count", 0),
                "thumbnail": video_meta.get("thumbnail", ""),
                "description": video_meta.get("description", ""),
                "audio_path": audio_path,

                "speaker": seg.get("speaker"),
                "segment_start": seg.get("start"),
                "segment_end": seg.get("end"),
                "segment_duration": seg.get("duration"),
                "text": seg.get("text"),

                "language": seg.get("language"),
                "language_confidence": seg.get("language_confidence"),
                "speaker_confidence": seg.get("speaker_confidence"),
                "avg_token_confidence": seg.get("avg_token_confidence"),
                "num_tokens": seg.get("num_tokens"),
                "speech_rate_wps": seg.get("speech_rate_wps"),
                "has_music": seg.get("has_music"),
                "has_noise": seg.get("has_noise"),
                "emotion": seg.get("emotion"),
                "gender": seg.get("gender"),
                "audio_energy": seg.get("audio_energy"),
                "audio_pitch": seg.get("audio_pitch"),
                "silence_before": seg.get("silence_before"),
                "silence_after": seg.get("silence_after"),

                # Audio features
                "audio_total_duration": audio_features.get("duration"),
                "audio_silence_ratio": audio_features.get("silence_ratio"),
                "audio_zero_crossing_rate": audio_features.get("zero_crossing_rate"),
                "audio_mean_volume": audio_features.get("mean_volume"),

                # Transcription metadata
                "total_audio_duration": transcription_metadata["total_audio_duration"],
                "num_speakers": transcription_metadata["num_speakers"],
                "speakers_list": ", ".join(transcription_metadata["speakers"]) if transcription_metadata["speakers"] else "",
                "avg_confidence": transcription_metadata["avg_confidence"],
                "detected_languages": json.dumps(transcription_metadata["detected_languages"], ensure_ascii=False) if transcription_metadata["detected_languages"] else "",
                "dataset_score": transcription_metadata["dataset_score"],

                # YouTube metadata
                "tags": json.dumps(extra_metadata.get("tags", []), ensure_ascii=False) if extra_metadata.get("tags") else json.dumps(video_meta.get("tags", []), ensure_ascii=False),
                "category_id": extra_metadata.get("category_id") or video_meta.get("category_id", ""),
                "topic_categories": json.dumps(extra_metadata.get("topic_categories", []), ensure_ascii=False) if extra_metadata.get("topic_categories") else json.dumps(video_meta.get("topic_categories", []), ensure_ascii=False),
                "top_comments": json.dumps(extra_metadata.get("top_comments", []), ensure_ascii=False) if extra_metadata.get("top_comments") else json.dumps(video_meta.get("top_comments", []), ensure_ascii=False),
            }
            rows.append(row)

    if not rows:
        print(f"  [WARN] No rows extracted from {transcription_dir}")
        return False

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"  [OK] Segments CSV: {output_csv.name} ({len(df)} rows, {len(df.columns)} cols, from {matched} videos)")
    return True


def rebuild_video_summary_csv(transcription_dir, research_videos, output_csv):
    """
    Tạo video_summary CSV: mỗi video 1 row.
    Tương đương export_video_summary_csv() trong script gốc.
    """
    rows = []

    for json_file in sorted(transcription_dir.glob("*_transcription.json")):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        video_id = data.get("video_id", "")
        video_meta = find_video_in_research(research_videos, video_id) or {}

        audio_features = data.get("audio_features", {})
        extra_metadata = data.get("youtube_metadata", {})

        duration_secs = parse_duration(video_meta.get("duration", ""))
        view_count = video_meta.get("view_count", 0)
        like_count = video_meta.get("like_count", 0)
        comment_count = video_meta.get("comment_count", 0)
        engagement = 0
        if view_count > 0:
            engagement = round((like_count + comment_count) / view_count * 100, 2)

        row = {
            "video_id": video_id,
            "title": data.get("title", video_meta.get("title", "")),
            "channel": data.get("channel", video_meta.get("channel", "")),
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "published_at": video_meta.get("published_at", ""),
            "duration_iso": video_meta.get("duration", ""),
            "duration_formatted": format_duration(duration_secs),
            "duration_seconds": duration_secs,
            "view_count": view_count,
            "like_count": like_count,
            "comment_count": comment_count,
            "engagement_ratio": engagement,

            "audio_path": data.get("audio_path", ""),
            "audio_total_duration": audio_features.get("duration", ""),
            "audio_silence_ratio": audio_features.get("silence_ratio", ""),
            "audio_zero_crossing_rate": audio_features.get("zero_crossing_rate", ""),
            "audio_mean_volume": audio_features.get("mean_volume", ""),

            "transcription_status": "completed",
            "transcription_audio_duration": data.get("total_audio_duration") or data.get("audio_duration", ""),
            "num_speakers": data.get("num_speakers"),
            "speakers_list": ", ".join(data.get("speakers", [])) if data.get("speakers") else "",
            "num_segments": len(data.get("segments", [])),
            "avg_confidence": data.get("avg_confidence"),
            "detected_languages": json.dumps(data.get("detected_languages", []), ensure_ascii=False),
            "dataset_score": data.get("dataset_score"),

            "transcript_language": data.get("language", ""),
            "transcript_is_auto": data.get("is_auto", False),
            "transcript_source": data.get("source", "youtube_captions"),

            "tags": json.dumps(extra_metadata.get("tags", []), ensure_ascii=False) if extra_metadata.get("tags") else json.dumps(video_meta.get("tags", []), ensure_ascii=False),
            "topic_categories": json.dumps(extra_metadata.get("topic_categories", []), ensure_ascii=False) if extra_metadata.get("topic_categories") else json.dumps(video_meta.get("topic_categories", []), ensure_ascii=False),
            "description_preview": (video_meta.get("description", "") or "")[:200],
        }
        rows.append(row)

    if not rows:
        print(f"  [WARN] No rows for video_summary")
        return False

    df = pd.DataFrame(rows)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"  [OK] Video summary CSV: {output_csv.name} ({len(df)} rows, {len(df.columns)} cols)")
    return True


def rebuild_research_csv(transcription_dir, research_videos, output_csv):
    """
    Tạo research CSV tổng hợp.
    Tương đương save_to_csv() trong script gốc (gộp metadata + transcription info).
    """
    import csv

    headers = [
        "video_id", "title", "channel", "url", "published_at",
        "duration_formatted", "duration_seconds",
        "view_count", "like_count", "comment_count", "engagement_ratio",
        "audio_path",
        "audio_total_duration", "audio_silence_ratio",
        "audio_zero_crossing_rate", "audio_mean_volume",
        "transcription_status", "transcription_audio_duration",
        "num_speakers", "speakers_list", "num_segments",
        "avg_confidence", "detected_languages", "dataset_score",
        "transcript_language", "transcript_source",
        "tags", "topic_categories", "description_preview",
    ]

    written = 0
    with open(output_csv, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for v in research_videos:
            video_id = v.get("video_id", "")
            duration_secs = parse_duration(v.get("duration", ""))
            view_count = v.get("view_count", 0)
            like_count = v.get("like_count", 0)
            comment_count = v.get("comment_count", 0)
            engagement = 0
            if view_count > 0:
                engagement = round((like_count + comment_count) / view_count * 100, 2)

            # Match với transcription JSON nếu có
            transcription_metadata = {}
            audio_features = {}
            audio_path = v.get("audio_filename", "")
            transcript_language = v.get("transcript_language", "")
            transcript_source = "youtube_captions"

            for json_file in transcription_dir.glob("*_transcription.json"):
                try:
                    with open(json_file, "r", encoding="utf-8") as jf:
                        tdata = json.load(jf)
                    if tdata.get("video_id") == video_id:
                        audio_features = tdata.get("audio_features", {})
                        transcription_metadata = {
                            "total_audio_duration": tdata.get("total_audio_duration") or tdata.get("audio_duration"),
                            "num_speakers": tdata.get("num_speakers"),
                            "speakers": tdata.get("speakers", []),
                            "avg_confidence": tdata.get("avg_confidence"),
                            "detected_languages": tdata.get("detected_languages", []),
                            "dataset_score": tdata.get("dataset_score"),
                            "num_segments": len(tdata.get("segments", [])),
                        }
                        if tdata.get("audio_path"):
                            audio_path = tdata.get("audio_path")
                        transcript_language = tdata.get("language", transcript_language)
                        break
                except Exception:
                    continue

            row = [
                video_id,
                v.get("title", ""),
                v.get("channel", ""),
                v.get("url", f"https://www.youtube.com/watch?v={video_id}"),
                v.get("published_at", ""),
                format_duration(duration_secs),
                duration_secs,
                view_count, like_count, comment_count, engagement,
                audio_path,
                audio_features.get("duration", ""),
                audio_features.get("silence_ratio", ""),
                audio_features.get("zero_crossing_rate", ""),
                audio_features.get("mean_volume", ""),
                "completed" if transcription_metadata else "pending",
                transcription_metadata.get("total_audio_duration", ""),
                transcription_metadata.get("num_speakers", ""),
                ", ".join(transcription_metadata.get("speakers", [])) if transcription_metadata.get("speakers") else "",
                transcription_metadata.get("num_segments", ""),
                transcription_metadata.get("avg_confidence", ""),
                json.dumps(transcription_metadata.get("detected_languages", []), ensure_ascii=False) if transcription_metadata.get("detected_languages") else "",
                transcription_metadata.get("dataset_score", ""),
                transcript_language,
                transcript_source,
                json.dumps(v.get("tags", []), ensure_ascii=False),
                json.dumps(v.get("topic_categories", []), ensure_ascii=False),
                (v.get("description", "") or "")[:200],
            ]
            writer.writerow(row)
            written += 1

    print(f"  [OK] Research CSV: {output_csv.name} ({written} rows)")
    return True


def rebuild_pipeline_summary(transcription_dir, research_videos, output_json, run_timestamp):
    """
    Tạo pipeline_summary JSON tổng hợp.
    Tương đương return value của process_videos_pipeline().
    """
    results = []
    transcribed_count = 0

    for json_file in sorted(transcription_dir.glob("*_transcription.json")):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue

        video_id = data.get("video_id", "")
        transcribed_count += 1

        results.append({
            "video_id": video_id,
            "title": data.get("title", ""),
            "status": "completed",
            "audio_filename": data.get("audio_path", ""),
            "transcription_filename": json_file.name,
            "transcript_language": data.get("language", ""),
            "transcript_is_auto": data.get("is_auto", False),
            "transcript_source": data.get("source", "youtube_captions"),
            "num_segments": len(data.get("segments", [])),
            "num_speakers": data.get("num_speakers"),
            "total_audio_duration": data.get("total_audio_duration") or data.get("audio_duration"),
            "avg_confidence": data.get("avg_confidence"),
            "dataset_score": data.get("dataset_score"),
        })

    summary = {
        "run_timestamp": run_timestamp,
        "total_videos_researched": len(research_videos),
        "total_videos_transcribed": transcribed_count,
        "transcription_dir": str(transcription_dir),
        "results": results,
        "stats": {
            "completed": transcribed_count,
            "skipped": len(research_videos) - transcribed_count,
            "failed": 0,
        },
    }

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print(f"  [OK] Pipeline summary: {output_json.name} ({transcribed_count} videos transcribed)")
    return True


# ================= MAIN =================

def main():
    parser = argparse.ArgumentParser(description="Rebuild CSV/JSON outputs từ transcriptions có sẵn")
    parser.add_argument("--channel-folder", required=True, help="Path tới folder kênh (vd: youtube_dataset/Top10HuyenBi)")
    parser.add_argument("--transcription-timestamp", default=None, help="Timestamp của folder transcriptions (vd: 20260617_120745). Mặc định: lấy folder mới nhất")
    parser.add_argument("--research-json", default=None, help="Path tới research JSON. Mặc định: lấy file mới nhất trong folder")
    parser.add_argument("--output-timestamp", default=None, help="Timestamp cho file output mới. Mặc định: dùng timestamp hiện tại")
    args = parser.parse_args()

    channel_folder = Path(args.channel_folder)
    if not channel_folder.exists():
        print(f"[ERROR] Folder không tồn tại: {channel_folder}")
        return 1

    print(f"Channel folder: {channel_folder}")

    # 1. Tìm transcription folder
    transcriptions_root = channel_folder / "transcriptions"
    if not transcriptions_root.exists():
        print(f"[ERROR] Không có folder transcriptions/")
        return 1

    if args.transcription_timestamp:
        transcription_dir = transcriptions_root / args.transcription_timestamp
    else:
        # Lấy folder mới nhất
        candidates = sorted([d for d in transcriptions_root.iterdir() if d.is_dir()], reverse=True)
        if not candidates:
            print(f"[ERROR] Không có folder timestamp trong transcriptions/")
            return 1
        transcription_dir = candidates[0]

    if not transcription_dir.exists():
        print(f"[ERROR] Folder transcriptions không tồn tại: {transcription_dir}")
        return 1

    print(f"Transcription dir: {transcription_dir}")
    transcription_files = list(transcription_dir.glob("*_transcription.json"))
    print(f"  -> {len(transcription_files)} file transcription")

    # 2. Tìm research JSON
    if args.research_json:
        research_json = Path(args.research_json)
    else:
        candidates = sorted(channel_folder.glob("research_*.json"), reverse=True)
        if not candidates:
            print(f"[ERROR] Không tìm thấy file research_*.json")
            return 1
        research_json = candidates[0]

    print(f"Research JSON: {research_json}")
    with open(research_json, "r", encoding="utf-8") as f:
        research_data = json.load(f)
    research_videos = research_data.get("videos", [])
    print(f"  -> {len(research_videos)} videos in research")

    # 3. Timestamp cho output mới
    if args.output_timestamp:
        run_timestamp = args.output_timestamp
    else:
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Lấy safe_name từ channel name (giống script gốc)
    channel_name = research_data.get("channel", channel_folder.name).replace(" ", "_")
    print(f"\nChannel name: {channel_name}")
    print(f"Output timestamp: {run_timestamp}")
    print("=" * 60)

    # 4. Tạo các file output
    print("\n[1/4] Building segments dataset CSV...")
    segments_csv = channel_folder / f"{channel_name}_segments_dataset_{run_timestamp}.csv"
    rebuild_segments_csv(transcription_dir, research_videos, segments_csv)

    print("\n[2/4] Building video summary CSV...")
    summary_csv = channel_folder / f"{channel_name}_video_summary_{run_timestamp}.csv"
    rebuild_video_summary_csv(transcription_dir, research_videos, summary_csv)

    print("\n[3/4] Building research CSV...")
    research_csv = channel_folder / f"research_{channel_name}_{run_timestamp}.csv"
    rebuild_research_csv(transcription_dir, research_videos, research_csv)

    print("\n[4/4] Building pipeline summary JSON...")
    pipeline_json = channel_folder / f"pipeline_summary_{run_timestamp}.json"
    rebuild_pipeline_summary(transcription_dir, research_videos, pipeline_json, run_timestamp)

    print("\n" + "=" * 60)
    print("[DONE] Tất cả file output đã được tái tạo:")
    print(f"  - {segments_csv}")
    print(f"  - {summary_csv}")
    print(f"  - {research_csv}")
    print(f"  - {pipeline_json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())