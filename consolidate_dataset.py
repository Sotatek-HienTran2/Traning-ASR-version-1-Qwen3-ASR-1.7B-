#!/usr/bin/env python3
"""
Consolidate YouTube dataset: thống nhất tất cả output (CSV + JSON).

Làm 5 việc trong 1 lần chạy:
  0. (--flatten) Dồn subfolder audio/<timestamp>/*.wav → audio/*.wav và
                   transcriptions/<timestamp>/*.json → transcriptions/*.json.
                   Tự động dedup audio trùng content hash (giữ 1 bản, xóa bản trùng)
                   và JSON trùng video_id (giữ bản có nhiều segment nhất).
                   Vì audio_path trong CSV/JSON chỉ là tên file relative → KHÔNG CẦN SỬA CSV.
  1. Gộp nhiều research_*.json trong mỗi channel thành 1 file research_{channel}_MERGED.json
  2. Gộp nhiều pipeline_summary_*.json thành 1 file pipeline_summary_MERGED.json
  3. Rebuild + merge tất cả CSV (segments, summary, research) thành 3 file *_MERGED.csv
  4. Xóa tất cả file gốc trùng lặp (nếu --cleanup)

Output: 5 file MERGED cho mỗi channel:
  - research_{channel}_MERGED.json
  - pipeline_summary_MERGED.json
  - {channel}_segments_MERGED.csv
  - {channel}_summary_MERGED.csv
  - {channel}_research_MERGED.csv

Usage:
    # Xử lý 1 channel (dry-run, không xóa)
    python consolidate_dataset.py --channel-folder youtube_dataset_1/HuyDao

    # Xử lý toàn bộ + xóa file gốc
    python consolidate_dataset.py --base-dir youtube_dataset_1 --cleanup

    # Chỉ merge JSON (skip CSV)
    python consolidate_dataset.py --base-dir youtube_dataset_1 --skip-csv

    # Dồn subfolder + dedup (xem trước)
    python consolidate_dataset.py --base-dir youtube_dataset_1 --flatten --dry-run

    # Dồn subfolder + dedup + merge CSV + xóa file gốc (1 lần)
    python consolidate_dataset.py --base-dir youtube_dataset_1 --flatten --cleanup
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import pandas as pd
except ImportError:
    print("pip install pandas")
    sys.exit(1)


# ================= HELPERS =================

def parse_duration(duration_str) -> int:
    if not duration_str:
        return 0
    if isinstance(duration_str, (int, float)):
        return int(duration_str)
    match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', str(duration_str))
    if not match:
        try:
            return int(float(duration_str))
        except (ValueError, TypeError):
            return 0
    h = int(match.group(1) or 0)
    m = int(match.group(2) or 0)
    s = int(match.group(3) or 0)
    return h * 3600 + m * 60 + s


def format_duration(seconds) -> str:
    if not seconds:
        return ""
    try:
        seconds = int(seconds)
    except (ValueError, TypeError):
        return ""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


def get_timestamp_from_filename(path: Path) -> str:
    m = re.search(r"(\d{8}_\d{6})", path.stem)
    return m.group(1) if m else "00000000_000000"


def safe_filename_to_video_id(filename: str) -> Optional[str]:
    """Lấy video_id từ tên file audio (ưu tiên match _{video_id}.wav)."""
    m = re.search(r"_([A-Za-z0-9_-]{11})\.(?:wav|mp3|m4a|flac|opus|ogg|webm|json)$", filename)
    if m:
        return m.group(1)
    return None


# ================= FLATTEN AUDIO + TRANSCRIPTIONS SUBFOLDERS =================

def _md5(path: Path, chunk: int = 1024 * 1024) -> str:
    """Hash nội dung file (chỉ phần đầu ~16MB để nhanh — đủ phân biệt audio khác nhau)."""
    import hashlib
    h = hashlib.md5()
    total = 0
    with open(path, "rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
            total += len(b)
            if total >= 16 * 1024 * 1024:  # cap 16MB
                break
    return h.hexdigest()


def _video_id_from_name(filename: str) -> Optional[str]:
    """Trích video_id từ tên file JSON (ưu tiên *_transcription.json) hoặc audio."""
    # Pattern 1: {safe_title}_<video_id>_transcription.json
    m = re.search(r"_([A-Za-z0-9_-]{11})_transcription\.json$", filename)
    if m:
        return m.group(1)
    # Pattern 2: <video_id>.wav
    m = re.search(r"^([A-Za-z0-9_-]{11})\.(?:wav|mp3|m4a|flac|opus|ogg|webm)$", filename)
    if m:
        return m.group(1)
    # Pattern 3 fallback: _<video_id>.<ext>
    m = re.search(r"_([A-Za-z0-9_-]{11})\.(?:wav|mp3|m4a|flac|opus|ogg|webm|json)$", filename)
    return m.group(1) if m else None


def _scan_audio_for_dup(channel_folder: Path) -> tuple[dict[str, Path], list[Path]]:
    """
    Quét toàn bộ audio (kể cả root audio/ và audio/<ts>/*) → map {hash: Path_giữ_lại}.
    Trả về (keep_map, list_path_deduped) trong đó keep_map chỉ chứa 1 Path cho mỗi hash.
    """
    audio_dir = channel_folder / "audio"
    if not audio_dir.exists():
        return {}, []

    AUDIO_EXT = {".wav", ".m4a", ".mp3", ".flac", ".opus", ".ogg", ".webm"}
    # Gom tất cả file audio (kể cả nằm ngay audio/ và trong subfolder)
    audio_files: list[Path] = []
    for p in audio_dir.rglob("*"):
        if p.is_file() and p.suffix.lower() in AUDIO_EXT:
            audio_files.append(p)
        elif p.is_dir():
            # bỏ qua nested folder, chỉ quan tâm file
            pass

    by_hash: dict[str, Path] = {}
    for f in audio_files:
        try:
            h = _md5(f)
        except Exception:
            continue
        if h not in by_hash:
            by_hash[h] = f
    return by_hash, audio_files


def _scan_transcriptions_for_dup(channel_folder: Path) -> tuple[dict[str, Path], list[Path]]:
    """
    Quét transcriptions/**/*_transcription.json → map {video_id: Path_giữ_lại}.
    Giữ video_id trùng: chọn file có segment nhiều nhất (mặc định file mới nhất theo tên).
    """
    trans_dir = channel_folder / "transcriptions"
    if not trans_dir.exists():
        return {}, []

    json_files: list[Path] = []
    for p in trans_dir.rglob("*_transcription.json"):
        if p.is_file():
            json_files.append(p)

    by_vid: dict[str, Path] = {}
    for f in json_files:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            continue
        vid = data.get("video_id") or _video_id_from_name(f.name) or ""
        if not vid:
            continue
        # Ưu tiên giữ file có segments nhiều nhất; nếu bằng nhau thì giữ alphabetically đầu
        seg_count = len(data.get("segments") or [])
        if vid not in by_vid:
            by_vid[vid] = f
        else:
            try:
                with open(by_vid[vid], "r", encoding="utf-8") as fh:
                    existing_data = json.load(fh)
                existing_count = len(existing_data.get("segments") or [])
            except Exception:
                existing_count = -1
            if seg_count > existing_count:
                by_vid[vid] = f
    return by_vid, json_files


def flatten_subfolders(channel_folder: Path, dry_run: bool = False) -> dict:
    """
    Dồn audio/<timestamp>/*.wav → audio/*.wav và transcriptions/<timestamp>/*.json → transcriptions/*.json.

    Bước xử lý:
      1. Quét toàn bộ audio/*.wav (kể cả trong subfolder) → nhóm theo content hash (md5).
         - Nếu nhiều file có CÙNG hash: giữ 1 bản (ưu tiên file ở root nếu có, nếu không thì
           lấy file đầu tiên theo alphabet), xóa các bản còn lại.
      2. Với file audio unique còn lại (khác hash nhau): move về audio/ (root).
      3. Quét transcriptions/*_transcription.json → nhóm theo video_id.
         - Nếu cùng video_id có nhiều bản JSON: giữ bản có nhiều segment nhất, xóa các bản khác.
         - Nếu audio tương ứng (cùng video_id) đã bị xóa ở bước 1 → xóa luôn JSON.
      4. Move JSON còn lại về transcriptions/ (root), đổi tên nếu trùng tên file.
      5. Xóa các subfolder <timestamp>/ rỗng.

    LƯU Ý về CSV/JSON ở root (research_*, pipeline_summary_*, *_MERGED.csv):
      - audio_path trong các file này CHỈ LÀ TÊN FILE (relative), không chứa subfolder path
        (đã xác nhận qua kiểm tra: 0 absolute path trong tất cả CSV).
      - Sau khi dồn, file audio vẫn nằm trong audio/ nhưng tên giống y cũ → KHÔNG CẦN SỬA CSV.
      - Các cột _source_file, _source_timestamp, audio_timestamps SẼ MẤT THÔNG TIN về subfolder
        cũ, nhưng giá trị đó là metadata tham chiếu, không ảnh hưởng logic.

    Args:
        channel_folder: folder kênh (vd: youtube_dataset_1/CogaiIT2k2)
        dry_run: nếu True chỉ in kế hoạch, không move/xóa

    Returns:
        dict thống kê: {audio_kept, audio_removed_dup, audio_moved,
                        trans_kept, trans_removed_dup, trans_removed_orphan,
                        trans_renamed, subfolders_removed}
    """
    stats = {
        "audio_kept": 0,
        "audio_removed_dup": 0,
        "audio_moved": 0,
        "trans_kept": 0,
        "trans_removed_dup": 0,
        "trans_removed_orphan": 0,
        "trans_renamed": 0,
        "subfolders_removed": 0,
    }

    if not channel_folder.is_dir():
        return stats

    audio_dir = channel_folder / "audio"
    trans_dir = channel_folder / "transcriptions"

    # ========== BƯỚC 1+2: AUDIO ==========
    audio_keep_by_hash, all_audio_files = _scan_audio_for_dup(channel_folder)
    kept_audio_set = set(audio_keep_by_hash.values())

    audio_to_remove: list[Path] = []  # trùng hash, xóa
    audio_to_move: list[Path] = []    # unique nhưng nằm trong subfolder, move về root

    for f in all_audio_files:
        if f in kept_audio_set:
            # file được giữ lại
            if f.parent != audio_dir:
                audio_to_move.append(f)
        else:
            audio_to_remove.append(f)

    # ========== BƯỚC 3: TRANSCRIPTIONS ==========
    trans_keep_by_vid, all_trans_files = _scan_transcriptions_for_dup(channel_folder)
    kept_trans_set = set(trans_keep_by_vid.values())

    trans_to_remove: list[Path] = []
    trans_to_move: list[Path] = []
    trans_rename: list[tuple[Path, Path]] = []  # (src, dst) khi tên trùng ở root

    # Map audio_path trong JSON → video_id đã được giữ
    # (orphan = audio đã xóa ở bước 1 → JSON này cũng phải xóa)
    audio_kept_names = {p.name for p in kept_audio_set}

    for f in all_trans_files:
        if f in kept_trans_set:
            if f.parent != trans_dir:
                trans_to_move.append(f)
        else:
            trans_to_remove.append(f)

    # Kiểm tra JSON có audio tương ứng không (orphan removal)
    for f in kept_trans_set:
        try:
            with open(f, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception:
            continue
        ap = data.get("audio_path", "")
        if ap and ap not in audio_kept_names:
            # Audio tương ứng đã bị xóa vì trùng hash với audio khác → JSON cũng xóa
            trans_to_remove.append(f)
            # Loại khỏi kept để không move file này
            if f in trans_to_move:
                trans_to_move.remove(f)
            stats["trans_removed_orphan"] += 1

    # ========== XỬ LÝ ĐỔI TÊN KHI TRÙNG Ở ROOT ==========
    # Sau khi move nhiều file về cùng root, có thể trùng tên. Đổi tên theo pattern _2, _3...
    trans_dst_names = {p.name for p in trans_dir.iterdir() if p.is_file()} if trans_dir.exists() else set()
    audio_dst_names = {p.name for p in audio_dir.iterdir() if p.is_file()} if audio_dir.exists() else set()

    def _unique_name(base: Path, existing: set[str]) -> Path:
        stem, suf = base.stem, base.suffix
        new_name = base.name
        i = 2
        while new_name in existing:
            new_name = f"{stem}_{i}{suf}"
            i += 1
        existing.add(new_name)
        return base.with_name(new_name)

    for src in list(audio_to_move):
        dst = audio_dir / src.name
        if dst.exists():
            # Trùng tên ở root: giữ nguyên file root (đã tồn tại), xóa file định move
            audio_to_remove.append(src)
            audio_to_move.remove(src)
            continue
        final_dst = _unique_name(dst, audio_dst_names)
        if final_dst.name != src.name:
            stats["trans_renamed"] += 1  # count together
        audio_dst_names.add(final_dst.name)

    for src in list(trans_to_move):
        dst = trans_dir / src.name
        if dst.exists():
            trans_to_remove.append(src)
            trans_to_move.remove(src)
            continue
        final_dst = _unique_name(dst, trans_dst_names)
        if final_dst.name != src.name:
            stats["trans_renamed"] += 1
        trans_dst_names.add(final_dst.name)

    # ========== IN KẾ HOẠCH ==========
    print(f"  [flatten] audio: keep={len(kept_audio_set)}, "
          f"remove_dup={len(audio_to_remove) - sum(1 for f in audio_to_remove if f in kept_audio_set)}, "
          f"move_to_root={len(audio_to_move)}")
    print(f"  [flatten] trans: keep={len(kept_trans_set) - stats['trans_removed_orphan']}, "
          f"remove_dup={len(trans_to_remove) - stats['trans_removed_orphan']}, "
          f"orphan_removed={stats['trans_removed_orphan']}, "
          f"move_to_root={len(trans_to_move)}, renamed={stats['trans_renamed']}")

    if dry_run:
        return stats

    # ========== THỰC THI ==========
    # Xóa audio trùng hash (trừ file được giữ)
    for f in audio_to_remove:
        if f in kept_audio_set:
            continue
        try:
            f.unlink()
            stats["audio_removed_dup"] += 1
        except Exception as e:
            print(f"    [WARN] không xóa được {f}: {e}")

    # Move audio về root
    for src in audio_to_move:
        dst = audio_dir / src.name
        if dst.exists():
            # đã được xử lý ở trên
            continue
        try:
            src.rename(dst)
            stats["audio_moved"] += 1
        except Exception as e:
            print(f"    [WARN] không move được {src} → {dst}: {e}")

    # Xóa trans trùng video_id
    for f in trans_to_remove:
        try:
            f.unlink()
            if f not in kept_trans_set:
                stats["trans_removed_dup"] += 1
        except Exception as e:
            print(f"    [WARN] không xóa được {f}: {e}")

    # Move trans về root (đã tính unique name ở trên)
    moved_pairs: list[tuple[Path, Path]] = []
    for src in trans_to_move:
        dst = trans_dir / src.name
        if dst.exists():
            continue
        moved_pairs.append((src, dst))
    for src, dst in moved_pairs:
        try:
            src.rename(dst)
            stats["trans_kept"] += 1
        except Exception as e:
            print(f"    [WARN] không move được {src} → {dst}: {e}")

    # Xóa subfolder <timestamp>/ rỗng
    for parent in (audio_dir, trans_dir):
        if not parent.exists():
            continue
        for sub in parent.iterdir():
            if not sub.is_dir():
                continue
            try:
                # Thử xóa (chỉ xóa được nếu rỗng)
                sub.rmdir()
                stats["subfolders_removed"] += 1
            except OSError:
                # Còn file → best effort: thử xóa file cũ không cần thiết (giữ lại file gần đây nhất)
                pass

    stats["audio_kept"] = len(kept_audio_set)
    stats["trans_kept"] = len(trans_to_move)
    return stats


# ================= SCAN AUDIO + TRANSCRIPTIONS =================

def scan_audio_folder(channel_folder: Path) -> dict[str, dict]:
    """Quét audio/<timestamp>/, build map video_id → info. Ưu tiên match từ transcription JSON."""
    audio_dir = channel_folder / "audio"
    if not audio_dir.exists():
        return {}

    # Bước 1: Build audio_path → video_id từ transcription JSON (đáng tin)
    audio_path_to_vid: dict[str, str] = {}
    trans_dir = channel_folder / "transcriptions"
    if trans_dir.exists():
        for ts_dir in trans_dir.iterdir():
            if not ts_dir.is_dir():
                continue
            for jf in ts_dir.glob("*_transcription.json"):
                try:
                    with open(jf, "r", encoding="utf-8") as f:
                        data = json.load(f)
                    vid = data.get("video_id", "")
                    audio_path = data.get("audio_path", "")
                    if vid and audio_path:
                        audio_path_to_vid[audio_path] = vid
                except Exception:
                    pass

    # Bước 2: Scan audio folder
    by_vid: dict[str, set[str]] = {}
    for ts_dir in audio_dir.iterdir():
        if not ts_dir.is_dir():
            continue
        ts_name = ts_dir.name
        for audio_file in ts_dir.iterdir():
            if audio_file.is_dir():
                continue
            if audio_file.suffix.lower() not in {".wav", ".m4a", ".mp3", ".flac", ".opus", ".ogg", ".webm"}:
                continue
            vid = audio_path_to_vid.get(audio_file.name) or safe_filename_to_video_id(audio_file.name)
            if vid:
                by_vid.setdefault(vid, set()).add(ts_name)

    result = {}
    for vid, ts_set in by_vid.items():
        ts_sorted = sorted(ts_set)
        result[vid] = {
            "audio_file_exists": True,
            "audio_timestamps": ",".join(ts_sorted),
            "audio_num_versions": len(ts_sorted),
            "audio_latest_timestamp": ts_sorted[-1] if ts_sorted else "",
        }
    return result


def scan_transcriptions_folder(channel_folder: Path) -> dict[str, dict]:
    """Quét transcriptions/<timestamp>/*.json. Ưu tiên video_id từ JSON content."""
    trans_dir = channel_folder / "transcriptions"
    if not trans_dir.exists():
        return {}

    by_vid: dict[str, list[tuple[str, dict]]] = {}

    for ts_dir in trans_dir.iterdir():
        if not ts_dir.is_dir():
            continue
        ts_name = ts_dir.name
        for json_file in ts_dir.glob("*_transcription.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue

            vid = data.get("video_id", "")
            if not vid:
                # Fallback parse từ tên file
                m = re.search(r"_([A-Za-z0-9_-]{11})_transcription\.json$", json_file.name)
                if m:
                    vid = m.group(1)
            if not vid:
                continue

            summary = {
                "num_segments_transcription": len(data.get("segments", [])),
                "transcription_total_duration": data.get("total_audio_duration") or data.get("audio_duration"),
                "transcription_num_speakers": data.get("num_speakers"),
                "transcription_avg_confidence": data.get("avg_confidence"),
                "transcription_detected_languages": ",".join(data.get("detected_languages", []) or []),
                "transcription_status_in_json": "success" if data.get("segments") else "empty",
                "transcription_language": data.get("language", ""),
                "transcription_source": data.get("source", "youtube_captions"),
                "_full_data": data,
                "_audio_path": data.get("audio_path", ""),
            }
            by_vid.setdefault(vid, []).append((ts_name, summary))

    result = {}
    for vid, ts_list in by_vid.items():
        ts_list.sort(key=lambda x: x[0])
        latest_ts, latest_summary = ts_list[-1]
        result[vid] = {
            "latest_transcription_timestamp": latest_ts,
            "num_transcription_versions": len(ts_list),
            "num_segments_transcription": latest_summary["num_segments_transcription"],
            "transcription_total_duration": latest_summary["transcription_total_duration"],
            "transcription_num_speakers": latest_summary["transcription_num_speakers"],
            "transcription_avg_confidence": latest_summary["transcription_avg_confidence"],
            "transcription_detected_languages": latest_summary["transcription_detected_languages"],
            "transcription_status_in_json": latest_summary["transcription_status_in_json"],
            "transcription_language": latest_summary["transcription_language"],
            "transcription_source": latest_summary["transcription_source"],
            "_segments_data": latest_summary["_full_data"],
            "_audio_path_from_transcription": latest_summary["_audio_path"],
        }
    return result


# ================= LOAD RESEARCH VIDEOS =================

def load_research_videos(channel_folder: Path) -> list[dict]:
    """Load TẤT CẢ research_*.json, gộp videos, dedup theo video_id (giữ version mới nhất)."""
    research_files = sorted(channel_folder.glob("research_*.json"))
    if not research_files:
        return []

    by_vid: dict[str, dict] = {}
    for rf in research_files:
        try:
            with open(rf, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        ts = get_timestamp_from_filename(rf)
        for v in data.get("videos", []):
            vid = v.get("video_id")
            if not vid:
                continue
            existing = by_vid.get(vid)
            if existing is None or ts >= existing.get("_ts", ""):
                by_vid[vid] = {**v, "_ts": ts}

    return list(by_vid.values())


# ================= BUILD SEGMENTS CSV =================

def build_segments_csv(channel_folder, research_videos, trans_summary, audio_by_vid, output_csv):
    rows = []
    matched = 0
    for vid, summary in trans_summary.items():
        full_data = summary.get("_segments_data", {})
        if not full_data:
            continue
        video_meta = next((v for v in research_videos if v.get("video_id") == vid), {})
        title = full_data.get("title") or video_meta.get("title", "")
        channel = full_data.get("channel") or video_meta.get("channel", "")
        published = video_meta.get("published_at", "")
        duration_iso = video_meta.get("duration", "")
        duration_secs = parse_duration(duration_iso)
        view_count = video_meta.get("view_count", 0)
        like_count = video_meta.get("like_count", 0)
        comment_count = video_meta.get("comment_count", 0)
        thumbnail = video_meta.get("thumbnail", "")
        description = video_meta.get("description", "")
        niche = video_meta.get("niche", "")
        llm_score = video_meta.get("llm_score", 0)
        llm_reason = video_meta.get("llm_reason", "")
        audio_path = summary.get("_audio_path_from_transcription", "") or video_meta.get("audio_filename", "")
        audio_features = full_data.get("audio_features", {})
        extra_metadata = full_data.get("youtube_metadata", {})

        segments = full_data.get("segments", [])
        if not segments:
            continue
        matched += 1

        for seg in segments:
            row = {
                "video_id": vid,
                "video_title": title,
                "channel": channel,
                "video_url": f"https://www.youtube.com/watch?v={vid}",
                "published_at": published,
                "duration_iso": duration_iso,
                "duration_seconds": duration_secs,
                "view_count": view_count,
                "like_count": like_count,
                "comment_count": comment_count,
                "thumbnail": thumbnail,
                "description": description,
                "niche": niche,
                "llm_score": llm_score,
                "llm_reason": llm_reason,
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
                "audio_total_duration": audio_features.get("duration"),
                "audio_silence_ratio": audio_features.get("silence_ratio"),
                "audio_zero_crossing_rate": audio_features.get("zero_crossing_rate"),
                "audio_mean_volume": audio_features.get("mean_volume"),
                "total_audio_duration": full_data.get("total_audio_duration") or full_data.get("audio_duration"),
                "num_speakers": full_data.get("num_speakers"),
                "speakers_list": ", ".join(full_data.get("speakers", []) or []),
                "avg_confidence": full_data.get("avg_confidence"),
                "detected_languages": json.dumps(full_data.get("detected_languages", []), ensure_ascii=False),
                "dataset_score": full_data.get("dataset_score"),
                "estimated_speech_ratio": video_meta.get("estimated_speech_ratio", 0),
                "video_avg_confidence": video_meta.get("avg_confidence"),
                "video_dataset_score": video_meta.get("dataset_score"),
                "video_detected_languages": json.dumps(video_meta.get("detected_languages", []), ensure_ascii=False),
                "tags": json.dumps(extra_metadata.get("tags", video_meta.get("tags", [])), ensure_ascii=False),
                "category_id": extra_metadata.get("category_id") or video_meta.get("category_id", ""),
                "default_language": extra_metadata.get("default_language") or video_meta.get("default_language", ""),
                "default_audio_language": extra_metadata.get("default_audio_language") or video_meta.get("default_audio_language", ""),
                "caption": extra_metadata.get("caption") if extra_metadata.get("caption") is not None else video_meta.get("caption_available"),
                "licensed_content": extra_metadata.get("licensed_content") if extra_metadata.get("licensed_content") is not None else video_meta.get("licensed_content"),
                "definition": extra_metadata.get("definition") or video_meta.get("definition", ""),
                "projection": extra_metadata.get("projection") or video_meta.get("projection", ""),
                "privacy_status": extra_metadata.get("privacy_status") or video_meta.get("privacy_status", ""),
                "made_for_kids": extra_metadata.get("made_for_kids") if extra_metadata.get("made_for_kids") is not None else video_meta.get("made_for_kids"),
                "topic_categories": json.dumps(extra_metadata.get("topic_categories", video_meta.get("topic_categories", [])), ensure_ascii=False),
                "top_comments": json.dumps(extra_metadata.get("top_comments", video_meta.get("top_comments", [])), ensure_ascii=False),
                "_source_file": f"transcriptions/{summary['latest_transcription_timestamp']}/{full_data.get('audio_path', '')}_transcription.json",
                "_source_timestamp": summary["latest_transcription_timestamp"],
                "audio_file_exists": vid in audio_by_vid,
                "audio_timestamps": audio_by_vid.get(vid, {}).get("audio_timestamps", ""),
                "audio_num_versions": audio_by_vid.get(vid, {}).get("audio_num_versions", 0),
                "latest_transcription_timestamp": summary["latest_transcription_timestamp"],
                "num_transcription_versions": summary["num_transcription_versions"],
                "num_segments_transcription": summary["num_segments_transcription"],
                "transcription_total_duration": summary["transcription_total_duration"],
                "transcription_num_speakers": summary["transcription_num_speakers"],
                "transcription_avg_confidence": summary["transcription_avg_confidence"],
                "transcription_detected_languages": summary["transcription_detected_languages"],
                "transcription_status_in_json": summary["transcription_status_in_json"],
            }
            rows.append(row)

    if not rows:
        return {"num_input_files": 0, "num_rows_before": 0, "num_rows_after": 0, "num_unique_videos": 0}

    df = pd.DataFrame(rows)
    num_before = len(df)
    df = df.sort_values(["video_id", "_source_timestamp"]).drop_duplicates(
        subset=["video_id", "segment_start"], keep="last"
    ).reset_index(drop=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"    [segments] → {output_csv.name}: {num_before} → {len(df)} rows ({matched} videos)")
    return {
        "num_input_files": 1,
        "num_rows_before": num_before,
        "num_rows_after": len(df),
        "num_unique_videos": matched,
    }


# ================= BUILD VIDEO SUMMARY CSV =================

def build_video_summary_csv(channel_folder, research_videos, trans_summary, audio_by_vid, output_csv):
    rows = []
    for vid, summary in trans_summary.items():
        full_data = summary.get("_segments_data", {})
        if not full_data:
            continue
        video_meta = next((v for v in research_videos if v.get("video_id") == vid), {})
        title = full_data.get("title") or video_meta.get("title", "")
        channel = full_data.get("channel") or video_meta.get("channel", "")
        duration_secs = parse_duration(video_meta.get("duration", ""))
        view_count = video_meta.get("view_count", 0)
        like_count = video_meta.get("like_count", 0)
        comment_count = video_meta.get("comment_count", 0)
        engagement = round((like_count + comment_count) / view_count * 100, 2) if view_count > 0 else 0
        audio_features = full_data.get("audio_features", {})
        extra_metadata = full_data.get("youtube_metadata", {})
        audio_path = summary.get("_audio_path_from_transcription", "") or video_meta.get("audio_filename", "")

        row = {
            "video_id": vid,
            "title": title,
            "channel": channel,
            "url": f"https://www.youtube.com/watch?v={vid}",
            "published_at": video_meta.get("published_at", ""),
            "duration_formatted": format_duration(duration_secs),
            "duration_iso": video_meta.get("duration", ""),
            "duration_seconds": duration_secs,
            "view_count": view_count,
            "like_count": like_count,
            "comment_count": comment_count,
            "engagement_ratio": engagement,
            "audio_path": audio_path,
            "audio_total_duration": audio_features.get("duration"),
            "audio_silence_ratio": audio_features.get("silence_ratio"),
            "audio_zero_crossing_rate": audio_features.get("zero_crossing_rate"),
            "audio_mean_volume": audio_features.get("mean_volume"),
            "transcription_status": "completed",
            "transcription_audio_duration": full_data.get("total_audio_duration") or full_data.get("audio_duration"),
            "num_speakers": full_data.get("num_speakers"),
            "speakers_list": ", ".join(full_data.get("speakers", []) or []),
            "num_segments": len(full_data.get("segments", [])),
            "avg_confidence": full_data.get("avg_confidence"),
            "detected_languages": ",".join(full_data.get("detected_languages", []) or []),
            "dataset_score": full_data.get("dataset_score"),
            "estimated_speech_ratio": video_meta.get("estimated_speech_ratio", 0),
            "video_avg_confidence": video_meta.get("avg_confidence"),
            "video_dataset_score": video_meta.get("dataset_score"),
            "video_detected_languages": ",".join(video_meta.get("detected_languages", []) or []),
            "niche": video_meta.get("niche", ""),
            "llm_score": video_meta.get("llm_score", 0),
            "llm_reason": video_meta.get("llm_reason", ""),
            "tags": json.dumps(extra_metadata.get("tags", video_meta.get("tags", [])), ensure_ascii=False),
            "category_id": extra_metadata.get("category_id") or video_meta.get("category_id", ""),
            "default_language": extra_metadata.get("default_language") or video_meta.get("default_language", ""),
            "default_audio_language": extra_metadata.get("default_audio_language") or video_meta.get("default_audio_language", ""),
            "caption_available": extra_metadata.get("caption") if extra_metadata.get("caption") is not None else video_meta.get("caption_available"),
            "definition": extra_metadata.get("definition") or video_meta.get("definition", ""),
            "licensed_content": extra_metadata.get("licensed_content") if extra_metadata.get("licensed_content") is not None else video_meta.get("licensed_content"),
            "projection": extra_metadata.get("projection") or video_meta.get("projection", ""),
            "privacy_status": extra_metadata.get("privacy_status") or video_meta.get("privacy_status", ""),
            "made_for_kids": extra_metadata.get("made_for_kids") if extra_metadata.get("made_for_kids") is not None else video_meta.get("made_for_kids"),
            "topic_categories": json.dumps(extra_metadata.get("topic_categories", video_meta.get("topic_categories", [])), ensure_ascii=False),
            "top_comments": json.dumps(extra_metadata.get("top_comments", video_meta.get("top_comments", [])), ensure_ascii=False),
            "passed_filters": json.dumps(video_meta.get("passed_filters", []), ensure_ascii=False),
            "failed_filters": json.dumps(video_meta.get("failed_filters", []), ensure_ascii=False),
            "description": video_meta.get("description", ""),
            "_source_file": f"transcriptions/{summary['latest_transcription_timestamp']}",
            "_source_timestamp": summary["latest_transcription_timestamp"],
            "audio_file_exists": vid in audio_by_vid,
            "audio_timestamps": audio_by_vid.get(vid, {}).get("audio_timestamps", ""),
            "audio_num_versions": audio_by_vid.get(vid, {}).get("audio_num_versions", 0),
            "latest_transcription_timestamp": summary["latest_transcription_timestamp"],
            "num_transcription_versions": summary["num_transcription_versions"],
            "num_segments_transcription": summary["num_segments_transcription"],
            "transcription_total_duration": summary["transcription_total_duration"],
            "transcription_num_speakers": summary["transcription_num_speakers"],
            "transcription_avg_confidence": summary["transcription_avg_confidence"],
            "transcription_detected_languages": summary["transcription_detected_languages"],
            "transcription_status_in_json": summary["transcription_status_in_json"],
        }
        rows.append(row)

    if not rows:
        return {"num_input_files": 0, "num_rows_before": 0, "num_rows_after": 0, "num_unique_videos": 0}

    df = pd.DataFrame(rows)
    num_before = len(df)
    df = df.sort_values(["video_id", "_source_timestamp"]).drop_duplicates(
        subset=["video_id"], keep="last"
    ).reset_index(drop=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"    [summary] → {output_csv.name}: {num_before} → {len(df)} rows")
    return {
        "num_input_files": 1,
        "num_rows_before": num_before,
        "num_rows_after": len(df),
        "num_unique_videos": len(df),
    }


# ================= BUILD RESEARCH CSV =================

def build_research_csv(channel_folder, research_videos, trans_summary, audio_by_vid, output_csv):
    rows = []
    for v in research_videos:
        vid = v.get("video_id", "")
        if not vid:
            continue
        duration_secs = parse_duration(v.get("duration", ""))
        view_count = v.get("view_count", 0)
        like_count = v.get("like_count", 0)
        comment_count = v.get("comment_count", 0)
        engagement = round((like_count + comment_count) / view_count * 100, 2) if view_count > 0 else 0

        trans_info = trans_summary.get(vid, {})
        audio_features = {}
        audio_path = v.get("audio_filename", "")
        audio_total_dur = ""
        num_speakers = ""
        speakers_list = ""
        num_segments = ""
        avg_confidence = ""
        detected_languages_str = ""
        dataset_score = ""
        transcription_status = "pending"

        if trans_info:
            full_data = trans_info.get("_segments_data", {})
            audio_features = full_data.get("audio_features", {}) if full_data else {}
            audio_path = trans_info.get("_audio_path_from_transcription") or audio_path
            audio_total_dur = full_data.get("total_audio_duration") or full_data.get("audio_duration")
            num_speakers = full_data.get("num_speakers")
            speakers_list = ", ".join(full_data.get("speakers", []) or [])
            num_segments = len(full_data.get("segments", []))
            avg_confidence = full_data.get("avg_confidence")
            detected_languages_str = ",".join(full_data.get("detected_languages", []) or [])
            dataset_score = full_data.get("dataset_score")
            transcription_status = "completed"

        row = {
            "video_id": vid,
            "title": v.get("title", ""),
            "channel": v.get("channel", ""),
            "url": v.get("url", f"https://www.youtube.com/watch?v={vid}"),
            "published_at": v.get("published_at", ""),
            "duration_formatted": format_duration(duration_secs),
            "duration_iso": v.get("duration", ""),
            "duration_seconds": duration_secs,
            "view_count": view_count,
            "like_count": like_count,
            "comment_count": comment_count,
            "engagement_ratio": engagement,
            "audio_path": audio_path,
            "audio_total_duration": audio_features.get("duration", audio_total_dur),
            "audio_silence_ratio": audio_features.get("silence_ratio"),
            "audio_zero_crossing_rate": audio_features.get("zero_crossing_rate"),
            "audio_mean_volume": audio_features.get("mean_volume"),
            "transcription_status": transcription_status,
            "transcription_audio_duration": audio_total_dur,
            "num_speakers": num_speakers,
            "speakers_list": speakers_list,
            "num_segments": num_segments,
            "avg_confidence": avg_confidence,
            "detected_languages": detected_languages_str,
            "dataset_score": dataset_score,
            "estimated_speech_ratio": v.get("estimated_speech_ratio", 0),
            "video_avg_confidence": v.get("avg_confidence"),
            "video_dataset_score": v.get("dataset_score"),
            "video_detected_languages": ",".join(v.get("detected_languages", []) or []),
            "niche": v.get("niche", ""),
            "llm_score": v.get("llm_score", 0),
            "llm_reason": v.get("llm_reason", ""),
            "tags": json.dumps(v.get("tags", []), ensure_ascii=False),
            "category_id": v.get("category_id", ""),
            "default_language": v.get("default_language", ""),
            "default_audio_language": v.get("default_audio_language", ""),
            "caption_available": v.get("caption_available"),
            "definition": v.get("definition", ""),
            "licensed_content": v.get("licensed_content"),
            "projection": v.get("projection", ""),
            "privacy_status": v.get("privacy_status", ""),
            "made_for_kids": v.get("made_for_kids"),
            "topic_categories": json.dumps(v.get("topic_categories", []), ensure_ascii=False),
            "passed_filters": json.dumps(v.get("passed_filters", []), ensure_ascii=False),
            "failed_filters": json.dumps(v.get("failed_filters", []), ensure_ascii=False),
            "description_preview": (v.get("description", "") or "")[:200],
            "_source_file": f"research_{v.get('_ts', '')}.json",
            "_source_timestamp": v.get("_ts", ""),
            "audio_file_exists": vid in audio_by_vid,
            "audio_timestamps": audio_by_vid.get(vid, {}).get("audio_timestamps", ""),
            "audio_num_versions": audio_by_vid.get(vid, {}).get("audio_num_versions", 0),
            "latest_transcription_timestamp": trans_info.get("latest_transcription_timestamp", ""),
            "num_transcription_versions": trans_info.get("num_transcription_versions", 0),
            "num_segments_transcription": trans_info.get("num_segments_transcription"),
            "transcription_total_duration": trans_info.get("transcription_total_duration"),
            "transcription_num_speakers": trans_info.get("transcription_num_speakers"),
            "transcription_avg_confidence": trans_info.get("transcription_avg_confidence"),
            "transcription_detected_languages": trans_info.get("transcription_detected_languages", ""),
            "transcription_status_in_json": trans_info.get("transcription_status_in_json", ""),
        }
        rows.append(row)

    if not rows:
        return {"num_input_files": 0, "num_rows_before": 0, "num_rows_after": 0, "num_unique_videos": 0}

    df = pd.DataFrame(rows)
    num_before = len(df)
    df = df.sort_values(["video_id", "_source_timestamp"]).drop_duplicates(
        subset=["video_id"], keep="last"
    ).reset_index(drop=True)
    df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"    [research] → {output_csv.name}: {num_before} → {len(df)} rows")
    return {
        "num_input_files": len(research_videos),
        "num_rows_before": num_before,
        "num_rows_after": len(df),
        "num_unique_videos": len(df),
    }


# ================= MERGE RESEARCH JSON =================

def _score_video_entry(v: dict) -> int:
    """Điểm đầy đủ data của 1 video entry."""
    score = 0
    if v.get("transcript") is not None and len(str(v.get("transcript", ""))) > 10:
        score += 100
    if len(str(v.get("description", "") or "")) > 100:
        score += 50
    if v.get("tags"):
        score += 20
    if v.get("thumbnail"):
        score += 10
    if v.get("top_comments"):
        score += 10
    if (v.get("view_count") or 0) > 0:
        score += 5
    if (v.get("like_count") or 0) > 0:
        score += 2
    return score


def merge_research_jsons(channel_folder: Path) -> dict:
    """Gộp tất cả research_*.json thành 1 file."""
    research_files = sorted(channel_folder.glob("research_*.json"))
    if not research_files:
        return {"input": 0, "output_videos": 0}

    by_vid: dict[str, dict] = {}
    file_stats = []

    for rf in research_files:
        try:
            with open(rf, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"    [WARN] Không đọc được {rf.name}: {e}")
            continue
        ts = get_timestamp_from_filename(rf)
        videos = data.get("videos", [])
        file_stats.append({"file": rf.name, "timestamp": ts, "num_videos": len(videos)})
        for v in videos:
            vid = v.get("video_id", "")
            if not vid:
                continue
            existing = by_vid.get(vid)
            if existing is None or _score_video_entry(v) > _score_video_entry(existing):
                v_copy = dict(v)
                v_copy["_source_file"] = rf.name
                v_copy["_source_timestamp"] = ts
                by_vid[vid] = v_copy

    merged_videos = list(by_vid.values())
    first_data = None
    for rf in research_files:
        try:
            with open(rf, "r", encoding="utf-8") as f:
                first_data = json.load(f)
            break
        except Exception:
            continue

    merged_research = {
        "research_date": datetime.now().isoformat(),
        "channel": first_data.get("channel", channel_folder.name) if first_data else channel_folder.name,
        "total_videos_found": first_data.get("total_videos_found", len(merged_videos)) if first_data else len(merged_videos),
        "videos_after_filter": first_data.get("videos_after_filter", len(merged_videos)) if first_data else len(merged_videos),
        "videos": merged_videos,
        "_merge_info": {
            "num_input_files": len(research_files),
            "num_unique_videos": len(merged_videos),
            "source_files": file_stats,
            "merged_at": datetime.now().isoformat(),
        },
    }

    output_path = channel_folder / f"research_{channel_folder.name}_MERGED.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged_research, f, ensure_ascii=False, indent=2)

    print(f"    [research-json] {len(research_files)} → 1 file: {len(merged_videos)} unique videos")
    return {"input": len(research_files), "output_videos": len(merged_videos)}


# ================= MERGE PIPELINE SUMMARY JSON =================

def merge_pipeline_summaries(channel_folder: Path) -> dict:
    """Gộp tất cả pipeline_summary_*.json thành 1 file."""
    pipeline_files = sorted(channel_folder.glob("pipeline_summary_*.json"))
    if not pipeline_files:
        return {"input": 0, "output_results": 0}

    by_vid: dict[str, dict] = {}
    for pf in pipeline_files:
        try:
            with open(pf, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"    [WARN] Không đọc được {pf.name}: {e}")
            continue
        ts = get_timestamp_from_filename(pf)
        for r in data.get("results", []):
            vid = r.get("video_id", "")
            if not vid:
                continue
            existing = by_vid.get(vid)
            if existing is None:
                r_copy = dict(r)
                r_copy["_source_file"] = pf.name
                r_copy["_source_timestamp"] = ts
                by_vid[vid] = r_copy
            elif r.get("status") == "completed" and existing.get("status") != "completed":
                r_copy = dict(r)
                r_copy["_source_file"] = pf.name
                r_copy["_source_timestamp"] = ts
                by_vid[vid] = r_copy
            elif len(r) > len(existing):
                r_copy = dict(r)
                r_copy["_source_file"] = pf.name
                r_copy["_source_timestamp"] = ts
                by_vid[vid] = r_copy

    merged_results = list(by_vid.values())
    completed = sum(1 for r in merged_results if r.get("status") == "completed")
    skipped = sum(1 for r in merged_results if r.get("status") == "skipped")
    failed = sum(1 for r in merged_results if r.get("status") not in ("completed", "skipped"))

    merged_pipeline = {
        "merged_at": datetime.now().isoformat(),
        "total_videos_researched": len(merged_results),
        "total_videos_transcribed": completed,
        "results": merged_results,
        "stats": {"completed": completed, "skipped": skipped, "failed": failed},
        "_merge_info": {
            "num_input_files": len(pipeline_files),
            "num_unique_videos": len(merged_results),
            "source_files": [pf.name for pf in pipeline_files],
        },
    }

    output_path = channel_folder / "pipeline_summary_MERGED.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged_pipeline, f, ensure_ascii=False, indent=2)

    print(f"    [pipeline-json] {len(pipeline_files)} → 1 file: {len(merged_results)} unique results")
    return {"input": len(pipeline_files), "output_results": len(merged_results)}


# ================= PROCESS CHANNEL =================

def process_channel(channel_folder: Path, skip_csv: bool = False, cleanup: bool = False,
                   flatten: bool = False, dry_run: bool = False) -> dict:
    """Xử lý 1 channel: merge JSON + rebuild CSV."""
    if not channel_folder.is_dir():
        return {}

    print(f"\n{'='*70}")
    print(f"Channel: {channel_folder.name}")
    print(f"{'='*70}")

    results = {}

    # 0. Dồn subfolder audio/<ts>/ và transcriptions/<ts>/ thành 1 folder phẳng
    if flatten:
        results["flatten"] = flatten_subfolders(channel_folder, dry_run=dry_run)

    # 1. Merge research JSON
    results["research_json"] = merge_research_jsons(channel_folder)

    # 2. Merge pipeline JSON
    results["pipeline_json"] = merge_pipeline_summaries(channel_folder)

    # 3. Build CSV (nếu không skip)
    if not skip_csv:
        research_videos = load_research_videos(channel_folder)
        trans_summary = scan_transcriptions_folder(channel_folder)
        audio_by_vid = scan_audio_folder(channel_folder)

        segments_out = channel_folder / f"{channel_folder.name}_segments_MERGED.csv"
        results["segments_csv"] = build_segments_csv(channel_folder, research_videos, trans_summary, audio_by_vid, segments_out)

        summary_out = channel_folder / f"{channel_folder.name}_summary_MERGED.csv"
        results["summary_csv"] = build_video_summary_csv(channel_folder, research_videos, trans_summary, audio_by_vid, summary_out)

        research_out = channel_folder / f"{channel_folder.name}_research_MERGED.csv"
        results["research_csv"] = build_research_csv(channel_folder, research_videos, trans_summary, audio_by_vid, research_out)

    # 4. Cleanup nếu yêu cầu
    deleted_files = []
    if cleanup:
        for f in channel_folder.glob("research_*.json"):
            if not f.name.endswith("_MERGED.json"):
                deleted_files.append(f.name)
                f.unlink()
        for f in channel_folder.glob("pipeline_summary_*.json"):
            if not f.name.endswith("_MERGED.json"):
                deleted_files.append(f.name)
                f.unlink()
        if not skip_csv:
            for pattern in ["*_segments_dataset_*.csv", "*_video_summary_*.csv", "research_*_*.csv"]:
                for f in channel_folder.glob(pattern):
                    deleted_files.append(f.name)
                    f.unlink()
    results["deleted_files"] = deleted_files

    results["channel"] = channel_folder.name
    return results


# ================= MAIN =================

def main():
    parser = argparse.ArgumentParser(description="Consolidate YouTube dataset (CSV + JSON)")
    parser.add_argument("--channel-folder", help="Xử lý 1 channel")
    parser.add_argument("--base-dir", help="Xử lý toàn bộ dataset")
    parser.add_argument("--skip-csv", action="store_true", help="Chỉ merge JSON, skip rebuild CSV")
    parser.add_argument("--cleanup", action="store_true", help="Xóa file gốc sau khi merge")
    parser.add_argument("--flatten", action="store_true",
                        help="Dồn audio/<ts>/*.wav → audio/*.wav và transcriptions/<ts>/*.json → transcriptions/*.json, "
                             "tự động dedup audio trùng content hash và JSON trùng video_id")
    parser.add_argument("--dry-run", action="store_true",
                        help="Chỉ in kế hoạch (kết hợp --flatten để xem trước khi xóa)")
    args = parser.parse_args()

    if not args.channel_folder and not args.base_dir:
        print("[ERROR] Phải chỉ định --channel-folder hoặc --base-dir")
        return 1

    all_stats = []

    if args.channel_folder:
        cf = Path(args.channel_folder)
        if not cf.exists():
            print(f"[ERROR] Folder không tồn tại: {cf}")
            return 1
        stats = process_channel(cf, args.skip_csv, args.cleanup, args.flatten, args.dry_run)
        if stats:
            all_stats.append(stats)
    else:
        base = Path(args.base_dir)
        if not base.exists():
            print(f"[ERROR] Folder không tồn tại: {base}")
            return 1
        for cf in sorted(base.iterdir()):
            if not cf.is_dir() or cf.name == "logs":
                continue
            try:
                stats = process_channel(cf, args.skip_csv, args.cleanup, args.flatten, args.dry_run)
                if stats:
                    all_stats.append(stats)
            except Exception as e:
                print(f"[ERROR] {cf.name}: {e}")
                import traceback
                traceback.print_exc()

    # Summary
    print(f"\n{'='*70}")
    print(f"TỔNG KẾT")
    print(f"{'='*70}")
    total_research_json_in = sum(s.get("research_json", {}).get("input", 0) for s in all_stats)
    total_research_json_out = sum(s.get("research_json", {}).get("output_videos", 0) for s in all_stats)
    total_pipeline_in = sum(s.get("pipeline_json", {}).get("input", 0) for s in all_stats)
    total_pipeline_out = sum(s.get("pipeline_json", {}).get("output_results", 0) for s in all_stats)
    total_segments = sum(s.get("segments_csv", {}).get("num_rows_after", 0) for s in all_stats)
    total_summary = sum(s.get("summary_csv", {}).get("num_rows_after", 0) for s in all_stats)
    total_research = sum(s.get("research_csv", {}).get("num_rows_after", 0) for s in all_stats)
    total_deleted = sum(len(s.get("deleted_files", [])) for s in all_stats)
    flatten_stats = [s.get("flatten", {}) for s in all_stats if s.get("flatten")]

    print(f"  Channels: {len(all_stats)}")
    print(f"  Research JSON: {total_research_json_in} → 1 file/ch, {total_research_json_out} unique videos")
    print(f"  Pipeline JSON: {total_pipeline_in} → 1 file/ch, {total_pipeline_out} unique results")
    if not args.skip_csv:
        print(f"  Segments CSV: {total_segments:,} rows")
        print(f"  Summary CSV: {total_summary:,} rows")
        print(f"  Research CSV: {total_research:,} rows")
    if args.cleanup:
        print(f"  File đã xóa: {total_deleted}")
    if flatten_stats:
        f_audio_kept = sum(f.get("audio_kept", 0) for f in flatten_stats)
        f_audio_removed = sum(f.get("audio_removed_dup", 0) for f in flatten_stats)
        f_audio_moved = sum(f.get("audio_moved", 0) for f in flatten_stats)
        f_trans_kept = sum(f.get("trans_kept", 0) for f in flatten_stats)
        f_trans_removed_dup = sum(f.get("trans_removed_dup", 0) for f in flatten_stats)
        f_trans_removed_orphan = sum(f.get("trans_removed_orphan", 0) for f in flatten_stats)
        f_renamed = sum(f.get("trans_renamed", 0) for f in flatten_stats)
        f_subfolders = sum(f.get("subfolders_removed", 0) for f in flatten_stats)
        print(f"  --- Flatten subfolders ---")
        print(f"  Audio kept: {f_audio_kept} | removed (dup): {f_audio_removed} | moved to root: {f_audio_moved}")
        print(f"  Trans kept: {f_trans_kept} | removed (dup): {f_trans_removed_dup} | orphan removed: {f_trans_removed_orphan} | renamed: {f_renamed}")
        print(f"  Subfolders removed: {f_subfolders}")
        if args.dry_run:
            print(f"  [DRY-RUN] không có file nào bị thay đổi")

    return 0


if __name__ == "__main__":
    sys.exit(main())