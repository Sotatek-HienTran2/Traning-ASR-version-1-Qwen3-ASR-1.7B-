#!/usr/bin/env python3
"""
Merge CSV files trong /home/hientran/sythetic_crawl_data/youtube_dataset_1.

Mỗi subfolder (1 kênh) có 3 LOẠI file CSV được tạo ra ở nhiều lần chạy:
  1. *_segments_dataset_*.csv  - segment-level (1 row/segment)
  2. *_video_summary_*.csv      - video-level (1 row/video)
  3. research_*.csv             - research-level (1 row/video, schema khác summary)

Script này gộp TẤT CẢ file CSV cùng loại trong 1 subfolder thành 1 file duy nhất,
lưu cùng tên subfolder + suffix _MERGED:
  - CogaiIT2k2/CogaiIT2k2_segments_MERGED.csv
  - CogaiIT2k2/CogaiIT2k2_summary_MERGED.csv
  - CogaiIT2k2/CogaiIT2k2_research_MERGED.csv

Logic merge:
  - Đọc tất cả file CSV cùng loại
  - Union tất cả columns (fill NaN cho file thiếu column)
  - Loại bỏ duplicate theo key phù hợp với từng loại
  - Sắp xếp theo video_id + timestamp

Bổ sung thông tin từ audio/ và transcriptions/ folder nếu có:
  - audio/<timestamp>/*.wav → check file tồn tại → thêm cột audio_file_exists
  - transcriptions/<timestamp>/*_transcription.json → đọc num_segments, detected_languages
"""

import sys
import json
import re
from pathlib import Path
from typing import Optional

try:
    import pandas as pd
except ImportError:
    print("pip install pandas")
    sys.exit(1)


BASE_DIR = Path("/home/hientran/sythetic_crawl_data/youtube_dataset_1")


def find_csv_files(folder: Path, pattern: str) -> list[Path]:
    """Tìm tất cả file CSV khớp pattern trong folder, sort theo tên."""
    return sorted(folder.glob(pattern))


def get_timestamp_from_filename(path: Path) -> str:
    """Trích timestamp YYYYMMDD_HHMMSS từ tên file (vd: ..._20260613_172811.csv → 20260613_172811)."""
    m = re.search(r"(\d{8}_\d{6})", path.stem)
    return m.group(1) if m else "00000000_000000"


def scan_audio_folder(channel_folder: Path) -> dict[str, set[str]]:
    """
    Quét folder audio/<timestamp>/, trả về dict:
      {video_id: {timestamp1, timestamp2, ...}}
    Map video_id → set các timestamp mà audio của video đó tồn tại.
    """
    audio_dir = channel_folder / "audio"
    if not audio_dir.exists():
        return {}

    result: dict[str, set[str]] = {}
    for ts_dir in audio_dir.iterdir():
        if not ts_dir.is_dir():
            continue
        ts_name = ts_dir.name  # vd: 20260613_120101
        for audio_file in ts_dir.glob("*.wav"):
            # Tên file: {safe_title}.wav hoặc {safe_title}_{video_id}.wav
            stem = audio_file.stem
            # Thử lấy video_id (11 ký tự, bắt đầu bằng chữ hoa/thường/số/_/-)
            # Pattern phổ biến: ..._{video_id} ở cuối
            m = re.search(r"([A-Za-z0-9_-]{11})$", stem)
            if m:
                vid = m.group(1)
            else:
                # Fallback: dùng full stem làm key
                vid = stem
            result.setdefault(vid, set()).add(ts_name)
    return result


def scan_transcriptions_folder(channel_folder: Path) -> dict[str, dict]:
    """
    Quét folder transcriptions/<timestamp>/, đọc file JSON, trả về dict:
      {video_id: {latest_timestamp: {...summary...}, all_timestamps: [...]}}
    """
    trans_dir = channel_folder / "transcriptions"
    if not trans_dir.exists():
        return {}

    # {video_id: [(timestamp, data_dict)]}
    by_vid: dict[str, list[tuple[str, dict]]] = {}

    for ts_dir in sorted(trans_dir.iterdir()):
        if not ts_dir.is_dir():
            continue
        ts_name = ts_dir.name
        for json_file in ts_dir.glob("*_transcription.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                continue

            # Lấy video_id từ data hoặc từ tên file
            vid = data.get("video_id", "")
            if not vid:
                # Fallback: parse tên file
                # Pattern: {safe_title}_transcription.json
                # → cần tìm video_id từ audio_filename hoặc mapping khác
                audio_path = data.get("audio_path", "")
                m = re.search(r"([A-Za-z0-9_-]{11})\.wav", audio_path)
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
            }
            by_vid.setdefault(vid, []).append((ts_name, summary))

    # Lấy bản mới nhất cho mỗi video
    result = {}
    for vid, ts_list in by_vid.items():
        ts_list.sort(key=lambda x: x[0])
        latest_ts, latest_summary = ts_list[-1]
        result[vid] = {
            "latest_transcription_timestamp": latest_ts,
            "num_transcription_versions": len(ts_list),
            **latest_summary,
        }
    return result


def merge_csv_files(
    channel_folder: Path,
    pattern: str,
    output_path: Path,
    dedup_keys: list[str],
    folder_label: str,
    extra_data_by_vid: Optional[dict] = None,
) -> dict:
    """
    Gộp tất cả file CSV khớp `pattern` trong `channel_folder` thành 1 file `output_path`.

    Args:
        channel_folder: folder chứa các file CSV
        pattern: glob pattern (vd: "*_segments_dataset_*.csv")
        output_path: đường dẫn file CSV output
        dedup_keys: list cột dùng để drop_duplicates
        folder_label: nhãn để log
        extra_data_by_vid: dict {video_id: {...extra info...}} để merge thêm

    Returns:
        dict với stats: num_input_files, num_rows_before, num_rows_after, num_unique_videos
    """
    csv_files = find_csv_files(channel_folder, pattern)
    if not csv_files:
        print(f"  [{folder_label}] Không tìm thấy file khớp pattern: {pattern}")
        return {"num_input_files": 0, "num_rows_before": 0, "num_rows_after": 0, "num_unique_videos": 0}

    print(f"  [{folder_label}] Tìm thấy {len(csv_files)} file:")
    for f in csv_files:
        print(f"    - {f.name}")

    # Đọc tất cả file, lưu timestamp để sort
    dfs = []
    for f in csv_files:
        try:
            df = pd.read_csv(f, encoding="utf-8-sig", low_memory=False)
            df["_source_file"] = f.name
            df["_source_timestamp"] = get_timestamp_from_filename(f)
            dfs.append(df)
        except Exception as e:
            print(f"    [WARN] Không đọc được {f.name}: {e}")

    if not dfs:
        print(f"  [{folder_label}] Không đọc được file nào")
        return {"num_input_files": 0, "num_rows_before": 0, "num_rows_after": 0, "num_unique_videos": 0}

    # Union tất cả columns
    all_columns = set()
    for df in dfs:
        all_columns.update(df.columns)
    all_columns = sorted([c for c in all_columns if not c.startswith("_")])

    # Thêm _source columns
    all_columns.extend(["_source_file", "_source_timestamp"])

    # Concat, fill NaN cho column thiếu
    merged = pd.concat(dfs, ignore_index=True, sort=False)
    num_rows_before = len(merged)

    # Đảm bảo tất cả columns tồn tại
    for col in all_columns:
        if col not in merged.columns:
            merged[col] = pd.NA

    # Sort theo video_id + timestamp
    sort_cols = []
    if "video_id" in merged.columns:
        sort_cols.append("video_id")
    if "_source_timestamp" in merged.columns:
        sort_cols.append("_source_timestamp")
    if sort_cols:
        merged = merged.sort_values(sort_cols).reset_index(drop=True)

    # Drop duplicates theo key
    valid_keys = [k for k in dedup_keys if k in merged.columns]
    if valid_keys:
        before = len(merged)
        # Giữ bản MỚI NHẤT (last) theo sort
        merged = merged.drop_duplicates(subset=valid_keys, keep="last").reset_index(drop=True)
        after = len(merged)
        print(f"  [{folder_label}] Drop duplicates theo {valid_keys}: {before} → {after} rows")
    else:
        after = num_rows_before
        print(f"  [{folder_label}] Không có dedup_keys hợp lệ, giữ nguyên {after} rows")

    # Merge extra data (audio, transcription)
    if extra_data_by_vid and "video_id" in merged.columns:
        extra_df = pd.DataFrame.from_dict(extra_data_by_vid, orient="index")
        extra_df.index.name = "video_id"
        extra_df = extra_df.reset_index()
        # Left join để không mất row nào
        merged = merged.merge(extra_df, on="video_id", how="left", suffixes=("", "_extra"))

    # Ghi file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(output_path, index=False, encoding="utf-8-sig")

    # Stats
    num_unique_videos = merged["video_id"].nunique() if "video_id" in merged.columns else 0
    print(f"  [{folder_label}] → {output_path.name}: {len(merged)} rows, {num_unique_videos} unique videos")
    return {
        "num_input_files": len(csv_files),
        "num_rows_before": num_rows_before,
        "num_rows_after": len(merged),
        "num_unique_videos": num_unique_videos,
    }


def process_channel(channel_folder: Path) -> dict:
    """
    Xử lý 1 subfolder (1 kênh): merge 3 loại CSV.
    """
    if not channel_folder.is_dir():
        return {}

    channel_name = channel_folder.name
    print(f"\n{'=' * 80}")
    print(f"Channel: {channel_name}")
    print(f"{'=' * 80}")

    # Scan audio + transcriptions trước
    audio_by_vid = scan_audio_folder(channel_folder)
    trans_by_vid = scan_transcriptions_folder(channel_folder)

    # Merge audio + transcription data
    extra_data: dict[str, dict] = {}
    all_vids = set(audio_by_vid.keys()) | set(trans_by_vid.keys())
    for vid in all_vids:
        info = {}
        if vid in audio_by_vid:
            ts_set = audio_by_vid[vid]
            info["audio_file_exists"] = True
            info["audio_timestamps"] = ",".join(sorted(ts_set))
            info["audio_num_versions"] = len(ts_set)
        else:
            info["audio_file_exists"] = False
            info["audio_timestamps"] = ""
            info["audio_num_versions"] = 0
        if vid in trans_by_vid:
            info.update(trans_by_vid[vid])
        extra_data[vid] = info

    results = {}

    # 1. Merge segments CSV
    segments_out = channel_folder / f"{channel_name}_segments_MERGED.csv"
    results["segments"] = merge_csv_files(
        channel_folder=channel_folder,
        pattern="*_segments_dataset_*.csv",
        output_path=segments_out,
        dedup_keys=["video_id", "segment_start"],
        folder_label="segments",
        extra_data_by_vid=extra_data,
    )

    # 2. Merge video summary CSV
    summary_out = channel_folder / f"{channel_name}_summary_MERGED.csv"
    results["summary"] = merge_csv_files(
        channel_folder=channel_folder,
        pattern="*_video_summary_*.csv",
        output_path=summary_out,
        dedup_keys=["video_id"],
        folder_label="summary",
        extra_data_by_vid=extra_data,
    )

    # 3. Merge research CSV
    research_out = channel_folder / f"{channel_name}_research_MERGED.csv"
    results["research"] = merge_csv_files(
        channel_folder=channel_folder,
        pattern="research_*.csv",
        output_path=research_out,
        dedup_keys=["video_id"],
        folder_label="research",
        extra_data_by_vid=extra_data,
    )

    return results


def main():
    if not BASE_DIR.exists():
        print(f"Folder không tồn tại: {BASE_DIR}")
        sys.exit(1)

    # Lấy danh sách subfolder
    channel_folders = sorted([
        d for d in BASE_DIR.iterdir()
        if d.is_dir() and d.name not in ("logs",)
    ])

    print(f"Base dir: {BASE_DIR}")
    print(f"Tìm thấy {len(channel_folders)} subfolder (kênh)")
    print(f"Loại trừ: logs/\n")

    all_stats = []
    for cf in channel_folders:
        try:
            stats = process_channel(cf)
            if stats:
                stats["channel"] = cf.name
                all_stats.append(stats)
        except Exception as e:
            print(f"\n[ERROR] Lỗi xử lý {cf.name}: {e}")
            import traceback
            traceback.print_exc()

    # Summary
    print(f"\n{'=' * 80}")
    print(f"TỔNG KẾT")
    print(f"{'=' * 80}")
    print(f"{'Channel':<40} {'Segments':<12} {'Summary':<12} {'Research':<12}")
    print("-" * 80)
    for s in all_stats:
        seg = f"{s.get('segments', {}).get('num_rows_after', 0)}/{s.get('segments', {}).get('num_input_files', 0)}f"
        summ = f"{s.get('summary', {}).get('num_rows_after', 0)}/{s.get('summary', {}).get('num_input_files', 0)}f"
        res = f"{s.get('research', {}).get('num_rows_after', 0)}/{s.get('research', {}).get('num_input_files', 0)}f"
        print(f"{s['channel']:<40} {seg:<12} {summ:<12} {res:<12}")

    # Tạo summary JSON ở root
    summary_path = BASE_DIR / "_merge_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(all_stats, f, ensure_ascii=False, indent=2)
    print(f"\nSummary: {summary_path}")


if __name__ == "__main__":
    main()
