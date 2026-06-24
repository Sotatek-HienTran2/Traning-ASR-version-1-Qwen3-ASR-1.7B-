#!/usr/bin/env python3
"""
Migrate cu: doi ten file JSON bản dịch theo title (pattern moi)
va cap nhat file CSV (audio_path) cho khop.

Chay 1 lan sau khi cap nhat code:
    python migrate_rename_to_title.py /home/hientran/sythetic_crawl_data/youtube_dataset

Se lam:
- Voi moi {video_id}_transcription.json, lay title tu JSON,
  ten file moi = {safe_title}_transcription.json
  (khop voi ten audio file tren disk)
- Doi ten file audio neu can (safe_title.wav thay cho video_id.wav)
- Cap nhat audio_path trong JSON
- Cap nhat cot audio_path trong moi file CSV trong dataset
"""

import csv
import json
import re
import sys
import unicodedata
from pathlib import Path


def safe_filename(title: str, fallback: str = "audio", max_length: int = 100) -> str:
    if not title:
        return fallback
    try:
        normalized = unicodedata.normalize("NFKD", title)
        cleaned = "".join(
            ch for ch in normalized if not unicodedata.combining(ch)
        )
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


def migrate(dataset_dir: str):
    dataset = Path(dataset_dir)
    audio_dir = dataset / "audio"
    trans_dir = dataset / "transcriptions"

    if not trans_dir.exists():
        print(f"Khong tim thay {trans_dir}")
        return

    # ===== 1) Migrate JSON files =====
    json_files = sorted(trans_dir.glob("*_transcription.json"))
    print(f"Tim thay {len(json_files)} file JSON bản dịch")

    renamed = 0
    for json_path in json_files:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        title = data.get("title") or data.get("source") or ""
        video_id = data.get("video_id") or json_path.stem.replace("_transcription", "")

        # Ten file moi (theo title, khop voi audio file)
        new_stem = safe_filename(title, fallback=video_id)
        new_json_path = trans_dir / f"{new_stem}_transcription.json"

        # Neu trung ten -> them video_id de phan biet
        if new_json_path.exists() and new_json_path != json_path:
            new_json_path = trans_dir / f"{new_stem}_{video_id}_transcription.json"

        # Cap nhat audio_path trong JSON (neu audio file co ten khac)
        old_audio_name = Path(data.get("audio_path", "")).name
        new_audio_name = f"{new_stem}.wav"
        if old_audio_name and old_audio_name != new_audio_name:
            print(f"  Cap nhat audio_path: {old_audio_name} -> {new_audio_name}")
            data["audio_path"] = new_audio_name

            # Doi ten file audio neu dang ton tai theo video_id
            for ext in [".wav", ".m4a", ".mp3", ".flac", ".opus", ".ogg"]:
                old_audio = audio_dir / f"{video_id}{ext}"
                new_audio = audio_dir / f"{new_stem}{ext}"
                if old_audio.exists() and old_audio != new_audio:
                    if new_audio.exists():
                        new_audio = audio_dir / f"{new_stem}_{video_id}{ext}"
                    print(f"  Doi ten audio: {old_audio.name} -> {new_audio.name}")
                    old_audio.rename(new_audio)
                    break

        # Ghi lai JSON (de cap nhat audio_path) va doi ten
        if new_json_path != json_path:
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            json_path.rename(new_json_path)
            print(f"  Doi ten JSON: {json_path.name} -> {new_json_path.name}")
            renamed += 1
        else:
            # Chi can ghi lai (cap nhat audio_path)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"\nDa doi ten {renamed} file JSON")

    # ===== 2) Cap nhat file CSV =====
    csv_files = list(dataset.glob("*.csv"))
    print(f"\nTim thay {len(csv_files)} file CSV, dang cap nhat audio_path...")

    for csv_path in csv_files:
        updated = False
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames

        if not rows or "video_id" not in fieldnames:
            continue

        # Cache: video_id -> audio_path (doc tu JSON moi)
        audio_cache = {}
        for json_path in trans_dir.glob("*_transcription.json"):
            try:
                with open(json_path, "r", encoding="utf-8") as jf:
                    data = json.load(jf)
                vid = data.get("video_id")
                ap = data.get("audio_path", "")
                if vid and ap:
                    audio_cache[vid] = ap
            except Exception:
                pass

        # Cap nhat cot audio_path
        if "audio_path" in fieldnames:
            for row in rows:
                vid = row.get("video_id")
                if vid in audio_cache and row.get("audio_path") != audio_cache[vid]:
                    row["audio_path"] = audio_cache[vid]
                    updated = True

        if updated:
            with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            print(f"  Cap nhat: {csv_path.name}")
        else:
            print(f"  Khong can cap nhat: {csv_path.name}")

    print("\nMigration hoan tat!")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python migrate_rename_to_title.py <dataset_dir>")
        sys.exit(1)
    migrate(sys.argv[1])
