#!/usr/bin/env python3
"""
Retro-fix: re-chunk các segments dài trong file transcription JSON cũ.

Áp dụng logic mới (max 18 words, max 8s, soft break tại ',;:').
Dùng cho các file đã được tạo TRƯỚC khi fix _build_segments_from_timestamps.

Usage:
    # Fix 1 file
    python retro_chunk_fix.py path/to/transcription.json

    # Fix tất cả JSON trong 1 folder
    python retro_chunk_fix.py /path/to/transcriptions/20260612_162018/

    # Fix tất cả JSON trong tất cả timestamp folders
    python retro_chunk_fix.py /path/to/transcriptions/
"""
import json
import re
import sys
from pathlib import Path


def rechunk_text(text: str, start: float, end: float,
                 max_words: int = 75, max_duration: float = 30.0) -> list:
    """Chia text dài thành segments theo config mặc định (max 75 từ / 30s)."""
    if not text.strip():
        return []

    # Hard breaks: . ? !
    parts = re.split(r'(?<=[.?!])\s+', text.strip())
    parts = [p.strip() for p in parts if p.strip()]

    # Soft break: , ; : — (nếu segment vẫn quá dài)
    final = []
    for p in parts:
        if len(p.split()) > max_words:
            sub = re.split(r'(?<=[,;:—–-])\s+', p)
            final.extend([s.strip() for s in sub if s.strip()])
        else:
            final.append(p)

    # Force split theo word count nếu vẫn quá dài
    final2 = []
    for p in final:
        if len(p.split()) > int(max_words * 1.5):
            words = p.split()
            for i in range(0, len(words), max_words):
                final2.append(" ".join(words[i:i + max_words]))
        else:
            final2.append(p)

    # Phân bố timing proportional theo char count
    total_chars = sum(len(p) for p in final2) or 1
    duration = end - start
    segments = []
    t = start
    for p in final2:
        ratio = len(p) / total_chars
        seg_dur = duration * ratio
        segments.append({
            "start": round(t, 3),
            "end": round(t + seg_dur, 3),
            "speaker": "SPEAKER_00",
            "text": p,
        })
        t += seg_dur
    if segments:
        segments[-1]["end"] = round(end, 3)
    return segments


def fix_file(fp: Path, max_seg_dur: float = 30.0, max_seg_words: int = 75,
             dry_run: bool = False) -> tuple:
    """Fix 1 file. Trả về (old_count, new_count, longest_s, max_words)."""
    with open(fp) as f:
        data = json.load(f)

    old_segs = data.get('segments', [])
    new_segs = []
    for s in old_segs:
        dur = s['end'] - s['start']
        words = len(s['text'].split())
        if dur > max_seg_dur or words > max_seg_words:
            new_segs.extend(rechunk_text(s['text'], s['start'], s['end']))
        else:
            new_segs.append(s)

    longest = max((s['end'] - s['start'] for s in new_segs), default=0)
    most_words = max((len(s['text'].split()) for s in new_segs), default=0)

    if not dry_run and len(new_segs) != len(old_segs):
        data['segments'] = new_segs
        with open(fp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    return len(old_segs), len(new_segs), longest, most_words


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    target = Path(sys.argv[1])
    dry_run = '--dry-run' in sys.argv

    if target.is_file():
        files = [target]
    elif target.is_dir():
        files = list(target.rglob("*_transcription.json"))
    else:
        print(f"❌ Not found: {target}")
        sys.exit(1)

    print(f"Found {len(files)} file(s){' [DRY-RUN]' if dry_run else ''}\n")
    total_old = total_new = 0
    for fp in sorted(files):
        old, new, longest, words = fix_file(fp, dry_run=dry_run)
        marker = "🔄" if new != old else "✅"
        print(f"{marker} {fp.name}")
        print(f"   {old} → {new} segments | longest={longest:.1f}s | max_words={words}")
        total_old += old
        total_new += new
    print(f"\n📊 Total: {total_old} → {total_new} segments")


if __name__ == "__main__":
    main()
