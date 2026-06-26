#!/usr/bin/env python3
"""
Patch v7 → v9: thay thế block download cũ bằng SmartDownloader.

Usage:
    cd /home/hientran/sythetic_crawl_data
    python patch_v7_to_v9.py

Kết quả:
    - File gốc được backup: youtube_researcher_audio_subs_multi_rotator_v7.py.bak
    - File mới: youtube_researcher_audio_subs_multi_rotator_v9.py
"""

import re
import shutil
import sys
from pathlib import Path

SRC = Path("/home/hientran/sythetic_crawl_data/youtube_researcher_audio_subs_multi_rotator_v7.py")
DST = Path("/home/hientran/sythetic_crawl_data/youtube_researcher_audio_subs_multi_rotator_v9.py")
BAK = SRC.with_suffix(".py.bak")

V9_SMART_HEADER = """

# === v9: Smart downloader with stuck-IP detection ===
V9_SMART_AVAILABLE = False
try:
    from v9_smart_downloader import (
        get_smart_downloader, classify_error, SmartDownloader,
    )
    V9_SMART_AVAILABLE = True
except Exception as _v9e:
    print(f"  [v9-warn] SmartDownloader không khả dụng: {_v9e}", flush=True)
"""


def main():
    if not SRC.exists():
        print(f"❌ Không tìm thấy {SRC}", file=sys.stderr)
        sys.exit(1)

    # 1. Backup
    if not BAK.exists():
        shutil.copy2(SRC, BAK)
        print(f"✅ Backup: {BAK}")
    else:
        print(f"⚠️  Backup đã tồn tại: {BAK}")

    # 2. Đọc source
    src = SRC.read_text(encoding="utf-8")
    print(f"📖 Source size: {len(src):,} chars")

    # 3. Chèn v9 import + flag V9_SMART_AVAILABLE ở đầu file
    #    (v7 lazy-import yt_dlp bên trong hàm, không có top-level import)
    if "V9_SMART_AVAILABLE" not in src:
        # Tìm dòng cuối block import standard library
        # Thường là "from dotenv import load_dotenv" trong v7
        m = re.search(r"^(from dotenv import load_dotenv[^\n]*\n)", src, flags=re.MULTILINE)
        if m:
            insert_pos = m.end()
            src = src[:insert_pos] + V9_SMART_HEADER + src[insert_pos:]
            print(f"✅ Insert v9 header sau 'from dotenv import load_dotenv' (line ~{src[:insert_pos].count(chr(10))})")
        else:
            # Fallback: chèn ngay sau "# ===" header đầu tiên
            m2 = re.search(r"^(import json\n)", src, flags=re.MULTILINE)
            if m2:
                insert_pos = m2.end()
                src = src[:insert_pos] + V9_SMART_HEADER + src[insert_pos:]
                print(f"✅ Insert v9 header sau 'import json' (fallback)")
            else:
                # Fallback cuối: chèn ở đầu file
                src = V9_SMART_HEADER + src
                print("⚠️  Insert v9 header ở đầu file (last resort)")
    else:
        print("⏭️  V9_SMART_AVAILABLE đã có sẵn")

    # 4. Patch init YouTubeResearcher để tạo smart_downloader
    # Tìm chỗ __init__ có 'self._http500_detector = HTTP500Detector('
    init_pattern = r"(self\._http500_detector = HTTP500Detector\([^)]*\))"
    init_replace = r"""\1
        # === v9: Smart downloader for stuck-IP detection ===
        self._smart_dl = get_smart_downloader(
            ip_controller=getattr(self, '_audio_ip_ctl', None),
            audio_rotator=getattr(self, '_audio_rotator', None),
            http500_detector=self._http500_detector,
        ) if V9_SMART_AVAILABLE else None"""
    if "self._smart_dl" not in src:
        new_src, n = re.subn(init_pattern, init_replace, src, count=1)
        if n:
            src = new_src
            print("✅ Inject self._smart_dl vào __init__")
        else:
            print("⚠️  Không tìm thấy self._http500_detector init — manual edit")

    # 5. Patch block download (3 nhánh if/elif/else)
    # Tìm đoạn:
    #   with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    #       info = ydl.extract_info(video.url, download=True)
    #       filename = ydl.prepare_filename(info)
    # và thay bằng gọi SmartDownloader.
    old_block_pattern = re.compile(
        r"(?:^(\s+))with yt_dlp\.YoutubeDL\(ydl_opts\) as ydl:\n"
        r"\s+info = ydl\.extract_info\(video\.url, download=True\)\n"
        r"\s+filename = ydl\.prepare_filename\(info\)",
        flags=re.MULTILINE,
    )

    def replace_block(m: re.Match) -> str:
        indent = m.group(1)
        # Giữ nguyên indent của nhánh if/elif/else đó
        return (
            f"{indent}if self._smart_dl is not None:\n"
            f"{indent}    # === v9: Smart retry — đổi IP NGAY khi timeout ===\n"
            f"{indent}    _smart_result = self._smart_dl.download_with_smart_retry(\n"
            f"{indent}        url=video.url,\n"
            f"{indent}        ydl_opts=ydl_opts,\n"
            f"{indent}        progress_hook=_audio_progress_hook,\n"
            f"{indent}    )\n"
            f"{indent}    if not _smart_result['ok']:\n"
            f"{indent}        raise RuntimeError(\n"
            f"{indent}            f\"SmartDownloader fail sau {{_smart_result['attempts']}} attempts: \"\n"
            f"{indent}            f\"{{_smart_result['last_error'][:200]}}\"\n"
            f"{indent}        )\n"
            f"{indent}    info = _smart_result['info']\n"
            f"{indent}    filename = _smart_result['filename']\n"
            f"{indent}else:\n"
            f"{indent}    # Fallback v7: chạy ydl.extract_info gốc\n"
            f"{indent}    with yt_dlp.YoutubeDL(ydl_opts) as ydl:\n"
            f"{indent}        info = ydl.extract_info(video.url, download=True)\n"
            f"{indent}        filename = ydl.prepare_filename(info)"
        )

    new_src, n_block = old_block_pattern.subn(replace_block, src)
    if n_block:
        src = new_src
        print(f"✅ Patch {n_block} block(s) download → SmartDownloader")
    else:
        print("⚠️  Không tìm thấy block download — kiểm tra indent/format")

    # 6. Patch version string
    src = re.sub(
        r"^v7 = youtube_researcher_audio_subs_multi_rotator_v6\.py.*$",
        "v9 = youtube_researcher_audio_subs_multi_rotator_v7.py + Smart stuck-IP detection\n"
        "(đổi IP ngay lần đầu timeout thay vì đợi yt-dlp retry 5×30s)",
        src,
        count=1,
        flags=re.MULTILINE,
    )

    # 7. Ghi file mới
    DST.write_text(src, encoding="utf-8")
    print(f"💾 Saved: {DST} ({len(src):,} chars)")
    print()
    print("🚀 Test nhanh:")
    print(f"   python -c \"import ast; ast.parse(open('{DST}').read()); print('syntax OK')\"")
    print()
    print("🚀 Chạy dry-run:")
    print(f"   python {DST.name} --help  # nếu có argparse")


if __name__ == "__main__":
    main()