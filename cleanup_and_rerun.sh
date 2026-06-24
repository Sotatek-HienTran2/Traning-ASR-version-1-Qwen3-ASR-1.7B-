#!/bin/bash
# Script dọn dẹp marker .no_transcript và file .part/.ytdl lỗi
# Sau đó chạy lại youtube_researcher để tải audio

set -e

DATASET_DIR="/home/hientran/sythetic_crawl_data/youtube_dataset"
SCRIPT="/home/hientran/sythetic_crawl_data/youtube_researcher_youtube_subs_multi_vpn_v2.py"

echo "=========================================="
echo "BƯỚC 1: Thống kê trước khi xóa"
echo "=========================================="
MARKER_COUNT=$(find "$DATASET_DIR" -name "*.no_transcript" -type f 2>/dev/null | wc -l)
PART_COUNT=$(find "$DATASET_DIR" \( -name "*.part" -o -name "*.ytdl" \) -type f 2>/dev/null | wc -l)
PART_SIZE=$(find "$DATASET_DIR" \( -name "*.part" -o -name "*.ytdl" \) -type f -exec du -b {} \; 2>/dev/null | awk '{sum+=$1} END {print sum/1024/1024}')

echo "  Marker .no_transcript : $MARKER_COUNT file"
echo "  File .part/.ytdl      : $PART_COUNT file (${PART_SIZE} MB)"
echo ""

# === BƯỚC 2: Xóa file .part / .ytdl nhỏ (< 100MB) ===
# File lớn (>= 100MB) có thể resume được, KHÔNG xóa
echo "=========================================="
echo "BƯỚC 2: Xóa file .part/.ytdl nhỏ (< 100MB)"
echo "=========================================="
SMALL_PARTS=$(find "$DATASET_DIR" \( -name "*.part" -o -name "*.ytdl" \) -type f -size -100M 2>/dev/null)
if [ -n "$SMALL_PARTS" ]; then
    echo "$SMALL_PARTS" | wc -l | xargs echo "  Sẽ xóa:"
    echo "$SMALL_PARTS" | while read f; do
        echo "    rm -f \"$f\""
    done
    # Bỏ comment dòng dưới để thực sự xóa:
    # echo "$SMALL_PARTS" | xargs rm -f
    echo "  ⚠️  CHƯA XÓA - bỏ comment dòng 'xargs rm -f' phía trên để chạy thật"
else
    echo "  Không có file .part/.ytdl nhỏ nào."
fi
echo ""

# === BƯỚC 3: Xóa marker .no_transcript ===
# Tùy chọn: chỉ xóa marker cho kênh cụ thể
echo "=========================================="
echo "BƯỚC 3: Xóa marker .no_transcript"
echo "=========================================="
echo "  Tổng số marker: $MARKER_COUNT"
echo ""
echo "  ⚠️  CẢNH BÁO: Xóa marker sẽ khiến script thử lại transcribe_with_youtube()"
echo "      cho tất cả video. Nếu VPN US-free vẫn bị YouTube chặn,"
echo "      script sẽ tạo lại marker .no_transcript ngay."
echo ""
echo "  Gợi ý: chỉ xóa marker của 1 kênh cụ thể trước để test."
echo "  Ví dụ: rm -f $DATASET_DIR/suluoc/transcriptions/*/*.no_transcript"
echo ""

# === BƯỚC 4: Hướng dẫn chạy lại ===
echo "=========================================="
echo "BƯỚC 4: Chạy lại script (sau khi xóa marker)"
echo "=========================================="
echo ""
echo "  # 1. Chạy lại với --skip-existing (chỉ tải phần còn thiếu):"
echo "  source /home/hientran/miniconda3/etc/profile.d/conda.sh && \\"
echo "    conda activate crawl && \\"
echo "    python $SCRIPT \\"
echo "      --channels-file /home/hientran/sythetic_crawl_data/channels_audio/channels_lich_su.txt \\"
echo "      --max-results 5000 --max-fetch 50000 \\"
echo "      --video-delay 2 --skip-existing --use-vpn"
echo ""
echo "  # 2. Nếu muốn tải lại từ đầu (kể cả video đã có audio):"
echo "  # Thêm --force-retranscribe"
echo ""
echo "  # 3. Nếu muốn chỉ tải audio, KHÔNG transcribe:"
echo "  # Thêm --no-transcribe (nhưng sẽ KHÔNG tạo JSON transcriptions)"
echo ""
