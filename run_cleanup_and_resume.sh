#!/bin/bash
# Script thực thi dọn dẹp + chạy lại
# USAGE: bash run_cleanup_and_resume.sh [channel_name]
#   Nếu không truyền channel_name → xóa TẤT CẢ marker
#   Nếu truyền channel_name → chỉ xóa marker của kênh đó

set -e

DATASET_DIR="/home/hientran/sythetic_crawl_data/youtube_dataset"
SCRIPT="/home/hientran/sythetic_crawl_data/youtube_researcher_youtube_subs_multi_vpn_v2.py"
CHANNELS_DIR="/home/hientran/sythetic_crawl_data/channels_audio"
TARGET_CHANNEL="${1:-all}"

echo "=========================================="
echo "DỌN DẸP VÀ CHẠY LẠI"
echo "Target: $TARGET_CHANNEL"
echo "=========================================="

# === BƯỚC 1: Xóa file .part / .ytdl nhỏ (< 100MB) ===
echo ""
echo "[1/4] Xóa file .part/.ytdl nhỏ (< 100MB) - giữ lại file lớn để resume..."

if [ "$TARGET_CHANNEL" = "all" ]; then
    find "$DATASET_DIR" \( -name "*.part" -o -name "*.ytdl" \) -type f -size -100M -delete
else
    find "$DATASET_DIR/$TARGET_CHANNEL" \( -name "*.part" -o -name "*.ytdl" \) -type f -size -100M -delete
fi

REMAINING_PARTS=$(find "$DATASET_DIR" \( -name "*.part" -o -name "*.ytdl" \) -type f 2>/dev/null | wc -l)
echo "    Còn lại $REMAINING_PARTS file .part/.ytdl (>= 100MB - sẽ resume)"

# === BƯỚC 2: Xóa marker .no_transcript ===
echo ""
echo "[2/4] Xóa marker .no_transcript..."

if [ "$TARGET_CHANNEL" = "all" ]; then
    find "$DATASET_DIR" -name "*.no_transcript" -type f -delete
else
    find "$DATASET_DIR/$TARGET_CHANNEL" -name "*.no_transcript" -type f -delete
fi

REMAINING_MARKERS=$(find "$DATASET_DIR" -name "*.no_transcript" -type f 2>/dev/null | wc -l)
echo "    Còn lại $REMAINING_MARKERS marker .no_transcript"

# === BƯỚC 3: Xác định channels file cần chạy ===
echo ""
echo "[3/4] Xác định channels file cần chạy..."

if [ "$TARGET_CHANNEL" = "all" ]; then
    # Tìm tất cả channels file
    CHANNELS_FILES=$(ls "$CHANNELS_DIR"/*.txt 2>/dev/null)
    if [ -z "$CHANNELS_FILES" ]; then
        echo "    ⚠️  Không tìm thấy channels file trong $CHANNELS_DIR"
        exit 1
    fi
    echo "    Sẽ chạy $(echo "$CHANNELS_FILES" | wc -l) file kênh"
    CHANNEL_FILE_TO_RUN=""
else
    # Tìm channels file chứa kênh này
    CHANNEL_FILE_TO_RUN=$(grep -l "$TARGET_CHANNEL" "$CHANNELS_DIR"/*.txt 2>/dev/null | head -1)
    if [ -z "$CHANNEL_FILE_TO_RUN" ]; then
        # Tìm theo tên file trùng với tên kênh
        CHANNEL_FILE_TO_RUN="$CHANNELS_DIR/channels_$(echo $TARGET_CHANNEL | tr '[:upper:]' '[:lower:]').txt"
        if [ ! -f "$CHANNEL_FILE_TO_RUN" ]; then
            echo "    ⚠️  Không tìm thấy channels file cho kênh $TARGET_CHANNEL"
            echo "    Gợi ý: tạo file $CHANNELS_DIR/channels_<tên_kenh>.txt với URL kênh"
            exit 1
        fi
    fi
    echo "    Channels file: $CHANNEL_FILE_TO_RUN"
fi

# === BƯỚC 4: Chạy lại script ===
echo ""
echo "[4/4] Chạy lại script..."
echo ""

if [ "$TARGET_CHANNEL" = "all" ]; then
    # Chạy tuần tự từng file
    for f in $CHANNELS_FILES; do
        echo "=========================================="
        echo "Chạy: $f"
        echo "=========================================="
        source /home/hientran/miniconda3/etc/profile.d/conda.sh && \
            conda activate crawl && \
            python "$SCRIPT" \
                --channels-file "$f" \
                --max-results 5000 \
                --max-fetch 50000 \
                --video-delay 2 \
                --skip-existing \
                --use-vpn \
                2>&1 | tee -a "$DATASET_DIR/logs/resume_run_$(date +%Y%m%d_%H%M%S).log"
    done
else
    echo "=========================================="
    echo "Chạy: $CHANNEL_FILE_TO_RUN (kênh: $TARGET_CHANNEL)"
    echo "=========================================="
    source /home/hientran/miniconda3/etc/profile.d/conda.sh && \
        conda activate crawl && \
        python "$SCRIPT" \
            --channels-file "$CHANNEL_FILE_TO_RUN" \
            --max-results 5000 \
            --max-fetch 50000 \
            --video-delay 2 \
            --skip-existing \
            --use-vpn
fi
