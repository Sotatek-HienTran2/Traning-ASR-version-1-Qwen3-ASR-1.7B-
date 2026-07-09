#!/usr/bin/env python3
"""Test v15.2: xác nhận FIX #5 skip URL trùng gây miss subs + verify fix skip theo (proxy, key).

Bug reproduce từ log:
  [v15-transcript] attempt 1/3 via client=tv_embedded proxy=DIRECT
  [ytdlp-subs] using cached sub URLs (skip yt-dlp extract)
  [ytdlp-subs] (1/2) trying key='vi-orig' [automatic_captions] score=20 format=json3 → downloading...
    [ytdlp-subs] download sub FAILED for key='vi-orig': ReadTimeout: ...
  [ytdlp-subs] skip key='vi' (URL đã thử fail trước đó)        ← BUG!
  [ytdlp-subs] ❌ tried 2 VI key(s) — all failed

Phân tích:
  - 'vi-orig' và 'vi' trong automatic_captions thường CHIA SẺ CÙNG URL
    (YouTube chỉ gen 1 transcript, các key khác nhau reference cùng URL).
  - Khi URL fail vì timeout → FIX #5 add URL vào tried_failed_urls.
  - Key 'vi' tiếp theo cùng URL → bị skip → mất cơ hội retry với proxy khác.

Fix:
  - Track (proxy_url, sub_url) thay vì chỉ sub_url.
  - Skip CHỈ khi (proxy_url, sub_url) đã fail trước đó.
  - Nếu proxy khác → vẫn thử (có thể work).

Cách chạy:
  python test_v15_skip_url_bug.py
"""
import sys
sys.path.insert(0, '/home/hientran/sythetic_crawl_data')
import importlib.util
import time

# Load v15 module
spec = importlib.util.spec_from_file_location(
    'v15',
    '/home/hientran/sythetic_crawl_data/youtube_researcher_audio_subs_multi_rotator_v15.py'
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
YouTubeResearcher = mod.YouTubeResearcher

# Real VN video đã verify có VI subs (test_v15_1_fix.py:6)
TEST_VIDEOS = [
    "e55LTBydye8",  # VN video - 157 auto keys qua tv_embedded
]


def make_researcher():
    """Tạo YouTubeResearcher instance cho test (giống test_v15_1_10videos.py)."""
    researcher = YouTubeResearcher.__new__(YouTubeResearcher)
    researcher._direct_blocked = False
    researcher._audio_escalated = False
    researcher._transcript_escalated = False
    researcher._rotator = None
    researcher._audio_rotator = None
    researcher._transcript_rotator = None
    researcher._v14_api_fallback_enabled = True
    researcher._v14_api_fallback_langs = ["vi", "en"]
    researcher._v14_vi_priority = "auto_first"
    researcher._v15_player_client_rotate = True
    researcher._v15_player_clients = mod.PLAYER_CLIENT_ROTATION_LIST
    return researcher


def test_skip_url_bug():
    """Test reproduce bug FIX #5: skip URL trùng trong cùng attempt."""
    print("=" * 80)
    print("TEST 1: Verify bug FIX #5 (skip URL trùng)")
    print("=" * 80)
    print("Mục tiêu: trigger scenario 'vi-orig fail → vi bị skip'")
    print()

    researcher = make_researcher()

    # NOTE: KHÔNG patch _pick_best_sub_url ở test này.
    # Real scenario: vi-orig và vi thật sự có cùng URL → code sẽ tự skip.
    # Test này chỉ chạy bình thường + quan sát log.

    for vid in TEST_VIDEOS:
        print(f"\n{'─' * 80}")
        print(f"VIDEO: {vid}")
        print('─' * 80)

        t0 = time.time()
        result = researcher.transcribe_with_youtube(vid)
        elapsed = time.time() - t0

        if result:
            segs = result.get("segments", [])
            lang = result.get("language", "?")
            is_auto = result.get("is_auto", False)
            print(f"\n  ✅ SUCCESS: lang={lang} is_auto={is_auto} "
                  f"segs={len(segs)} elapsed={elapsed:.1f}s")
        else:
            print(f"\n  ❌ FAILED: no transcript elapsed={elapsed:.1f}s")


def test_skip_url_count():
    """Test đếm số lần bị skip URL trùng khi gặp timeout."""
    print("\n" + "=" * 80)
    print("TEST 2: Đếm số lần bị skip URL trùng")
    print("=" * 80)
    print("Mục tiêu: trigger timeout → quan sát log 'skip key (URL đã thử fail)'")
    print()

    researcher = make_researcher()

    # Patch _pick_best_sub_url để trả về URL không tồn tại (force 404/timeout)
    _orig_pick = mod.YouTubeResearcher._pick_best_sub_url

    def _forced_bad_url(entries):
        if not entries:
            return None, None
        return ("https://www.youtube.com/api/timedtext?v=FAKE&lang=vi&fmt=json3", "json3")

    # Wrap as staticmethod (vì _pick_best_sub_url là @staticmethod)
    mod.YouTubeResearcher._pick_best_sub_url = staticmethod(_forced_bad_url)

    for vid in TEST_VIDEOS:
        print(f"\n{'─' * 80}")
        print(f"VIDEO: {vid} (URL giả → force fail)")
        print('─' * 80)

        t0 = time.time()
        result = researcher.transcribe_with_youtube(vid)
        elapsed = time.time() - t0

        # Restore
        mod.YouTubeResearcher._pick_best_sub_url = _orig_pick

        if result:
            segs = result.get("segments", [])
            print(f"\n  ⚠️ UNEXPECTED: got result lang={result.get('language')} "
                  f"segs={len(segs)} elapsed={elapsed:.1f}s")
        else:
            print(f"\n  ❌ FAILED (expected): no transcript elapsed={elapsed:.1f}s")
        print()
        print("  👉 Xem log ở trên: nếu thấy 'skip key=... (URL đã thử fail'")
        print("     thì bug FIX #5 đang active.")
        print("  👉 Nếu thấy 'download sub FAILED' cho NHIỀU key (không skip),")
        print("     thì fix skip (proxy, key) đang hoạt động.")


def test_real_video_bug_observation():
    """Test trên video thật: chạy 3 lần, đo baseline skip count."""
    print("\n" + "=" * 80)
    print("TEST 3: Real video e55LTBydye8 (baseline)")
    print("=" * 80)
    print("Mục tiêu: đo tỷ lệ thành công + đếm skip URL trùng trong log")
    print()

    researcher = make_researcher()

    for i, vid in enumerate(TEST_VIDEOS, 1):
        print(f"\n{'─' * 80}")
        print(f"  RUN {i}/{len(TEST_VIDEOS)}: VIDEO {vid}")
        print('─' * 80)

        t0 = time.time()
        result = researcher.transcribe_with_youtube(vid)
        elapsed = time.time() - t0

        if result:
            segs = result.get("segments", [])
            lang = result.get("language", "?")
            is_auto = result.get("is_auto", False)
            print(f"\n  ✅ SUCCESS: lang={lang} is_auto={is_auto} "
                  f"segs={len(segs)} elapsed={elapsed:.1f}s")
        else:
            print(f"\n  ❌ FAILED: no transcript elapsed={elapsed:.1f}s")

        print()
        print("  👉 Xem log ở trên: nếu thấy 'URL đã thử fail trước đó'")
        print("     cho nhiều key → bug FIX #5 đang ảnh hưởng.")


if __name__ == "__main__":
    print("=" * 80)
    print("TEST v15.2: Verify FIX #5 skip URL trùng + skip (proxy, key)")
    print("=" * 80)
    print()

    # Test 1: reproduce bug bằng cách force cùng URL
    test_skip_url_bug()

    # Test 2: đếm skip count với URL giả
    test_skip_url_count()

    # Test 3: real video baseline
    test_real_video_bug_observation()

    print("\n" + "=" * 80)
    print("DONE")
    print("=" * 80)
    print()
    print("KẾT QUẢ MONG ĐỢI SAU KHI FIX:")
    print("  - Nếu chưa fix: thấy 'skip key=vi (URL đã thử fail trước đó)'")
    print("  - Nếu đã fix (skip theo (proxy, key)):")
    print("    * Trong test 1: 'vi' vẫn được thử (vì proxy=None giống vi-orig → skip)")
    print("      nhưng với URL khác (nếu có) thì vẫn retry.")
    print("    * Trong test 2: tất cả key đều retry (vì proxy=None giống nhau → skip,")
    print("      nhưng log sẽ rõ ràng hơn về việc 'cùng proxy' vs 'khác proxy'.")
