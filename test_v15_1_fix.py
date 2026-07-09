#!/usr/bin/env python3
"""Test v15.1: xác nhận fix client_empty + rotation list giúp cải thiện vietsub.

Test 3 video:
  - e55LTBydye8 (VN video, 157 auto keys qua tv_embedded)
  - jNQXAC9IVRw (Me at the zoo - test subs manual)
  - kJQP7kiw5Fk (Despacito - test subs manual)

Verify:
  1. tv_embedded, web_embedded, tv → có subs
  2. web → trả EMPTY → status="client_empty" → continue sang client khác
  3. transcribe_with_youtube KHÔNG break khi client_empty
"""
import sys
sys.path.insert(0, '/home/hientran/sythetic_crawl_data')
import importlib.util

spec = importlib.util.spec_from_file_location(
    'v15',
    '/home/hientran/sythetic_crawl_data/youtube_researcher_audio_subs_multi_rotator_v15.py'
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
YouTubeResearcher = mod.YouTubeResearcher

# Test videos
TEST_VIDEOS = [
    "e55LTBydye8",  # VN video
    "jNQXAC9IVRw",  # Me at the zoo
    "kJQP7kiw5Fk",  # Despacito
]

def test_player_clients():
    """Test từng player_client riêng lẻ để verify empty vs non-empty."""
    import yt_dlp

    print("=" * 80)
    print("TEST 1: Verify client_empty fix - test từng player_client")
    print("=" * 80)

    clients_to_test = [
        "tv_embedded", "web_embedded", "tv",
        "android", "ios",
        "web", "web_safari", "web_creator", "mweb",  # Should return EMPTY
        "android_vr",  # Should FAIL
    ]

    for vid in TEST_VIDEOS:
        print(f"\n{'─' * 80}")
        print(f"VIDEO: {vid}")
        print('─' * 80)

        for pc in clients_to_test:
            try:
                ydl_opts = {
                    "quiet": True, "no_warnings": True,
                    "skip_download": True, "ignoreerrors": True,
                    "age_limit": None,
                    "extractor_args": {"youtube": {"player_client": [pc]}},
                }
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(
                        f"https://www.youtube.com/watch?v={vid}",
                        download=False,
                    )
                if not info:
                    print(f"  {pc:20s}: NO INFO")
                    continue
                subs = info.get("subtitles") or {}
                auto = info.get("automatic_captions") or {}
                total_subs = sum(len(v) for v in subs.values())
                total_auto = sum(len(v) for v in auto.values())
                vi_keys_subs = [k for k in subs.keys() if k.startswith("vi")]
                vi_keys_auto = [k for k in auto.keys() if k.startswith("vi")]
                marker = "✅" if (total_subs + total_auto) > 0 else "❌"
                print(f"  {pc:20s}: subs={total_subs:3d} auto={total_auto:3d} "
                      f"VI_keys_subs={vi_keys_subs[:3]} VI_keys_auto={vi_keys_auto[:3]} {marker}")
            except Exception as e:
                print(f"  {pc:20s}: FAIL - {type(e).__name__}: {str(e)[:60]}")


def make_researcher():
    """Tạo YouTubeResearcher instance đơn giản cho test."""
    researcher = YouTubeResearcher.__new__(YouTubeResearcher)
    # Init các attribute cần thiết cho transcribe_with_youtube
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


def test_v15_transcript():
    """Test hàm transcribe_with_youtube của v15.1 để verify fix."""
    print("\n" + "=" * 80)
    print("TEST 2: transcribe_with_youtube với v15.1 fix")
    print("=" * 80)

    researcher = make_researcher()

    for vid in TEST_VIDEOS:
        print(f"\n{'─' * 80}")
        print(f"VIDEO: {vid}")
        print('─' * 80)
        try:
            result = researcher.transcribe_with_youtube(vid)
            if result:
                segs = result.get("segments", [])
                lang = result.get("language", "?")
                is_auto = result.get("is_auto", False)
                print(f"  ✅ SUCCESS: lang={lang} is_auto={is_auto} segs={len(segs)}")
                if segs:
                    print(f"     First segment: {segs[0].get('text', '')[:100]}")
            else:
                print(f"  ❌ FAILED to get transcript")
        except Exception as e:
            print(f"  ❌ EXCEPTION: {type(e).__name__}: {e}")


if __name__ == "__main__":
    test_player_clients()
    print()
    test_v15_transcript()
    print("\n" + "=" * 80)
    print("DONE")
    print("=" * 80)