#!/usr/bin/env python3
"""
YouTube Researcher -- AUDIO + YOUTUBE SUBS (MULTI-ROTATOR) -- v15
================================================================================

v15 = v14 + AUTO-DETECT NETWORK (không hardcode wifi nhà, chạy được trên mạng
       nội bộ công ty hoặc bất kỳ network interface nào).

Lịch sử:
  v10: SmartDownloader + 6 cải tiến cycle IP khi IP bị Google rate-limit.
  v11: Per-instance tunnel kill — fix vấn đề `kill_all_vpn_tunnels()` ở v10
       kill TẤT CẢ openvpn của user (kể cả instance khác khi cycle IP).
  v12: Smart downloader thêm mid-download rotate hooks.
  v13: FIX bug "MidDownloadRotate bị outer try/except nuốt".
  v14: VIETSUB ENGINE NÂNG CẤP + youtube-transcript-api FALLBACK.
  v15: Auto-detect gateway/interface — chạy trên mạng nội bộ công ty.
       1) VI-sub scoring engine (`_score_vi_subs`): thay vì chỉ lấy key
          bắt đầu bằng 'vi', v14 SCORE TẤT CẢ key VI (vi-orig, vi-VN,
          vi-VN-x-*, vi, vi-*) theo priority "auto_first" hoặc "manual_first".
          → Tăng tỉ lệ tìm được vi-orig (auto-gen gốc của YouTube).
       2) Best sub-URL picker (`_pick_best_sub_url`): ưu tiên json3 > vtt >
          ttml > srv3/2/1, check URL hợp lệ trước khi dùng.
       3) URL validity check (`_is_valid_subtitle_url`).
       4) Cookies hot-reload (`_reload_cookies_if_changed`): check mtime
          cookies.txt mỗi 60s → reload nếu user cập nhật cookies.
       5) youtube-transcript-api FALLBACK (`_get_youtube_transcript_via_api`):
          khi yt-dlp fail (captcha/bot check), fallback sang
          `youtube-transcript-api` library gọi timedtext API (endpoint KHÁC
          yt-dlp, thường bypass captcha tốt hơn).
       6) Multi-key fallback (vi-orig → vi-VN → vi-VN-x-* → vi → vi-*):
          nếu key A fail download/parse → thử key B, ...
       7) Constructor params mới:
          - vi_sub_priority: "auto_first" | "manual_first" (mặc định "auto_first")
          - no_api_fallback: bool (mặc định False = BẬT API fallback)
          - api_fallback_langs: str (mặc định "vi,en")
          - no_marker_ttl_days, respect_no_transcript_marker, retry_no_transcript, ...
       8) transcribe_with_youtube() cải tiến:
          - Multi-attempt retry (2 attempts với proxy rotation)
          - Sau khi yt-dlp fail → fallback youtube-transcript-api
          - Track transcript_source metadata để debug
          - Log verbose để biết candidate nào đang được thử

GIỮ NGUYÊN TỪ V13:
  - SmartDownloader + 11 patterns + catch-all + MidDownloadRotate fix
  - HTTP500Detector stall fire mỗi 30s
  - AudioIPController force_real_after_2_fake_fails
  - Per-instance tunnel kill (`kill_tunnel_by_instance`)
  - Live unavailable marker
  - 3 rotator tách biệt (audio / transcript / metadata)

Output:
  <output>/
    audio/<run_ts>/*.wav                       : audio (BẮT BUỘC)
    transcriptions/<run_ts>/*_transcription.json : {URL + metadata + segments}
    pipeline_summary_<run_ts>.json
    <channel>_segments_minimal_<run_ts>.csv
    research_<channel>_<run_ts>.json
    _multi_channel_summary_<run_ts>.json
    logs/crawl_<instance_id>.log

CÁCH DÙNG:
    # Chạy 1 instance
    python youtube_researcher_audio_subs_multi_rotator_v14.py \
        --channels-file ./channels_audio/channels_khoa_hoc_2.txt \
        --output ./youtube_dataset_resumable \
        --use-vpn --vpn-isolated \
        --instance-id inst1 \
        --video-delay 5 --skip-existing

    # Tùy chỉnh vi-sub priority + API fallback
    python ..._v14.py \
        --vi-sub-priority manual_first \
        --no-api-fallback \
        --api-fallback-langs vi,en

    # Cleanup thủ công tunnel của 1 instance
    python -c "from youtube_researcher_audio_subs_multi_rotator_v14 import \
        kill_tunnel_by_instance; kill_tunnel_by_instance('inst1')"

    # Rebuild CSV/summary từ JSON có sẵn (không gọi API, không tải audio)
    python youtube_researcher_audio_subs_multi_rotator_v14.py \
        --rebuild-from-transcripts
"""

import json
import os
import re
import sys
import time
import threading
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv


# === v13: Smart downloader with stuck-IP detection + FIX mid-download slow-speed rotation ===
# v13: clone v12 + FIX bug "MidDownloadRotate bị outer try/except nuốt".
#
# *** V12 BUG (đã fix trong v13) ***
# Trong v12, hook raise MidDownloadRotate ngay giữa chunk download.
# Inner `except MidDownloadRotate: raise` re-raise đúng, NHƯNG outer
# `except Exception as hook_err:` ở cuối hook catch lại, chỉ in log,
# nuốt mất exception → yt-dlp KHÔNG BAO GIỜ nhận được MidDownloadRotate
# → SmartDownloader KHÔNG BAO GIỜ catch MidDownloadRotate
# → KHÔNG BAO GIỜ rotate mid-download.
#
# *** FIX V13 (3 lớp) ***
#   1. MidDownloadRotate giờ là subclass của DownloadError → yt-dlp propagate
#      đúng (v13_smart_downloader.py đã sửa).
#   2. Hook vẫn raise + còn ghi "marker file" /tmp/v13_mid_download_rotate.*
#      để SmartDownloader detect qua fallback path (kể cả khi yt-dlp nuốt).
#   3. Hook KHÔNG bị nuốt nữa — outer except được rewrite để log warning
#      cho non-MidDownloadRotate exceptions, còn MidDownloadRotate được
#      re-raise đúng cách.
V13_SMART_AVAILABLE = False
try:
    from v13_smart_downloader import (
        get_smart_downloader, classify_error, SmartDownloader,
        MidDownloadRotate,
        write_mid_download_marker, consume_mid_download_marker,
    )
    V13_SMART_AVAILABLE = True
except Exception as _v13e:
    # Fallback: thử v12 (MidDownloadRotate có thể không kế thừa DownloadError)
    try:
        from v12_smart_downloader import (
            get_smart_downloader, classify_error, SmartDownloader,
            MidDownloadRotate,
        )
        write_mid_download_marker = None  # type: ignore[assignment]
        consume_mid_download_marker = None  # type: ignore[assignment]
        V13_SMART_AVAILABLE = True
        print(f"  [v13-warn] Dùng v12_smart_downloader (fallback). "
              f"MidDownloadRotate có thể KHÔNG kế thừa DownloadError — "
              f"cơ chế marker file sẽ KHÔNG hoạt động.",
              flush=True)
    except Exception as _v12e:
        # Fallback cuối: thử v11
        try:
            from v11_smart_downloader import (
                get_smart_downloader, classify_error, SmartDownloader,
            )
            MidDownloadRotate = None
            write_mid_download_marker = None  # type: ignore[assignment]
            consume_mid_download_marker = None  # type: ignore[assignment]
            V13_SMART_AVAILABLE = True
            print(f"  [v13-warn] Dùng v11_smart_downloader (fallback cuối). "
                  f"MidDownloadRotate không có sẵn — slow-speed mid-download "
                  f"rotation TẮT hoàn toàn.",
                  flush=True)
        except Exception as _v11e:
            print(f"  [v13-warn] SmartDownloader không khả dụng: "
                  f"v13err={_v13e}, v12err={_v12e}, v11err={_v11e}",
                  flush=True)
            MidDownloadRotate = None
            write_mid_download_marker = None  # type: ignore[assignment]
            consume_mid_download_marker = None  # type: ignore[assignment]

load_dotenv(Path(__file__).parent / ".env")

# v15: SourceAddressAdapter cho requests — bind source IP để trigger policy routing
class _SourceAddressAdapter:
    """HTTPAdapter bind source IP cho requests.Session.
    Lazy import requests.adapters để không fail nếu requests chưa cài.
    """
    _cls = None

    @classmethod
    def create(cls, source_address: str):
        if cls._cls is None:
            try:
                import requests.adapters
                from urllib3.util.connection import allowed_gai_family
                import socket

                class _Adapter(requests.adapters.HTTPAdapter):
                    def __init__(self, src_addr, **kwargs):
                        self._src_addr = src_addr
                        super().__init__(**kwargs)

                    def init_poolmanager(self, *args, **kwargs):
                        kwargs['source_address'] = (self._src_addr, 0)
                        super().init_poolmanager(*args, **kwargs)

                cls._cls = _Adapter
            except Exception:
                return None
        return cls._cls(source_address)


def _make_bound_session(source_address: str):
    """Tạo requests.Session bind đến source IP (policy routing)."""
    try:
        import requests as _req
        session = _req.Session()
        adapter = _SourceAddressAdapter.create(source_address)
        if adapter:
            session.mount('http://', adapter)
            session.mount('https://', adapter)
        return session
    except Exception:
        return None


# v18-FIX: Force IPv4-only DNS resolution khi dùng VPN tunnel.
# Tunnel OpenVPN (policy routing) CHỈ hỗ trợ IPv4. Nếu DNS trả về IPv6 address,
# socket.bind(tun_ip) → socket cố connect IPv6 → EADDRNOTAVAIL (errno -9).
# Monkey-patch socket.getaddrinfo để filter bỏ AF_INET6, chỉ trả AF_INET.
import socket as _socket_mod
_original_getaddrinfo = _socket_mod.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=_socket_mod.AF_UNSPEC, *args, **kwargs):
    """Force IPv4-only: nếu family=AF_UNSPEC → gọi original nhưng filter AF_INET6."""
    if family == _socket_mod.AF_UNSPEC:
        family = _socket_mod.AF_INET  # Chỉ yêu cầu IPv4
    return _original_getaddrinfo(host, port, family, *args, **kwargs)


_socket_mod.getaddrinfo = _ipv4_only_getaddrinfo
print("[v18-ipv4] Patched socket.getaddrinfo → force AF_INET only (VPN tunnel is IPv4-only)")


# v11: Module-level global cho INSTANCE_ID, dùng bởi main() và atexit handler.
# Khởi tạo = None, sẽ được set trong main() khi parse args.
INSTANCE_ID: Optional[str] = None

# ================= VPN ROTATOR (BẮT BUỘC - OpenVPN) =================
# Chỉ dùng ProtonVPN OpenVPN tunnel để fake IP.
# - 5 server free (CA/MX/NL/SG/US/JP), rotate random theo --vpn-strategy
# - Auth: ./proton_config/auth.txt (chmod 600)
# - Cần: sudo setcap cap_net_admin+ep /usr/sbin/openvpn (chạy 1 lần)
try:
    from vpn_rotator_v5 import (
        get_vpn_rotator_from_config,
        VPNRotator,
        is_proxy_dead_error,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from vpn_rotator_v5 import (  # type: ignore
        get_vpn_rotator_from_config,
        VPNRotator,
        is_proxy_dead_error,
    )


# ================= KILL ALL VPN TUNNELS (v5.13) =================
def kill_all_vpn_tunnels():
    """v5.13: Kill TẤT CẢ tunnel OpenVPN "fake IP" còn sót của user hiện tại.

    Lý do cần (Option A — reset về REAL mỗi video yêu cầu):
      - Mỗi audio mới phải dùng IP thật để reset rate-limit counter.
      - Nếu tunnel OpenVPN cũ còn sống, system routing vẫn đi qua VPN →
        IP "thật" thực ra vẫn là IP fake, phá vỡ logic state machine.
      - Ngoài ra, tunnel OpenVPN chiếm /dev/net/tun → tunnel mới (khi
        reconnect VPN cho audio kế tiếp) sẽ KHÔNG start được nếu còn
        tunnel cũ.

    Cách làm (CHỈ kill process của user hiện tại, an toàn cho multi-user):
      1) Quét tất cả PID file `/tmp/openvpn-proton-*.pid.*.*` (do IsolatedVPNRotator
         tạo qua openvpn --writepid). Với mỗi PID còn sống → SIGTERM, đợi 2s,
         nếu vẫn sống → SIGKILL. SAFETY CHECK trước khi kill:
           - Process thuộc user hiện tại (UID match).
           - /proc/<pid>/cmdline chứa "openvpn" + "proton_config".
         Nếu KHÔNG match → skip (tránh kill nhầm VPN khác).
      2) Fallback pkill -u <uid> -f "openvpn.*proton_config" để dọn các
         openvpn process KHÔNG có PID file.
      3) Đợi tối đa 5s cho đến khi KHÔNG còn openvpn-proton process nào.
      4) KHÔNG xóa file .pid / .log cũ (để debug).
    """
    import glob as _glob
    import os as _os
    import signal as _sig
    import time as _t

    my_uid = _os.getuid()
    print(f"[kill-vpn] Dọn tất cả tunnel OpenVPN fake-IP của uid={my_uid}...")

    killed_pid = 0
    killed_pkill = 0

    # --- Bước 1: Kill theo PID file ---
    pid_files = sorted(_glob.glob("/tmp/openvpn-proton-*.pid.*.*"))
    if not pid_files:
        print("[kill-vpn]   (không có PID file nào trong /tmp/openvpn-proton-*.pid.*.*)")
    else:
        print(f"[kill-vpn]   Tìm thấy {len(pid_files)} PID file → kill theo PID chính xác...")
        for pf in pid_files:
            try:
                with open(pf, "r") as f:
                    raw = f.read().strip()
                pid = int(raw)
            except (ValueError, OSError):
                continue
            try:
                _os.kill(pid, 0)
            except (ProcessLookupError, PermissionError):
                continue
            try:
                st = _os.stat(f"/proc/{pid}")
            except OSError:
                continue
            if st.st_uid != my_uid:
                print(f"[kill-vpn]     • PID {pid} ({_os.path.basename(pf)}) "
                      f"không thuộc user hiện tại (uid={st.st_uid}) → skip")
                continue
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as cf:
                    cmdline = cf.read().decode("utf-8", errors="replace")
                    cmdline = cmdline.replace("\x00", " ").strip()
            except OSError:
                continue
            if "openvpn" not in cmdline or "proton_config" not in cmdline:
                print(f"[kill-vpn]     • PID {pid} KHÔNG phải openvpn+proton_config "
                      f"(cmdline='{cmdline[:80]}...') → skip")
                continue
            print(f"[kill-vpn]     • PID {pid} ({_os.path.basename(pf)}) → SIGTERM")
            try:
                _os.kill(pid, _sig.SIGTERM)
                killed_pid += 1
            except ProcessLookupError:
                continue
            except PermissionError:
                continue
            for _ in range(4):
                try:
                    _os.kill(pid, 0)
                    _t.sleep(0.5)
                except ProcessLookupError:
                    break
                except PermissionError:
                    break
            else:
                try:
                    print(f"[kill-vpn]     • PID {pid} vẫn sống sau 2s → SIGKILL")
                    _os.kill(pid, _sig.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass

    # --- Bước 2: Fallback pkill ---
    try:
        import subprocess as _sp
        out = _sp.run(
            ["pgrep", "-u", str(my_uid), "-f", "openvpn.*proton_config"],
            capture_output=True, text=True, timeout=5,
        )
        if out.returncode == 0 and out.stdout.strip():
            print(f"[kill-vpn]   Fallback: pkill -u {my_uid} -f 'openvpn.*proton_config'...")
            _sp.run(
                ["pkill", "-9", "-u", str(my_uid), "-f", "openvpn.*proton_config"],
                capture_output=True, timeout=5,
            )
            killed_pkill += 1
    except Exception as e:
        print(f"[kill-vpn]   pkill fallback error (ignored): {e}")

    # --- Bước 3: Đợi process biến mất ---
    try:
        for _ in range(10):
            try:
                out = _sp.run(
                    ["pgrep", "-u", str(my_uid), "-f", "openvpn.*proton_config"],
                    capture_output=True, text=True, timeout=3,
                )
                has_procs = (out.returncode == 0 and bool(out.stdout.strip()))
            except Exception:
                has_procs = False
            if not has_procs:
                break
            _t.sleep(0.5)
    except Exception:
        pass

    print(f"[kill-vpn] Tổng: {killed_pid} PID (theo file) + {killed_pkill} nhóm fallback pkill")
    _t.sleep(1)

# ================= KILL TUNNEL BY INSTANCE (v11) =================
def kill_tunnel_by_instance(instance_id_prefix: str):
    """v11: Kill openvpn tunnels của MỘT instance cụ thể (per-instance kill).

    Khác với kill_all_vpn_tunnels(): chỉ kill các PID file có prefix
    `/tmp/openvpn-proton-{instance_id_prefix}.*.pid.*.*` (đúng instance này).
    An toàn cho multi-instance cùng user account — không giết nhầm tunnel
    của instance khác.

    Args:
        instance_id_prefix: phần đầu của PID file. Mặc định instance_id được
            tạo bởi CLI `--instance-id` hoặc auto-generated `pid{os.getpid()}_t{...}`.

    Returns:
        Số tunnel đã kill thành công.

    Safety check 3 lớp (giống kill_all_vpn_tunnels):
      1) Process thuộc user hiện tại (UID match).
      2) /proc/<pid>/cmdline chứa "openvpn" + "proton_config".
      3) PID file name khớp instance_id_prefix.
    """
    import glob as _glob
    import os as _os
    import signal as _sig
    import time as _t

    if not instance_id_prefix:
        print(f"[kill-by-inst] instance_id_prefix rỗng → skip")
        return 0

    my_uid = _os.getuid()
    pattern = f"/tmp/openvpn-proton-{instance_id_prefix}*.pid.*.*"
    pid_files = sorted(_glob.glob(pattern))
    if not pid_files:
        print(f"[kill-by-inst] (không có tunnel nào cho instance={instance_id_prefix})")
        return 0

    print(f"[kill-by-inst] Tìm thấy {len(pid_files)} PID file cho instance={instance_id_prefix} → kill...")

    killed = 0
    for pf in pid_files:
        try:
            with open(pf, "r") as f:
                pid = int(f.read().strip())
        except (ValueError, OSError):
            continue
        try:
            _os.kill(pid, 0)
        except (ProcessLookupError, PermissionError):
            continue
        try:
            st = _os.stat(f"/proc/{pid}")
        except OSError:
            continue
        if st.st_uid != my_uid:
            print(f"[kill-by-inst]   • PID {pid} ({_os.path.basename(pf)}) "
                  f"không thuộc user hiện tại → skip")
            continue
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as cf:
                cmdline = cf.read().decode("utf-8", errors="replace")
                cmdline = cmdline.replace("\x00", " ").strip()
        except OSError:
            continue
        if "openvpn" not in cmdline or "proton_config" not in cmdline:
            print(f"[kill-by-inst]   • PID {pid} KHÔNG phải openvpn+proton_config → skip")
            continue
        print(f"[kill-by-inst]   • PID {pid} ({_os.path.basename(pf)}) → SIGTERM")
        try:
            _os.kill(pid, _sig.SIGTERM)
        except (ProcessLookupError, PermissionError):
            continue
        killed += 1
        for _ in range(4):
            try:
                _os.kill(pid, 0)
                _t.sleep(0.5)
            except ProcessLookupError:
                break
            except PermissionError:
                break
        else:
            try:
                print(f"[kill-by-inst]   • PID {pid} vẫn sống sau 2s → SIGKILL")
                _os.kill(pid, _sig.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass

    print(f"[kill-by-inst] Killed {killed} tunnel(s) cho instance={instance_id_prefix}")
    return killed


# ================= YOUTUBE KEY ROTATOR (v6 port từ sub_playlist.py) =================
def _is_youtube_quota_error(e: Exception) -> bool:
    """Nhận diện lỗi YouTube quotaExceeded / rateLimitExceeded / dailyLimitExceeded."""
    msg = str(e).lower()
    if any(kw in msg for kw in [
        "quotaexceeded", "ratelimitexceeded", "dailylimitexceeded",
        "userexceeded", "forbidden", "quota exceeded", "rate limit",
        "daily limit", "quota_limit", "quotalimit",
    ]):
        return True
    resp = getattr(e, "resp", None)
    status = getattr(resp, "status", None)
    if status == 403 and ("quota" in msg or "limit" in msg):
        return True
    return False


def _youtube_key_rotator_from_env() -> Optional["YouTubeKeyRotator"]:
    """v6: Load YOUTUBE_API_KEY + _1.._7 từ env. Trả None nếu không có."""
    keys: list[str] = []
    base = os.environ.get("YOUTUBE_API_KEY", "").strip()
    if base:
        keys.append(base)
    for k in ["YOUTUBE_API_KEY_1", "YOUTUBE_API_KEY_2", "YOUTUBE_API_KEY_3",
              "YOUTUBE_API_KEY_4", "YOUTUBE_API_KEY_5", "YOUTUBE_API_KEY_6",
              "YOUTUBE_API_KEY_7"]:
        v = os.environ.get(k, "").strip()
        if v and v not in keys:
            keys.append(v)
    if not keys:
        return None
    return YouTubeKeyRotator(keys)


class YouTubeKeyRotator:
    """v6: Rotate YouTube API key khi quotaExceeded.

    Thứ tự ưu tiên key:
      1) YOUTUBE_API_KEY
      2) YOUTUBE_API_KEY_1, _2, ...

    Mỗi key exhausted sẽ KHÔNG thử lại trong cùng session.
    """

    def __init__(self, keys: list[str]):
        self.keys = [k for k in (keys or []) if k]
        self.exhausted: set[str] = set()
        self.current_index = 0
        self._lock = threading.Lock()

    def __len__(self) -> int:
        return len(self.keys)

    def is_empty(self) -> bool:
        return len(self.keys) == 0

    def is_exhausted(self) -> bool:
        """True nếu TẤT CẢ key đã exhausted."""
        return len(self.exhausted) >= len(self.keys)

    def current_key(self) -> Optional[str]:
        if not self.keys:
            return None
        if self.current_index >= len(self.keys):
            self.current_index = 0
        key = self.keys[self.current_index]
        if key in self.exhausted:
            return None
        return key

    def build(self):
        """Tạo googleapiclient YouTube client với key hiện tại."""
        from googleapiclient.discovery import build as _gbuild
        k = self.current_key()
        if not k:
            raise RuntimeError("YouTube: không còn key nào khả dụng (tất cả exhausted)")
        return _gbuild("youtube", "v3", developerKey=k, cache_discovery=False)

    def mark_exhausted(self, key: str):
        with self._lock:
            self.exhausted.add(key)
        print(f"  [YouTube] Key {key[:8]}...{key[-4:]} đã đánh dấu exhausted "
              f"({len(self.exhausted)}/{len(self.keys)} keys)")

    def rotate(self) -> Optional[str]:
        """Chuyển sang key tiếp theo chưa exhausted."""
        with self._lock:
            if self.is_exhausted():
                return None
            n = len(self.keys)
            for _ in range(n):
                self.current_index = (self.current_index + 1) % n
                cand = self.keys[self.current_index]
                if cand not in self.exhausted:
                    return cand
            return None

    def execute_with_retry(self, request_factory, label: str = ""):
        """Thực thi 1 googleapiclient request. Khi quotaExceeded → rotate."""
        if self.is_empty():
            raise RuntimeError("YouTube: chưa có API key nào")

        last_err = None
        for attempt in range(len(self.keys) + 1):
            try:
                youtube = self.build()
                req = request_factory(youtube)
                return req.execute()
            except Exception as e:
                last_err = e
                if not _is_youtube_quota_error(e):
                    raise
                cur = self.current_key()
                if cur:
                    self.mark_exhausted(cur)
                if self.is_exhausted():
                    print(f"  [YouTube] TẤT CẢ {len(self.keys)} key đã exhausted → dừng")
                    raise
                new_key = self.rotate()
                if not new_key:
                    raise
                tag = f" [{label}]" if label else ""
                print(f"  [YouTube]{tag} Quota exceeded → switch to key "
                      f"#{self.current_index + 1} ({new_key[:8]}...{new_key[-4:]})")
        raise last_err


def resolve_channel_id_v6(rotator, channel_input: str) -> Optional[str]:
    """v6: Resolve channel URL/handle/ID → channel ID qua API.

    Hỗ trợ:
      - https://www.youtube.com/@ChannelHandle
      - https://www.youtube.com/channel/UCxxxxx
      - https://www.youtube.com/c/ChannelName
      - https://www.youtube.com/user/UserName
      - UCxxxxx (direct)
    """
    from googleapiclient.discovery import build as _gbuild

    if isinstance(rotator, str):
        api_key = rotator
        def _get_youtube(k=api_key):
            return _gbuild("youtube", "v3", developerKey=k, cache_discovery=False)
    else:
        def _get_youtube():
            return rotator.build()

    channel_input = channel_input.strip().rstrip("/")

    # Direct UC...
    if channel_input.startswith("UC") and len(channel_input) == 24:
        return channel_input

    # Match channel/UCxxx
    m = re.search(r"youtube\.com/channel/([^/\s?]+)", channel_input)
    if m and m.group(1).startswith("UC") and len(m.group(1)) == 24:
        return m.group(1)

    # Match @handle
    handle_match = re.search(r"@([^/\s?]+)", channel_input)
    if handle_match:
        handle = handle_match.group(1)
        for attempt in range(2):
            try:
                resp = rotator.execute_with_retry(
                    lambda y, h=handle: y.channels().list(part="id", forHandle="@" + h),
                    label="channels.forHandle",
                )
                items = resp.get("items", [])
                if items:
                    return items[0]["id"]
                break  # valid response, just no items
            except Exception as e:
                if attempt == 0 and ("ssl" in str(e).lower() or "timeout" in str(e).lower()):
                    time.sleep(2)
                    continue
                raise

    # Match /c/Name hoặc /user/Name
    custom_match = re.search(r"youtube\.com/c/([^/\s?]+)", channel_input)
    user_match = re.search(r"youtube\.com/user/([^/\s?]+)", channel_input)
    if custom_match or user_match:
        username = (custom_match or user_match).group(1)
        try:
            resp = rotator.execute_with_retry(
                lambda y, u=username: y.channels().list(part="id", forUsername=u),
                label="channels.forUsername",
            )
            items = resp.get("items", [])
            if items:
                return items[0]["id"]
        except Exception:
            pass

    return None


# ================= ISOLATED VPN ROTATOR (multi-instance safe) =================
class IsolatedVPNRotator:
    """Wrapper cho VPNRotator để an toàn khi chạy song song nhiều instance.

    v15: Policy routing — mỗi instance dùng custom routing table riêng.
         Main table KHÔNG BAO GIỜ bị sửa → IP thật + mạng LAN luôn hoạt động.

    Fix 3 vấn đề của VPNRotator gốc khi chạy nhiều process cùng lúc:
      1. OPENVPN_LOG constant → 2 instance ghi đè log của nhau.
      2. pkill fallback trong _disconnect() → kill nhầm tunnel instance khác.
      3. pgrep trong _is_connected() → thấy tunnel instance khác → tưởng mình đã connected.
    """

    _used_table_ids: "set[int]" = set()
    _table_id_lock = threading.Lock()

    @staticmethod
    def _derive_table_id(instance_id: str) -> int:
        """Derive stable table ID from instance_id (range 100-249).

        Dùng hash thay vì counter tuần tự → 2 process khác nhau với instance_id
        khác nhau sẽ luôn có table ID khác nhau, không phụ thuộc thứ tự init.
        """
        import hashlib
        h = int(hashlib.md5(instance_id.encode()).hexdigest()[:8], 16)
        return 100 + (h % 150)

    def __init__(self, instance_id: str, **vpn_kwargs):
        self.instance_id = instance_id
        self._instance_log = f"/tmp/openvpn-proton-{instance_id}.log"
        self._instance_pid_prefix = f"/tmp/openvpn-proton-{instance_id}.pid"
        self._routes_added = False
        self._sudo_ok = False
        self._vpn_host_route = None
        self._tun_local_ip: Optional[str] = None
        self._ip_rule_added = False

        with IsolatedVPNRotator._table_id_lock:
            self._table_id = self._derive_table_id(instance_id)
            if self._table_id in IsolatedVPNRotator._used_table_ids:
                for offset in range(1, 150):
                    candidate = 100 + ((self._table_id - 100 + offset) % 150)
                    if candidate not in IsolatedVPNRotator._used_table_ids:
                        self._table_id = candidate
                        break
            IsolatedVPNRotator._used_table_ids.add(self._table_id)

        import vpn_rotator_v5 as _vr_mod
        _vr_mod.OPENVPN_LOG = self._instance_log
        self._vr_mod = _vr_mod

        self._inner = VPNRotator(**vpn_kwargs)
        self._ensure_sudo_cached()
        self._patch_connect_server_pid()

    def _ensure_sudo_cached(self):
        """Verify sudo đã cached (run_crawl_v14.sh gọi sudo -v ở đầu).
        Nếu chưa cached (chạy Python trực tiếp) → prompt user.
        """
        import subprocess as _sp
        try:
            result = _sp.run(["sudo", "-n", "true"], capture_output=True, timeout=3)
            if result.returncode == 0:
                self._sudo_ok = True
                return
        except Exception:
            pass
        # Shell script chưa cache → prompt interactive
        print(f"\n    [vpn-route] 🔑 Cần quyền sudo để add route VPN. Nhập password:", flush=True)
        try:
            ret = _sp.run(["sudo", "-v"], timeout=60)
            self._sudo_ok = (ret.returncode == 0)
        except Exception as e:
            print(f"    [vpn-route] ⚠️ sudo error: {e}", flush=True)

    # ---- v15: Policy routing — custom table per instance ----
    # Main table KHÔNG BAO GIỜ bị sửa. Mỗi instance có routing table riêng.
    # Traffic chỉ đi qua VPN khi bind source_address = tun local IP.

    def _prepare_config_no_route(self, ovpn_path: Path) -> Path:
        """Tạo config OpenVPN chỉ tunnel-only, KHÔNG add route.
        Openvpn chỉ cần tạo tun device + encrypt traffic, route do Python quản lý.
        """
        original = ovpn_path.read_text(encoding="utf-8", errors="replace")
        cleaned_lines = []
        for line in original.splitlines():
            stripped = line.strip()
            if (stripped.startswith("up ") or stripped.startswith("down ")) and "/etc/openvpn/update-resolv-conf" in stripped:
                cleaned_lines.append(f"# {line}  # disabled")
            elif stripped.startswith("redirect-gateway"):
                cleaned_lines.append(f"# {line}  # disabled")
            elif stripped == "route-nopull":
                cleaned_lines.append(f"# {line}  # disabled")
            elif stripped.startswith("route ") and "vpn_gateway" in stripped:
                cleaned_lines.append(f"# {line}  # disabled - Python manages routes")
            else:
                cleaned_lines.append(line)
        cleaned_lines.append("")
        cleaned_lines.append("# === vpn_rotator v15: tunnel-only, policy routing ===")
        cleaned_lines.append("route-nopull")
        cleaned_lines.append('pull-filter ignore "redirect-gateway"')
        cleaned_lines.append('pull-filter ignore "redirect-private"')
        cleaned_lines.append('pull-filter ignore "route "')
        cleaned_lines.append('pull-filter ignore "dhcp-option"')
        cleaned_lines.append('pull-filter accept ""')

        cleaned = "\n".join(cleaned_lines) + "\n"
        temp_path = ovpn_path.parent / f".{ovpn_path.stem}.no-route.{self.instance_id}.ovpn"
        temp_path.write_text(cleaned, encoding="utf-8")
        return temp_path

    def _get_tun_gateway(self) -> "str | None":
        """Lấy peer/gateway IP của tunnel device (dùng cho ip route add).
        Hỗ trợ cả topology p2p (có peer) và subnet (dùng .1 làm gateway).
        """
        import subprocess as _sp
        import ipaddress as _ipaddr
        dev = self._inner._tunnel_device
        try:
            result = _sp.run(
                ["ip", "-4", "addr", "show", "dev", dev],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode != 0:
                return None
            subnet_gw = None
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.startswith("inet ") and "peer" in line:
                    # "inet 10.8.0.6 peer 10.8.0.5/32 scope global tun_audio"
                    parts = line.split()
                    for i, p in enumerate(parts):
                        if p == "peer" and i + 1 < len(parts):
                            return parts[i + 1].split("/")[0]
                elif line.startswith("inet "):
                    # Topology subnet: "inet 10.96.0.13/16" → gateway = .1
                    parts = line.split()
                    ip_cidr = parts[1]  # "10.96.0.13/16"
                    try:
                        iface = _ipaddr.IPv4Interface(ip_cidr)
                        subnet_gw = str(iface.network.network_address + 1)
                    except Exception:
                        subnet_gw = None
            # Fallback 1: ip route show dev → look for "via"
            result2 = _sp.run(
                ["ip", "route", "show", "dev", dev],
                capture_output=True, text=True, timeout=5,
            )
            if result2.returncode == 0:
                for line in result2.stdout.splitlines():
                    parts = line.split()
                    if "via" in parts:
                        idx = parts.index("via")
                        if idx + 1 < len(parts):
                            return parts[idx + 1]
            # Fallback 2: subnet topology → use .1 as gateway
            if subnet_gw:
                return subnet_gw
            return None
        except Exception:
            return None

    def _get_vpn_server_ip_from_ovpn(self, ovpn_path: Path) -> "str | None":
        """Lấy IP VPN server từ file .ovpn (dòng 'remote <ip> <port>')."""
        try:
            for line in ovpn_path.read_text(encoding="utf-8", errors="replace").splitlines():
                stripped = line.strip()
                if stripped.startswith("remote "):
                    parts = stripped.split()
                    if len(parts) >= 2:
                        return parts[1]
        except Exception:
            pass
        return None

    def _get_default_gateway(self) -> "tuple[str, str] | tuple[None, None]":
        """Lấy (gateway_ip, interface) từ default route hiện tại."""
        import subprocess as _sp
        try:
            result = _sp.run(
                ["ip", "route", "show", "default"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode != 0:
                return None, None
            line = result.stdout.strip().split('\n')[0]
            parts = line.split()
            gw_ip = None
            dev = None
            for i, p in enumerate(parts):
                if p == "via" and i + 1 < len(parts):
                    gw_ip = parts[i + 1]
                if p == "dev" and i + 1 < len(parts):
                    dev = parts[i + 1]
            return gw_ip, dev
        except Exception:
            return None, None

    def _get_tun_local_ip(self) -> "str | None":
        """Lấy local IP của tunnel device (dùng cho source_address binding)."""
        import subprocess as _sp
        dev = self._inner._tunnel_device
        try:
            result = _sp.run(
                ["ip", "-4", "addr", "show", "dev", dev],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode != 0:
                return None
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.startswith("inet "):
                    parts = line.split()
                    return parts[1].split("/")[0]
            return None
        except Exception:
            return None

    def _do_add_policy_routes(self, ovpn_path: "Path | None" = None) -> bool:
        """v15: Add source-based policy route vào custom table.

        MAIN TABLE KHÔNG BAO GIỜ BỊ SỬA.

        Logic:
          1. Lấy tun local IP (source address cho binding)
          2. Lấy tun peer/gateway IP
          3. Add host route VPN server vào custom table (qua real gateway)
          4. Add default route via tun gateway vào custom table
          5. Add ip rule: from <tun_local_ip> lookup <table_id>

        Khi yt-dlp bind socket đến tun_local_ip → kernel match rule → dùng
        custom table → traffic đi qua VPN. Mọi traffic khác đi main table.
        """
        import subprocess as _sp
        dev = self._inner._tunnel_device
        table = str(self._table_id)

        # Lấy tun IP/gateway — nếu chưa sẵn sàng, return False nhanh
        # để outer loop (trong _connect_server) tiếp tục đợi và retry
        import time as _time_mod
        tun_ip = None
        gw = None
        for _attempt in range(3):
            if not tun_ip:
                tun_ip = self._get_tun_local_ip()
            if tun_ip and not gw:
                gw = self._get_tun_gateway()
            if tun_ip and gw:
                break
            _time_mod.sleep(0.3)

        if not tun_ip:
            return False
        self._tun_local_ip = tun_ip

        if not gw:
            return False

        vpn_server_ip = None
        if ovpn_path:
            vpn_server_ip = self._get_vpn_server_ip_from_ovpn(ovpn_path)
        wifi_gw, wifi_dev = self._get_default_gateway()

        if vpn_server_ip and wifi_gw and wifi_dev:
            cmd = ["sudo", "ip", "route", "replace",
                   f"{vpn_server_ip}/32", "via", wifi_gw, "dev", wifi_dev,
                   "table", table]
            result = _sp.run(cmd, capture_output=True, text=True, timeout=5)
            if result.returncode != 0:
                print(f"    [vpn-route-v15] WARN: host route in table {table}: "
                      f"{result.stderr.strip()}", flush=True)
            else:
                self._vpn_host_route = (vpn_server_ip, wifi_gw, wifi_dev)
                print(f"    [vpn-route-v15] host route {vpn_server_ip}/32 via {wifi_gw} "
                      f"→ table {table}", flush=True)

        cmd_default = ["sudo", "ip", "route", "replace",
                       "default", "via", gw, "dev", dev, "table", table]
        result = _sp.run(cmd_default, capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            print(f"    [vpn-route-v15] FAIL default route in table {table}: "
                  f"{result.stderr.strip()}", flush=True)
            return False

        _sp.run(["sudo", "ip", "rule", "del", "from", tun_ip, "table", table],
                capture_output=True, text=True, timeout=5)
        priority = str(200 + self._table_id)
        cmd_rule = ["sudo", "ip", "rule", "add", "from", tun_ip,
                    "table", table, "priority", priority]
        result = _sp.run(cmd_rule, capture_output=True, text=True, timeout=5)
        if result.returncode != 0:
            print(f"    [vpn-route-v15] FAIL ip rule: {result.stderr.strip()}", flush=True)
            return False

        self._ip_rule_added = True
        self._routes_added = True
        print(f"    [vpn-route-v15] OK: from {tun_ip} table {table} "
              f"(default via {gw} dev {dev}, priority {priority})", flush=True)
        return True

    def _do_cleanup_policy_routes(self):
        """v15: Remove policy route + flush custom table. Safe nếu đã removed."""
        import subprocess as _sp
        if not self._routes_added:
            return
        table = str(self._table_id)
        tun_ip = self._tun_local_ip

        if tun_ip:
            _sp.run(["sudo", "ip", "rule", "del", "from", tun_ip, "table", table],
                    capture_output=True, text=True, timeout=5)

        _sp.run(["sudo", "ip", "route", "flush", "table", table],
                capture_output=True, text=True, timeout=5)

        self._routes_added = False
        self._ip_rule_added = False
        self._tun_local_ip = None
        self._vpn_host_route = None

    def get_source_address(self) -> "str | None":
        """Trả về tun local IP cho callers bind socket.

        Khi VPN active, callers bind source_address = IP này → kernel match
        ip rule → dùng custom table → traffic đi qua VPN tunnel.
        Returns None khi VPN không connected (dùng real IP / default route).

        v18.1 FIX: Luôn re-verify IP còn tồn tại trên system trước khi trả về.
        Tunnel có thể die bất cứ lúc nào (OpenVPN crash, network flap) → IP
        bị kernel release → _tun_local_ip cache stale → yt-dlp bind() fail
        Errno 99. Verify bằng cách thử bind socket (non-destructive, không
        chiếm port) → nếu fail → auto-invalidate cache → return None.
        """
        if self._routes_added and self._tun_local_ip:
            import socket as _gs_sock
            try:
                _test = _gs_sock.socket(_gs_sock.AF_INET, _gs_sock.SOCK_STREAM)
                _test.setsockopt(_gs_sock.SOL_SOCKET, _gs_sock.SO_REUSEADDR, 1)
                _test.bind((self._tun_local_ip, 0))
                _test.close()
                return self._tun_local_ip
            except OSError:
                # IP không còn tồn tại → tunnel đã die → clear cache
                self._routes_added = False
                self._tun_local_ip = None
                return None
        return None

    def _verify_tunnel_alive(self, timeout_s: float = 4.0) -> bool:
        """Verify tunnel process vẫn sống sau timeout_s giây.
        Trả True nếu process vẫn chạy, False nếu đã die (route add fail).
        """
        import time as _t
        pid = getattr(self._inner, "_current_pid", None)
        if pid is None:
            return False
        _t.sleep(timeout_s)
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def _patch_connect_server_pid(self):
        instance_pid_prefix = self._instance_pid_prefix

        def _patched(idx: int, retry: int = 0) -> bool:
            import subprocess as _sp
            import time as _t
            import logging as _log

            ovpn = self._inner._ovpn_files[idx]
            _log.getLogger("vpn_rotator").info(
                "VPN[isolated=%s]: connecting to %s (attempt %d)",
                self.instance_id, ovpn.name, retry + 1,
            )
            # Cleanup policy route cũ trước khi disconnect tunnel
            self._do_cleanup_policy_routes()
            self._inner._disconnect()
            # v15: dùng config KHÔNG có route — OpenVPN chỉ tạo tunnel,
            # Python tự add policy route sau khi tunnel lên
            prepared_config = self._prepare_config_no_route(ovpn)
            old_pid = getattr(self._inner, "_current_pid", None)
            new_pid = None
            try:
                log_path = f"{self._instance_log}.{idx}.{retry}"
                pid_file = Path(f"{instance_pid_prefix}.{idx}.{retry}")
                try:
                    pid_file.unlink(missing_ok=True)
                except Exception:
                    pass
                proc = _sp.Popen(
                    [
                        "/usr/sbin/openvpn", "--config", str(prepared_config),
                        "--dev", self._inner._tunnel_device,
                        "--auth-user-pass", str(self._inner._auth_file),
                        "--auth-retry", "nointeract", "--auth-nocache",
                        "--daemon", "--log", log_path, "--writepid", str(pid_file),
                        "--script-security", "2", "--up", "/bin/true", "--down", "/bin/true",
                    ],
                    stdout=_sp.DEVNULL, stderr=_sp.DEVNULL, start_new_session=True,
                )
                try:
                    proc.wait(timeout=10)
                except _sp.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                    return False
                for _ in range(20):
                    _t.sleep(0.1)
                    if pid_file.exists():
                        try:
                            new_pid = int(pid_file.read_text().strip())
                            break
                        except Exception:
                            pass
                if new_pid is None:
                    return False
                if old_pid is not None and new_pid == old_pid:
                    try:
                        os.kill(new_pid, 9)
                    except Exception:
                        pass
                    return False
                self._inner._current_pid = new_pid
            except Exception as e:
                _log.getLogger("vpn_rotator").error("VPN[isolated=%s]: %s", self.instance_id, e)
                return False

            # Đợi tunnel device lên
            routes_added_this_connect = False
            for i in range(self._vr_mod.CONNECT_TIMEOUT):
                _t.sleep(1)
                if self._inner._has_tun0():
                    try:
                        os.kill(new_pid, 0)
                    except (ProcessLookupError, PermissionError):
                        print(f"    [vpn-connect] openvpn PID {new_pid} died ngay sau khi "
                              f"tun lên — retry", flush=True)
                        return False
                    if not routes_added_this_connect:
                        route_ok = self._do_add_policy_routes(ovpn_path=ovpn)
                        if not route_ok:
                            # Interface UP nhưng IP chưa sẵn sàng — tiếp tục loop thay vì fail ngay
                            continue
                        routes_added_this_connect = True
                    ip = self._inner._get_current_ip()
                    real_ip = self._inner._last_known_real_ip
                    if ip and ip != real_ip:
                        self._inner._current_ip = ip
                        self._inner._current_idx = idx
                        self._inner._usage_count[idx] = self._inner._usage_count.get(idx, 0) + 1
                        self._inner._request_count = 0
                        self._inner._last_connect_time = _t.time()
                        _t.sleep(3)
                        try:
                            os.kill(new_pid, 0)
                        except (ProcessLookupError, PermissionError):
                            print(f"    [vpn-connect] openvpn PID {new_pid} died sau 3s "
                                  f"— cleanup policy routes + retry", flush=True)
                            self._do_cleanup_policy_routes()
                            self._inner._current_pid = None
                            return False
                        print(f"    [vpn-connect] tunnel stable: PID={new_pid}, "
                              f"IP={ip}, dev={self._inner._tunnel_device}, "
                              f"table={self._table_id}", flush=True)
                        return True
                elif i >= 3:
                    ip = self._inner._get_current_ip()
                    real_ip = self._inner._last_known_real_ip
                    if ip and real_ip and ip != real_ip:
                        if not routes_added_this_connect:
                            route_ok = self._do_add_policy_routes(ovpn_path=ovpn)
                            if not route_ok:
                                continue
                            routes_added_this_connect = True
                        self._inner._current_ip = ip
                        self._inner._current_idx = idx
                        self._inner._usage_count[idx] = self._inner._usage_count.get(idx, 0) + 1
                        self._inner._request_count = 0
                        self._inner._last_connect_time = _t.time()
                        _t.sleep(3)
                        try:
                            os.kill(new_pid, 0)
                        except (ProcessLookupError, PermissionError):
                            print(f"    [vpn-connect] openvpn PID {new_pid} died "
                                  f"— cleanup policy routes + retry", flush=True)
                            self._do_cleanup_policy_routes()
                            self._inner._current_pid = None
                            return False
                        print(f"    [vpn-connect] tunnel stable (no-dev): PID={new_pid}, "
                              f"IP={ip}, table={self._table_id}", flush=True)
                        return True
            # Timeout — tunnel không lên hoặc route không add được
            if not routes_added_this_connect:
                print(f"    [vpn-connect] Policy route add failed after timeout — skip VPN",
                      flush=True)
            self._do_cleanup_policy_routes()
            self._inner._disconnect()
            return False

        self._inner._connect_server = _patched

    def _is_connected(self) -> bool:
        pid = getattr(self._inner, "_current_pid", None)
        if pid is None:
            return False
        try:
            os.kill(pid, 0)
            return True
        except (ProcessLookupError, PermissionError):
            return False

    def _disconnect(self):
        # v15: Xóa policy route TRƯỚC khi kill tunnel
        self._do_cleanup_policy_routes()
        self._inner._cleanup_routes()
        pid = getattr(self._inner, "_current_pid", None)
        if pid is None:
            return
        try:
            os.kill(pid, 15)
        except ProcessLookupError:
            self._inner._current_pid = None
            return
        except PermissionError:
            return
        import time as _t
        deadline = _t.time() + 10
        while _t.time() < deadline:
            try:
                os.kill(pid, 0)
                _t.sleep(0.5)
            except ProcessLookupError:
                self._inner._current_pid = None
                _t.sleep(2)
                return
            except PermissionError:
                pass
        try:
            os.kill(pid, 9)
            _t.sleep(0.5)
        except (ProcessLookupError, PermissionError):
            pass
        self._inner._current_pid = None
        _t.sleep(2)

    def __getattr__(self, name):
        if name in ("_inner", "instance_id", "_instance_log", "_instance_pid_prefix",
                    "_routes_added", "_vr_mod", "_sudo_ok", "_vpn_host_route",
                    "_table_id", "_tun_local_ip", "_ip_rule_added"):
            raise AttributeError(name)
        return getattr(self._inner, name)

    def __len__(self) -> int:
        return len(self._inner)

    def __bool__(self) -> bool:
        return bool(self._inner)

    def disconnect(self):
        self._disconnect()
        self._inner._current_idx = None
        self._inner._current_ip = None


def get_isolated_vpn_rotator_from_config(
    instance_id: str,
    config_dir: Optional[str] = None,
    rotate_every: int = 0,
    strategy: str = "random",
    real_ip_cycle: int = 0,
    tunnel_device: str = "tun0",
) -> Optional["IsolatedVPNRotator"]:
    try:
        return IsolatedVPNRotator(
            instance_id=instance_id,
            config_dir=Path(config_dir) if config_dir else None,
            rotate_every=rotate_every,
            strategy=strategy,
            real_ip_cycle=real_ip_cycle,
            tunnel_device=tunnel_device,
        )
    except FileNotFoundError as e:
        import logging as _log
        _log.getLogger("vpn_rotator").warning("IsolatedVPN rotator không khả dụng: %s", e)
        return None


# ================= HTTP 500 DETECTOR (v7) =================
# v7: Bắt pattern "HTTP Error 500: Internal Server Error" từ yt-dlp stderr
# (real-time qua custom stderr hook) + track per-fragment.
#
# Background:
#   Khi IP bị Google rate-limit, YouTube trả về HTTP 500 thay vì 403/429 để
#   khó debug. yt-dlp default retry fragment đó INFINITE LẦN, tốn bandwidth
#   nhưng KHÔNG đổi IP → có thể stuck 30+ phút ở 1 video.
#
# v7 fix:
#   - Track số fragment 500 liên tiếp.
#   - Nếu vượt ngưỡng (mặc định 5) → gọi AudioIPController.on_download_complete()
#     với ok=False để trigger cycle IP (REAL → FAKE, hoặc FAKE khác, hoặc về REAL).
#   - Cộng dồn số fragment 500 trong toàn bộ session để thống kê.

class HTTP500Detector:
    """v7: Detect & đếm HTTP 500 errors từ yt-dlp.

    Args:
        threshold: số fragment 500 tối đa trước khi trigger cycle IP.
            Default 5. Nếu gặp 5 fragment liên tiếp trả 500 → gọi
            on_http500_threshold() callback.
        stall_seconds: nếu bytes không tăng trong N giây (progress_hook báo
            downloaded_bytes không đổi) → flag stuck → cũng trigger callback.
        on_http500_threshold: callback(fragment_500_count, total_fragments)
            khi vượt ngưỡng. Caller thường là `audio_ip_ctl.on_download_complete(
            bytes_dl=0, elapsed_s=0, ok=False)`.
    """

    def __init__(
        self,
        threshold: int = 5,
        stall_seconds: float = 30.0,
        on_http500_threshold=None,
    ):
        self.threshold = threshold
        self.stall_seconds = stall_seconds
        self.on_http500_threshold = on_http500_threshold

        # Per-download state
        self._reset_per_download()

        # Session-wide stats
        self.total_500_count = 0
        self.total_500_per_video = []
        self.total_stall_events = 0

    def _reset_per_download(self):
        """Reset state khi bắt đầu download mới."""
        self._fragment_500_count = 0
        self._last_500_frag_idx = None
        # Stall detection
        self._last_progress_bytes = 0
        self._last_progress_t = None
        self._stall_flag = False
        # v10: timestamp lần cuối fire stall callback. Dùng để fire lặp lại
        # mỗi `stall_seconds` khi vẫn stuck (trước đây fire 1 lần rồi tắt →
        # nếu yt-dlp vẫn retry trên IP chết, không có signal nhảy IP lần 2).
        self._last_stall_fire_t = None

    def reset(self):
        """Public API: gọi khi bắt đầu video mới (C-{i})."""
        self._reset_per_download()

    def on_stderr_line(self, line: str) -> bool:
        """Hook vào yt-dlp stderr output. Trả True nếu line chứa HTTP 500
        HOẶC Read timed out (HTTPSConnectionPool timeout).

        Args:
            line: 1 dòng stderr (đã strip newline).

        Returns:
            True nếu line là HTTP 500/503 error HOẶC timeout/network error.
        """
        if not line:
            return False
        # Pattern thực tế từ log:
        #   "[download] Got error: HTTP Error 500: Internal Server Error"
        #   "[download] Got error: HTTPSConnectionPool(...): Read timed out. (read timeout=30.0)"
        if ("HTTP Error 500" in line
            or "HTTP Error 503" in line
            or "Read timed out" in line             # <-- MỚI: bắt timeout
            or "HTTPSConnectionPool" in line        # <-- MỚI: bắt timeout ở host khác
            or "ConnectionTimeout" in line          # <-- MỚI: bắt connect timeout
            or "Connection reset" in line           # <-- MỚI: bắt connection reset
            or "Connection aborted" in line         # <-- MỚI: bắt aborted
            or "ConnectionRefusedError" in line     # <-- MỚI: bắt refused
            or "Connection refused" in line         # <-- MỚI: bắt refused text
        ):
            return True
        return False

    def on_fragment_500(self, frag_idx: int, total_frags: int) -> bool:
        """Gọi khi 1 fragment fail với HTTP 500.

        Args:
            frag_idx: index của fragment hiện tại (vd: 932).
            total_frags: tổng fragment (vd: 2329).

        Returns:
            True nếu ĐÃ VƯỢT ngưỡng → caller cần cycle IP.
        """
        self._fragment_500_count += 1
        self._last_500_frag_idx = frag_idx
        self.total_500_count += 1

        if self._fragment_500_count >= self.threshold:
            if self.on_http500_threshold:
                try:
                    self.on_http500_threshold(
                        self._fragment_500_count, total_frags)
                except Exception as e:
                    print(f"    [http500-detector] callback error: {e}",
                          flush=True)
            return True
        return False

    def on_progress_check_stall(self, bytes_dl: int, now: float) -> bool:
        """Gọi từ progress_hook mỗi lần. Phát hiện "stuck" (bytes không tăng).

        v10 FIX: Fire callback MỖI `stall_seconds` khi vẫn stuck (không chỉ
        1 lần như trước). Lý do: nếu IP bị stuck, yt-dlp sẽ retry fragment
        đó nhiều lần. Nếu stall detector chỉ fire 1 lần, IP sẽ bị cycle
        1 lần rồi vẫn stuck ở IP mới (cũng fail) → không có signal nhảy lần 2.

        Args:
            bytes_dl: bytes đã tải (từ yt-dlp progress hook).
            now: time.time() hiện tại.

        Returns:
            True nếu phát hiện stall (bytes không tăng > stall_seconds).
        """
        if self._last_progress_t is None:
            self._last_progress_t = now
            self._last_progress_bytes = bytes_dl
            return False

        if bytes_dl > self._last_progress_bytes:
            # Có tiến triển → reset stall detection
            self._last_progress_t = now
            self._last_progress_bytes = bytes_dl
            self._stall_flag = False
            self._last_stall_fire_t = None  # v10: reset để lần sau fire lại từ đầu
            return False

        # bytes không tăng → check stall
        elapsed = now - self._last_progress_t
        if elapsed < self.stall_seconds:
            return False

        # v10: Fire mỗi `stall_seconds` khi vẫn stuck (không phải chỉ 1 lần)
        last_fire = self._last_stall_fire_t
        if last_fire is not None and (now - last_fire) < self.stall_seconds:
            # Đã fire gần đây rồi → đợi thêm
            return False

        self._stall_flag = True
        self._last_stall_fire_t = now
        self.total_stall_events += 1
        if self.on_http500_threshold:
            try:
                self.on_http500_threshold(
                    self._fragment_500_count, 0)
            except Exception as e:
                print(f"    [http500-detector] stall callback error: {e}",
                      flush=True)
        return True

    def is_stalled(self) -> bool:
        return self._stall_flag

    def fragment_500_count(self) -> int:
        return self._fragment_500_count

    def stats(self) -> dict:
        return {
            "threshold": self.threshold,
            "stall_seconds": self.stall_seconds,
            "session_total_500": self.total_500_count,
            "session_total_stalls": self.total_stall_events,
            "current_download_500": self._fragment_500_count,
        }


# ================= IP ROUTING INFO (v15) =================
@dataclass
class IPRoutingInfo:
    """v15: Routing info trả về từ AudioIPController.on_download_start().

    Callers dùng source_address để bind socket → kernel match ip rule
    → traffic đi qua custom table (VPN tunnel).
    """
    source_address: Optional[str] = None
    using_real_ip: bool = True


# ================= AUDIO IP CONTROLLER (v5) =================
class AudioIPController:
    """v5: State machine quản lý IP cho audio download.

    Logic:
      - Lần đầu tiên trong session: LUÔN dùng IP THẬT (default route, VPN
        tunnel bị disconnect). Lý do: IP thật có thể đã được Google trust
        nếu user dùng lâu dài, hoặc dùng IP thật mới cho lần đầu sẽ
        reset rate-limit counter khi chuyển sang fake.
      - Đo tốc độ LIÊN TỤC qua yt-dlp progress hook. Nếu tốc độ < ngưỡng
        (mặc định 1 MB/s) → đổi IP. Nếu tốc độ OK → giữ nguyên IP hiện tại.
      - Cycle 6: Sau `fake_before_real` (mặc định 5) lần IP-fake mà tốc độ
        vẫn < min → lần thứ 6 chuyển về IP THẬT (disconnect VPN hoàn toàn).
      - Phải đổi IP THẬT SỰ (real ↔ fake), không chỉ rotate giữa các
        VPN server trong cùng 1 tunnel.

    Phân biệt với v3/v4:
      - v3/v4: cycle "N fake → 1 real" dựa trên COUNTER (số request), không
        quan tâm tốc độ thực tế.
      - v5: cycle dựa trên KẾT QUẢ ĐO TỐC ĐỘ. Chỉ khi IP fake thật sự
        chậm (`fake_before_real` lần liên tiếp) mới quay về real.

    Args:
        audio_rotator: VPNRotator (hoặc IsolatedVPNRotator) cho audio.
        min_speed_mbps: tốc độ tối thiểu (MB/s) để coi là OK. Default 1.0.
        fake_before_real: số lần IP-fake chậm liên tiếp trước khi về real
            (cycle = fake_before_real + 1, mặc định 5 → cycle 6).
        min_bytes_for_speed: tối thiểu bytes đã tải trước khi đánh giá
            tốc độ (tránh false positive với file rất nhỏ). Default 256KB.
        min_window_seconds: tối thiểu thời gian (giây) trước khi đánh giá
            tốc độ (tránh false positive khi tải mới bắt đầu). Default 5s.
        speed_avg_window_seconds: cửa sổ (giây) để tính TỐC ĐỘ TRUNG BÌNH
            (rolling average) thay vì tốc độ tức thời. Mỗi chunk mới sẽ
            được lưu vào buffer, tốc độ TB = (bytes mới nhất - bytes cũ
            nhất trong window) / window_size. Default 10s.

            Tại sao cần rolling average?
            - Tốc độ tức thời từ yt-dlp dao động mạnh (chunk này 5 MB/s,
              chunk sau 0.3 MB/s do buffering/IO). Nếu dùng tức thời sẽ
              trigger đổi IP sai.
            - Rolling average làm mượt, phản ánh throughput thực tế hơn.
            - Window 10s nghĩa là: lấy trung bình throughput trong 10s
              gần nhất. Nếu < 1 MB/s → slow.

    Public API:
        on_download_start() -> Optional[str]:
            Trả proxy URL cho audio download hiện tại.
            None = dùng IP thật (default route, KHÔNG qua VPN).
            str = dùng IP fake qua VPN tunnel (sau khi đã trigger connect).
        on_download_complete(bytes_dl, elapsed_s, ok):
            Callback sau khi download xong hoặc mỗi chunk.
            Quyết định đổi IP hay không dựa trên tốc độ + ok status.
        on_chunk_progress(bytes_dl, elapsed_s, speed_bps):
            Callback mid-download (gọi từ yt-dlp progress hook).
            Chia download thành các window CỐ ĐỊNH (window_size = speed_avg_window_seconds,
            mặc định 30s, skip 5s đầu). MỖI window đánh giá avg speed.
            Nếu avg < min → set _slow_flag → hook raise MidDownloadRotate.
        on_download_start_reset_slow_log():
            Reset _slow_flag + window baseline khi bắt đầu attempt mới.
        get_state() -> str: "real" hoặc "fake".
        reset(): reset state về REAL + counter về 0.
    """

    STATE_REAL = "real"
    STATE_FAKE = "fake"

    def __init__(
        self,
        audio_rotator,
        min_speed_mbps: float = 1.0,
        fake_before_real: int = 5,
        min_bytes_for_speed: int = 256 * 1024,
        min_window_seconds: float = 5.0,
        speed_avg_window_seconds: float = 30.0,
        force_real_after_n_fake_fails: int = 2,    # v10
    ):
        self.audio_rotator = audio_rotator
        self.min_speed_mbps = min_speed_mbps
        self.fake_before_real = fake_before_real
        self.min_bytes_for_speed = min_bytes_for_speed
        self.min_window_seconds = min_window_seconds
        self.speed_avg_window_seconds = speed_avg_window_seconds
        # v10: Số lần fail liên tiếp ở FAKE (ok=False) trước khi cycle về REAL
        # thay vì chỉ force_rotate VPN. Mặc định 2. Trước đây cần đợi
        # fake_before_real=5 lần → quá chậm khi IP bị stuck/block.
        self.force_real_after_n_fake_fails = force_real_after_n_fake_fails

        # State
        self._state = self.STATE_REAL
        self._consecutive_fake_slow = 0
        self._total_real_uses = 0
        self._total_fake_uses = 0
        self._total_rotates = 0
        self._slow_flag = False  # set bởi on_chunk_progress, consume bởi hook

        # v13.4: Fixed window baseline (window_size = speed_avg_window_seconds)
        # Chia download thành các window cố định [5, 5+W], [5+W, 5+2W], ...
        # MỖI window đánh giá avg speed 1 LẦN → set _slow_flag nếu < threshold.
        self._window_baseline_bytes = 0
        self._window_baseline_idx = -1

        # v5.1 (deprecated): rolling window samples - không dùng nữa
        self._speed_samples: list = []

    # ----- Public API -----

    def on_download_start(self) -> "IPRoutingInfo":
        """v15: Trả IPRoutingInfo cho audio download hiện tại.

        Returns:
            IPRoutingInfo với:
              - using_real_ip=True: dùng IP thật (default route)
              - using_real_ip=False + source_address: bind socket đến tun IP
                → kernel match policy route → traffic đi qua VPN tunnel
        """
        if self._state == self.STATE_REAL:
            self._ensure_vpn_disconnected()
            self._total_real_uses += 1
            print(f"    [audio-ip] STATE=REAL (IP thật, no VPN tunnel)", flush=True)
            return IPRoutingInfo(using_real_ip=True)
        else:
            # FAKE: cần VPN tunnel active
            if hasattr(self.audio_rotator, "next"):
                try:
                    self.audio_rotator.next()
                except Exception as e:
                    print(f"    [audio-ip] next() error (ignored): {e}", flush=True)
            self._total_fake_uses += 1
            src_addr = None
            if hasattr(self.audio_rotator, "get_source_address"):
                src_addr = self.audio_rotator.get_source_address()
            try:
                cur_idx = getattr(self.audio_rotator, "_current_idx", None)
                cur_ip = getattr(self.audio_rotator, "_current_ip", None)
                ovpn_files = getattr(self.audio_rotator, "_ovpn_files", [])
                server_name = ovpn_files[cur_idx].name if (cur_idx is not None and cur_idx < len(ovpn_files)) else "?"
                country = "?"
                if (hasattr(self.audio_rotator, "_extract_country")
                        and cur_idx is not None and cur_idx < len(ovpn_files)):
                    try:
                        country = self.audio_rotator._extract_country(server_name)
                    except Exception:
                        pass
                print(
                    f"    [audio-ip] STATE=FAKE "
                    f"(server=[{cur_idx}]{server_name}({country}), ip={cur_ip}, "
                    f"src_addr={src_addr})",
                    flush=True,
                )
            except Exception:
                print(f"    [audio-ip] STATE=FAKE (src_addr={src_addr})", flush=True)
            return IPRoutingInfo(source_address=src_addr, using_real_ip=False)

    def on_download_complete(self, bytes_dl: int, elapsed_s: float, ok: bool):
        """Callback sau khi download xong (ok) hoặc fail (ok=False).

        PRIORITY LOGIC (v10):
          - Ưu tiên #1: Download nhanh (tốc độ >= 1.0 MB/s) → GIỮ IP hiện tại
          - Ưu tiên #2: Khi chậm → nhảy IP (REAL→FAKE, hay rotate VPN server)
          - Ưu tiên #3 (v10 MỚI): Cycle về REAL sau `force_real_after_n_fake_fails`
            (mặc định 2) lần FAIL liên tiếp ở FAKE (ok=False). Trước đây cần
            đợi `fake_before_real` (5) lần → quá chậm khi IP bị stuck.
          - Ưu tiên #4: Cycle về REAL sau `fake_before_real` (5) lần FAKE
            CHẬM (speed < threshold) — giữ logic cũ cho speed-based.

        Args:
            bytes_dl: tổng bytes đã tải được (có thể = 0 nếu fail ngay).
            elapsed_s: tổng thời gian (giây).
            ok: True nếu download thành công, False nếu fail (exception).
        """
        speed_mbps = (
            (bytes_dl / elapsed_s / (1024 * 1024))
            if (elapsed_s > 0 and bytes_dl > 0) else 0.0
        )
        speed_ok = (
            ok
            and bytes_dl >= self.min_bytes_for_speed
            and elapsed_s >= self.min_window_seconds
            and speed_mbps >= self.min_speed_mbps
        )

        # Nếu chunk-progress hook đã flag "slow" (window avg < threshold)
        # thì coi như speed không OK, cần nhảy IP
        if self._slow_flag and ok:
            speed_ok = False

        if self._state == self.STATE_REAL:
            if speed_ok:
                # ✅ REAL IP TỐT: tốc độ >= 1.0 MB/s → GIỮ REAL
                print(f"    [audio-ip] ✅ REAL OK: {bytes_dl//1024}KB in {elapsed_s:.1f}s "
                      f"= {speed_mbps:.2f} MB/s (>= {self.min_speed_mbps} MB/s) → GIỮ REAL [reason=speed_ok]", flush=True)
                self._slow_flag = False
                self._consecutive_fake_slow = 0  # reset counter (just in case)
                return
            # ❌ REAL IP CHẬM: tốc độ < 1.0 MB/s → thử FAKE (VPN)
            # v13.1: ghi log rõ ràng transition REAL → FAKE
            print(f"    [audio-ip] ❌ REAL SLOW/FAIL: {bytes_dl//1024}KB in {elapsed_s:.1f}s "
                  f"= {speed_mbps:.2f} MB/s (ok={ok}) "
                  f"→ NHẢY sang FAKE (test VPN) [reason=rotate_to_fake]", flush=True)
            print(
                f"    [audio-ip] 🔄 STATE TRANSITION: REAL → FAKE "
                f"(IP thật chậm, sẽ thử VPN tunnel)",
                flush=True,
            )
            self._state = self.STATE_FAKE
            self._consecutive_fake_slow = 0  # reset counter (chưa thử FAKE nào cả)
            self._slow_flag = False
            self._ensure_vpn_disconnected()
            return

        # State == FAKE
        if speed_ok:
            # ✅ FAKE IP TỐT: tốc độ >= 1.0 MB/s → GIỮ FAKE (không rotate)
            print(f"    [audio-ip] ✅ FAKE OK: {bytes_dl//1024}KB in {elapsed_s:.1f}s "
                  f"= {speed_mbps:.2f} MB/s → GIỮ VPN server hiện tại [reason=speed_ok]", flush=True)
            self._consecutive_fake_slow = 0  # reset counter (tốc độ phục hồi)
            self._slow_flag = False
            return

        # ❌ FAKE IP CHẬM/FAIL: speed không OK → tăng counter
        # v10 FIX: Nếu ok=False (download fail, không phải chỉ chậm) VÀ đã
        # fail >= force_real_after_n_fake_fails lần liên tiếp ở FAKE → cycle
        # về REAL NGAY. Lý do: fail lặp lại = IP VPN/host bị stuck, force_rotate
        # sang VPN server khác không giúp ích vì tunnel vẫn đi qua cùng egress
        # hoặc host `rr1---sn-*.googlevideo.com` vẫn bị Google block.
        if not ok and (self._consecutive_fake_slow + 1) >= self.force_real_after_n_fake_fails:
            # Đã fail liên tiếp >= force_real_after_n_fake_fails lần ở FAKE → CYCLE REAL
            # v13.1: log đầy đủ IP fake cũ trước khi cycle về REAL
            old_fake_ip = "?"
            old_fake_idx = None
            old_fake_server = "?"
            old_fake_country = "?"
            if self.audio_rotator:
                old_fake_ip = getattr(self.audio_rotator, "_current_ip", "?")
                old_fake_idx = getattr(self.audio_rotator, "_current_idx", None)
                if old_fake_idx is not None and hasattr(self.audio_rotator, "_ovpn_files"):
                    try:
                        old_fake_server = self.audio_rotator._ovpn_files[old_fake_idx].name
                        if hasattr(self.audio_rotator, "_extract_country"):
                            old_fake_country = self.audio_rotator._extract_country(old_fake_server)
                    except Exception:
                        pass
            print(f"    [audio-ip] ❌ FAKE FAIL {self._consecutive_fake_slow + 1}/{self.force_real_after_n_fake_fails} lần "
                  f"liên tiếp (ok=False): {bytes_dl//1024}KB in {elapsed_s:.1f}s "
                  f"= {speed_mbps:.2f} MB/s → CYCLE về REAL [reason=force_real_after_{self.force_real_after_n_fake_fails}_fake_fails]",
                  flush=True)
            print(
                f"    [audio-ip] 🔄 STATE TRANSITION: FAKE → REAL "
                f"(drop fake=[{old_fake_idx}]{old_fake_server}({old_fake_country})@{old_fake_ip})",
                flush=True,
            )
            self._state = self.STATE_REAL
            self._consecutive_fake_slow = 0
            self._slow_flag = False
            self._total_rotates += 1
            self._ensure_vpn_disconnected()
            return

        self._consecutive_fake_slow += 1

        if self._consecutive_fake_slow >= self.fake_before_real:
            # Đã chậm liên tiếp fake_before_real lần ở FAKE → CYCLE về REAL
            # Lý do: Có thể REAL rate-limit đã reset, hoặc VPN tunnel chất lượng kém
            # v13.1: log đầy đủ IP fake cũ trước khi cycle về REAL
            old_fake_ip = "?"
            old_fake_idx = None
            old_fake_server = "?"
            old_fake_country = "?"
            if self.audio_rotator:
                old_fake_ip = getattr(self.audio_rotator, "_current_ip", "?")
                old_fake_idx = getattr(self.audio_rotator, "_current_idx", None)
                if old_fake_idx is not None and hasattr(self.audio_rotator, "_ovpn_files"):
                    try:
                        old_fake_server = self.audio_rotator._ovpn_files[old_fake_idx].name
                        if hasattr(self.audio_rotator, "_extract_country"):
                            old_fake_country = self.audio_rotator._extract_country(old_fake_server)
                    except Exception:
                        pass
            print(f"    [audio-ip] ❌ FAKE SLOW {self._consecutive_fake_slow}/{self.fake_before_real} lần "
                  f"liên tiếp: {bytes_dl//1024}KB in {elapsed_s:.1f}s "
                  f"= {speed_mbps:.2f} MB/s → CYCLE về REAL (reset rate-limit) [reason=fake_before_real_reached]",
                  flush=True)
            print(
                f"    [audio-ip] 🔄 STATE TRANSITION: FAKE → REAL "
                f"(drop fake=[{old_fake_idx}]{old_fake_server}({old_fake_country})@{old_fake_ip})",
                flush=True,
            )
            self._state = self.STATE_REAL
            self._consecutive_fake_slow = 0
            self._slow_flag = False
            self._total_rotates += 1
            self._ensure_vpn_disconnected()
            return

        # Chưa đủ ngưỡng → force_rotate sang VPN server khác
        # (chỉ áp dụng khi speed chậm nhưng OK, hoặc fail lần đầu tiên)
        # v13.1: Lấy thông tin IP fake TRƯỚC khi rotate để log đầy đủ
        old_ip = "?"
        old_idx = None
        old_server = "?"
        old_country = "?"
        if self.audio_rotator:
            old_ip = getattr(self.audio_rotator, "_current_ip", "?")
            old_idx = getattr(self.audio_rotator, "_current_idx", None)
            if old_idx is not None and hasattr(self.audio_rotator, "_ovpn_files"):
                try:
                    old_server = self.audio_rotator._ovpn_files[old_idx].name
                    if hasattr(self.audio_rotator, "_extract_country"):
                        old_country = self.audio_rotator._extract_country(old_server)
                except Exception:
                    pass

        print(f"    [audio-ip] ❌ FAKE SLOW/FAIL {self._consecutive_fake_slow}/{self.fake_before_real}: "
              f"{bytes_dl//1024}KB in {elapsed_s:.1f}s = {speed_mbps:.2f} MB/s "
              f"→ force_rotate từ [{old_idx}]{old_server}({old_country})@{old_ip} "
              f"[reason=audio-slow-{self._consecutive_fake_slow}]",
              flush=True)
        self._slow_flag = False
        self._total_rotates += 1
        try:
            if hasattr(self.audio_rotator, "force_rotate"):
                self.audio_rotator.force_rotate(f"audio-slow-{self._consecutive_fake_slow}")
                # v13.1: Lấy thông tin IP fake SAU khi rotate
                new_ip = getattr(self.audio_rotator, "_current_ip", "?")
                new_idx = getattr(self.audio_rotator, "_current_idx", "?")
                new_server = "?"
                new_country = "?"
                if isinstance(new_idx, int) and hasattr(self.audio_rotator, "_ovpn_files"):
                    try:
                        new_server = self.audio_rotator._ovpn_files[new_idx].name
                        if hasattr(self.audio_rotator, "_extract_country"):
                            new_country = self.audio_rotator._extract_country(new_server)
                    except Exception:
                        pass
                print(
                    f"      [audio-ip] ✅ IP-FAKE ROTATED: "
                    f"[{old_idx}]{old_server}({old_country})@{old_ip} "
                    f"→ [{new_idx}]{new_server}({new_country})@{new_ip}",
                    flush=True,
                )
        except Exception as e:
            print(f"      [audio-ip] force_rotate ERROR: {e}", flush=True)

    def on_chunk_progress(self, bytes_dl: int, elapsed_s: float, speed_bps: float):
        """Callback mid-download từ yt-dlp progress hook.

        v13.4: Fixed window logic với window_size configurable qua CLI
        (`--audio-speed-avg-window-seconds`, mặc định 30s).

        Bỏ qua 5s đầu (handshake YouTube). Sau đó chia download thành các
        window CỐ ĐỊNH, KHÔNG TRÙNG:
          - window 0: (5, 5+W]s
          - window 1: (5+W, 5+2W]s
          - window 2: (5+2W, 5+3W]s
          - ...

        MỖI window đánh giá avg speed 1 LẦN ở ĐẦU window TIẾP THEO:
          avg_speed_MBps = bytes_trong_window_vừa_kết_thúc / (W × 1024²)

        Nếu avg speed < `min_speed_mbps` (MB/s) → set `_slow_flag = True`.

        Hysteresis: phục hồi nếu avg >= min × 1.1.

        KHÔNG raise ở đây — hook progress đọc _slow_flag và raise MidDownloadRotate.
        """
        if bytes_dl < self.min_bytes_for_speed:
            return

        # SKIP 5s đầu (handshake YouTube)
        if elapsed_s < 5.0:
            return

        window_size = self.speed_avg_window_seconds  # mặc định 30s qua CLI
        measure_elapsed = elapsed_s - 5.0
        window_idx = int(measure_elapsed / window_size)

        # Nếu đã ở window này rồi (cùng idx) → skip
        if window_idx == self._window_baseline_idx:
            return

        # Vào window MỚI → tính speed của window VỪA KẾT THÚC
        if self._window_baseline_idx >= 0:
            prev_window_idx = window_idx - 1
            bytes_in_prev_window = bytes_dl - self._window_baseline_bytes
            if bytes_in_prev_window >= self.min_bytes_for_speed:
                prev_speed_mbps = bytes_in_prev_window / (window_size * 1024 * 1024)
                t_start_win = 5.0 + prev_window_idx * window_size
                t_end_win = 5.0 + (prev_window_idx + 1) * window_size
                if prev_speed_mbps < self.min_speed_mbps and not self._slow_flag:
                    self._slow_flag = True
                    if not getattr(self, "_slow_logged_this_dl", False):
                        print(f"    [audio-ip] CHUNK-SLOW window({t_start_win:.0f},{t_end_win:.0f}] "
                              f"{bytes_in_prev_window//1024}KB in {window_size:.0f}s "
                              f"= {prev_speed_mbps:.2f} MB/s "
                              f"(< {self.min_speed_mbps} MB/s) → sẽ nhảy IP",
                              flush=True)
                        self._slow_logged_this_dl = True
                elif prev_speed_mbps >= self.min_speed_mbps * 1.1 and self._slow_flag:
                    # Hysteresis: phục hồi nếu speed OK với 10% cushion
                    self._slow_flag = False

        # Cập nhật baseline cho window mới
        self._window_baseline_idx = window_idx
        self._window_baseline_bytes = bytes_dl

    def on_download_start_reset_slow_log(self):
        """Reset slow state khi bắt đầu attempt mới."""
        self._slow_logged_this_dl = False
        self._slow_flag = False
        self._window_baseline_bytes = 0
        self._window_baseline_idx = -1

    def get_state(self) -> str:
        return self._state

    def stats(self) -> dict:
        return {
            "state": self._state,
            "consecutive_fake_slow": self._consecutive_fake_slow,
            "total_real_uses": self._total_real_uses,
            "total_fake_uses": self._total_fake_uses,
            "total_rotates": self._total_rotates,
            "min_speed_mbps": self.min_speed_mbps,
            "fake_before_real": self.fake_before_real,
            "speed_avg_window_seconds": self.speed_avg_window_seconds,
        }

    def reset(self):
        """Reset state về REAL + counter về 0."""
        self._state = self.STATE_REAL
        self._consecutive_fake_slow = 0
        self._slow_flag = False
        self._speed_samples = []  # deprecated rolling window
        self._window_baseline_bytes = 0
        self._window_baseline_idx = -1
        self._ensure_vpn_disconnected()

    # ----- Internal -----

    def _ensure_vpn_disconnected(self):
        """Đảm bảo VPN tunnel đã tắt → traffic đi qua default route = IP thật."""
        try:
            if hasattr(self.audio_rotator, "disconnect"):
                self.audio_rotator.disconnect()
        except Exception as e:
            print(f"    [audio-ip] disconnect error (ignored): {e}")

    def _ensure_vpn_connected(self):
        """Đảm bảo VPN tunnel đang lên (nếu rotator có cache tunnel)."""
        # next() sẽ trigger reconnect nếu cần
        try:
            if hasattr(self.audio_rotator, "next"):
                self.audio_rotator.next()
        except Exception as e:
            print(f"    [audio-ip] next() error (ignored): {e}")


# ================= COOKIES =================
COOKIES_FILE = Path(__file__).parent / "cookies.txt"
COOKIES_FILE_STR = str(COOKIES_FILE) if COOKIES_FILE.exists() else None

# ================= v14: COOKIES RELOAD TRACKING =================
# v14: Track mtime của cookies.txt để reload nếu file thay đổi.
# Vấn đề v13: COOKIES_FILE_STR được set 1 lần lúc module import. Nếu user
# update cookies.txt giữa chức năng (vd qua browser extension) thì code vẫn
# dùng cookies cũ.
#
# Fix v14: Check mtime mỗi lần gọi `_reload_cookies_if_changed()`. Nếu
# cookies.txt đã đổi → reload path. Cache kết quả trong _COOKIES_LAST_MTIME
# để tránh stat() liên tục.
_COOKIES_LAST_MTIME: Optional[float] = None
_COOKIES_LAST_LOAD_TIME: float = 0.0
_COOKIES_TTL_SECONDS: float = 60.0  # reload tối đa mỗi 60s nếu mtime đổi

# ================= v14 (port từ v15): PLAYER_CLIENT ROTATION LIST =================
# Mỗi player_client của yt-dlp trả về SET SUB TRACKS KHÁC NHAU cho cùng 1 video.
# Rotate qua nhiều client tăng tỷ lệ tìm được "vi-orig" (auto-gen gốc của YouTube).
#
# yt-dlp 2026.06.09 available clients:
#   tv, tv_downgraded, tv_simply, web, web_safari, web_embedded,
#   web_creator, web_music, mweb, android, android_vr, ios
#
# CHÚ Ý:
#   - tv_embedded KHÔNG CÒN TỒN TẠI (removed từ yt-dlp ~2025)
#   - android, ios: SUPPORTS_COOKIES=False → hay bị block khi dùng cookies
#   - ios: priority=0, hay lỗi "Failed to extract player response"
#   - tv: priority cao nhất (40), support cookies
#
# Khi gặp client trả EMPTY → KHÔNG phải "video no subs" mà là "client này ko trả subs"
# → caller retry client tiếp theo (status="client_empty" mới).
PLAYER_CLIENT_ROTATION_LIST = [
    "web_embedded",     # support cookies, trả subs tốt
    "tv",               # priority cao nhất, support cookies
    "tv_embedded",      # có thể trả EMPTY nhưng giữ lại theo yêu cầu
    "web_creator",      # support cookies, ít bị rate-limit
    "mweb",             # support cookies, fallback
    "web_safari",       # cuối cùng
]

# ================= CONFIG =================
_YOUTUBE_API_KEYS: list = []
for _k in [
    "YOUTUBE_API_KEY",
    "YOUTUBE_API_KEY_1", "YOUTUBE_API_KEY_2", "YOUTUBE_API_KEY_3",
    "YOUTUBE_API_KEY_4", "YOUTUBE_API_KEY_5", "YOUTUBE_API_KEY_6",
    "YOUTUBE_API_KEY_7",
]:
    _v = os.environ.get(_k, "")
    if _v and _v not in _YOUTUBE_API_KEYS:
        _YOUTUBE_API_KEYS.append(_v)

# ================= FILTER CONFIG =================
FILTER_MIN_DURATION = 120      # 30 phút
FILTER_MAX_DURATION = 10000000   # không cap
FILTER_MIN_VIEW_COUNT = 0


@dataclass
class VideoCandidate:
    video_id: str
    title: str
    channel: str
    description: str
    published_at: str
    duration: str
    view_count: int = 0
    like_count: int = 0
    comment_count: int = 0
    transcript: Optional[str] = None
    transcript_language: str = ""
    transcript_is_auto: bool = False
    thumbnail: str = ""
    url: str = ""
    tags: list = field(default_factory=list)
    category_id: str = ""
    categories: list = field(default_factory=list)
    default_language: str = ""
    default_audio_language: str = ""
    caption_available: bool = False
    definition: str = ""
    channel_id: str = ""
    channel_url: str = ""
    channel_follower_count: int = 0
    uploader: str = ""
    uploader_id: str = ""
    uploader_url: str = ""
    duration_string: str = ""
    audio_filename: str = ""
    # === Subs URLs cache (Phase 2 yt-dlp extract_info) ===
    # Dùng cho Bucket B (TRANSCRIBE-ONLY) để skip gọi yt-dlp lần 2.
    subtitles: dict = field(default_factory=dict)          # Manual subtitles URLs
    automatic_captions: dict = field(default_factory=dict) # Auto-caption URLs
    passed_filters: list = field(default_factory=list)
    failed_filters: list = field(default_factory=list)

    # === v3: YouTube API metadata bổ sung (12 field) ===
    dimension: str = "2d"                    # "2d" / "3d"
    licensed_content: bool = False         # Video có license YouTube
    projection: str = "rectangular"        # "rectangular" / "360"
    privacy_status: str = ""               # public/unlisted/private
    embeddable: bool = True                # Có embed được không
    made_for_kids: bool = False            # Designed for kids
    live_broadcast_content: str = "none"   # none/live/upcoming
    topic_categories: list = field(default_factory=list)  # Wikipedia topics
    recording_location: str = ""           # Vị trí địa lý (nếu có)
    live_status: str = "not_live"          # not_live/is_live/was_live
    was_live: bool = False                 # Đã live trước đó
    availability: str = ""                 # public/unlisted/private/...

    @property
    def video_url(self) -> str:
        return f"https://www.youtube.com/watch?v={self.video_id}"


@dataclass
class FilterCriteria:
    published_after: Optional[datetime] = None
    min_duration: Optional[int] = None
    max_duration: Optional[int] = None
    min_view_count: int = 0


# ================= HELPERS =================
def parse_duration(duration_str: str) -> int:
    if not duration_str:
        return 0
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    match = re.match(pattern, duration_str)
    if not match:
        return 0
    hours = int(match.group(1) or 0)
    minutes = int(match.group(2) or 0)
    seconds = int(match.group(3) or 0)
    return hours * 3600 + minutes * 60 + seconds


def format_duration(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"


def format_number(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    elif n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


# ================= CHANNEL ID RESOLUTION =================
def _channel_id_cache_path() -> Path:
    return Path(__file__).parent / ".cache_shared" / "channel_id_cache.json"


def _load_channel_id_cache() -> dict:
    p = _channel_id_cache_path()
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _save_channel_id_cache(cache: dict):
    p = _channel_id_cache_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  [warn] khong luu duoc channel_id cache: {e}")


def resolve_channel_id(api_key: str, channel_input: str,
                        proxy_url: Optional[str] = None) -> Optional[str]:
    if channel_input.startswith("UC") and len(channel_input) == 24:
        return channel_input
    url = channel_input.strip().rstrip("/")
    channel_match = re.search(r'youtube\.com/channel/([^/\s?]+)', url)
    if channel_match:
        cid = channel_match.group(1)
        if cid.startswith("UC") and len(cid) == 24:
            return cid
    cache = _load_channel_id_cache()
    if channel_input in cache:
        return cache[channel_input]
    if _YOUTUBE_API_KEYS:
        import requests as _req
        handle_match = re.search(r"youtube\.com/@([^/\s?]+)", url)
        custom_match = re.search(r"youtube\.com/c/([^/\s?]+)", url)
        user_match = re.search(r"youtube\.com/user/([^/\s?]+)", url)
        bare_handle_match = None
        if not handle_match and not custom_match and not user_match:
            if url.startswith("@"):
                bare_handle_match = re.match(r"@([^/\s?]+)", url)
            elif re.match(r"^[\w.-]+$", url) and not url.startswith("UC"):
                bare_handle_match = re.match(r"([\w.-]+)", url)
        try:
            for api_key in _YOUTUBE_API_KEYS:
                if handle_match:
                    resp = _req.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"key": api_key, "part": "id",
                                "forHandle": "@" + handle_match.group(1)},
                        timeout=10,
                    )
                elif custom_match or user_match:
                    m = custom_match or user_match
                    resp = _req.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"key": api_key, "part": "id",
                                "forUsername": m.group(1)},
                        timeout=10,
                    )
                elif bare_handle_match:
                    resp = _req.get(
                        "https://www.googleapis.com/youtube/v3/channels",
                        params={"key": api_key, "part": "id",
                                "forHandle": "@" + bare_handle_match.group(1)},
                        timeout=10,
                    )
                else:
                    return None
                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get("items", [])
                    if items:
                        cid = items[0].get("id", "")
                        if cid:
                            cache[channel_input] = cid
                            _save_channel_id_cache(cache)
                            return cid
                elif resp.status_code == 403:
                    continue
        except Exception as e:
            print(f"  [API] resolve_channel_id error: {e}")
    # Fallback yt-dlp
    try:
        import yt_dlp
        ydl_opts = {
            "quiet": True, "no_warnings": True, "skip_download": True,
            "ignoreerrors": True, "playlistend": 1,
        }
        if proxy_url:
            ydl_opts["proxy"] = proxy_url
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
        if info:
            cid = info.get("channel_id") or info.get("id")
            if cid and cid.startswith("UC"):
                cache[channel_input] = cid
                _save_channel_id_cache(cache)
                return cid
    except Exception as e:
        print(f"  [yt-dlp] resolve_channel_id error: {e}")
    return None


# ================= YT-DLP INFO FETCH =================
def fetch_video_info_via_ytdlp(video_id: str,
                               proxy_url: Optional[str] = None) -> dict | None:
    try:
        import yt_dlp
    except ImportError:
        return None
    ydl_opts = {
        "quiet": True, "no_warnings": True, "skip_download": True,
        "ignoreerrors": True, "js_runtimes": {"node": {}},
    }
    YouTubeResearcher._apply_auth_skip(ydl_opts)
    if COOKIES_FILE_STR:
        ydl_opts["cookiefile"] = COOKIES_FILE_STR
    if proxy_url:
        ydl_opts["proxy"] = proxy_url
    target_url = f"https://www.youtube.com/watch?v={video_id}"
    last_err = None
    for attempt in range(1, 3):
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(target_url, download=False)
            return info
        except Exception as e:
            last_err = e
            err_msg = str(e).lower()
            is_blocked = any(k in err_msg for k in [
                'captcha', 'challenge', 'sign in', 'bot', '429',
                'too many', 'rate limit', 'forbidden', '403', 'blocked',
            ])
            if is_blocked and attempt < 2:
                time.sleep(5)
                continue
            return None
    return None


# ================= YouTubeResearcher =================
class YouTubeResearcher:
    """Lấy video theo kênh (channel URL) → tải audio + YouTube subs.

    3 rotator TÁCH BIỆT (optional, default fallback về proxy_rotator):
      - proxy_rotator           : cho metadata (channel listing, Data API)
      - audio_proxy_rotator     : cho audio download
      - transcript_proxy_rotator: cho transcript fetch (yt-dlp subs + API fallback)
    """

    def __init__(self, api_key: str, output_dir: str = "./researched_videos",
                 proxy_rotator: Optional[VPNRotator] = None,
                 audio_proxy_rotator: Optional[VPNRotator] = None,
                 transcript_proxy_rotator: Optional[VPNRotator] = None,
                 key_rotator: Optional["YouTubeKeyRotator"] = None,
                 proxy_mode: str = "auto",
                 audio_min_speed_mbps: float = 1.0,
                 audio_fake_before_real: int = 5,
                 audio_min_bytes_for_speed: int = 256 * 1024,
                 audio_min_window_seconds: float = 5.0,
                 audio_speed_avg_window_seconds: float = 30.0,
                 audio_500_threshold: int = 5,
                 audio_stall_seconds: float = 30.0,
                 audio_force_real_after_fails: int = 2,    # v10
                 # === v13: mid-download slow-speed rotation ===
                 # v13: default threshold 500 KB/s (sau khi sửa từ 100 KB/s).
                 # Lý do: khớp với default 500.0 KB/s của run_crawl_v13.sh và
                 # argparse. YouTube audio 192kbps peak ~250-500 KB/s. Threshold
                 # 500 KB/s bắt được "2x real-time" = CHẬM. Threshold < 100 KB/s
                 # = false positive. Threshold > 1000 KB/s = không bao giờ fire.
                 audio_slow_speed_kbps: float = 500.0,
                 audio_slow_window_seconds: float = 30.0,
                 audio_max_rotate_per_video: int = 3,
                 # === v14: TỐI ƯU VIETSUB ===
                 vi_sub_priority: str = "auto_first",
                 no_marker_ttl_days: float = 7.0,
                 respect_no_transcript_marker: bool = False,
                 retry_no_transcript: bool = False,
                 retry_no_transcript_force: bool = False,
                 # === v14: youtube-transcript-api fallback ===
                 no_api_fallback: bool = False,
                 api_fallback_langs: str = "vi,en",
                 # === v14 (port từ v15): PLAYER_CLIENT ROTATION ===
                 player_client_rotate: bool = True,
                 player_clients: Optional[str] = None):
        self.api_key = api_key
        # v6: YouTubeKeyRotator cho Phase 1 (playlistItems.list).
        # Nếu không truyền → tự tạo từ api_key (single-key mode, không rotate).
        if key_rotator is not None:
            self.key_rotator = key_rotator
        elif api_key and api_key != "ytdlp":
            self.key_rotator = YouTubeKeyRotator([api_key])
        else:
            self.key_rotator = None
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._videos: list[VideoCandidate] = []
        self._filtered_videos: list[VideoCandidate] = []
        # === METADATA rotator (proxy_rotator) ===
        self._rotator = proxy_rotator
        # === AUDIO rotator (riêng biệt) ===
        # Nếu không truyền → fallback dùng proxy_rotator (backward-compat)
        self._audio_rotator = (
            audio_proxy_rotator if audio_proxy_rotator is not None else proxy_rotator
        )
        # === TRANSCRIPT rotator (riêng biệt) ===
        # Nếu không truyền → fallback dùng proxy_rotator (backward-compat)
        self._transcript_rotator = (
            transcript_proxy_rotator
            if transcript_proxy_rotator is not None
            else proxy_rotator
        )
        self._proxy_mode = proxy_mode
        self._use_vpn_tunnel = True  # BẮT BUỘC
        self._direct_blocked = False
        self._audio_escalated = False
        self._transcript_escalated = False

        # === v5: AudioIPController cho audio download ===
        # Controller quản lý state machine REAL/FAKE dựa trên đo tốc độ
        # liên tục qua yt-dlp progress hook. Chỉ áp dụng cho audio,
        # KHÔNG ảnh hưởng metadata/transcript.
        self._audio_ip_ctl = AudioIPController(
            audio_rotator=self._audio_rotator,
            min_speed_mbps=audio_min_speed_mbps,
            fake_before_real=audio_fake_before_real,
            min_bytes_for_speed=audio_min_bytes_for_speed,
            min_window_seconds=audio_min_window_seconds,
            speed_avg_window_seconds=audio_speed_avg_window_seconds,
            force_real_after_n_fake_fails=audio_force_real_after_fails,  # v10
        )

        # === v7: HTTP500Detector (bắt HTTP 500 liên tiếp → cycle IP) ===
        # Khởi tạo SAU _audio_ip_ctl vì callback cycle IP dùng controller.
        self._http500_detector = HTTP500Detector(
            threshold=audio_500_threshold,
            stall_seconds=audio_stall_seconds,
            on_http500_threshold=self._on_http500_threshold,
        )
        # === v13: Mid-download slow-speed rotation config ===
        # Lưu vào self để _audio_progress_hook đọc và raise MidDownloadRotate.
        self._v13_slow_speed_kbps = audio_slow_speed_kbps
        self._v13_slow_window_seconds = audio_slow_window_seconds
        self._v13_max_rotate_per_video = audio_max_rotate_per_video
        # === v13: Smart downloader for stuck-IP detection ===
        self._smart_dl = get_smart_downloader(
            ip_controller=getattr(self, '_audio_ip_ctl', None),
            audio_rotator=getattr(self, '_audio_rotator', None),
            http500_detector=self._http500_detector,
        ) if V13_SMART_AVAILABLE else None

        # === v14: marker TTL config ===
        # TTL mặc định 7 ngày cho marker `.no_transcript`. Sau TTL → retry.
        self._v14_marker_ttl_days = no_marker_ttl_days
        self._v14_respect_marker = respect_no_transcript_marker
        # === v14: VI sub priority mode ===
        # "auto_first" | "manual_first"
        # - auto_first (default): auto-captions được ưu tiên hơn manual.
        #   Phù hợp cho video VN mà chỉ có auto-gen.
        # - manual_first: manual subs được bump lên top.
        #   Phù hợp cho VTV, FAPTV có uploader tự upload sub.
        self._v14_vi_priority = vi_sub_priority
        # === v14: cho phép retry video đã mark .no_transcript (ghi đè TTL) ===
        # retry_no_transcript = bỏ qua marker cũ (> TTL)
        # retry_no_transcript_force = bỏ qua cả marker MỚI
        self._v14_retry_no_transcript = retry_no_transcript or retry_no_transcript_force
        # === v14: youtube-transcript-api fallback ===
        # v18-FIX: Mặc định BẬT (chỉ TẮT khi --no-api-fallback được set).
        # Trước v18, bug: cần cả env TRANSCRIPT_API_FALLBACK=1 mới bật
        # → fallback không bao giờ chạy dù shell script quảng cáo "mặc định BẬT".
        self._v14_api_fallback_enabled = (not no_api_fallback)
        # Lưu danh sách ngôn ngữ ưu tiên cho api fallback
        self._v14_api_fallback_langs = [
            s.strip() for s in (api_fallback_langs or "vi,en").split(",")
            if s.strip()
        ]
        # === v14: Every-10-audio IP rotate counter ===
        # Cứ mỗi N audio đã xử lý → tự động force_rotate IP fake của transcript_rotator.
        # Tránh Google rate-limit cùng 1 IP sau nhiều request liên tiếp.
        # Default 10 (TRANSCRIPT_ROTATE_EVERY=10), set 0 = tắt.
        self._v14_transcript_video_counter = 0
        # === v14 (port từ v15): PLAYER_CLIENT ROTATION ===
        # Mỗi attempt dùng player_client khác nhau để bypass captcha/EMPTY.
        self._v14_player_client_rotate = player_client_rotate
        if player_clients:
            self._v14_player_clients = [
                s.strip() for s in player_clients.split(",") if s.strip()
            ]
        else:
            self._v14_player_clients = list(PLAYER_CLIENT_ROTATION_LIST)

    def _next_proxy(self) -> Optional[str]:
        if not self._rotator:
            return None
        url = self._rotator.next()
        if url:
            try:
                from urllib.parse import urlparse
                p = urlparse(url)
                short = f"{p.hostname}:{p.port}"
            except Exception:
                short = url[:40]
            print(f"    [proxy] → {short}")
        return url

    def _next_proxy_for_transcript(self) -> Optional[str]:
        """Lấy proxy URL tiếp theo từ transcript_rotator (riêng biệt)."""
        if not self._transcript_rotator:
            return None
        url = self._transcript_rotator.next()
        if url:
            try:
                from urllib.parse import urlparse
                p = urlparse(url)
                short = f"{p.hostname}:{p.port}"
            except Exception:
                short = url[:40]
            print(f"    [transcript-proxy] → {short}")
        return url

    def _next_proxy_for_audio(self) -> Optional[str]:
        """Lấy proxy URL tiếp theo từ audio_rotator (riêng biệt)."""
        if not self._audio_rotator:
            return None
        url = self._audio_rotator.next()
        if url:
            try:
                from urllib.parse import urlparse
                p = urlparse(url)
                short = f"{p.hostname}:{p.port}"
            except Exception:
                short = url[:40]
            print(f"    [audio-proxy] → {short}")
        return url

    def _proxy_guard(self):
        if not self._rotator:
            class _NoOp:
                def __enter__(self): return self
                def __exit__(self, *a): return False
            return _NoOp()
        return self._rotator.acquire()

    def _proxy_guard_for_transcript(self):
        """Context manager bảo vệ tunnel transcript_rotator."""
        if not self._transcript_rotator:
            class _NoOp:
                def __enter__(self): return self
                def __exit__(self, *a): return False
            return _NoOp()
        return self._transcript_rotator.acquire()

    def _proxy_guard_for_audio(self):
        """Context manager bảo vệ tunnel audio_rotator."""
        if not self._audio_rotator:
            class _NoOp:
                def __enter__(self): return self
                def __exit__(self, *a): return False
            return _NoOp()
        return self._audio_rotator.acquire()

    def _mark_proxy_failed(self, proxy_url: Optional[str]):
        if self._rotator and proxy_url:
            self._rotator.mark_failed(proxy_url)

    def _mark_proxy_dead(self, proxy_url: Optional[str]):
        if self._rotator and proxy_url:
            self._rotator.remove_proxy(proxy_url)

    def _mark_audio_proxy_failed(self, proxy_url: Optional[str]):
        """Mark failed trên audio_rotator (không ảnh hưởng metadata/transcript)."""
        if self._audio_rotator and proxy_url:
            try:
                self._audio_rotator.mark_failed(proxy_url)
            except Exception:
                pass

    def _mark_transcript_proxy_failed(self, proxy_url: Optional[str]):
        """Mark failed trên transcript_rotator (không ảnh hưởng metadata/audio)."""
        if self._transcript_rotator and proxy_url:
            try:
                self._transcript_rotator.mark_failed(proxy_url)
            except Exception:
                pass

    def _mark_audio_proxy_dead(self, proxy_url: Optional[str]):
        """Mark dead trên audio_rotator."""
        if self._audio_rotator and proxy_url:
            try:
                self._audio_rotator.remove_proxy(proxy_url)
            except Exception:
                pass

    def _mark_transcript_proxy_dead(self, proxy_url: Optional[str]):
        """Mark dead trên transcript_rotator."""
        if self._transcript_rotator and proxy_url:
            try:
                self._transcript_rotator.remove_proxy(proxy_url)
            except Exception:
                pass

    def _proxy_for_fallback(self) -> Optional[str]:
        if self._direct_blocked and self._rotator and len(self._rotator) > 0:
            return self._next_proxy()
        return None

    def _proxy_for_transcript_fallback(self) -> Optional[str]:
        """Direct-first cho transcript: trả None nếu chưa bị block,
        trả proxy từ transcript_rotator nếu đã bị block.
        """
        if self._direct_blocked and self._transcript_rotator and len(self._transcript_rotator) > 0:
            return self._next_proxy_for_transcript()
        return None

    def _proxy_for_audio_fallback(self) -> Optional[str]:
        """Direct-first cho audio: trả None nếu chưa bị block,
        trả proxy từ audio_rotator nếu đã bị block.
        """
        if self._direct_blocked and self._audio_rotator and len(self._audio_rotator) > 0:
            return self._next_proxy_for_audio()
        return None

    def _is_youtube_blocked_error(self, err) -> bool:
        err_str = str(err).lower()
        keys = ('429', '500', '503', 'too many requests', 'rate limit',
                'quota exceeded', 'internal server error',
                'service unavailable', 'bad gateway', 'gateway timeout',
                '403', 'forbidden', 'blocked', 'access denied',
                'sign in to confirm', 'not a bot', 'bot check',
                'captcha', 'challenge',
                'timed out', 'connect timeout', 'read timeout',
                'connection reset', 'broken pipe', 'ssl')
        return any(k in err_str for k in keys)

    def _on_http500_threshold(self, fragment_500_count: int, total_frags: int):
        """v7: Callback khi HTTP500Detector vượt ngưỡng.

        Trigger cycle IP thông qua AudioIPController.on_download_complete()
        với ok=False. Controller sẽ:
          - Nếu đang REAL → chuyển sang FAKE (test VPN).
          - Nếu đang FAKE + consecutive_fake_slow < force_real_after_n_fake_fails (2)
            → force_rotate sang VPN server khác.
          - Nếu đang FAKE + consecutive_fake_slow >= force_real_after_n_fake_fails (2)
            → cycle về REAL NGAY (v10: thay vì chờ fake_before_real=5).

        Args:
            fragment_500_count: số fragment 500 trong download hiện tại.
            total_frags: tổng fragment (0 nếu trigger từ stall detector).
        """
        kind = "stall" if total_frags == 0 else "HTTP 500"
        current_state = self._audio_ip_ctl.get_state()
        print(f"  [v7-detector] ⚠️  {kind} threshold hit: "
              f"{fragment_500_count} fragments, {total_frags} total frags. "
              f"state={current_state} "
              f"→ cycle IP via AudioIPController [reason={kind.lower().replace(' ', '_')}_detected]",
              flush=True)

        # Báo cho AudioIPController: fail = ok=False. Controller sẽ tự quyết
        # định state tiếp theo dựa trên state hiện tại + counter.
        try:
            # Dùng elapsed dummy để tránh div-by-zero (controller tính speed
            # nhưng với ok=False thì speed_ok=False, không ảnh hưởng).
            elapsed = 1.0
            self._audio_ip_ctl.on_download_complete(
                bytes_dl=0, elapsed_s=elapsed, ok=False,
            )
        except Exception as e:
            print(f"  [v7-detector] on_download_complete error: {e}", flush=True)

    def _on_youtube_blocked(self, err, proxy_url, context: str):
        if not proxy_url:
            self._direct_blocked = True
            return
        if is_proxy_dead_error(err):
            self._mark_proxy_dead(proxy_url)
        else:
            self._mark_proxy_failed(proxy_url)

    def _on_youtube_blocked_transcript(self, err, proxy_url, context: str):
        """Handler riêng cho transcript rotator."""
        if not proxy_url:
            # Đang dùng IP thật → escalate dùng transcript_rotator
            self._direct_blocked = True
            return
        if is_proxy_dead_error(err):
            self._mark_transcript_proxy_dead(proxy_url)
        else:
            self._mark_transcript_proxy_failed(proxy_url)

    def _on_youtube_blocked_audio(self, err, proxy_url, context: str):
        """Handler riêng cho audio rotator."""
        if not proxy_url:
            # Đang dùng IP thật → escalate dùng audio_rotator
            self._direct_blocked = True
            return
        if is_proxy_dead_error(err):
            self._mark_audio_proxy_dead(proxy_url)
        else:
            self._mark_audio_proxy_failed(proxy_url)

    @staticmethod
    def _apply_cookies(ydl_opts: dict) -> dict:
        if COOKIES_FILE_STR:
            ydl_opts["cookiefile"] = COOKIES_FILE_STR
        return ydl_opts

    @staticmethod
    def _reload_cookies_if_changed() -> Optional[str]:
        """v14: Check mtime cookies.txt → reload COOKIES_FILE_STR nếu file đổi.

        Vấn đề v13: COOKIES_FILE_STR được set 1 lần lúc module import. Nếu user
        update cookies.txt giữa chức năng (vd qua browser extension) thì code vẫn
        dùng cookies cũ.

        Fix v14: Check mtime mỗi lần gọi. Nếu cookies.txt đã đổi → reload path.
        Cache kết quả trong _COOKIES_LAST_MTIME để tránh stat() liên tục.

        Returns:
            Path string hiện tại của cookies.txt (None nếu không tồn tại).
        """
        global _COOKIES_LAST_MTIME, _COOKIES_LAST_LOAD_TIME, COOKIES_FILE_STR
        if not COOKIES_FILE.exists():
            return None
        try:
            mtime = COOKIES_FILE.stat().st_mtime
            now = time.time()
            # Reload nếu file đổi HOẶC đã quá TTL
            if (mtime != _COOKIES_LAST_MTIME
                    or (now - _COOKIES_LAST_LOAD_TIME) > _COOKIES_TTL_SECONDS):
                COOKIES_FILE_STR = str(COOKIES_FILE)
                _COOKIES_LAST_MTIME = mtime
                _COOKIES_LAST_LOAD_TIME = now
                age_sec = now - mtime
                print(f"    [v14-cookies] reloaded cookies.txt "
                      f"(age={age_sec:.0f}s, mtime={mtime:.0f})")
        except Exception as e:
            print(f"    [v14-cookies] stat failed: {e}")
        return COOKIES_FILE_STR

    @staticmethod
    def _apply_auth_skip(ydl_opts: dict, player_client: Optional[str] = None) -> dict:
        """v14 (port từ v15): Apply yt-dlp auth_skip + optional player_client override.

        Args:
            ydl_opts: yt-dlp options dict (will be mutated).
            player_client: nếu set → override player_client mặc định
                (dùng 1 client cụ thể, vd "tv_embedded").
                Nếu None → giữ default ["web_safari", "web"] (giống v14).
        """
        ydl_opts.setdefault("extractor_args", {})
        if "youtube" not in ydl_opts["extractor_args"]:
            ydl_opts["extractor_args"]["youtube"] = {}
        yt_args = ydl_opts["extractor_args"]["youtube"]
        if "skip" not in yt_args:
            yt_args["skip"] = []
        if "authcheck" not in yt_args["skip"]:
            yt_args["skip"].append("authcheck")
        if player_client:
            # v15: dùng 1 client cụ thể (rotate từ caller)
            yt_args["player_client"] = [player_client]
        elif "player_client" not in yt_args:
            yt_args["player_client"] = ["tv", "web_embedded"]
        if "js_runtimes" not in ydl_opts:
            # v18-FIX: Dùng shutil.which("node") thay vì hardcode path user cụ thể
            import shutil
            node_path = shutil.which("node") or "node"
            ydl_opts["js_runtimes"] = {"node": {"path": node_path}}
        # EJS (External JavaScript) challenge solver: tải script từ GitHub.
        # Cần thiết vì YouTube đã đổi signature scheme — không có EJS thì
        # yt-dlp chỉ lấy được formats ảnh (WARNING: Only images are available).
        # Format: list chuỗi "ejs:github" (KHÔNG phải dict — dict sẽ bị ignore).
        ydl_opts["remote_components"] = ["ejs:github"]  # noqa: E501
        ydl_opts["extractor_args"].setdefault("youtubepot-bgutilhttp", {})
        if "base_url" not in ydl_opts["extractor_args"]["youtubepot-bgutilhttp"]:
            # v18: Đọc POT_PORT từ env (shell export), fallback 4416
            _pot_port = os.environ.get("POT_PORT", "4416")
            ydl_opts["extractor_args"]["youtubepot-bgutilhttp"]["base_url"] = [
                f"http://127.0.0.1:{_pot_port}"]
        return ydl_opts

    @staticmethod
    def _apply_timeouts(ydl_opts: dict, socket_timeout: int = 30) -> dict:
        ydl_opts["socket_timeout"] = socket_timeout
        return ydl_opts

    @staticmethod
    def _short_proxy(proxy_url: Optional[str]) -> str:
        if not proxy_url:
            return "no-proxy"
        try:
            from urllib.parse import urlparse
            p = urlparse(proxy_url)
            return f"{p.hostname}:{p.port}"
        except Exception:
            return proxy_url[:40]

    @staticmethod
    def _safe_filename(title: str, fallback: str = "audio", max_length: int = 100) -> str:
        if not title:
            return fallback
        try:
            import unicodedata
            normalized = unicodedata.normalize("NFKD", title)
            cleaned = "".join(ch for ch in normalized
                              if not unicodedata.combining(ch))
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

    # ================== LIVE UNAVAILABLE MARKER (v13.1) ==================
    @staticmethod
    def _live_unavailable_marker_path(transcriptions_root, video_id: str) -> "Path":
        """Path của marker file "live not started" cho 1 video.

        Format: <transcriptions_root>/<video_id>.live_unavailable
        Tương tự pattern `.no_transcript` đã có sẵn trong code (xem
        `_write_no_transcript_marker`).
        """
        return Path(transcriptions_root) / f"{video_id}.live_unavailable"

    @staticmethod
    def _write_live_unavailable_marker(transcriptions_root, video_id: str,
                                       reason: str = "live_not_started") -> bool:
        """v13.1: Ghi marker file khi gặp live stream chưa bắt đầu.

        Tác dụng: ở các lần chạy SAU, code skip ngay video này — không
        tốn công gọi yt-dlp, không tốn 3 attempts vô ích.

        Returns:
            True nếu ghi marker thành công, False nếu lỗi.
        """
        if not transcriptions_root or not video_id:
            return False
        try:
            marker_path = YouTubeResearcher._live_unavailable_marker_path(
                transcriptions_root, video_id)
            marker_path.parent.mkdir(parents=True, exist_ok=True)
            marker_path.write_text(
                f"# live_unavailable marker\n"
                f"# reason: {reason}\n"
                f"# written_at: {datetime.utcnow().isoformat()}Z\n",
                encoding="utf-8",
            )
            print(
                f"  [v13.1] 📝 Wrote live_unavailable marker: {marker_path.name}",
                flush=True,
            )
            return True
        except Exception as e:
            print(f"  [v13.1] ⚠️  Failed to write live marker: {e}", flush=True)
            return False

    @staticmethod
    def _has_live_unavailable_marker(transcriptions_root, video_id: str) -> bool:
        """v13.1: Check marker live_unavailable có sẵn không."""
        if not transcriptions_root or not video_id:
            return False
        return YouTubeResearcher._live_unavailable_marker_path(
            transcriptions_root, video_id).exists()

    # ================== AUDIO + JSON LOOKUP ==================
    @staticmethod
    def find_transcription_json(transcription_dir, video, audio_filename: str = "",
                                search_all_runs: bool = False) -> "Path | None":
        if not transcription_dir:
            return None
        td = Path(transcription_dir)
        candidates = []
        if audio_filename:
            stem = Path(audio_filename).stem
            candidates.append(td / f"{stem}_transcription.json")
        if getattr(video, "title", None):
            try:
                safe_title = YouTubeResearcher._safe_filename(
                    video.title, fallback=video.video_id)
                candidates.append(td / f"{safe_title}_transcription.json")
            except Exception:
                pass
        if getattr(video, "video_id", None):
            candidates.append(td / f"{video.video_id}_transcription.json")
        for c in candidates:
            if c.exists():
                return c
        if not search_all_runs:
            return None
        parent = td.parent if td.name else td
        if not parent.exists():
            return None
        for sub in sorted(parent.iterdir(), reverse=True):
            if not sub.is_dir() or sub == td:
                continue
            for c in candidates:
                alt = sub / c.name
                if alt.exists():
                    return alt
        return None

    @staticmethod
    def find_existing_audio(audio_root, video, target_filename: str = "",
                            min_size_bytes: int = 50 * 1024) -> "Path | None":
        """Tìm file audio đã có cho video. Skip file < min_size_bytes (corrupt).

        v13: scan CẢ root + subfolders (trước đây chỉ scan subfolders → bỏ sót
        audio copy thủ công vào root, ví dụ:
          youtube_dataset/luatsuquynh.87/audio/04_thoi_quen_ngay_tet_...wav
          youtube_dataset/luatsuquynh.87/audio/20260703_135934/...
        ).
        Thứ tự ưu tiên: root → subfolder mới nhất → subfolder cũ hơn.
        """
        if not audio_root:
            return None
        root = Path(audio_root)
        if not root.exists():
            return None
        candidates = []
        if target_filename:
            candidates.append(target_filename)
        if getattr(video, "video_id", None):
            candidates.append(f"{video.video_id}.wav")
            if target_filename:
                stem = Path(target_filename).stem
                candidates.append(f"{stem}_{video.video_id}.wav")
        # v13 FIX: check root TRƯỚC, sau đó mới check subfolders (mới nhất trước)
        search_dirs = [root] + sorted(
            [d for d in root.iterdir() if d.is_dir()], reverse=True
        )
        for d in search_dirs:
            for name in candidates:
                p = d / name
                if p.exists():
                    try:
                        if p.stat().st_size >= min_size_bytes:
                            return p
                    except OSError:
                        continue
        return None

    @staticmethod
    def _build_audio_index(audio_root, min_size_bytes: int = 50 * 1024) -> dict:
        """Build index {stem: Path} cho CẢ root + subfolder audio/, lấy file mới nhất.

        v6 fix:
          - CHỈ nhận file `.wav` (đã postprocess xong), KHÔNG nhận raw
            `.webm`/`.m4a`/`.mp4`/`.opus`/`.ogg`. Lý do: yt-dlp config
            (line ~4312-4316) dùng `FFmpegExtractAudio preferredcodec: wav`
            → audio HOÀN THIỆN cuối cùng LUÔN là `.wav`. File raw chỉ là
            intermediate. Nếu tồn tại mà KHÔNG có `.wav` tương ứng →
            postprocess đã bị KILL → cần re-download.
          - Verify WAV header + data integrity (không tin size >= 50KB).

        v13 fix: scan CẢ root + subfolders (trước đây chỉ scan subfolders).
        Thứ tự ưu tiên: root → subfolder mới nhất → subfolder cũ hơn.

        Returns:
            dict {stem: Path} của các file `.wav` hợp lệ.
        """
        index: dict = {}
        if not audio_root:
            return index
        root = Path(audio_root)
        if not root.exists():
            return index
        # v6: chỉ nhận .wav (postprocess output), reject raw intermediates
        audio_exts = {".wav"}
        # v13 FIX: scan root + subfolders. Root có priority cao nhất.
        search_dirs = [root] + sorted(
            [d for d in root.iterdir() if d.is_dir()], reverse=True
        )
        for d in search_dirs:
            try:
                for f in d.iterdir():
                    if not f.is_file() or f.suffix.lower() not in audio_exts:
                        continue
                    try:
                        if f.stat().st_size < min_size_bytes:
                            continue
                    except OSError:
                        continue
                    # v6: verify WAV header + data integrity
                    if not YouTubeResearcher._is_valid_wav(f):
                        try:
                            sz_kb = f.stat().st_size // 1024
                        except OSError:
                            sz_kb = 0
                        print(f"    [audio-index] Skipping invalid/corrupt WAV: "
                              f"{f.name} (size={sz_kb}KB, header or data corrupt)",
                              flush=True)
                        continue
                    key = f.stem
                    if key and key not in index:
                        index[key] = f
            except Exception:
                continue
        return index

    @staticmethod
    def _build_json_index(transcriptions_root, min_size_bytes: int = 100) -> dict:
        """Build index {stem: Path} cho CẢ root + subfolder transcriptions/, lấy file mới nhất.

        Skip file JSON: size < min_size_bytes, parse fail, hoặc thiếu video_id/segments.
        Mặc định 100 bytes — JSON transcription thật > 1KB.

        v13 fix: scan CẢ root + subfolders (trước đây chỉ scan subfolders).
        """
        index: dict = {}
        if not transcriptions_root:
            return index
        root = Path(transcriptions_root)
        if not root.exists():
            return index
        suffix = "_transcription.json"
        # v13 FIX: scan root + subfolders. Root có priority cao nhất.
        search_dirs = [root] + sorted(
            [d for d in root.iterdir() if d.is_dir()], reverse=True
        )
        for d in search_dirs:
            try:
                for f in d.iterdir():
                    if not f.is_file() or not f.name.endswith(suffix):
                        continue
                    try:
                        if f.stat().st_size < min_size_bytes:
                            continue
                    except OSError:
                        continue
                    stem = f.name[: -len(suffix)]
                    if not stem or stem in index:
                        continue
                    if not YouTubeResearcher._is_valid_transcription_json(f):
                        continue
                    index[stem] = f
            except Exception:
                continue
        return index

    @staticmethod
    def _is_valid_transcription_json(path: Path) -> bool:
        """Check file JSON có phải transcription hợp lệ không.

        Tiêu chí:
          - Parse OK (không phải text rác / JSON broken)
          - Là dict
          - Có 'video_id' (string không rỗng)
          - Có 'segments' là list
        """
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return False
        if not isinstance(data, dict):
            return False
        if not data.get("video_id"):
            return False
        if not isinstance(data.get("segments"), list):
            return False
        return True

    @staticmethod
    def _is_valid_wav(path: Path) -> bool:
        """v6: Verify WAV file có header hợp lệ + có audio data thực sự.

        Lý do: File `.wav` có thể CORRUPT (postprocess ffmpeg bị kill giữa
        chừng → chỉ có header 44 bytes + vài KB data). Nếu không verify →
        run sau match nhầm → pipeline fail lặp lặp.

        Validation:
          - wave.open() parse OK (không raise wave.Error / EOFError)
          - nchannels in [1, 8]
          - sampwidth in [1, 2, 3, 4] bytes
          - framerate in [1000, 100000] Hz
          - nframes >= 1000 (>= ~1 giây audio)
          - actual file size >= expected_size * 0.95 (allow 5% padding)

        Returns:
            True: file `.wav` hợp lệ.
            False: corrupt, header-only, hoặc không phải WAV.
        """
        try:
            import wave as _wave_mod
            with _wave_mod.open(str(path), "rb") as _wf:
                nchannels = _wf.getnchannels()
                sampwidth = _wf.getsampwidth()
                framerate = _wf.getframerate()
                nframes = _wf.getnframes()
                if nchannels < 1 or nchannels > 8:
                    return False
                if sampwidth not in (1, 2, 3, 4):
                    return False
                if framerate < 1000 or framerate > 100000:
                    return False
                if nframes < 1000:
                    return False
                expected_size = 44 + nframes * nchannels * sampwidth
                actual_size = path.stat().st_size
                if actual_size < int(expected_size * 0.95):
                    return False
                return True
        except Exception:
            return False

    # ============= Bucket-based pipeline (v5.1) =============
    # Refactor giống youtube_researcher_youtube_subs_multi_vpn_v2.py:
    # Phân chia video thành 3 bucket TRƯỚC khi xử lý → code sạch, dễ debug.
    #
    # Bucket A: có CẢ audio + JSON matching → SKIP nhanh (0 I/O)
    # Bucket B: có audio (run cũ), thiếu JSON → chỉ transcribe
    # Bucket C: chưa có audio → full pipeline (download + transcribe + save)

    def _partition_videos_for_pipeline(
        self, audio_root, transcriptions_root, skip_existing: bool,
    ) -> tuple:
        """Partition filtered videos thành 3 bucket (1 disk scan).

        Returns:
            (bucket_a, bucket_b, bucket_c):
              bucket_a: list[(video, audio_path, json_path)]  -- SKIP
              bucket_b: list[(video, audio_path, audio_filename)]  -- transcribe-only
              bucket_c: list[(video, target_name, target_filename)]  -- full pipeline
        """
        if not skip_existing:
            bucket_c = []
            for video in self._filtered_videos:
                target_name = self._safe_filename(video.title, fallback=video.video_id)
                target_filename = f"{target_name}.wav"
                bucket_c.append((video, target_name, target_filename))
            return [], [], bucket_c

        audio_index = YouTubeResearcher._build_audio_index(audio_root)
        json_index = YouTubeResearcher._build_json_index(transcriptions_root)

        bucket_a: list = []
        bucket_b: list = []
        bucket_c: list = []

        for video in self._filtered_videos:
            target_name = self._safe_filename(video.title, fallback=video.video_id)
            target_filename = f"{target_name}.wav"

            audio_path = (
                audio_index.get(target_name)
                or audio_index.get(video.video_id)
                or audio_index.get(f"{target_name}_{video.video_id}")
            )

            json_path = None
            if audio_path:
                expected_json_stem = f"{audio_path.stem}_transcription"
                for key in [audio_path.stem, video.video_id, target_name]:
                    cand = json_index.get(key)
                    if cand and cand.stem == expected_json_stem:
                        json_path = cand
                        break

            audio_filename = audio_path.name if audio_path else target_filename

            if audio_path and json_path:
                bucket_a.append((video, audio_path, json_path))
            elif audio_path and not json_path:
                bucket_b.append((video, audio_path, audio_filename))
            else:
                bucket_c.append((video, target_name, target_filename))

        return bucket_a, bucket_b, bucket_c

    def _cleanup_orphan_part_files(self, audio_dir: Path, min_size_mb: int = 100,
                                cleanup_all_subdirs: bool = False) -> int:
        """v6: Cleanup .part/.ytdl orphan files.

        Args:
            audio_dir: thư mục audio (một subfolder timestamp).
            min_size_mb: threshold (MB) để quyết định xóa hay giữ.
            cleanup_all_subdirs: nếu True, scan tất cả subfolders trong
                `audio_dir.parent` (khớp với `_build_audio_index`).

        Fix so với bản cũ:
          - Glob `*.part` không match `*.part-Frag*.part` → fix thành `*.part*`.
          - Không xóa file rỗng (0 bytes) → fix thêm check.
          - Chỉ scan 1 subdir hiện tại, không khớp với index scope → fix
            thêm `cleanup_all_subdirs=True`.
        """
        if not audio_dir.exists():
            return 0
        min_size_bytes = min_size_mb * 1024 * 1024
        deleted = 0
        targets = ([d for d in audio_dir.parent.iterdir() if d.is_dir()]
                   if cleanup_all_subdirs else [audio_dir])
        for target_dir in targets:
            # v6: glob match cả .part, .ytdl, .part-Frag*.part
            all_orphans = (list(target_dir.glob("*.part*")) +
                           list(target_dir.glob("*.ytdl")))
            wav_stems = {p.stem for p in target_dir.glob("*.wav")}
            for part_file in all_orphans:
                try:
                    size = part_file.stat().st_size
                except OSError:
                    continue
                # v6: file rỗng (0 bytes) — luôn xóa (không có ích)
                if size == 0:
                    try:
                        print(f"  [CLEANUP] Xóa orphan rỗng "
                              f"{target_dir.name}/{part_file.name} (0 bytes)",
                              flush=True)
                        part_file.unlink()
                        deleted += 1
                    except Exception:
                        pass
                    continue
                if size < min_size_bytes:
                    continue
                original_stem = part_file.name
                for suffix in (".part-Frag", ".part", ".ytdl"):
                    if suffix in original_stem:
                        original_stem = original_stem.split(suffix)[0]
                        break
                original_stem = original_stem.rsplit(".", 1)[0]
                if original_stem in wav_stems:
                    continue
                try:
                    size_mb = size / (1024 * 1024)
                    print(f"  [CLEANUP] Xóa orphan "
                          f"{target_dir.name}/{part_file.name} "
                          f"({size_mb:.1f}MB) - không có .wav tương ứng",
                          flush=True)
                    part_file.unlink()
                    deleted += 1
                except Exception:
                    pass
        return deleted

    # ============= FIX v2 Skip #2: No-transcript marker =============
    # Khi 1 video được xác nhận là KHÔNG có transcript YouTube (transcript_unavailable),
    # ghi marker file rỗng `{video_id}.no_transcript` vào transcriptions_dir.
    # Ở các run sau, video có marker sẽ được skip ngay → không tốn thời gian
    # gọi yt-dlp extract_info() / download sub URL nữa.

    @staticmethod
    def _has_no_transcript_marker(video_id: str, transcriptions_dir: Path) -> bool:
        """Check video có marker .no_transcript không (đã thử fail ở run trước)."""
        marker = transcriptions_dir / f"{video_id}.no_transcript"
        return marker.exists()

    @staticmethod
    def _mark_no_transcript(video_id: str, transcriptions_dir: Path) -> None:
        """Ghi marker .no_transcript để các run sau skip video này."""
        try:
            transcriptions_dir.mkdir(parents=True, exist_ok=True)
            marker = transcriptions_dir / f"{video_id}.no_transcript"
            marker.touch(exist_ok=True)
        except Exception:
            pass

    # ================== FETCH VIDEOS ==================
    def fetch_channel_videos(self, channel_input: str, max_results: int = 20000,
                              batch_size: int = 200, max_batches: int = 100,
                              socket_timeout: int = 30, fetch_delay: int = 5,
                              max_retries: int = 2,
                              published_after: Optional[datetime] = None,
                              order: str = "date",
                              ) -> list[VideoCandidate]:
        try:
            import yt_dlp
        except ImportError:
            print("pip install yt-dlp")
            sys.exit(1)
        t0 = time.time()
        proxy_url = self._proxy_for_fallback()
        channel_id = resolve_channel_id(self.api_key, channel_input,
                                        proxy_url=proxy_url)
        if not channel_id and not proxy_url:
            self._direct_blocked = True
            proxy_url = self._next_proxy()
            channel_id = resolve_channel_id(self.api_key, channel_input,
                                            proxy_url=proxy_url)
        if not channel_id:
            print(f"Khong tim thay kenh: {channel_input}")
            return []
        print(f"  [TIMING] resolve_channel_id: {time.time()-t0:.1f}s, ID={channel_id}")

        if channel_input.startswith("UC") and len(channel_input) == 24:
            channel_url = f"https://www.youtube.com/channel/{channel_id}/videos"
        else:
            url = channel_input.strip().rstrip("/")
            channel_url = url + "/videos" if not url.endswith("/videos") else url

        # === Phase 1: playlistItems.list (NO 750 limit) ===
        # v6: Thay yt-dlp flat extract (giới hạn ~750 items) bằng YouTube Data API
        # playlistItems.list paginate qua nextPageToken. Lấy được TẤT CẢ video
        # của channel (chỉ giới hạn bởi API quota, ~10000 units/key/day).
        print(f"\n  [Phase 1] Fetching playlistItems.list (no limit, target={max_results})...")
        phase1_start = time.time()

        if self.key_rotator is None:
            print(f"  [Phase 1] FAIL: YouTubeKeyRotator chưa được khởi tạo")
            return []

        # Resolve channel_id
        channel_id = resolve_channel_id_v6(self.key_rotator, channel_input)
        if not channel_id:
            print(f"  [Phase 1] FAIL: cannot resolve channel_id from {channel_input}")
            return []
        print(f"  [Phase 1] Resolved channel_id: {channel_id}")

        uploads_playlist_id = "UU" + channel_id[2:]
        video_ids = []
        page_token = None
        fetched = 0
        page_count = 0
        # v14-fix: retry giới hạn cho MỖI page (không phải toàn batch) để một
        # transient network stall không giết cả crawl và vứt hết video đã fetch.
        page_transient_retries = 0
        MAX_PAGE_TRANSIENT_RETRIES = 6

        while fetched < max_results:
            page_count += 1
            pl_params = {
                "playlistId": uploads_playlist_id,
                "part": "contentDetails,snippet",
                "maxResults": min(max_results - fetched, 50),
            }
            if page_token:
                pl_params["pageToken"] = page_token

            try:
                response = self.key_rotator.execute_with_retry(
                    lambda y, p=pl_params: y.playlistItems().list(**p),
                    label=f"playlistItems.list (page {page_count})",
                )
            except Exception as e:
                err = str(e).lower()
                # v14-fix: mở rộng keyword match. Lỗi thực tế
                # "The read operation timed out" chứa "timed out" (KHÔNG phải
                # "timeout") → code cũ không bắt được → raise → mất hết video.
                transient_kws = ["ssl", "timeout", "timed out", "connection",
                                 "handshake", "eof", "reset", "read operation",
                                 "broken pipe", "temporarily unavailable"]
                if any(kw in err for kw in transient_kws):
                    page_transient_retries += 1
                    if page_transient_retries > MAX_PAGE_TRANSIENT_RETRIES:
                        # Stall kéo dài → dừng gracefully, GIỮ video đã fetch
                        # thay vì raise và mất tất cả.
                        print(f"  [Phase 1] page {page_count}: transient error "
                              f"lặp lại {page_transient_retries}x → dừng phân "
                              f"trang, giữ {fetched} video đã lấy. err={e}",
                              flush=True)
                        break
                    # Backoff giới hạn (không phải 3*page_count → 150s ở page 50).
                    backoff = min(3 * page_transient_retries, 30)
                    print(f"  [Phase 1] page {page_count}: transient error "
                          f"(retry {page_transient_retries}/"
                          f"{MAX_PAGE_TRANSIENT_RETRIES}, sleep {backoff}s). "
                          f"err={e}", flush=True)
                    # Rotate VPN tunnel để đổi IP nếu stall do tunnel.
                    try:
                        if (self._transcript_rotator is not None
                                and hasattr(self._transcript_rotator,
                                            "force_rotate")):
                            self._transcript_rotator.force_rotate(
                                f"phase1-page{page_count}-retry"
                                f"{page_transient_retries}")
                    except Exception:
                        pass
                    time.sleep(backoff)
                    page_count -= 1  # retry cùng page (page_token giữ nguyên)
                    continue
                raise
            # Page thành công → reset bộ đếm retry cho page kế tiếp.
            page_transient_retries = 0

            items_this_page = 0
            for item in response.get("items", []):
                vid_id = item["contentDetails"]["videoId"]
                pub_date = (item["contentDetails"].get("videoPublishedAt") or
                            item["snippet"].get("publishedAt", ""))

                if published_after and pub_date:
                    try:
                        video_pub = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                        cutoff = published_after
                        if cutoff.tzinfo is None:
                            from datetime import timezone
                            cutoff = cutoff.replace(tzinfo=timezone.utc)
                        if video_pub < cutoff:
                            continue
                    except (ValueError, ImportError):
                        pass

                video_ids.append({
                    "id": vid_id,
                    "title": item["snippet"].get("title", ""),
                    "duration": 0,
                })
                fetched += 1
                items_this_page += 1
                if fetched >= max_results:
                    break

            page_token = response.get("nextPageToken")
            if items_this_page > 0 and page_count % 5 == 0:
                elapsed = time.time() - phase1_start
                print(f"  [Phase 1] page {page_count}: fetched {fetched} videos "
                      f"({elapsed:.0f}s)")
            if not page_token:
                break

        if not video_ids:
            print(f"  [Phase 1] FAIL: no videos found in channel")
            return []

        phase1_time = time.time() - phase1_start
        print(f"  [Phase 1] Done: {len(video_ids)} videos trong "
              f"{phase1_time:.1f}s ({page_count} API pages)")

        pre_filter = video_ids

        if not pre_filter:
            return []


        # === Phase 2: YouTube Data API v3 (videos.list batch=50) ===
        import requests as _req
        all_entries = []
        failed_count = 0
        api_batch_size = 50

        if not _YOUTUBE_API_KEYS:
            print("  [Phase 2] YOUTUBE_API_KEY not set → fallback yt-dlp concurrent")
            for e in pre_filter:
                vid = e.get("id")
                if not vid:
                    continue
                proxy_url = self._next_proxy()
                info = fetch_video_info_via_ytdlp(vid, proxy_url=proxy_url)
                if info:
                    all_entries.append(self._api_item_to_ytdlp_dict(info))
        else:
            current_key_idx = 0
            api_key = _YOUTUBE_API_KEYS[current_key_idx]
            print(f"\n  [Phase 2] YouTube Data API v3 (batch=50, {len(_YOUTUBE_API_KEYS)} key(s))...")
            phase2_start = time.time()
            for batch_start in range(0, len(pre_filter), api_batch_size):
                batch_end = min(batch_start + api_batch_size, len(pre_filter))
                batch_items = pre_filter[batch_start:batch_end]
                video_ids = [e.get("id") for e in batch_items if e.get("id")]
                if not video_ids:
                    continue
                url = "https://www.googleapis.com/youtube/v3/videos"
                resp = None
                for attempt in range(1, max_retries + 1):
                    params = {
                        "key": api_key,
                        "id": ",".join(video_ids),
                        "part": "snippet,statistics,contentDetails,status,topicDetails",
                    }
                    try:
                        resp = _req.get(url, params=params, timeout=15)
                        if resp.status_code == 200:
                            break
                        elif resp.status_code == 403:
                            current_key_idx += 1
                            if current_key_idx < len(_YOUTUBE_API_KEYS):
                                api_key = _YOUTUBE_API_KEYS[current_key_idx]
                                print(f"  [API] Key {current_key_idx} quota het, chuyển key {current_key_idx + 1}")
                                resp = None
                                continue
                            else:
                                print(f"  [API] Tất cả {len(_YOUTUBE_API_KEYS)} key đã hết quota!")
                                break
                        elif resp.status_code == 429:
                            time.sleep(5 * attempt)
                        else:
                            break
                    except Exception as e:
                        if attempt < max_retries:
                            time.sleep(2 ** attempt)
                if not resp or resp.status_code != 200:
                    failed_count += len(video_ids)
                    if current_key_idx >= len(_YOUTUBE_API_KEYS):
                        break
                    continue
                data = resp.json()
                for item in data.get("items", []):
                    info = self._api_item_to_ytdlp_dict(item)
                    upload_date = info.get("upload_date", "")
                    if published_after and upload_date and len(upload_date) == 8:
                        try:
                            vd = datetime.strptime(upload_date, "%Y%m%d")
                            if published_after.tzinfo:
                                vd = vd.replace(tzinfo=published_after.tzinfo)
                            if vd < published_after:
                                continue
                        except ValueError:
                            pass
                    all_entries.append(info)
                processed = min(batch_end, len(pre_filter))
                elapsed = time.time() - phase2_start
                rate = processed / max(elapsed, 0.1)
                eta = (len(pre_filter) - processed) / max(rate, 0.1)
                print(f"  [Phase 2] [{processed}/{len(pre_filter)}] "
                      f"ok={len(all_entries)} fail={failed_count} "
                      f"({elapsed:.0f}s, {rate:.1f} v/s, ETA ~{eta:.0f}s)")
                if len(all_entries) >= max_results:
                    break
            phase2_time = time.time() - phase2_start
            print(f"  [Phase 2] Done: {len(all_entries)} video trong {phase2_time:.1f}s")

        # Early filter
        if all_entries:
            pre_count = len(all_entries)
            all_entries = [
                e for e in all_entries
                if int(e.get("view_count") or 0) >= FILTER_MIN_VIEW_COUNT
                and FILTER_MIN_DURATION <= (e.get("duration") or 0) <= FILTER_MAX_DURATION
            ]
            print(f"  [Early filter] {pre_count} → {len(all_entries)}")

        if not all_entries:
            return []

        print(f"\nBuild {len(all_entries)} VideoCandidate...")
        detailed_videos = []
        for i, info in enumerate(all_entries, 1):
            try:
                video = self._build_video_from_ytdlp(info)
                detailed_videos.append(video)
            except Exception as e:
                print(f"  [{i}] Build failed: {e}")
                continue
        detailed_videos = detailed_videos[:max_results]
        self._videos = detailed_videos
        if not self._videos:
            return []
        print(f"Tim thay {len(self._videos)} video từ kênh "
              f"'{self._videos[0].channel if self._videos else channel_input}'")
        return self._videos

    def _api_item_to_ytdlp_dict(self, item: dict) -> dict:
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})
        status = item.get("status", {})
        topic = item.get("topicDetails", {})
        duration_iso = content.get("duration", "PT0S")
        duration_secs = 0
        m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", duration_iso)
        if m:
            h, mn, s = (int(x) if x else 0 for x in m.groups())
            duration_secs = h * 3600 + mn * 60 + s
        pub_at = snippet.get("publishedAt", "")
        upload_date = pub_at[:10].replace("-", "") if pub_at and len(pub_at) >= 10 else ""
        thumbs = snippet.get("thumbnails", {})
        thumbnail_url = ""
        for key in ("high", "medium", "default"):
            if key in thumbs:
                thumbnail_url = thumbs[key].get("url", "")
                break
        vid = item.get("id", "")
        return {
            "id": vid,
            "title": snippet.get("title", ""),
            "channel": snippet.get("channelTitle", ""),
            "channel_id": snippet.get("channelId", ""),
            "description": snippet.get("description", ""),
            "upload_date": upload_date,
            "duration": duration_secs,
            "view_count": int(stats.get("viewCount", 0)),
            "like_count": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
            "tags": snippet.get("tags", []),
            "categories": [snippet.get("categoryId", "")],
            "thumbnails": [{"url": thumbnail_url, "id": "high"}] if thumbnail_url else [],
            "webpage_url": f"https://www.youtube.com/watch?v={vid}",
            "channel_url": f"https://www.youtube.com/channel/{snippet.get('channelId', '')}",
            "default_language": snippet.get("defaultLanguage", ""),
            "default_audio_language": snippet.get("defaultAudioLanguage", ""),
            "caption_available": "caption" in content and content["caption"] == "true",
            "definition": "hd",
            "privacy_status": status.get("privacyStatus", "public"),
            "made_for_kids": status.get("madeForKids", False),
            # v3: giữ NGUYÊN full URL (không split) để khớp _vpn_v2.py
            "topic_categories": list(topic.get("topicCategories", []) or []),
            "embeddable": status.get("embeddable", True),
            "licensed_content": content.get("licensedContent", False),

            # === v3: 8 key mới ===
            # dimension/projection: contentDetails có thể có nhưng thường thiếu → default "2d"/"rectangular"
            "dimension": content.get("dimension", "2d"),
            "projection": content.get("projection", "rectangular"),
            # availability: từ status.privacyStatus (sync với privacy_status)
            "availability": status.get("privacyStatus", "public"),
            # playable_in_embed: alias cho embeddable
            "playable_in_embed": status.get("embeddable", True),
            # live_broadcast_content + derived fields
            "live_broadcast_content": snippet.get("liveBroadcastContent", "none"),
            "is_live": snippet.get("liveBroadcastContent") == "live",
            "live_status": snippet.get("liveBroadcastContent", "not_live"),
            "was_live": snippet.get("liveBroadcastContent") == "completed",
            # license: alias cho licensed_content
            "license": content.get("licensedContent", False),
            # height: từ definition (hd=1080, sd=480)
            "height": 1080 if (content.get("definition", "hd") == "hd") else 480,
        }

    def _build_video_from_ytdlp(self, info: dict) -> VideoCandidate:
        duration_secs = info.get("duration") or 0
        if isinstance(duration_secs, (int, float)):
            duration_iso = f"PT{int(duration_secs)}S"
        else:
            duration_iso = ""
        upload_date = info.get("upload_date", "")
        if upload_date and len(upload_date) == 8:
            published_at = f"{upload_date[:4]}-{upload_date[4:6]}-{upload_date[6:8]}T00:00:00Z"
        else:
            published_at = ""
        thumbs = info.get("thumbnails") or []
        thumbnail = ""
        for t in thumbs:
            if isinstance(t, dict) and t.get("id") in ("high", "medium", "default"):
                thumbnail = t.get("url", "")
                break
        if not thumbnail and thumbs:
            thumbnail = (thumbs[0].get("url", "") if isinstance(thumbs[0], dict) else "")
        tags = info.get("tags") or []
        categories = info.get("categories") or []
        category_id = categories[0] if categories else ""
        subtitles = info.get("subtitles") or {}
        auto_captions = info.get("automatic_captions") or {}
        caption_available = bool(subtitles) or bool(auto_captions)

        return VideoCandidate(
            video_id=info.get("id", ""),
            title=info.get("title", ""),
            channel=info.get("channel") or info.get("uploader") or "",
            description=info.get("description", ""),
            published_at=published_at,
            duration=duration_iso,
            duration_string=info.get("duration_string", ""),
            view_count=int(info.get("view_count") or 0),
            like_count=int(info.get("like_count") or 0),
            comment_count=int(info.get("comment_count") or 0),
            url=info.get("webpage_url") or f"https://www.youtube.com/watch?v={info.get('id', '')}",
            tags=tags,
            categories=categories,
            category_id=category_id,
            default_language=info.get("language", "") or info.get("default_language", ""),
            default_audio_language=info.get("audio_language", "") or info.get("default_audio_language", ""),
            caption_available=caption_available,
            definition="hd" if (info.get("height") or 0) >= 720 else "sd",
            channel_id=info.get("channel_id", ""),
            channel_url=info.get("channel_url", ""),
            channel_follower_count=int(info.get("channel_follower_count") or 0),
            uploader=info.get("uploader", ""),
            uploader_id=info.get("uploader_id", ""),
            uploader_url=info.get("uploader_url", ""),
            thumbnail=thumbnail,
            subtitles=subtitles,
            automatic_captions=auto_captions,
            # === v3: 12 field metadata bổ sung ===
            dimension=info.get("dimension", "2d"),
            licensed_content=bool(info.get("licensed_content") or info.get("license")),
            projection=info.get("projection", "rectangular"),
            privacy_status=info.get("privacy_status") or info.get("availability", ""),
            embeddable=bool(info.get("embeddable", info.get("playable_in_embed", True))),
            made_for_kids=bool(info.get("made_for_kids", False)),
            live_broadcast_content=info.get("live_broadcast_content", "none"),
            topic_categories=info.get("topic_categories", []) or [],
            live_status=info.get("live_status", "not_live"),
            was_live=bool(info.get("was_live", False)),
            availability=info.get("availability", info.get("privacy_status", "")),
        )

    # ================== FILTERS ==================
    def apply_filters(self, criteria: FilterCriteria) -> list[VideoCandidate]:
        self._filtered_videos = []
        for video in self._videos:
            video.failed_filters = []
            video.passed_filters = []
            duration_secs = parse_duration(video.duration)
            if criteria.min_duration and duration_secs < criteria.min_duration:
                video.failed_filters.append("duration_too_short")
            if criteria.max_duration and duration_secs > criteria.max_duration:
                video.failed_filters.append("duration_too_long")
            if criteria.published_after and video.published_at:
                try:
                    pub_date = datetime.fromisoformat(
                        video.published_at.replace("Z", "+00:00"))
                    if pub_date < criteria.published_after:
                        video.failed_filters.append("too_old")
                except ValueError:
                    pass
            if video.view_count < criteria.min_view_count:
                video.failed_filters.append("view_count_low")
            if video.failed_filters:
                continue
            video.passed_filters.append("passed_all_criteria")
            self._filtered_videos.append(video)
        print(f"Filter: {len(self._filtered_videos)}/{len(self._videos)} passed")
        return self._filtered_videos

    def print_video_table(self, videos=None):
        videos = videos or self._filtered_videos
        if not videos:
            print("No videos")
            return
        print("\n" + "=" * 130)
        print(f"{'#':<3} {'Title':<45} {'Duration':<10} "
              f"{'Views':<8} {'Likes':<7} {'Caption':<8} {'Published':<12}")
        print("=" * 130)
        for i, v in enumerate(videos):
            duration_secs = parse_duration(v.duration)
            title = v.title[:42] + "..." if len(v.title) > 45 else v.title
            pub_date = v.published_at[:10] if v.published_at else ""
            print(f"{i+1:<3} {title:<45} {format_duration(duration_secs):<10} "
                  f"{format_number(v.view_count):<8} {format_number(v.like_count):<7} "
                  f"{str(v.caption_available):<8} {pub_date:<12}")
        print("=" * 130)
        print("\nVideo URLs:")
        for i, v in enumerate(videos):
            print(f"  {i+1}. {v.video_url}")

    # ================== TRANSCRIPT ==================
    # NOTE: _get_youtube_transcript() (dùng youtube-transcript-api) đã bị XOÁ.
    # Bây giờ chỉ dùng _get_youtube_transcript_via_ytdlp() duy nhất.
    # Xem transcribe_with_youtube() để biết retry logic (2 attempts qua transcript_rotator).

    def _merge_youtube_segments_to_sentences(self, raw_parsed, max_duration=33.0,
                                             min_words=1):
        if not raw_parsed:
            return []
        SENT_END = {".", "?", "!", "…"}
        words = []
        for seg in raw_parsed:
            ws = seg["text"].split()
            if not ws:
                continue
            dur = seg["end"] - seg["start"]
            tpw = dur / len(ws) if ws else 0
            for j, w in enumerate(ws):
                ws_start = seg["start"] + j * tpw
                ws_end = seg["start"] + (j + 1) * tpw
                words.append((w, ws_start, ws_end))
        if not words:
            return []
        sents = []
        cur = []
        cur_start = words[0][1]
        for word, ws, we in words:
            cur.append(word)
            cur_end = we
            stripped = word.rstrip().rstrip('"').rstrip("'").rstrip(")")
            duration = cur_end - cur_start
            is_end = bool(stripped) and stripped[-1] in SENT_END
            is_over = (max_duration > 0) and (duration > max_duration)
            if is_end or is_over:
                text = " ".join(cur).strip()
                if text and (len(cur) >= min_words or text[-1] in SENT_END):
                    sents.append({
                        "start": round(cur_start, 3),
                        "end": round(cur_end, 3),
                        "speaker": "SPEAKER_00",
                        "text": text,
                    })
                cur = []
                cur_start = cur_end
        if cur:
            text = " ".join(cur).strip()
            if text and (len(cur) >= min_words or text[-1] in SENT_END):
                sents.append({
                    "start": round(cur_start, 3),
                    "end": round(cur_end if 'cur_end' in dir() else cur_start, 3),
                    "speaker": "SPEAKER_00",
                    "text": text,
                })
        return sents

    def _iso_lang_to_vietnamese(self, code: str) -> str:
        m = {
            "vi": "Tiếng Việt", "en": "Tiếng Anh",
            "zh": "Tiếng Trung", "zh-CN": "Tiếng Trung (Giản thể)",
            "zh-Hans": "Tiếng Trung (Giản thể)",
            "zh-Hant": "Tiếng Trung (Phồn thể)",
            "zh-TW": "Tiếng Trung (Phồn thể)",
            "ja": "Tiếng Nhật", "ko": "Tiếng Hàn",
            "fr": "Tiếng Pháp", "de": "Tiếng Đức",
            "es": "Tiếng Tây Ban Nha", "pt": "Tiếng Bồ Đào Nha",
            "ru": "Tiếng Nga", "th": "Tiếng Thái",
            "id": "Tiếng Indonesia", "ms": "Tiếng Mã Lai",
            "ar": "Tiếng Ả Rập", "hi": "Tiếng Hindi",
            "it": "Tiếng Ý", "nl": "Tiếng Hà Lan",
            "pl": "Tiếng Ba Lan", "tr": "Tiếng Thổ Nhĩ Kỳ",
            "uk": "Tiếng Ukraina",
        }
        if not code:
            return "Tiếng Việt"
        return m.get(code, f"Tiếng {code.upper()}")

    # ================== _safe_extract_info (yt-dlp wrapper) ==================
    def _safe_extract_info(self, url: str, ydl_opts: dict, max_attempts: int = 3,
                            context: str = "extract"):
        """
        Wrapper an toàn cho yt-dlp extract_info() có retry + escalate proxy.
        """
        import yt_dlp
        opts_base = dict(ydl_opts)
        last_err = None
        for attempt in range(1, max_attempts + 1):
            proxy_url = self._next_proxy() if self._rotator else None
            opts = dict(opts_base)
            if proxy_url:
                opts["proxy"] = proxy_url
            elif "proxy" in opts:
                del opts["proxy"]
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                if proxy_url and self._rotator:
                    try:
                        self._rotator.mark_success(proxy_url)
                    except Exception:
                        pass
                return info
            except Exception as e:
                last_err = e
                err_short = f"{type(e).__name__}: {str(e)[:120]}"
                is_blocked = self._is_youtube_blocked_error(e)
                if is_blocked and attempt < max_attempts:
                    if not self._rotator:
                        print(f"    [{context}] attempt {attempt}/{max_attempts} blocked "
                              f"(no rotator): {err_short}")
                    else:
                        self._on_youtube_blocked(e, proxy_url, context=context)
                        print(f"    [{context}] attempt {attempt}/{max_attempts} blocked, "
                              f"retry in {3 * attempt}s...")
                    time.sleep(3 * attempt)
                    continue
                if attempt == max_attempts:
                    print(f"    [{context}] attempt {attempt}/{max_attempts} fail: {err_short}")
                break
        return None

    # ================== v14: VIET-SUB SCORING ENGINE + URL PICKER ==================
    @staticmethod
    def _score_vi_subs(subtitles: dict, auto_captions: dict,
                       priority: str = "auto_first",
                       verbose: bool = False) -> list:
        """v14: Tính score cho TẤT CẢ key VI trong subtitles + automatic_captions.

        Args:
            subtitles: dict từ yt-dlp `subtitles` (manual subs).
            auto_captions: dict từ yt-dlp `automatic_captions` (auto-gen subs).
            priority: "auto_first" | "manual_first"
                - "auto_first" (default): auto-captions được cộng bonus sớm hơn.
                  Phù hợp cho video VN mà chỉ có auto-gen.
                - "manual_first": manual subs được cộng bonus sớm hơn.
                  Phù hợp cho video VTV, FAPTV có uploader tự upload sub.
            verbose: in log scoring.

        Returns:
            list[(key, source_dict, source_type, score, lang_code)]
            SORT giảm dần theo score. Source_dict là reference gốc
            (subtitles hoặc auto_captions), source_type là "manual"/"auto".

        Score formula (ưu tiên cao → thấp):
          - vi-orig (auto gốc)   : 20 (cao nhất vì là sub gốc do YouTube gen)
          - vi-VN (auto VN)      : 15
          - vi-VN-x-* (taglish)  : 12
          - vi (manual)          : 10
          - vi (auto)            :  8
          - vi-* khác            :  3

        Lý do đặt vi-orig cao hơn vi manual:
          - "vi" manual có thể là sub do uploader upload nhưng EMPTY/CHƯA CÓ
            URL working.
          - "vi-orig" là sub GỐC YouTube gen → có URL json3 ổn định nhất.
          - Trong v13, nếu "vi" manual được chọn nhưng URL fail → return None.
            v14 sẽ fallback xuống "vi-orig".
        """
        scored = []  # (key, dict, type, score, lang_code)

        def _score_key(kl: str, src_type: str) -> int:
            base = 0
            if src_type == "subtitles":
                base = 10  # manual
            else:
                base = 8   # auto
            if kl == "vi-orig":
                return max(base, 20)  # auto-gen gốc = ưu tiên cao nhất
            if kl == "vi-vn":
                return max(base, 15)
            if kl.startswith("vi-vn-x-"):
                return max(base, 12)
            if kl == "vi":
                return base
            # vi-* khác
            return 3

        def _add_from(d: dict, src_type: str):
            if not d:
                return
            for k in d.keys():
                if not k or not k.lower().startswith("vi"):
                    continue
                kl = k.lower()
                score = _score_key(kl, src_type)
                scored.append((k, d, src_type, score, k))

        # Thêm TẤT CẢ key VI (cả subtitles + auto_captions), score sẽ tự sắp xếp
        _add_from(subtitles, "subtitles")
        _add_from(auto_captions, "automatic_captions")

        # Sort giảm dần theo score
        scored.sort(key=lambda x: (-x[3], x[0]))

        # Áp dụng priority: nếu manual_first → manual được bump lên top
        if priority == "manual_first":
            manual_items = [x for x in scored if x[2] == "subtitles"]
            auto_items = [x for x in scored if x[2] != "subtitles"]
            scored = manual_items + auto_items

        if verbose and scored:
            print(f"    [v14-vi-score] {len(scored)} VI candidate(s) "
                  f"(priority={priority}):")
            for key, _dict, src_type, score, lang in scored:
                entries_n = len(_dict.get(key, []))
                print(f"      score={score:3d} {src_type:18s} key='{key}' "
                      f"({entries_n} entries)")
        return scored

    @staticmethod
    def _is_valid_subtitle_url(url: str) -> bool:
        """Check URL sub có hợp lệ không (không rỗng, là http/https)."""
        if not url or not isinstance(url, str):
            return False
        u = url.lower()
        return u.startswith("http://") or u.startswith("https://")

    @staticmethod
    def _pick_best_sub_url(entries: list) -> tuple:
        """Chọn URL tốt nhất từ list of dict [{url, ext}].

        Preference:
          1) json3 (parsing dễ nhất)
          2) vtt
          3) ttml
          4) srv3, srv2, srv1
          5) fallback: entry[0] nếu URL hợp lệ
        Returns (url, format) hoặc (None, None) nếu không có.
        """
        if not entries:
            return (None, None)
        for fmt in ["json3", "vtt", "ttml", "srv3", "srv2", "srv1"]:
            for entry in entries:
                if not entry:
                    continue
                if entry.get("ext") == fmt:
                    url = entry.get("url", "")
                    if YouTubeResearcher._is_valid_subtitle_url(url):
                        return (url, fmt)
            # v18: Fallback theo URL path extension (fmt= tham số trong query string)
            # Thay vì `fmt in url` (substring match sai), parse từ url path
            for entry in entries:
                if not entry:
                    continue
                url = entry.get("url", "") or ""
                if not YouTubeResearcher._is_valid_subtitle_url(url):
                    continue
                # fmt= tham số trong YouTube timedtext URL
                _fmt_match = re.search(r'[?&]fmt=(\w+)', url)
                if _fmt_match and _fmt_match.group(1) == fmt:
                    return (url, fmt)
        # Fallback: entry[0]
        for entry in entries:
            if not entry:
                continue
            url = entry.get("url", "")
            if YouTubeResearcher._is_valid_subtitle_url(url):
                ext = entry.get("ext", "vtt") or "vtt"
                return (url, ext)
        return (None, None)

    # ================== yt-dlp subtitles downloader ==================
    def _get_youtube_transcript_via_ytdlp(self, video_id: str,
                                            proxy_url: Optional[str] = None,
                                            info_cached: Optional[dict] = None,
                                            player_client: Optional[str] = None
                                            ) -> tuple[Optional[dict], str]:
        """
        v14 (port từ v15): Lấy phụ đề qua yt-dlp với VIETSUB SCORING ENGINE + PLAYER_CLIENT ROTATION.

        Khác v13:
          - v14 dùng `_score_vi_subs()` để SCORE TẤT CẢ key VI.
          - v14 dùng `_pick_best_sub_url()` (ưu tiên json3 > vtt > ttml > srv3/2/1).
          - v14 reload cookies mỗi lần (check mtime).

        Khác v14 cũ (port từ v15):
          - Trả tuple (result, status):
              * status="ok":            result có segments
              * status="no_subs":       video thật sự không có sub VI
              * status="client_empty":  client này trả subs=0+auto=0
                                        (caller retry client khác qua rotation)
              * status="extract_failed": yt-dlp fail (captcha/VPN/timeout)
          - player_client override: nếu set → dùng 1 client cụ thể,
            caller chịu trách nhiệm rotate qua list.
        """
        try:
            import yt_dlp
        except ImportError:
            return None, "extract_failed"

        # v14: reload cookies fresh nếu file đã thay đổi
        self._reload_cookies_if_changed()

        # === Bước 1: lấy info (sub URLs) ===
        info = None

        # v14 FIX: Chỉ dùng info_cached khi CÓ subs (positive cache).
        # Nếu empty (Phase 2 cache "no subs") → vẫn re-extract với proxy mới.
        if info_cached is not None:
            has_subs = bool(info_cached.get("subtitles")) or bool(info_cached.get("automatic_captions"))
            if has_subs:
                info = {
                    "subtitles": info_cached.get("subtitles") or {},
                    "automatic_captions": info_cached.get("automatic_captions") or {},
                }
                print(f"  [ytdlp-subs] using cached sub URLs (skip yt-dlp extract)")
            else:
                print(f"  [ytdlp-subs] v14: cache empty → vẫn re-extract "
                      f"(bỏ negative cache)")

        if info is None:
            ydl_opts = {
                "quiet": True, "no_warnings": True,
                "skip_download": True, "ignoreerrors": True,
                "js_runtimes": {"node": {}}, "age_limit": None,
            }
            self._apply_auth_skip(ydl_opts, player_client=player_client)
            self._apply_cookies(ydl_opts)
            self._apply_timeouts(ydl_opts, socket_timeout=30)
            # v15: dùng source_address policy routing thay vì proxy
            # v18.1: get_source_address() đã tự verify bind → nếu trả None là tunnel dead.
            # Re-check ngay trước yt-dlp để thu hẹp race window (tunnel có thể die
            # trong vài ms giữa verify và actual bind).
            _tr_src = None
            if self._transcript_rotator and hasattr(self._transcript_rotator, 'get_source_address'):
                _tr_src = self._transcript_rotator.get_source_address()
            if _tr_src:
                ydl_opts["source_address"] = _tr_src
            elif proxy_url:
                ydl_opts["proxy"] = proxy_url

            import concurrent.futures
            _extract_timeout = int(os.environ.get("YTDLP_EXTRACT_TIMEOUT", "30"))
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(
                        lambda: yt_dlp.YoutubeDL(ydl_opts).extract_info(
                            f"https://www.youtube.com/watch?v={video_id}", download=False
                        )
                    )
                    try:
                        info = future.result(timeout=_extract_timeout)
                    except concurrent.futures.TimeoutError:
                        print(f"  [ytdlp-subs] extract_info timeout {_extract_timeout}s, killing")
                        if proxy_url:
                            # Dùng transcript_rotator riêng để mark dead
                            self._mark_transcript_proxy_dead(proxy_url)
                        return None, "timeout"
            except Exception as e:
                err_str = str(e)
                # v18: Detect SSL/TLS + Errno-99 errors (KHÔNG phải IP block)
                _is_network_error = (
                    "SSL" in type(e).__name__
                    or "SSL" in err_str
                    or "ssl" in err_str
                    or "UNEXPECTED_EOF" in err_str
                    or "WRONG_VERSION_NUMBER" in err_str
                    or "Cannot assign requested address" in err_str
                    or "Errno 99" in err_str
                )
                if _is_network_error:
                    print(f"  [ytdlp-subs] Network error (tunnel issue, "
                          f"KHÔNG phải IP block): {type(e).__name__}: {err_str[:120]}")
                    return None, "network_error"
                print(f"  [ytdlp-subs] extract_info error: {type(e).__name__}: {err_str[:200]}")
                # v4: Neu gap captcha -> force rotate NGAY
                if self._transcript_rotator and hasattr(self._transcript_rotator, 'is_captcha_error'):
                    try:
                        if self._transcript_rotator.is_captcha_error(e):
                            print(f"  [ytdlp-subs] CAPTCHA DETECTED -> force rotate")
                            self._transcript_rotator.increment_captcha_hit()
                            self._transcript_rotator.force_rotate("captcha-extract")
                    except Exception:
                        pass
                if proxy_url and is_proxy_dead_error(e):
                    self._mark_transcript_proxy_dead(proxy_url)
                elif proxy_url:
                    self._mark_transcript_proxy_failed(proxy_url)
                return None, "extract_failed"

        if not info:
            print(f"  [ytdlp-subs] no info returned for {video_id}")
            return None, "extract_failed"

        subtitles = info.get("subtitles") or {}
        auto_captions = info.get("automatic_captions") or {}

        # v15 (port): Nếu client trả subs=0 AND auto=0 → status='client_empty'
        # → caller retry với player_client khác trong rotation list.
        if not subtitles and not auto_captions:
            client_log = player_client or "default"
            print(f"  [v15-empty] client={client_log} trả EMPTY "
                  f"(subs=0, auto=0) - có thể client này không trả subs. "
                  f"→ status='client_empty' (retry client khác)")
            return None, "client_empty"

        # ============ v14: VIET-SUB SCORING ENGINE ============
        priority_mode = getattr(self, "_v14_vi_priority", "auto_first")
        scored = self._score_vi_subs(
            subtitles, auto_captions,
            priority=priority_mode, verbose=True,
        )

        if not scored:
            all_langs = list(subtitles.keys()) + list(auto_captions.keys())
            print(f"  [ytdlp-subs] ❌ no Vi sub found (any variant). "
                  f"All available langs ({len(all_langs)}): {all_langs[:15]}"
                  f"{'...' if len(all_langs) > 15 else ''}")
            # v15: extract OK nhưng không có VI → status='no_subs' (thật sự)
            return None, "no_subs"

        # Log TẤT CẢ languages để debug
        all_langs = sorted(list(subtitles.keys()) + list(auto_captions.keys()))
        print(f"  [ytdlp-subs] 🌐 all available langs ({len(all_langs)}): "
              f"{all_langs[:15]}{'...' if len(all_langs) > 15 else ''}")
        print(f"  [ytdlp-subs] 🇻🇳 VI candidates: {len(scored)} "
              f"(priority_mode={priority_mode})")

        # ============ v14: TRY EACH SCORED KEY (multi-key fallback) ============
        # Nếu key A fail (download/parse) → thử key B, ...
        # Dừng khi: success | hết key.
        best_result = None
        tried_keys = []
        for key_idx, (key, _dict, source_type, score, lang_code) in enumerate(scored):
            entries = _dict.get(key) or []
            if not entries:
                print(f"    [ytdlp-subs] skip key='{key}' (entries empty)")
                continue
            tried_keys.append(key)
            sub_url, sub_format = self._pick_best_sub_url(entries)
            if not sub_url:
                print(f"    [ytdlp-subs] skip key='{key}' (no valid URL "
                      f"in {len(entries)} entries)")
                continue

            print(f"  [ytdlp-subs] ({key_idx + 1}/{len(scored)}) "
                  f"trying key='{key}' [{source_type}] score={score} "
                  f"format={sub_format} → downloading...")

            # === Bước 2: download sub file với INLINE ROTATE (max N VPN khác) ===
            # v14 (port từ v15): Auto-rotate proxy khi 429 trong cùng key attempt.
            #   - 429 → force_rotate() → retry URL với proxy mới
            #   - max N lần rotate (đồng nhất với số client attempts)
            #   - Nếu hết rotate → skip key này → thử key tiếp theo.
            content = None
            inline_rotates = 0
            # v18: Tách riêng max rotate cho inline download (chỉ 429 retry, không phải client rotate)
            max_inline_rotates = int(os.environ.get("TRANSCRIPT_INLINE_ROTATES", "2"))
            current_proxy = proxy_url

            try:
                import requests as _requests
                import concurrent.futures as _cf

                cookies_dict = {}
                if COOKIES_FILE_STR:
                    try:
                        from http.cookiejar import MozillaCookieJar
                        cj = MozillaCookieJar(COOKIES_FILE_STR)
                        cj.load(ignore_discard=True, ignore_expires=True)
                        for c in cj:
                            if c.domain.endswith("youtube.com"):
                                cookies_dict[c.name] = c.value
                    except Exception as ce:
                        print(f"    [ytdlp-subs] warn: load cookies failed: {ce}")

                while inline_rotates <= max_inline_rotates:
                    # v15: dùng source_address binding thay vì proxy
                    _tr_src_addr = None
                    if self._transcript_rotator and hasattr(self._transcript_rotator, 'get_source_address'):
                        _tr_src_addr = self._transcript_rotator.get_source_address()
                    try:
                        if _tr_src_addr:
                            _bound_sess = _make_bound_session(_tr_src_addr)
                            if _bound_sess:
                                resp = _bound_sess.get(sub_url, cookies=cookies_dict,
                                                       timeout=(10, 20))
                            else:
                                resp = _requests.get(sub_url, cookies=cookies_dict,
                                                     timeout=(10, 20))
                        else:
                            resp = _requests.get(sub_url, cookies=cookies_dict,
                                                 timeout=(10, 20))
                        if resp.status_code == 429:
                            # 429: rotate proxy ngay (nếu có rotator)
                            inline_rotates += 1
                            if inline_rotates >= max_inline_rotates:
                                # Đã hết lượt rotate (đủ 2 lần thử) → skip key
                                print(f"    [ytdlp-subs] HTTP 429 "
                                      f"(key='{key}') — hết {max_inline_rotates} "
                                      f"lần thử (đã retry 1 IP fake khác) → skip key")
                                break  # break while → continue key tiếp
                            # Còn lượt rotate → force_rotate sang tunnel khác.
                            # VPNRotator dùng OpenVPN tunnel hệ thống: force_rotate()
                            # đổi tunnel và traffic tự đi qua tun0 → proxy=None.
                            # (KHÔNG dùng acquire() ở đây — acquire() trả về
                            #  _WorkerGuard context manager, KHÔNG phải proxy URL.)
                            rotated = False
                            if self._transcript_rotator and hasattr(
                                    self._transcript_rotator, 'force_rotate'):
                                try:
                                    self._transcript_rotator.force_rotate(
                                        f"429-on-key-{key}")
                                    rotated = True
                                except Exception:
                                    rotated = False
                            if rotated:
                                print(f"    [ytdlp-subs] HTTP 429 "
                                      f"(key='{key}') → rotate #{inline_rotates}/"
                                      f"{max_inline_rotates} → tunnel mới "
                                      f"(IP fake), retry qua policy route")
                                continue  # retry — source_address sẽ refresh từ rotator
                            else:
                                # Không rotate được → skip
                                print(f"    [ytdlp-subs] HTTP 429 (key='{key}') "
                                      f"nhưng không rotate được tunnel → skip")
                                break
                        if resp.status_code == 403:
                            print(f"    [ytdlp-subs] HTTP 403 (forbidden) "
                                  f"for key='{key}'")
                            break  # skip key này
                        resp.raise_for_status()
                        content = resp.text
                        break  # SUCCESS → break while
                    except Exception as ue:
                        # Lỗi khác (timeout, network) → raise để except ngoài xử lý
                        raise
            except Exception as e:
                err_str = str(e)
                print(f"    [ytdlp-subs] download sub FAILED for key='{key}': "
                      f"{type(e).__name__}: {err_str[:120]} — try next key")
                # v4: captcha detection trong download sub
                if self._transcript_rotator and hasattr(self._transcript_rotator, 'is_captcha_error'):
                    try:
                        if self._transcript_rotator.is_captcha_error(e):
                            print(f"  [ytdlp-subs] CAPTCHA DETECTED (download) -> force rotate")
                            self._transcript_rotator.increment_captcha_hit()
                            self._transcript_rotator.force_rotate("captcha-download")
                    except Exception:
                        pass
                if current_proxy and is_proxy_dead_error(e):
                    self._mark_transcript_proxy_dead(current_proxy)
                elif current_proxy:
                    self._mark_transcript_proxy_failed(current_proxy)
                continue

            # === Bước 3: parse ===
            segs = []
            is_auto = (source_type == "automatic_captions")

            if sub_format == "json3":
                try:
                    data = json.loads(content)
                    segs = self._parse_json3_subtitle(data)
                except Exception as e:
                    print(f"    [ytdlp-subs] json3 parse error for key='{key}': "
                          f"{e} — try next key")
                    continue
            elif sub_format in ("vtt", "ttml", "srv1", "srv2", "srv3"):
                segs = self._parse_vtt_subtitle(content)

            if not segs:
                print(f"    [ytdlp-subs] parse returned 0 segments for "
                      f"key='{key}' (format={sub_format}, len={len(content)} bytes) "
                      f"— try next key")
                continue

            # SUCCESS cho key này
            source_label = "manual" if source_type == "subtitles" else "auto"
            best_result = {
                "segments": segs,
                "language": lang_code,
                "is_auto": is_auto,
                "source": f"yt-dlp-{sub_format}-{source_label}",
                "_v14_chosen_key": key,
                "_v14_chosen_score": score,
            }
            print(f"  [ytdlp-subs] ✅ found {len(segs)} segments for "
                  f"key='{key}' [{source_type}] (tried {key_idx + 1} keys so far)")
            break

        if not best_result:
            # v18-FIX: Phân biệt "thật sự không có sub VI" vs "có sub nhưng download fail".
            # - scored empty → extract_info trả về 0 key VI → status="no_subs" (đúng)
            # - scored NOT empty + all download fail → status="extract_failed"
            #   (có sub nhưng network/SSL/tunnel die → caller retry client khác
            #   hoặc fallback API — KHÔNG skip video này)
            if scored:
                print(f"  [ytdlp-subs] ❌ tried {len(tried_keys)} VI key(s) "
                      f"({tried_keys}) — all download/parse FAILED "
                      f"(có {len(scored)} subs nhưng network error) "
                      f"→ status='extract_failed' (không phải no_subs)")
                return None, "extract_failed"
            print(f"  [ytdlp-subs] ❌ tried {len(tried_keys)} VI key(s) "
                  f"({tried_keys}) — all failed (download/parse error "
                  f"sau {max_inline_rotates} lần IP rotate)")
            return None, "no_subs"

        return best_result, "ok"

    # ================== youtube-transcript-api FALLBACK (v14) ==================
    # v14: Fallback SAU yt-dlp khi yt-dlp fail. Dùng `youtube-transcript-api`
    # library gọi timedtext API của YouTube (endpoint KHÁC yt-dlp) → thường
    # bypass captcha/bot-check tốt hơn yt-dlp khi IP/proxy đã bị Google flag.
    #
    # Pattern:
    #   1) Build session với proxy + cookies (inject vào http_client)
    #   2) Gọi api.list(video_id) → TranscriptList
    #   3) Ưu tiên MANUAL theo prefer_languages (mặc định ['vi','en'])
    #   4) Fallback AUTO-GENERATED theo prefer_languages
    #   5) Fallback: enumerate, lấy VI trước, rồi EN
    #   6) Fallback cuối: get_transcript(video_id, languages=['vi','en'])
    #
    # Returns: dict giống `_get_youtube_transcript_via_ytdlp` để tương thích.
    def _get_youtube_transcript_via_api(self, video_id: str,
                                          proxy_url: Optional[str] = None,
                                          prefer_languages: Optional[list] = None
                                          ) -> dict | None:
        """v14: Fallback engine dùng youtube-transcript-api.

        Args:
            video_id: YouTube video ID.
            proxy_url: proxy URL (optional).
            prefer_languages: list languages ưu tiên, mặc định ['vi', 'en'].

        Returns:
            dict {
              'segments': [{'text', 'start', 'duration'}, ...],
              'language': 'vi' | 'vi-orig' | ...,
              'is_auto': bool,
              'source': 'youtube-transcript-api-manual' | '-auto' | '-any' | '-direct',
            }
            hoặc None nếu thất bại.
        """
        if prefer_languages is None:
            prefer_languages = ["vi", "en"]
        # Luôn cho 'vi' đứng đầu (nếu user truyền list khác)
        if "vi" not in [p.lower() for p in prefer_languages]:
            prefer_languages = ["vi"] + list(prefer_languages)

        # Thử import động (youtube-transcript-api có thể chưa cài)
        try:
            from youtube_transcript_api import YouTubeTranscriptApi
        except ImportError:
            print(f"  [api-fallback] youtube-transcript-api KHÔNG cài → skip")
            return None
        except Exception as e:
            print(f"  [api-fallback] import error: {type(e).__name__}: {e}")
            return None

        # Build session với source_address binding + cookies (v15 policy routing)
        http_client = None
        _tr_src_addr = None
        if self._transcript_rotator and hasattr(self._transcript_rotator, 'get_source_address'):
            _tr_src_addr = self._transcript_rotator.get_source_address()

        if _tr_src_addr or proxy_url or COOKIES_FILE_STR:
            try:
                import requests
                if _tr_src_addr:
                    session = _make_bound_session(_tr_src_addr)
                    if session is None:
                        session = requests.Session()
                else:
                    session = requests.Session()
                if proxy_url and not _tr_src_addr:
                    session.proxies.update({"http": proxy_url, "https": proxy_url})
                if COOKIES_FILE_STR:
                    try:
                        from http.cookiejar import MozillaCookieJar
                        cj = MozillaCookieJar(COOKIES_FILE_STR)
                        cj.load(ignore_discard=True, ignore_expires=True)
                        for cookie in cj:
                            session.cookies.set_cookie(cookie)
                        n = sum(1 for c in session.cookies
                                if c.domain.endswith('youtube.com'))
                        if n:
                            print(f"  [api-fallback] loaded {n} youtube.com cookies")
                    except Exception as ce:
                        print(f"  [api-fallback] warn: load cookies failed: {ce}")
                http_client = session
            except Exception as e:
                print(f"  [api-fallback] warn: build proxy session failed: {e}")
                http_client = None

        # Instantiate API (new style: nhận http_client; old style: không nhận)
        api_instance = None
        try:
            if http_client is not None:
                try:
                    api_instance = YouTubeTranscriptApi(http_client=http_client)
                except TypeError:
                    # Old API không nhận http_client
                    api_instance = YouTubeTranscriptApi()
            else:
                api_instance = YouTubeTranscriptApi()
        except TypeError:
            api_instance = None
        except Exception as e:
            print(f"  [api-fallback] YouTubeTranscriptApi() init error: "
                  f"{type(e).__name__}: {e}")
            api_instance = None

        # Helper gọi method theo cả 2 style
        def _call(method_name, *args, **kwargs):
            if api_instance is not None and hasattr(api_instance, method_name):
                return getattr(api_instance, method_name)(*args, **kwargs)
            if hasattr(YouTubeTranscriptApi, method_name):
                return getattr(YouTubeTranscriptApi, method_name)(*args, **kwargs)
            raise AttributeError(f"No '{method_name}' on YouTubeTranscriptApi")

        # === Bước 1: list transcripts ===
        transcript_list = None
        try:
            transcript_list = _call("list", video_id)
        except AttributeError:
            transcript_list = None
        except Exception as e:
            err_str = str(e).lower()
            # Bắt các lỗi phổ biến
            if any(kw in err_str for kw in ["no transcript", "transcriptsdisabled",
                                            "notranscriptsfound"]):
                print(f"  [api-fallback] no transcripts for {video_id}")
                return None
            if any(kw in err_str for kw in ["captcha", "bot", "sign in",
                                            "forbidden", "429"]):
                print(f"  [api-fallback] list_transcripts BLOCKED: "
                      f"{type(e).__name__}: {str(e)[:120]}")
                # Đánh dấu proxy fail (rate limit / captcha)
                if proxy_url:
                    try:
                        self._mark_transcript_proxy_failed(proxy_url)
                    except Exception:
                        pass
                return None
            print(f"  [api-fallback] list_transcripts error: "
                  f"{type(e).__name__}: {str(e)[:150]}")
            transcript_list = None

        # === Bước 2: thử manual trước ===
        if transcript_list is not None:
            for lang in prefer_languages:
                try:
                    t = transcript_list.find_manually_created_transcript([lang])
                    fetched = t.fetch()
                    return {
                        "segments": fetched,
                        "language": t.language_code,
                        "is_auto": False,
                        "source": "youtube-transcript-api-manual",
                    }
                except Exception:
                    continue

            # === Bước 3: thử auto-generated ===
            for lang in prefer_languages:
                try:
                    t = transcript_list.find_generated_transcript([lang])
                    fetched = t.fetch()
                    return {
                        "segments": fetched,
                        "language": t.language_code,
                        "is_auto": True,
                        "source": "youtube-transcript-api-auto",
                    }
                except Exception:
                    continue

            # === Bước 4: enumerate, lấy VI trước rồi EN ===
            try:
                for t in transcript_list:
                    code = getattr(t, "language_code", "") or ""
                    if code.lower().startswith("vi"):
                        fetched = t.fetch()
                        return {
                            "segments": fetched,
                            "language": t.language_code,
                            "is_auto": getattr(t, "is_generated", True),
                            "source": "youtube-transcript-api-any-vi",
                        }
            except Exception:
                pass
            try:
                for t in transcript_list:
                    code = getattr(t, "language_code", "") or ""
                    if code.lower().startswith("en"):
                        fetched = t.fetch()
                        return {
                            "segments": fetched,
                            "language": t.language_code,
                            "is_auto": getattr(t, "is_generated", True),
                            "source": "youtube-transcript-api-any-en",
                        }
            except Exception:
                pass

        # === Bước 5: API cũ - get_transcript trực tiếp ===
        try:
            fetched = _call("get_transcript", video_id, languages=prefer_languages)
            return {
                "segments": fetched,
                "language": prefer_languages[0],
                "is_auto": True,
                "source": "youtube-transcript-api-direct",
            }
        except AttributeError:
            pass
        except Exception as e:
            err_str = str(e).lower()
            if any(kw in err_str for kw in ["captcha", "bot", "sign in"]):
                print(f"  [api-fallback] get_transcript BLOCKED: "
                      f"{type(e).__name__}: {str(e)[:120]}")
                if proxy_url:
                    try:
                        self._mark_transcript_proxy_failed(proxy_url)
                    except Exception:
                        pass
            else:
                print(f"  [api-fallback] get_transcript failed: "
                      f"{type(e).__name__}: {str(e)[:120]}")

        return None

    @staticmethod
    def _parse_vtt_subtitle(content: str) -> list:
        """Parse WebVTT / SRV* subtitle → list [{start, duration, text}]."""
        ts_pattern = re.compile(
            r"(?P<sh>\d{2}):(?P<sm>\d{2}):(?P<ss>\d{2})[.,](?P<sms>\d{3})"
            r"\s+-->\s+"
            r"(?P<eh>\d{2}):(?P<em>\d{2}):(?P<es>\d{2})[.,](?P<ems>\d{3})"
        )
        segs = []
        blocks = re.split(r"\n\s*\n", content)
        for block in blocks:
            lines = block.strip().split("\n")
            if not lines:
                continue
            ts_line = None
            text_start = 0
            for i, line in enumerate(lines):
                if "-->" in line:
                    ts_line = line
                    text_start = i + 1
                    break
            if not ts_line:
                continue
            m = ts_pattern.search(ts_line)
            if not m:
                continue
            sh = int(m["sh"]); sm = int(m["sm"]); ss = int(m["ss"]); sms = int(m["sms"])
            eh = int(m["eh"]); em = int(m["em"]); es = int(m["es"]); ems = int(m["ems"])
            start = sh * 3600 + sm * 60 + ss + sms / 1000
            end = eh * 3600 + em * 60 + es + ems / 1000
            text_lines = lines[text_start:]
            text_lines = [l for l in text_lines if l.strip() and not l.strip().isdigit()]
            text = " ".join(text_lines)
            text = re.sub(r"<[^>]+>", "", text)
            text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
            text = text.strip()
            if not text:
                continue
            segs.append({
                "start": round(start, 3),
                "duration": round(end - start, 3),
                "text": text,
            })
        return segs

    @staticmethod
    def _parse_json3_subtitle(data: dict) -> list:
        """Parse JSON3 subtitle format từ YouTube.

        Format thuc te YouTube tra ve (json3):
          {
            "wireMagic": "pb3",
            "events": [
              {
                "tStartMs": 0, "dDurationMs": 1500,
                "segs": [{"utf8": "Hello"}, {"utf8": " world"}]
              },
              ...
            ]
          }
        """
        segs = []
        for ev in data.get("events", []):
            start_ms = ev.get("tStartMs", ev.get("t"))
            dur_ms = ev.get("dDurationMs", ev.get("d"))
            if start_ms is None:
                continue
            start = float(start_ms) / 1000.0
            dur = float(dur_ms) / 1000.0 if dur_ms is not None else 0.0
            parts = []
            for s in ev.get("segs", []) or []:
                txt = s.get("utf8", s.get("text", ""))
                if txt:
                    parts.append(txt)
            text = "".join(parts).strip()
            if not text:
                continue
            segs.append({
                "start": round(start, 3),
                "duration": round(dur, 3),
                "text": text,
            })
        return segs

    def transcribe_with_youtube(self, video_id: str, audio_path: Path = None,
                                 lang: list = None, max_sentence_duration: float = 33.0,
                                 min_sentence_words: int = 1,
                                 info_cached: Optional[dict] = None,
                                 attempt: int = 1) -> tuple[Optional[dict], str]:
        """
        v18: Lấy phụ đề YouTube với multi-attempt retry + API fallback.

        Returns:
            tuple (result, status):
              - (dict, "ok"): có transcript
              - (None, "no_subs"): video thật sự không có sub VI → nên mark .no_transcript
              - (None, "failed"): network/tunnel/timeout → KHÔNG mark, có thể retry sau
        """
        if lang is None:
            lang = ["vi"]

        print(f"  Fetching YouTube transcript via yt-dlp (langs={lang})...")

        # === v18: Every-N-audio IP rotate (chu kỳ) ===
        # Cứ mỗi N audio đã xử lý → tự động force_rotate IP fake của transcript_rotator.
        # v18: Default 20 (tăng từ 10 vì không rotate mỗi attempt nữa).
        # v18-FIX: Không rotate khi parallel mode (BUCKET_B_WORKERS > 1)
        # vì sẽ phá route của các workers khác đang dùng cùng IP.
        rotate_every_n = int(os.environ.get("TRANSCRIPT_ROTATE_EVERY", "20"))
        _v18_parallel = int(os.environ.get("BUCKET_B_WORKERS", "1")) > 1
        # Thread-safe counter increment
        if not hasattr(self, '_v18_counter_lock'):
            self._v18_counter_lock = threading.Lock()
        with self._v18_counter_lock:
            self._v14_transcript_video_counter += 1
            counter = self._v14_transcript_video_counter
            should_rotate = (rotate_every_n > 0
                             and counter > 0
                             and (counter % rotate_every_n == 0)
                             and not _v18_parallel  # v18-FIX: không rotate khi parallel
                             and self._transcript_rotator
                             and hasattr(self._transcript_rotator, 'force_rotate'))
        if should_rotate:
            print(f"  [v17-every-{rotate_every_n}] audio #{counter} reached → "
                  f"force_rotate IP fake (chu kỳ)")
            try:
                self._transcript_rotator.force_rotate(
                    f"every-{rotate_every_n}-audios-counter-{counter}")
                # Sau rotate, sleep ngắn để tất cả workers pick up IP mới
                time.sleep(2)
            except Exception as e:
                print(f"  [v17-every-{rotate_every_n}] force_rotate error: {e}")

        # v14 (port từ v15): Player_client rotation list
        # Nếu có self._v14_player_clients → dùng list rotate
        # Nếu không → fallback dùng ["web_safari", "web"] (default cũ)
        rotate_enabled = getattr(self, "_v14_player_client_rotate", True)
        player_clients = getattr(
            self, "_v14_player_clients",
            ["tv", "web_embedded", "web_creator"],  # default v17
        )

        # v14: Retry tối đa N attempts yt-dlp với proxy rotation.
        # N và backoff lấy từ env (TRANSCRIPT_MAX_ATTEMPTS, TRANSCRIPT_BACKOFF_SECONDS)
        # → cho phép shell script override để giảm retry time.
        max_attempts = int(os.environ.get("TRANSCRIPT_MAX_ATTEMPTS", "5"))
        result = None
        last_status = "extract_failed"  # conservative default
        all_statuses = []  # để debug
        backoff_seconds = float(os.environ.get("TRANSCRIPT_BACKOFF_SECONDS", "2"))
        # Note: nhánh direct→vpn cũ (TRANSCRIPT_VPN_RETRY) đã bỏ vì attempt 1
        # nay LUÔN đi qua tunnel VPN (next() connect tunnel). Retry khi fail
        # do vòng lặp max_attempts + force_rotate ở attempt N>1 đảm nhiệm.

        # v17: Logic mới — giữ IP fake hiện tại, thử TẤT CẢ clients trước khi rotate.
        # Chỉ rotate khi: (1) đến chu kỳ rotate_every_n, hoặc (2) có dấu hiệu bị block.
        # Với mỗi IP: thử lần lượt các client (tối đa max_attempts lần).
        # Nếu 1 client trả OK → dùng luôn, không thử client khác.

        # v17: Đảm bảo transcript_rotator đã connect LẦN ĐẦU (1 lần duy nhất)
        # next() chỉ connect nếu chưa connected, nếu đã connected thì no-op (tăng counter)
        if self._transcript_rotator and len(self._transcript_rotator) > 0:
            if not self._transcript_rotator._is_connected():
                try:
                    self._transcript_rotator.next()
                except Exception:
                    pass

        blocked_statuses = {"extract_failed"}  # CHỈ force_rotate khi thật sự block/captcha
        # v18: timeout, network_error, client_empty → KHÔNG force_rotate (tunnel issue, not IP block)
        retry_statuses = {"timeout", "network_error", "client_empty"}
        _same_ip_retry_count = 0
        _same_ip_max_before_rotate = 3

        # v17: Khi chạy parallel (BUCKET_B_WORKERS > 1), KHÔNG force_rotate giữa chừng
        # vì sẽ phá route của các workers khác đang dùng cùng IP.
        # Chỉ retry bằng cách đổi client. Rotate IP chỉ xảy ra theo chu kỳ (mỗi N video).
        _parallel_mode = int(os.environ.get("BUCKET_B_WORKERS", "1")) > 1

        for a in range(max_attempts):
            # v17: Chọn player_client cho attempt này (round-robin qua list)
            if rotate_enabled and player_clients:
                pc = player_clients[a % len(player_clients)]
            else:
                pc = None

            # v18: Logic rotate thông minh:
            # - extract_failed → force_rotate (IP thật sự bị block/captcha)
            # - timeout/network_error/client_empty → retry cùng IP, đổi client
            # - Sau N lần same-IP retry liên tiếp → force_rotate (tunnel có vấn đề thật)
            # - KHÔNG rotate khi parallel mode
            if a > 0 and last_status in blocked_statuses and not _parallel_mode:
                if self._transcript_rotator:
                    try:
                        print(f"    [v18-rotate] IP bị BLOCK (status={last_status}) "
                              f"→ force_rotate trước attempt {a+1}")
                        self._transcript_rotator.force_rotate(
                            f"blocked-attempt-{a+1}")
                    except Exception:
                        pass
                # Backoff dài hơn sau rotate để tunnel ổn định (tối thiểu 5s)
                backoff = max(backoff_seconds, 5.0)
                print(f"    [v18-retry] sleeping {backoff:.0f}s after force_rotate "
                      f"before attempt {a+1}")
                time.sleep(backoff)
                _same_ip_retry_count = 0
            elif a > 0 and last_status in retry_statuses:
                # v18: timeout/network/client_empty → KHÔNG đổi IP, retry client khác
                _same_ip_retry_count += 1
                retry_sleep = 3.0 if last_status == "timeout" else 1.5
                print(f"    [v18-retry] status={last_status} (same-IP #{_same_ip_retry_count}) "
                      f"→ KHÔNG đổi IP, sleep {retry_sleep}s, thử client khác")
                time.sleep(retry_sleep)
                # Sau N lần same-IP fail → rotate tunnel
                if _same_ip_retry_count >= _same_ip_max_before_rotate and not _parallel_mode:
                    if self._transcript_rotator:
                        try:
                            print(f"    [v18-retry] {_same_ip_retry_count} same-IP fails "
                                  f"→ force_rotate (tunnel kém)")
                            self._transcript_rotator.force_rotate(
                                f"same-ip-fail-{_same_ip_retry_count}")
                            _same_ip_retry_count = 0
                            # v18.1: Đợi tunnel mới ổn định trước khi verify
                            time.sleep(5.0)
                        except Exception:
                            pass
            elif a > 0:
                # Không rotate, chỉ đổi client — sleep ngắn
                time.sleep(0.5)

            proxy_str = "VPN-FAKE"
            pc_str = pc or "default"

            # v18.1: Verify tunnel + source_address TRƯỚC khi gọi yt-dlp.
            # get_source_address() đã tự verify bind (v18.1 fix) → nếu trả None
            # tức là tunnel thực sự đã chết (IP bị kernel release).
            # → force_rotate để tạo tunnel mới (không dùng next() vì tunnel đã chết).
            _tr_src = None
            if self._transcript_rotator and hasattr(self._transcript_rotator, 'get_source_address'):
                _tr_src = self._transcript_rotator.get_source_address()
            if _tr_src is None and self._transcript_rotator and len(self._transcript_rotator) > 0:
                if not _parallel_mode:
                    print(f"    [v18-verify] tunnel DEAD → force_rotate before attempt {a+1}")
                    try:
                        self._transcript_rotator.force_rotate(f"tunnel-dead-attempt-{a+1}")
                        # Đợi tunnel mới ổn định (OpenVPN cần ~3-5s để connect + route)
                        _stabilize = 5.0
                        print(f"    [v18-verify] waiting {_stabilize:.0f}s for new tunnel to stabilize...")
                        time.sleep(_stabilize)
                        _tr_src = self._transcript_rotator.get_source_address()
                    except Exception as _rot_err:
                        print(f"    [v18-verify] force_rotate failed: {_rot_err}")
                if _tr_src is None:
                    print(f"    [v18-verify] ⚠️  source_address still None → skip yt-dlp, fallback API")
                    all_statuses.append("tunnel_dead")
                    last_status = "tunnel_dead"
                    break
            proxy_label = f"VPN({_tr_src})" if _tr_src else "VPN-FAKE"
            print(f"  [transcript-ytdlp] attempt {a+1}/{max_attempts} via "
                  f"client={pc_str} proxy={proxy_label} "
                  f"(transcript_rotator={'ON' if self._transcript_rotator else 'OFF'})")
            cached = info_cached if a == 0 else None
            result, status = self._get_youtube_transcript_via_ytdlp(
                video_id, proxy_url=None, info_cached=cached,
                player_client=pc,
            )
            all_statuses.append(status)
            last_status = status
            if status == "ok":
                break
            if status in ("network_error", "timeout"):
                print(f"  [v18-transcript] status='{status}' "
                      f"(tunnel issue, KHÔNG phải IP block) → retry client khác cùng IP")
                continue
            if status == "client_empty":
                print(f"  [v18-transcript] status='client_empty' "
                      f"(client={pc_str} trả EMPTY) → thử client khác (KHÔNG đổi IP)")
                continue
            if status == "no_subs":
                print(f"  [v18-transcript] status='no_subs' (video thật sự không có sub). "
                      f"Skip các attempts còn lại.")
                break

        # === v18: FALLBACK youtube-transcript-api ===
        # Fallback khi có thể có sub nhưng network/tunnel fail.
        # KHÔNG fallback khi no_subs (video thật sự không có sub).
        _should_fallback = (
            last_status not in ("ok", "no_subs")
        )
        if not result and _should_fallback \
                and getattr(self, "_v14_api_fallback_enabled", True):
            print(f"  [v14-fallback] yt-dlp fail ({last_status}) "
                  f"→ thử youtube-transcript-api engine...")
            if self._transcript_rotator and not _parallel_mode:
                try:
                    self._transcript_rotator.force_rotate(
                        "yt-dlp-failed-try-api")
                except Exception:
                    pass
            api_proxy = self._proxy_for_transcript_fallback()
            api_result = self._get_youtube_transcript_via_api(
                video_id, proxy_url=api_proxy,
                prefer_languages=getattr(self, "_v14_api_fallback_langs",
                                         ["vi", "en"]),
            )
            if api_result:
                result = api_result
                print(f"  [v14-fallback] ✅ youtube-transcript-api SUCCESS: "
                      f"lang={result.get('language')!r} "
                      f"is_auto={result.get('is_auto')} "
                      f"source={result.get('source')}")
            else:
                print(f"  [v14-fallback] ❌ youtube-transcript-api cũng fail")

        if not result:
            status_summary = ", ".join(all_statuses)
            _final_status = "no_subs" if last_status == "no_subs" else "failed"
            print(f"  [v14-transcript] ❌ No transcript after all attempts "
                  f"(statuses=[{status_summary}], decision={_final_status})")
            return None, _final_status

        raw_segments = result["segments"]
        raw_parsed = []
        for seg in raw_segments:
            if isinstance(seg, dict):
                start = float(seg.get("start", 0.0))
                dur = float(seg.get("duration", 0.0))
                text = (seg.get("text") or "").replace("\n", " ").strip()
            else:
                start = float(getattr(seg, "start", 0.0))
                dur = float(getattr(seg, "duration", 0.0))
                text = (getattr(seg, "text", "") or "").replace("\n", " ").strip()
            if not text:
                continue
            raw_parsed.append({
                "start": round(start, 3),
                "end": round(start + dur, 3),
                "text": text,
            })
        if not raw_parsed:
            return None, "failed"
        segments = self._merge_youtube_segments_to_sentences(
            raw_parsed, max_duration=max_sentence_duration,
            min_words=min_sentence_words)
        if not segments:
            return None, "failed"
        audio_duration = 0.0
        if audio_path and Path(audio_path).exists():
            try:
                audio_path_str = str(audio_path)
                # Cách 1: WAV thuần → đọc header instant (vài KB)
                if audio_path_str.lower().endswith(".wav"):
                    try:
                        import wave as _wave_mod
                        with _wave_mod.open(audio_path_str, "rb") as _wf:
                            _frames = _wf.getnframes()
                            _rate = _wf.getframerate()
                            if _rate > 0:
                                audio_duration = round(_frames / _rate, 3)
                    except Exception:
                        pass
                # Cách 2: format khác (m4a, webm, mp4, ...) → ffprobe (subprocess, ~50-200ms)
                if audio_duration <= 0:
                    try:
                        import subprocess
                        _ff = subprocess.run(
                            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                             "-of", "default=noprint_wrappers=1:nokey=1", audio_path_str],
                            capture_output=True, text=True, timeout=10,
                        )
                        if _ff.returncode == 0 and _ff.stdout.strip():
                            audio_duration = round(float(_ff.stdout.strip()), 3)
                    except Exception:
                        pass
                # Cách 3: fallback cuối → sf.SoundFile (chỉ metadata, không load sample)
                if audio_duration <= 0:
                    try:
                        import soundfile as _sf
                        with _sf.SoundFile(audio_path_str) as _sf_f:
                            _sr = _sf_f.samplerate
                            _frames = len(_sf_f)
                            if _sr > 0:
                                audio_duration = round(_frames / _sr, 3)
                    except Exception:
                        pass
            except Exception:
                pass
        if audio_duration <= 0:
            audio_duration = float(segments[-1]["end"])
        lang_name = self._iso_lang_to_vietnamese(result["language"])
        return {
            "segments": segments,
            "audio_duration": audio_duration,
            "detected_languages": [lang_name],
            "transcript_language": lang_name,
            "transcript_is_auto": result["is_auto"],
            "transcript_source": result["source"],
        }, "ok"

    def has_youtube_subs(self, video_id: str, info_cached: Optional[dict] = None,
                          video_obj: Optional[VideoCandidate] = None) -> bool:
        """Check video có YouTube subs không. Ưu tiên O(1) từ info_cached / video_obj,
        fallback gọi yt-dlp extract_info() (KHÔNG dùng youtube-transcript-api nữa)."""
        if video_obj and video_obj.caption_available:
            return True
        if info_cached:
            if info_cached.get("subtitles") or info_cached.get("automatic_captions"):
                return True
            return False   # đã có info_cached rỗng → biết chắc không có sub
        # Không có info_cached → gọi yt-dlp 1 lần để check
        info = fetch_video_info_via_ytdlp(video_id)
        if not info:
            return False
        return bool(info.get("subtitles")) or bool(info.get("automatic_captions"))

    # ================== SAVE (MỞ RỘNG) ==================
    def _save_transcription(self, output_path: Path, segments: list, video,
                            audio_duration: float, audio_filename: str = "",
                            audio_downloaded_at: Optional[str] = None,
                            extra_metadata: dict = None):
        """Lưu transcription JSON chứa ĐẦY ĐỦ URL + metadata + segments.

        File này là NGUỒN DỮ LIỆU DUY NHẤT để extract lại metadata mà không
        cần gọi API.
        """
        speakers = sorted(set(str(s.get("speaker", "SPEAKER_00")) for s in segments))
        em = extra_metadata or {}

        result = {
            # === Video metadata (extract lại được từ đây) ===
            "video_id": video.video_id,
            "url": video.video_url,
            "title": video.title,
            "channel": video.channel,
            "channel_id": getattr(video, "channel_id", ""),
            "channel_url": getattr(video, "channel_url", ""),
            "published_at": video.published_at,
            "duration": video.duration,
            "duration_seconds": parse_duration(video.duration),
            "duration_string": getattr(video, "duration_string", ""),
            "view_count": int(getattr(video, "view_count", 0)),
            "like_count": int(getattr(video, "like_count", 0)),
            "comment_count": int(getattr(video, "comment_count", 0)),
            "description": getattr(video, "description", ""),
            "tags": list(getattr(video, "tags", []) or []),
            "category_id": getattr(video, "category_id", ""),
            "categories": list(getattr(video, "categories", []) or []),
            "default_language": getattr(video, "default_language", ""),
            "default_audio_language": getattr(video, "default_audio_language", ""),
            "thumbnail": getattr(video, "thumbnail", ""),
            "caption_available": bool(getattr(video, "caption_available", False)),
            "definition": getattr(video, "definition", ""),
            "channel_follower_count": int(getattr(video, "channel_follower_count", 0)),
            "uploader": getattr(video, "uploader", ""),
            "uploader_id": getattr(video, "uploader_id", ""),
            "uploader_url": getattr(video, "uploader_url", ""),

            # === v3: YouTube API metadata bổ sung (12 field) ===
            "dimension": getattr(video, "dimension", "2d"),
            "licensed_content": bool(getattr(video, "licensed_content", False)),
            "projection": getattr(video, "projection", "rectangular"),
            "privacy_status": getattr(video, "privacy_status", ""),
            "embeddable": bool(getattr(video, "embeddable", True)),
            "made_for_kids": bool(getattr(video, "made_for_kids", False)),
            "live_broadcast_content": getattr(video, "live_broadcast_content", "none"),
            "topic_categories": list(getattr(video, "topic_categories", []) or []),
            "recording_location": getattr(video, "recording_location", ""),
            "live_status": getattr(video, "live_status", "not_live"),
            "was_live": bool(getattr(video, "was_live", False)),
            "availability": getattr(video, "availability", getattr(video, "privacy_status", "")),

            # === Audio ===
            "audio_filename": audio_filename or "",
            "audio_path": audio_filename or "",
            "audio_duration": float(audio_duration or 0.0),
            "audio_downloaded_at": audio_downloaded_at,

            # === Transcript ===
            "transcript_language": em.get("transcript_language", ""),
            "transcript_is_auto": em.get("transcript_is_auto", False),
            "transcript_source": em.get("transcript_source", ""),
            "detected_languages": em.get("detected_languages", []),

            # === Soniox-compatible (cho CSV/JSON cũ) ===
            "num_speakers": len(speakers),
            "speakers": speakers,
            "source_files": [],
            "segments": segments,
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

    def save_research(self, filename="research_result.json"):
        output_file = self.output_dir / filename
        videos_data = []
        for v in self._filtered_videos:
            data = asdict(v)
            data["video_url"] = v.video_url
            videos_data.append(data)
        data_out = {
            "research_date": datetime.now().isoformat(),
            "channel": self._videos[0].channel if self._videos else "",
            "total_videos_found": len(self._videos),
            "videos_after_filter": len(self._filtered_videos),
            "videos": videos_data,
        }
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data_out, f, ensure_ascii=False, indent=2)
        print(f"Saved to {output_file}")


# ================= CSV EXPORT =================
def export_segments_minimal_csv(output_csv, videos, transcription_dir):
    """CSV 11 cột: video_id, video_title, channel, video_url,
    segment_start, segment_end, segment_duration, text, language,
    audio_path, audio_duration_seconds.
    """
    import csv
    headers = [
        "video_id", "video_title", "channel", "video_url",
        "segment_start", "segment_end", "segment_duration",
        "text", "language", "audio_path", "audio_duration_seconds",
    ]
    rows = []
    for video in videos:
        json_path = YouTubeResearcher.find_transcription_json(
            transcription_dir, video,
            audio_filename=getattr(video, "audio_filename", ""),
            search_all_runs=True,
        )
        if not json_path:
            continue
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        segments = data.get("segments", [])
        audio_path = data.get("audio_path", "") or data.get("audio_filename", "")
        audio_dur = data.get("audio_duration", 0.0)
        language = data.get("transcript_language", "")
        for seg in segments:
            rows.append([
                video.video_id, video.title, video.channel, video.video_url,
                seg.get("start"), seg.get("end"),
                round((seg.get("end") or 0) - (seg.get("start") or 0), 3),
                seg.get("text"), language, audio_path, audio_dur,
            ])
    if not rows:
        print("No rows to export")
        return
    with open(output_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    print(f"CSV exported: {output_csv} ({len(rows)} rows)")


def export_video_summary_csv(output_csv, videos, transcription_dir):
    """v3: Export video-level summary CSV (1 row/video) với 40+ cột YouTube metadata.

    Cột (40+):
      - Core (10):    video_id, title, channel, url, published_at,
                      duration_formatted, duration_seconds,
                      view_count, like_count, comment_count
      - Engagement (2): engagement_ratio, audio_path
      - Audio (4):    audio_duration_seconds, num_segments,
                      num_speakers, speakers_list
      - YouTube v2 (12): tags, category_id, default_language, default_audio_language,
                         caption_available, definition, channel_id, channel_url,
                         channel_follower_count, uploader, uploader_id, uploader_url
      - YouTube v3 (12): dimension, licensed_content, projection, privacy_status,
                         embeddable, made_for_kids, live_broadcast_content,
                         live_status, was_live, availability, recording_location,
                         topic_categories
      - Filter (3):   passed_filters, failed_filters, description_short

    Dùng csv.writer (không pandas) để giữ nhẹ.
    """
    import csv
    headers = [
        # Core
        "video_id", "title", "channel", "url", "published_at",
        "duration_formatted", "duration_seconds",
        "view_count", "like_count", "comment_count",
        # Engagement
        "engagement_ratio", "audio_path",
        # Audio (từ JSON nếu có)
        "audio_duration_seconds", "num_segments", "num_speakers", "speakers_list",
        # YouTube v2
        "tags", "category_id", "default_language", "default_audio_language",
        "caption_available", "definition", "channel_id", "channel_url",
        "channel_follower_count", "uploader", "uploader_id", "uploader_url",
        # YouTube v3 (12 field mới)
        "dimension", "licensed_content", "projection", "privacy_status",
        "embeddable", "made_for_kids", "live_broadcast_content",
        "live_status", "was_live", "availability", "recording_location",
        "topic_categories",
        # Filter
        "passed_filters", "failed_filters", "description_short",
    ]
    rows = []
    for video in videos:
        # Lookup JSON để lấy audio stats
        json_path = YouTubeResearcher.find_transcription_json(
            transcription_dir, video,
            audio_filename=getattr(video, "audio_filename", ""),
            search_all_runs=True,
        )
        audio_dur = 0.0
        num_segments = 0
        num_speakers = 0
        speakers_list = ""
        if json_path:
            try:
                with open(json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                audio_dur = data.get("audio_duration", 0.0)
                segments = data.get("segments", [])
                num_segments = len(segments)
                speakers_set = sorted(set(
                    str(s.get("speaker", "SPEAKER_00")) for s in segments))
                num_speakers = len(speakers_set)
                speakers_list = ", ".join(speakers_set)
            except Exception:
                pass

        duration_secs = parse_duration(video.duration)
        engagement_ratio = 0.0
        view_count = int(getattr(video, "view_count", 0) or 0)
        like_count = int(getattr(video, "like_count", 0) or 0)
        comment_count = int(getattr(video, "comment_count", 0) or 0)
        if view_count > 0:
            engagement_ratio = round(
                (like_count + comment_count) / view_count * 100, 2)

        rows.append([
            # Core
            video.video_id, video.title, video.channel, video.url,
            video.published_at,
            format_duration(duration_secs), duration_secs,
            view_count, like_count, comment_count,
            # Engagement
            engagement_ratio,
            getattr(video, "audio_filename", ""),
            # Audio
            audio_dur, num_segments, num_speakers, speakers_list,
            # YouTube v2
            json.dumps(getattr(video, "tags", []) or [], ensure_ascii=False),
            getattr(video, "category_id", ""),
            getattr(video, "default_language", ""),
            getattr(video, "default_audio_language", ""),
            bool(getattr(video, "caption_available", False)),
            getattr(video, "definition", ""),
            getattr(video, "channel_id", ""),
            getattr(video, "channel_url", ""),
            int(getattr(video, "channel_follower_count", 0) or 0),
            getattr(video, "uploader", ""),
            getattr(video, "uploader_id", ""),
            getattr(video, "uploader_url", ""),
            # YouTube v3
            getattr(video, "dimension", "2d"),
            bool(getattr(video, "licensed_content", False)),
            getattr(video, "projection", "rectangular"),
            getattr(video, "privacy_status", ""),
            bool(getattr(video, "embeddable", True)),
            bool(getattr(video, "made_for_kids", False)),
            getattr(video, "live_broadcast_content", "none"),
            getattr(video, "live_status", "not_live"),
            bool(getattr(video, "was_live", False)),
            getattr(video, "availability", getattr(video, "privacy_status", "")),
            getattr(video, "recording_location", ""),
            json.dumps(getattr(video, "topic_categories", []) or [],
                       ensure_ascii=False),
            # Filter
            " | ".join(getattr(video, "passed_filters", []) or []),
            " | ".join(getattr(video, "failed_filters", []) or []),
            (getattr(video, "description", "") or "")[:200],
        ])
    if not rows:
        print("No rows to export")
        return
    with open(output_csv, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)
    print(f"Video summary CSV exported: {output_csv} ({len(rows)} rows, "
          f"{len(headers)} columns)")


# ================= CHANNEL LOADERS =================
def load_channels_from_file(path: str) -> list[str]:
    p = Path(path)
    if not p.exists():
        print(f"Không tìm thấy file channels: {path}")
        return []
    channels = []
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        channels.append(s)
    print(f"Đọc được {len(channels)} kênh từ {path}")
    for i, c in enumerate(channels, 1):
        print(f"  {i}. {c}")
    return channels


def safe_channel_name(channel_url: str, fallback: str = "unknown") -> str:
    if not channel_url:
        return fallback
    s = channel_url.strip().rstrip("/")
    m = re.search(r"@([^/\s?]+)", s)
    if m:
        return m.group(1)
    m = re.search(r"youtube\.com/channel/([^/\s?]+)", s)
    if m:
        return m.group(1)
    m = re.search(r"youtube\.com/(?:c|user)/([^/\s?]+)", s)
    if m:
        return m.group(1)
    if s.startswith("UC") and len(s) == 24:
        return s
    return s.split("/")[-1] or fallback


# ================= ARGS =================
def parse_args():
    import argparse
    p = argparse.ArgumentParser(
        description="YouTube Researcher - Audio + YouTube Subs (Multi-Rotator: 3 VPN tunnels độc lập)"
    )
    p.add_argument("--channel", "-c", help="URL kênh YouTube đơn lẻ")
    p.add_argument("--channels-file", "-f",
                   default="./channels_audio/channels.txt",
                   help="File txt chứa danh sách URL kênh")
    p.add_argument("--output", "-o", default="./youtube_dataset",
                   help="Folder output gốc (giống youtube_researcher_youtube_subs_multi_vpn_v2.py)")
    p.add_argument("--max-results", "-m", type=int, default=20000)
    p.add_argument("--max-fetch", type=int, default=20000)
    p.add_argument("--max-batches", type=int, default=8000)
    p.add_argument("--fetch-delay", type=int, default=2)
    p.add_argument("--order", default="date")
    p.add_argument("--audio-format", default="m4a")
    p.add_argument("--force-retranscribe", action="store_true")
    p.add_argument("--force-redownload", action="store_true",
                   help="Ép tải lại audio kể cả khi đã có file")
    p.add_argument("--audio-only", action="store_true",
                   help="Chỉ tải audio, KHÔNG tạo file JSON transcript. "
                        "Bucket A/B đều skip, Bucket C chỉ tải audio (không transcribe).")
    p.add_argument("--skip-existing", action="store_true",
                   help="Skip channel đã có output đầy đủ")
    p.add_argument("--rebuild-from-transcripts", action="store_true",
                   help="CHỈ đọc JSON có sẵn, tạo CSV/summary. "
                        "Không gọi API, không tải audio, không lấy transcript.")
    p.add_argument("--metadata-only", action="store_true",
                   help="Chỉ fetch metadata (không audio, không transcript)")
    p.add_argument("--use-vpn", action="store_true", default=True,
                   help="(BẮT BUỘC trong bản này)")
    p.add_argument("--vpn-rotate-every", type=int, default=10,
                   help="Áp dụng cho metadata_rotator + transcript_rotator. "
                        "Số request trước khi tự rotate IP qua VPN. "
                        "0 = chỉ rotate khi gặp 429/403 (mặc định: 10).")
    p.add_argument("--vpn-real-ip-cycle", type=int, default=6,
                   help="Áp dụng CHO audio_rotator (CHỈ rotator này cycle). "
                        "Cycle 'N fake VPN → 1 IP thật' (mặc định: 11). "
                        "Sau N request fake VPN thì request kế tiếp sẽ disconnect VPN "
                        "và dùng IP thật (default route), rồi lại reconnect VPN. "
                        "Vd: 11 → cứ 10 fake VPN thì 1 IP thật. "
                        "0 = TẮT cycle.")
    p.add_argument("--vpn-strategy", choices=["random", "sequential", "least_used"],
                   default="random")
    p.add_argument("--video-delay", type=int, default=5,
                   help="Delay giữa các video (giây)")
    p.add_argument("--socket-timeout", type=int, default=30)
    p.add_argument("--max-retries", type=int, default=2)
    p.add_argument("--max-sentence-duration", type=int, default=31.0)
    p.add_argument("--min-sentence-words", type=int, default=1) 
    p.add_argument("--instance-id", default=None)
    p.add_argument("--cache-dir", default=None)
    p.add_argument("--vpn-isolated", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--proxy-mode", choices=["auto", "always", "split", "never"],
                   default="split")
    # === v5: AudioIPController (chỉ áp dụng cho audio download) ===
    p.add_argument("--audio-min-speed-mbps", type=float, default=1,
                   help="v5: Tốc độ tối thiểu (MB/s) cho audio download. "
                        "Nếu < ngưỡng này → đổi IP (real → fake, hoặc fake khác). "
                        "Nếu ≥ ngưỡng → giữ nguyên IP. Mặc định: 1.0")
    p.add_argument("--audio-fake-before-real", type=int, default=5,
                   help="v5: Số lần IP fake chậm liên tiếp trước khi quay về IP thật "
                        "(cycle = fake_before_real + 1). Mặc định: 5 → cycle 6.")
    p.add_argument("--audio-min-bytes-for-speed", type=int, default=256 * 1024,
                   help="v5: Tối thiểu bytes đã tải trước khi đánh giá tốc độ "
                        "(tránh false positive với file nhỏ). Mặc định: 262144 (256KB).")
    p.add_argument("--audio-min-window-seconds", type=float, default=30,
                   help="v5: Tối thiểu thời gian (giây) trước khi đánh giá tốc độ. "
                        "Mặc định: 10.0")
    p.add_argument("--audio-speed-avg-window-seconds", type=float, default=30,
                   help="v5: Cửa sổ (giây) để tính TỐC ĐỘ TRUNG BÌNH (rolling average). "
                        "Mỗi chunk mới sẽ được lưu vào buffer, tốc độ TB = "
                        "(bytes mới nhất - bytes cũ nhất trong window) / window_size. "
                        "Làm mượt dao động tốc độ tức thời, phản ánh throughput "
                        "thực tế hơn. Mặc định: 10.0")
    # === v7: HTTP 500 + stall detection ===
    p.add_argument("--audio-500-threshold", type=int, default=5,
                   help="v7: Số fragment HTTP 500 tối đa trước khi cycle IP. "
                        "Mặc định: 5. Nếu gặp 5 fragment liên tiếp trả 500 → "
                        "gọi AudioIPController.on_download_complete(ok=False) "
                        "để cycle IP (REAL → FAKE, hoặc FAKE khác, hoặc về REAL).")
    p.add_argument("--audio-stall-seconds", type=float, default=30.0,
                   help="v7: Nếu bytes không tăng trong N giây (progress hook "
                        "báo downloaded_bytes không đổi) → flag stuck → cũng "
                        "trigger cycle IP. Mặc định: 30.0")
    # === v10: Force REAL sau N lần FAIL liên tiếp ở FAKE ===
    p.add_argument("--audio-force-real-after-fails", type=int, default=2,
                   help="v10: Số lần FAIL liên tiếp ở FAKE (ok=False) trước khi "
                        "cycle về REAL NGAY thay vì chỉ force_rotate VPN. "
                        "Trước đây cần đợi fake_before_real=5 lần → quá chậm. "
                        "Mặc định: 2. Set 0 = tắt (giữ logic cũ).")
    # === v11: Per-instance tunnel cleanup on exit ===
    p.add_argument("--cleanup-on-exit", action="store_true",
                   help="v11: Khi instance thoát (bình thường hoặc SIGTERM), tự "
                        "động kill TẤT CẢ openvpn tunnel của CHÍNH instance này "
                        "(theo instance_id), KHÔNG ảnh hưởng instance khác. "
                        "Mặc định: TẮT. Bật khi chạy multi-instance để tránh "
                        "leak tunnel khi instance bị kill đột ngột.")
    # === v13: Mid-download slow-speed rotation ===
    p.add_argument("--audio-slow-speed-kbps", type=float, default=500.0,
                   help="v13: Ngưỡng tốc độ tối thiểu (KB/s) trong cửa sổ "
                        "slow-window-seconds để coi là OK. Nếu rolling avg < "
                        "ngưỡng này TRONG KHI ĐANG TẢI (không phải chờ "
                        "DownloadError) → force_rotate IP NGAY và resume "
                        "file .part. Mặc định: 500.0 KB/s (hợp lý với audio "
                        "YouTube 192kbps peak ~250-500 KB/s — threshold này "
                        "bắt được tốc độ 'chỉ còn 2x real-time' = CHẬM). "
                        "Threshold < 100 KB/s = false positive (speed bình "
                        "thường cũng > 100). Threshold > 1000 KB/s = không bao "
                        "giờ fire. Set 0 = tắt.")
    p.add_argument("--audio-slow-window-seconds", type=float, default=30.0,
                   help="v13: Cửa sổ thời gian (giây) để tính rolling avg "
                        "speed cho slow-speed detection. Nếu avg speed trong "
                        "cửa sổ này < audio-slow-speed-kbps → rotate. "
                        "Mặc định: 30.0s. Phải > 2s.")
    p.add_argument("--audio-max-rotate-per-video", type=int, default=3,
                   help="v13: Số lần rotate tối đa DO SLOW-SPEED cho mỗi video "
                        "(không tính rotate do DownloadError). Nếu vượt → "
                        "stop video đó. Mặc định: 3. Set 0 = không giới hạn.")

    # ================= v14: VIETSUB + API FALLBACK ARGS =================
    p.add_argument("--vi-sub-priority",
                   choices=["auto_first", "manual_first"],
                   default="auto_first",
                   help="v14: Thứ tự ưu tiên khi có nhiều VI key. "
                        "'auto_first' (default) ưu tiên auto-captions (vi-orig, "
                        "vi-VN) - phù hợp cho video VN chỉ có auto-gen. "
                        "'manual_first' ưu tiên manual subs - phù hợp cho VTV, "
                        "FAPTV có uploader upload sub.")
    p.add_argument("--no-marker-ttl-days", type=float, default=7.0,
                   help="v14: TTL (ngày) cho marker .no_transcript. Sau TTL → "
                        "retry. Mặc định: 7 ngày.")
    p.add_argument("--respect-no-transcript-marker", action="store_true",
                   help="v14: Tôn trọng marker .no_transcript (skip video đã "
                        "đánh dấu trong vòng TTL). Mặc định: bỏ qua marker, "
                        "luôn retry.")
    p.add_argument("--retry-no-transcript", action="store_true",
                   help="v14: Retry video có marker cũ (> TTL).")
    p.add_argument("--retry-no-transcript-force", action="store_true",
                   help="v14: Retry video bỏ qua CẢ marker mới (ghi đè TTL).")
    p.add_argument("--no-api-fallback", action="store_true",
                   help="v14: TẮT fallback sang youtube-transcript-api khi "
                        "yt-dlp fail. Mặc định: BẬT.")
    p.add_argument("--api-fallback-langs", type=str, default="vi,en",
                   help="v14: Danh sách ngôn ngữ ưu tiên cho API fallback "
                        "(phân cách dấu phẩy). Mặc định: 'vi,en'.")

    # ================= v14 (port từ v15): PLAYER_CLIENT ROTATION =================
    p.add_argument("--no-player-client-rotate", action="store_true",
                   help="v14: TẮT rotation player_client (dùng default cố định "
                        "['web_safari', 'web']). Mặc định: BẬT rotation.")
    p.add_argument("--player-clients", type=str, default=None,
                   help="v14: Custom list player_clients để rotate (phân cách "
                        "dấu phẩy). Mặc định: 'web_safari,web,web_embedded,"
                        "tv,android,ios'. Dùng khi muốn ưu tiên client cụ thể. "
                        "Vd: 'tv_embedded,android_vr,ios,web_safari,web'.")
    p.add_argument("--bucket-b-workers", type=int, default=10,
                   help="v17: Số worker parallel cho Bucket B (transcript only). "
                        "Mặc định 10. Override qua env BUCKET_B_WORKERS.")

    return p.parse_args()


# ================= RUN LOGGER =================
class RunLogger:
    def __init__(self, log_path, script_path=""):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"{'='*80}\nRUN LOG {datetime.now().isoformat()}\n")
            if script_path:
                f.write(f"Script: {script_path}\n")
            f.write(f"{'='*80}\n\n")

    def log(self, msg, also_print=True):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}"
        with self._lock:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        if also_print:
            print(line)

    def log_channel_start(self, idx, total, channel_url, channel_name, run_ts):
        self.log(f"CHANNEL {idx}/{total}: {channel_url} (name={channel_name}, ts={run_ts})")

    def log_channel_end(self, idx, total, channel_url, status, summary=None, error=None):
        self.log(f"CHANNEL {idx}/{total} DONE: status={status}")
        if error:
            self.log(f"  ERROR: {error}")

    def log_batch_start(self, channels_file, total_channels, command="", script_path=""):
        self.log(f"BATCH START: {total_channels} channels from {channels_file}")
        if command:
            self.log(f"  command: {command}")

    def log_batch_end(self, total_channels, success, failed, all_results):
        self.log(f"BATCH END: {total_channels} channels, "
                 f"{success} success, {failed} failed")


# ================= process_one_channel =================
def process_one_channel(
    channel_url: str, *, youtube_key: str, output_root: str,
    max_results: int, max_fetch: int, order: str, audio_format: str,
    skip_existing: bool, force_retranscribe: bool = False,
    force_redownload: bool = False,
    audio_only: bool = False,
    max_batches: int = 400, fetch_delay: int = 5,
    proxy_rotator=None, audio_proxy_rotator=None,
    transcript_proxy_rotator=None,
    key_rotator=None,
    video_delay: int = 5,
    socket_timeout: int = 100, max_retries: int = 3,
    max_sentence_duration: float = 31.0, min_sentence_words: int = 1,
    run_logger=None, channel_idx: int = 0, total_channels: int = 0,
    metadata_only: bool = False,
    rebuild_from_transcripts: bool = False,
    audio_min_speed_mbps: float = 1.0,
    audio_fake_before_real: int = 5,
    audio_min_bytes_for_speed: int = 256 * 1024,
    audio_min_window_seconds: float = 5.0,
    audio_speed_avg_window_seconds: float = 30.0,
    audio_500_threshold: int = 5,           # v7
    audio_stall_seconds: float = 30.0,      # v7
    audio_force_real_after_fails: int = 2, # v10
    # === v12: mid-download slow-speed rotation ===
    audio_slow_speed_kbps: float = 500.0,  # khớp với argparse + run_crawl_v13.sh
    audio_slow_window_seconds: float = 30.0,
    audio_max_rotate_per_video: int = 3,
    # === v14: VIETSUB + API FALLBACK ===
    vi_sub_priority: str = "auto_first",
    no_marker_ttl_days: float = 7.0,
    respect_no_transcript_marker: bool = False,
    retry_no_transcript: bool = False,
    retry_no_transcript_force: bool = False,
    no_api_fallback: bool = False,
    api_fallback_langs: str = "vi,en",
    player_client_rotate: bool = True,
    player_clients: Optional[str] = None,
) -> dict:
    channel_name = safe_channel_name(channel_url)
    channel_output = Path(output_root) / channel_name
    run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    print("\n" + "=" * 80)
    print(f"CHANNEL: {channel_url}")
    print(f"Output: {channel_output}")
    print("=" * 80)
    if run_logger:
        run_logger.log_channel_start(channel_idx, total_channels, channel_url,
                                     channel_name, run_timestamp)

    # === REBUILD MODE: chỉ đọc JSON có sẵn ===
    if rebuild_from_transcripts:
        return _rebuild_from_transcripts(
            channel_url, channel_name, Path(output_root),
            run_timestamp, run_logger, channel_idx, total_channels,
        )

    # === METADATA-ONLY MODE: chỉ fetch metadata ===
    if metadata_only:
        return _metadata_only_mode(
            channel_url, channel_name, channel_output, run_timestamp,
            youtube_key, max_results, max_fetch, order,
            max_batches, fetch_delay, socket_timeout, max_retries,
            proxy_rotator, run_logger, channel_idx, total_channels,
        )

    # === KHÔNG check skip-existing ở CHANNEL level nữa ===
    # Lý do: run_timestamp là timestamp mới của lần chạy hiện tại,
    # file pipeline_summary_<NEW_TIMESTAMP>.json chưa tồn tại → check vô dụng.
    # Logic skip chính xác nằm ở _process_videos_pipeline (per-video) —
    # build index từ TẤT CẢ subfolders audio/ + transcriptions/ cũ,
    # video có đủ audio + JSON hợp lệ → skip, thiếu → tải phần thiếu.
    channel_output.mkdir(parents=True, exist_ok=True)
    researcher = YouTubeResearcher(
        api_key=youtube_key, output_dir=str(channel_output),
        proxy_rotator=proxy_rotator,
        audio_proxy_rotator=audio_proxy_rotator,
        transcript_proxy_rotator=transcript_proxy_rotator,
        key_rotator=key_rotator,  # v6: truyền rotator cho playlistItems.list
        audio_min_speed_mbps=audio_min_speed_mbps,
        audio_fake_before_real=audio_fake_before_real,
        audio_min_bytes_for_speed=audio_min_bytes_for_speed,
        audio_min_window_seconds=audio_min_window_seconds,
        audio_speed_avg_window_seconds=audio_speed_avg_window_seconds,
        audio_500_threshold=audio_500_threshold,         # v7
        audio_stall_seconds=audio_stall_seconds,         # v7
        audio_force_real_after_fails=audio_force_real_after_fails,  # v10
        # v12: mid-download slow-speed rotation
        audio_slow_speed_kbps=audio_slow_speed_kbps,
        audio_slow_window_seconds=audio_slow_window_seconds,
        audio_max_rotate_per_video=audio_max_rotate_per_video,
        # v14: VIETSUB OPTIMIZATION
        vi_sub_priority=vi_sub_priority,
        no_marker_ttl_days=no_marker_ttl_days,
        respect_no_transcript_marker=respect_no_transcript_marker,
        retry_no_transcript=retry_no_transcript,
        retry_no_transcript_force=retry_no_transcript_force,
        no_api_fallback=no_api_fallback,
        api_fallback_langs=api_fallback_langs,
        player_client_rotate=player_client_rotate,
        player_clients=player_clients,
    )
    try:
        researcher.fetch_channel_videos(
            channel_input=channel_url, max_results=max_fetch, order=order,
            batch_size=200, max_batches=max_batches,
            socket_timeout=socket_timeout, fetch_delay=fetch_delay,
            max_retries=max_retries,
        )
    except Exception as e:
        if run_logger:
            run_logger.log_channel_end(channel_idx, total_channels, channel_url,
                                       "error", error=str(e))
        raise
    if not researcher._videos:
        return {"channel": channel_url, "status": "no_videos", "output": str(channel_output)}

    criteria = FilterCriteria(
        min_duration=FILTER_MIN_DURATION,
        max_duration=FILTER_MAX_DURATION,
        min_view_count=FILTER_MIN_VIEW_COUNT,
    )
    researcher.apply_filters(criteria)
    if len(researcher._filtered_videos) > max_results:
        researcher._filtered_videos = researcher._filtered_videos[:max_results]
    researcher.print_video_table()

    resolved_channel_name = researcher._videos[0].channel if researcher._videos else channel_name
    safe_name = resolved_channel_name.replace(" ", "_")
    researcher.save_research(f"research_{safe_name}_{run_timestamp}.json")

    print("\nRunning pipeline (audio download + YouTube transcript)...")
    summary = researcher.process_videos_pipeline(
        output_dir=str(channel_output),
        run_timestamp=run_timestamp,
        skip_existing_transcripts=not force_retranscribe,
        force_redownload=force_redownload,
        audio_only=audio_only,
        video_delay=video_delay,
        audio_format=audio_format,
        run_logger=run_logger,
    )
    summary_path = channel_output / f"pipeline_summary_{run_timestamp}.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    transcription_dir = str(channel_output / "transcriptions" / run_timestamp)
    csv_path = channel_output / f"{safe_name}_segments_minimal_{run_timestamp}.csv"
    export_segments_minimal_csv(
        output_csv=str(csv_path),
        videos=researcher._filtered_videos,
        transcription_dir=transcription_dir,
    )
    # v3: thêm video-level summary CSV với 40+ cột YouTube metadata
    summary_csv_path = channel_output / f"{safe_name}_video_summary_{run_timestamp}.csv"
    export_video_summary_csv(
        output_csv=str(summary_csv_path),
        videos=researcher._filtered_videos,
        transcription_dir=transcription_dir,
    )
    print(f"\n[DONE] {channel_url} -> {channel_output}")
    if run_logger:
        run_logger.log_channel_end(channel_idx, total_channels, channel_url, "success",
                                   summary=summary)
    return {
        "channel": channel_url, "channel_name": resolved_channel_name,
        "status": "success", "output": str(channel_output),
        "run_timestamp": run_timestamp,
        "summary": summary,
    }


# ================= REBUILD FROM TRANSCRIPTS =================
def _resolve_channel_folder(output_root: Path, channel_url: str,
                            channel_name: str) -> Optional[Path]:
    """Tìm folder kênh trong output_root bằng nhiều cách:
      1. exact match channel_name
      2. normalized match (bỏ diacritics, lowercase, bỏ space)
      3. handle @xxx extracted từ URL làm substring
    """
    if not output_root.exists():
        return None
    # 1. exact match
    exact = output_root / channel_name
    if exact.exists():
        return exact
    # 2. normalize
    def _norm(s):
        import unicodedata
        n = unicodedata.normalize("NFKD", s)
        n = "".join(ch for ch in n if not unicodedata.combining(ch))
        return n.lower().replace(" ", "")
    target = _norm(channel_name)
    # 3. Lấy handle gốc từ URL
    handle = None
    m = re.search(r"@([^/\s?]+)", channel_url or "")
    if m:
        handle = m.group(1)
    for sub in output_root.iterdir():
        if not sub.is_dir():
            continue
        sub_norm = _norm(sub.name)
        if sub_norm == target:
            return sub
        # 4. Match bằng handle gốc (lowercase, no diacritics)
        if handle and _norm(handle) in sub_norm:
            return sub
    return None


def _rebuild_from_transcripts(channel_url, channel_name, output_root, run_ts,
                               run_logger, channel_idx, total_channels):
    """Đọc JSON có sẵn trong transcriptions/ để tạo CSV + summary.

    KHÔNG gọi API, KHÔNG tải audio, KHÔNG lấy transcript.
    Dùng khi user muốn extract lại metadata từ JSON bất kỳ lúc nào.
    """
    print(f"\n[REBUILD MODE] Đọc JSON có sẵn...")
    # Tìm folder kênh linh hoạt (vì handle URL != tên folder thật)
    channel_output = _resolve_channel_folder(output_root, channel_url, channel_name)
    if channel_output is None:
        print(f"  [WARN] Không tìm thấy folder kênh trong {output_root}. Bỏ qua.")
        return {"channel": channel_url, "status": "no_channel_folder", "rebuild": True}
    print(f"  Channel folder: {channel_output}")
    transcriptions_dir = channel_output / "transcriptions"
    if not transcriptions_dir.exists():
        print(f"  [WARN] Không có folder transcriptions/. Bỏ qua.")
        return {"channel": channel_url, "status": "no_transcripts", "rebuild": True}

    # Lấy tất cả JSON từ tất cả subfolder timestamp
    all_jsons = []
    for sub in sorted(transcriptions_dir.iterdir(), reverse=True):
        if sub.is_dir():
            for f in sub.glob("*_transcription.json"):
                all_jsons.append(f)
    if not all_jsons:
        print(f"  [WARN] Không có file JSON. Bỏ qua.")
        return {"channel": channel_url, "status": "no_json", "rebuild": True}

    print(f"  Tìm thấy {len(all_jsons)} file JSON")

    # Build danh sách VideoCandidate từ JSON
    videos = []
    for jp in all_jsons:
        try:
            with open(jp, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            continue
        v = VideoCandidate(
            video_id=data.get("video_id", ""),
            title=data.get("title", ""),
            channel=data.get("channel", ""),
            description=data.get("description", ""),
            published_at=data.get("published_at", ""),
            duration=data.get("duration", ""),
            view_count=int(data.get("view_count", 0)),
            like_count=int(data.get("like_count", 0)),
            comment_count=int(data.get("comment_count", 0)),
            url=data.get("url", ""),
            tags=data.get("tags", []),
            category_id=data.get("category_id", ""),
            categories=data.get("categories", []),
            default_language=data.get("default_language", ""),
            default_audio_language=data.get("default_audio_language", ""),
            caption_available=bool(data.get("caption_available", False)),
            definition=data.get("definition", ""),
            channel_id=data.get("channel_id", ""),
            channel_url=data.get("channel_url", ""),
            duration_string=data.get("duration_string", ""),
            audio_filename=data.get("audio_filename", ""),
        )
        videos.append(v)
    resolved_channel_name = videos[0].channel if videos else channel_name
    safe_name = resolved_channel_name.replace(" ", "_")

    # Save research JSON
    channel_output.mkdir(parents=True, exist_ok=True)
    research_data = {
        "research_date": datetime.now().isoformat(),
        "channel": resolved_channel_name,
        "total_videos_found": len(videos),
        "videos_after_filter": len(videos),
        "videos": [asdict(v) | {"video_url": v.video_url} for v in videos],
        "rebuild_mode": True,
    }
    research_path = channel_output / f"research_{safe_name}_{run_ts}.json"
    with open(research_path, "w", encoding="utf-8") as f:
        json.dump(research_data, f, ensure_ascii=False, indent=2)
    print(f"  Saved research: {research_path}")

    # CSV từ JSON mới nhất
    transcription_dir = str(transcriptions_dir / max(
        d.name for d in transcriptions_dir.iterdir() if d.is_dir()))
    csv_path = channel_output / f"{safe_name}_segments_minimal_{run_ts}.csv"
    export_segments_minimal_csv(
        output_csv=str(csv_path), videos=videos, transcription_dir=transcription_dir)
    # v3: video-level summary CSV
    summary_csv_path = channel_output / f"{safe_name}_video_summary_{run_ts}.csv"
    export_video_summary_csv(
        output_csv=str(summary_csv_path), videos=videos,
        transcription_dir=transcription_dir)

    summary = {
        "rebuild": True, "total": len(videos), "success": len(videos),
        "results": [{"video_id": v.video_id, "title": v.title, "status": "rebuilt"}
                    for v in videos],
    }
    summary_path = channel_output / f"pipeline_summary_{run_ts}.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    if run_logger:
        run_logger.log_channel_end(channel_idx, total_channels, channel_url,
                                   "rebuild_success", summary=summary)
    print(f"\n[REBUILD DONE] {channel_url}")
    return {"channel": channel_url, "status": "rebuild_success",
            "output": str(channel_output), "summary": summary}


# ================= METADATA ONLY =================
def _metadata_only_mode(channel_url, channel_name, channel_output, run_ts,
                          youtube_key, max_results, max_fetch, order,
                          max_batches, fetch_delay, socket_timeout, max_retries,
                          proxy_rotator, run_logger, channel_idx, total_channels):
    """Chỉ fetch metadata, KHÔNG tải audio, KHÔNG lấy transcript."""
    print(f"\n[METADATA-ONLY MODE]")
    channel_output.mkdir(parents=True, exist_ok=True)
    researcher = YouTubeResearcher(
        api_key=youtube_key, output_dir=str(channel_output),
        proxy_rotator=proxy_rotator,
    )
    try:
        researcher.fetch_channel_videos(
            channel_input=channel_url, max_results=max_fetch, order=order,
            batch_size=200, max_batches=max_batches,
            socket_timeout=socket_timeout, fetch_delay=fetch_delay,
            max_retries=max_retries,
        )
    except Exception as e:
        if run_logger:
            run_logger.log_channel_end(channel_idx, total_channels, channel_url,
                                       "error", error=str(e))
        raise
    if not researcher._videos:
        return {"channel": channel_url, "status": "no_videos"}
    criteria = FilterCriteria(
        min_duration=FILTER_MIN_DURATION, max_duration=FILTER_MAX_DURATION,
        min_view_count=FILTER_MIN_VIEW_COUNT,
    )
    researcher.apply_filters(criteria)
    if len(researcher._filtered_videos) > max_results:
        researcher._filtered_videos = researcher._filtered_videos[:max_results]
    researcher.print_video_table()
    resolved_channel_name = researcher._videos[0].channel if researcher._videos else channel_name
    safe_name = resolved_channel_name.replace(" ", "_")
    researcher.save_research(f"research_{safe_name}_{run_ts}.json")
    if run_logger:
        run_logger.log_channel_end(channel_idx, total_channels, channel_url,
                                   "metadata_only_success")
    return {"channel": channel_url, "status": "metadata_only_success",
            "output": str(channel_output)}


# ================= PIPELINE =================
def _process_videos_pipeline(self, output_dir, run_timestamp="",
                              skip_existing_transcripts=True,
                              force_redownload=False,
                              force_retranscribe=False,
                              audio_only=False,
                              video_delay=10,
                              max_sentence_duration=33.0,
                              min_sentence_words=1,
                              audio_format="m4a",
                              run_logger=None) -> dict:
    """Pipeline: tải audio + lấy YouTube subs transcript.

    Refactor v5.1: 3 bucket A/B/C (giống v2) — code sạch, dễ debug, dễ maintain.
    v5.1 giữ nguyên logic AudioIPController (state machine REAL/FAKE) cho audio.
      - Bucket A: có CẢ audio + JSON matching → SKIP nhanh (0 I/O)
      - Bucket B: có audio (run cũ), thiếu JSON → chỉ transcribe
      - Bucket C: chưa có audio → full pipeline (download + transcribe + save)

    Args:
        audio_only: nếu True, chỉ tải audio, KHÔNG transcribe:
          - Bucket A: vẫn skip
          - Bucket B: skip (đã có audio, mục đích đạt)
          - Bucket C: chỉ download audio, KHÔNG transcribe + save JSON
    """
    output_dir = Path(output_dir)
    if not run_timestamp:
        run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    transcriptions_dir = output_dir / "transcriptions" / run_timestamp
    transcriptions_dir.mkdir(parents=True, exist_ok=True)
    audio_dir = output_dir / "audio" / run_timestamp
    audio_dir.mkdir(parents=True, exist_ok=True)

    def _log(msg, also_print=True):
        if run_logger:
            run_logger.log(msg, also_print=also_print)
        elif also_print:
            print(msg)

    results = []
    if skip_existing_transcripts:
        # v6: cleanup TẤT CẢ subfolders (không chỉ run hiện tại) để khớp với
        # _build_audio_index scan scope.
        self._cleanup_orphan_part_files(audio_dir, min_size_mb=100,
                                         cleanup_all_subdirs=True)

    # === Pre-partition: 1 disk scan -> 3 bucket (A: skip, B: transcribe-only, C: full) ===
    bucket_a, bucket_b, bucket_c = self._partition_videos_for_pipeline(
        audio_root=audio_dir.parent,
        transcriptions_root=transcriptions_dir.parent,
        skip_existing=skip_existing_transcripts and not force_redownload,
    )

    total = len(self._filtered_videos)
    _log(f"Pipeline partition (total={total}):")
    _log(f"  Bucket A (audio+json da co, SKIP)         : {len(bucket_a)}")
    _log(f"  Bucket B (co audio, chua co json)         : {len(bucket_b)}")
    _log(f"  Bucket C (chua co audio, can download)    : {len(bucket_c)}")

    # ============================================================
    # BUCKET A: co ca audio + json -> SKIP nhanh (khong I/O)
    # ============================================================
    for i, (video, audio_path, json_path) in enumerate(bucket_a, 1):
        audio_filename = audio_path.name
        try:
            audio_size_kb = audio_path.stat().st_size // 1024
        except OSError:
            audio_size_kb = 0
        try:
            audio_rel = audio_path.relative_to(audio_dir.parent)
            json_rel = json_path.relative_to(transcriptions_dir.parent)
        except ValueError:
            audio_rel = Path(audio_path.name)
            json_rel = Path(json_path.name)
        print(f"\n[A-{i}/{len(bucket_a)}] {video.title[:60]}")
        print(f"  [SKIP] audio + JSON đã có sẵn")
        print(f"    audio: {audio_rel} ({audio_size_kb} KB)")
        print(f"    json:  {json_rel}")
        _log(f"[A-{i}/{len(bucket_a)}] {video.video_id} | {video.title[:50]} "
             f"-> SKIP (audio: {audio_filename}, json: {json_path.name})",
             also_print=False)
        # Load thông tin từ JSON để giữ audio_filename cho CSV
        try:
            with open(json_path, "r", encoding="utf-8") as jf:
                existing = json.load(jf)
            video.audio_filename = existing.get("audio_path", audio_filename)
        except Exception:
            video.audio_filename = audio_filename
        results.append({
            "video_id": video.video_id, "title": video.title,
            "status": "skipped",
            "audio_filename": video.audio_filename,
            "transcription_filename": json_path.name,
            "transcript_language": "N/A",
            "transcript_is_auto": None,
            "transcript_source": "existing",
        })

    # ============================================================
    # BUCKET B: co audio (o run cu), chua co json -> chi transcribe
    # v17: PARALLEL processing với N workers (env BUCKET_B_WORKERS, default 10)
    # ============================================================
    _bucket_b_workers = int(os.environ.get("BUCKET_B_WORKERS", "10"))

    # Filter ra các video cần skip trước (audio-only, no_transcript marker)
    _bucket_b_todo = []  # list of (idx, video, audio_path, audio_filename)
    for i, (video, audio_path, audio_filename) in enumerate(bucket_b, 1):
        if audio_only:
            print(f"\n[B-{i}/{len(bucket_b)}] {video.title[:60]}")
            print(f"  [SKIP-AUDIO-ONLY] đã có audio ({audio_filename}), "
                  f"không cần JSON (--audio-only)")
            _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | SKIP (audio-only, "
                 f"audio: {audio_filename})", also_print=False)
            results.append({
                "video_id": video.video_id, "title": video.title,
                "status": "skipped",
                "audio_filename": audio_filename,
                "transcription_filename": None,
                "transcript_language": None,
                "transcript_is_auto": None,
                "transcript_source": "audio_only_mode",
            })
            continue
        if YouTubeResearcher._has_no_transcript_marker(video.video_id, transcriptions_dir):
            print(f"\n[B-{i}/{len(bucket_b)}] {video.title[:60]}")
            print(f"  [SKIP-NO-TRANSCRIPT] marker exists, skip yt-dlp "
                  f"(video={video.video_id}, audio: {audio_filename})")
            _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | SKIP (no_transcript marker)", also_print=False)
            results.append({
                "video_id": video.video_id, "title": video.title,
                "status": "transcript_unavailable",
                "audio_filename": audio_filename,
                "audio_downloaded_at": None,
            })
            continue
        _bucket_b_todo.append((i, video, audio_path, audio_filename))

    if _bucket_b_todo:
        import concurrent.futures as _cf_b
        _lock_b = threading.Lock()

        def _process_bucket_b_item(item):
            """Worker xử lý 1 video Bucket B (transcript only)."""
            idx, vid, a_path, a_fname = item
            json_stem = Path(a_fname).stem
            json_path = transcriptions_dir / f"{json_stem}_transcription.json"

            info_cached_b = {}
            try:
                if getattr(vid, "subtitles", None):
                    info_cached_b["subtitles"] = vid.subtitles
                if getattr(vid, "automatic_captions", None):
                    info_cached_b["automatic_captions"] = vid.automatic_captions
            except Exception:
                pass

            print(f"\n[B-{idx}/{len(bucket_b)}] {vid.title[:60]}")
            print(f"  [SKIP-DOWNLOAD] audio có sẵn ở "
                  f"{a_path.parent.name}/{a_fname}, lấy transcript YouTube...")

            try:
                result, status = self.transcribe_with_youtube(
                    video_id=vid.video_id, audio_path=a_path,
                    lang=["vi", "en"],
                    max_sentence_duration=max_sentence_duration,
                    min_sentence_words=min_sentence_words,
                    info_cached=info_cached_b if info_cached_b else None,
                    attempt=1,
                )
            except Exception as e:
                _log(f"[B-{idx}/{len(bucket_b)}] {vid.video_id} | transcript error: {e}")
                return {
                    "video_id": vid.video_id, "title": vid.title,
                    "status": "transcript_error",
                    "audio_filename": a_fname,
                    "audio_downloaded_at": None,
                    "error": str(e),
                }

            if result:
                vid.audio_filename = a_fname
                self._save_transcription(
                    output_path=json_path, segments=result["segments"],
                    video=vid, audio_duration=result["audio_duration"],
                    audio_filename=a_fname or "",
                    audio_downloaded_at=None,
                    extra_metadata={
                        "transcript_language": result.get("transcript_language", ""),
                        "transcript_is_auto": result.get("transcript_is_auto", False),
                        "transcript_source": result.get("transcript_source", ""),
                        "detected_languages": result.get("detected_languages", []),
                    },
                )
                print(f"  [B-{idx}] Done ({len(result['segments'])} segments, "
                      f"lang={result.get('transcript_language')}, "
                      f"auto={result.get('transcript_is_auto')})")
                _log(f"[B-{idx}/{len(bucket_b)}] {vid.video_id} | DONE "
                     f"({len(result['segments'])} seg, lang={result.get('transcript_language')}, "
                     f"audio: {a_fname})", also_print=False)
                return {
                    "video_id": vid.video_id, "title": vid.title,
                    "status": "success", "audio_filename": a_fname,
                    "transcription_filename": json_path.name,
                    "transcript_language": result.get("transcript_language", ""),
                    "transcript_is_auto": result.get("transcript_is_auto", False),
                    "transcript_source": result.get("transcript_source", ""),
                    "audio_downloaded_at": None,
                    "transcribed_at": datetime.now().isoformat(),
                }
            else:
                # v18: Chỉ mark .no_transcript khi video THẬT SỰ không có sub (status=no_subs).
                # Network/tunnel fail (status=failed) → KHÔNG mark, có thể retry run sau.
                if status == "no_subs":
                    print(f"  [B-{idx}] No YouTube transcript (confirmed: no Vi subs)")
                    _log(f"[B-{idx}/{len(bucket_b)}] {vid.video_id} | NO TRANSCRIPT "
                         f"(audio: {a_fname})", also_print=False)
                    self._mark_no_transcript(vid.video_id, transcriptions_dir)
                    return {
                        "video_id": vid.video_id, "title": vid.title,
                        "status": "transcript_unavailable",
                        "audio_filename": a_fname,
                        "audio_downloaded_at": None,
                    }
                else:
                    print(f"  [B-{idx}] Transcript failed (network/tunnel, status={status}) — "
                          f"KHÔNG mark .no_transcript, sẽ retry run sau")
                    _log(f"[B-{idx}/{len(bucket_b)}] {vid.video_id} | TRANSCRIPT FAILED "
                         f"(status={status}, audio: {a_fname})", also_print=False)
                    return {
                        "video_id": vid.video_id, "title": vid.title,
                        "status": "transcript_error",
                        "audio_filename": a_fname,
                        "audio_downloaded_at": None,
                        "error": f"all attempts failed: {status}",
                    }

        print(f"\n[BUCKET-B] Processing {len(_bucket_b_todo)} videos with "
              f"{_bucket_b_workers} parallel workers...")
        with _cf_b.ThreadPoolExecutor(max_workers=_bucket_b_workers) as pool_b:
            futures_b = {pool_b.submit(_process_bucket_b_item, item): item
                         for item in _bucket_b_todo}
            for future in _cf_b.as_completed(futures_b):
                try:
                    r = future.result()
                    if r:
                        results.append(r)
                except Exception as e:
                    item = futures_b[future]
                    idx, vid, _, a_fname = item
                    _log(f"[B-{idx}] {vid.video_id} | UNEXPECTED ERROR: {e}")
                    results.append({
                        "video_id": vid.video_id, "title": vid.title,
                        "status": "transcript_error",
                        "audio_filename": a_fname,
                        "error": str(e),
                    })

    # ============================================================
    # BUCKET C: chua co audio -> download + transcribe + save
    # v5.1: giữ nguyên AudioIPController + on_download_start/complete
    # ============================================================
    for i, (video, target_name, target_filename) in enumerate(bucket_c, 1):
        # FIX v2 Skip #2: nếu video đã được đánh dấu no_transcript ở run trước
        # → skip download audio luôn, tiết kiệm bandwidth.
        if YouTubeResearcher._has_no_transcript_marker(video.video_id, transcriptions_dir):
            print(f"\n[C-{i}/{len(bucket_c)}] {video.title[:60]}")
            print(f"  [SKIP-NO-TRANSCRIPT] marker exists, skip download audio + yt-dlp "
                  f"(video={video.video_id})")
            _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | SKIP (no_transcript marker)", also_print=False)
            results.append({
                "video_id": video.video_id, "title": video.title,
                "status": "transcript_unavailable",
                "audio_filename": None,
                "audio_downloaded_at": None,
            })
            continue

        # v13.1: SKIP nếu video đã bị đánh dấu live_unavailable ở run trước
        # (live stream chưa bắt đầu / premiere upcoming). Không tốn công retry.
        if YouTubeResearcher._has_live_unavailable_marker(video.video_id, transcriptions_dir):
            print(f"\n[C-{i}/{len(bucket_c)}] {video.title[:60]}")
            print(f"  [SKIP-LIVE-UNAVAILABLE] marker exists, skip download audio + yt-dlp "
                  f"(video={video.video_id})")
            _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | SKIP (live_unavailable marker)",
                 also_print=False)
            results.append({
                "video_id": video.video_id, "title": video.title,
                "status": "live_unavailable",
                "audio_filename": None,
                "audio_downloaded_at": None,
            })
            continue

        print(f"\n[C-{i}/{len(bucket_c)}] {video.title[:60]}")
        _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | {video.title[:50]} "
             f"-> download + transcribe", also_print=False)

        # v5.13 OPTION A: Đầu MỖI VIDEO MỚI → RESET state về REAL + KILL
        # audio tunnel (CHỈ của audio_rotator, KHÔNG kill hết user).
        # v11: Đổi từ `kill_all_vpn_tunnels()` (v10) sang per-instance kill.
        # Lý do: kill_all giết cả tunnel của instance khác đang chạy song song.
        #
        # Reset về REAL mỗi video đảm bảo:
        #   1) Mỗi video LUÔN thử IP thật trước → có cơ hội reset rate-limit
        #      counter (YouTube clear rate-limit cho IP thật hơn so với IP VPN).
        #   2) Kill audio tunnel → tránh /dev/net/tun bị chiếm (chỉ của audio).
        #   3) Nếu IP thật OK (>= 1MB/s) → giữ REAL cho cả download → đỡ tốn
        #      thời gian test 2 lần.
        #   4) Nếu IP thật fail → tự động switch sang FAKE như cũ.
        if i >= 1:
            print(f"  [audio-ip] RESET state → REAL + KILL audio tunnel (per-instance, "
                  f"video #{i}, video_id={video.video_id})",
                  flush=True)
            # v11: Chỉ kill tunnel CỦA audio_rotator của instance này.
            # KHÔNG kill tunnel của metadata_rotator/transcript_rotator trong
            # cùng instance, VÀ KHÔNG kill tunnel của instance khác.
            try:
                rotator = self._audio_ip_ctl.audio_rotator
                if rotator is not None:
                    # IsolatedVPNRotator._disconnect() — kill CHỈ PID của audio_rotator
                    if hasattr(rotator, "_disconnect"):
                        rotator._disconnect()
                    elif hasattr(rotator, "disconnect"):
                        rotator.disconnect()
                    else:
                        # Fallback: gọi kill_tunnel_by_instance với INSTANCE_ID
                        # (lấy từ global, set bởi main()).
                        if INSTANCE_ID:
                            kill_tunnel_by_instance(INSTANCE_ID)
            except Exception as e:
                print(f"  [audio-ip] audio_rotator disconnect error (ignored): {e}",
                      flush=True)
            # Set state=REAL + reset counters
            self._audio_ip_ctl._state = self._audio_ip_ctl.STATE_REAL
            self._audio_ip_ctl._consecutive_fake_slow = 0
            self._audio_ip_ctl._slow_flag = False
            # v7: Reset HTTP500Detector cho video mới
            try:
                self._http500_detector.reset()
            except Exception as e:
                print(f"  [v7-detector] reset error (ignored): {e}", flush=True)

        # Delay giữa các video để tránh YouTube rate limit (429)
        if i > 1 and video_delay > 0:
            time.sleep(video_delay)

        # Tên file JSON sẽ match với tên audio
        new_json_stem = target_name
        new_json_path = transcriptions_dir / f"{new_json_stem}_transcription.json"

        audio_path = None
        audio_filename = None
        info_cache: dict = {}

        # Bước 0: tải audio về (luôn giữ audio) - direct-first, fallback proxy khi 429/block
        try:
            import yt_dlp
            # v4: tăng từ 3 -> 5 retries cho audio download
            dl_retries = 5
            info = None
            # Resume support: xóa .ytdl cũ (yt-dlp tạo file này khi bị kill)
            video_id_stem = audio_dir / video.video_id
            for stale in [".ytdl"]:
                stale_file = video_id_stem.with_suffix(stale)
                if stale_file.exists():
                    try:
                        stale_file.unlink()
                    except Exception:
                        pass
            for dl_attempt in range(1, dl_retries + 1):
                # v10: Reset HTTP500Detector cho mỗi attempt mới để stall
                # detector có thể fire LẠI nếu IP mới vẫn bị stuck. Nếu không
                # reset, _stall_flag vẫn True từ attempt trước → stall không
                # trigger lần 2 → IP sẽ bị stuck vô thời hạn trên attempt mới.
                try:
                    self._http500_detector.reset()
                except Exception:
                    pass

                # v5: Chọn IP qua AudioIPController (state machine REAL/FAKE).
                # Controller quyết định:
                #   - IP thật: KHÔNG set proxy, KHÔNG acquire tunnel guard
                #   - IP fake: acquire tunnel guard + (KHÔNG set proxy vì
                #     OpenVPN là system-level tunnel)
                # Lưu ý: KHÔNG gọi force_rotate thủ công ở đây — để
                # AudioIPController.on_download_complete() quyết định dựa
                # trên tốc độ đo được.
                # v15: on_download_start() trả IPRoutingInfo thay vì proxy URL
                _routing_info = self._audio_ip_ctl.on_download_start()
                self._audio_ip_ctl.on_download_start_reset_slow_log()
                using_real_ip = _routing_info.using_real_ip
                _source_address = _routing_info.source_address

                # Biến track cho progress hook
                _dl_state = {
                    "t_start": time.time(),
                    "last_chunk_bytes": 0,
                    "last_chunk_t": time.time(),
                    "bytes_dl_max": 0,
                    "downloaded_bytes": 0,
                    "elapsed_download": 0.0,
                    # v13.4: CHUNK-SLOW fire-once-per-attempt tracking
                    "_v13_chunk_slow_fired": False,
                    "_v13_prev_bytes": 0,
                    # video_id for marker file fallback
                    "_v13_video_id": video.video_id if hasattr(video, "video_id") else "",
                }

                def _audio_progress_hook(d):
                    """yt-dlp progress hook. v7: stall. v12: slow-speed mid-dl rotate.
                    v13: FIX bug "outer except nuốt MidDownloadRotate".
                    """
                    # === v13 FIX: BỎ outer try/except ở đây.
                    # v12 BUG: hook có outer `except Exception as hook_err` ở
                    # cuối catch lại MidDownloadRotate (sau khi inner re-raise)
                    # → exception bị nuốt → yt-dlp không nhận được.
                    # v13 FIX: Inner try/except cho slow-speed check vẫn giữ
                    # (để tránh logic khác crash hook), nhưng MidDownloadRotate
                    # raise ra NGOÀI outer try → không bị nuốt.
                    #
                    # v13.3: Đã bỏ HOÀN TOÀN slow-speed check trong hook này.
                    # Quyết định đổi IP giờ chỉ diễn ra ở:
                    #   1. SmartDownloader._handle_mid_download_slow (cơ chế riêng)
                    #   2. AudioIPController.on_download_complete() (đánh giá cuối)

                    status = d.get("status", "")
                    if status == "downloading":
                        bytes_now = int(d.get("downloaded_bytes") or 0)
                        speed_bps = float(d.get("speed") or 0)
                        elapsed = time.time() - _dl_state["t_start"]
                        _dl_state["bytes_dl_max"] = max(
                            _dl_state["bytes_dl_max"], bytes_now)
                        _dl_state["downloaded_bytes"] = bytes_now
                        _dl_state["elapsed_download"] = elapsed

                        # === stall detection (HTTP500Detector) ===
                        try:
                            self._http500_detector.on_progress_check_stall(
                                bytes_dl=bytes_now, now=time.time(),
                            )
                        except Exception as e:
                            print(f"    [audio-ip] stall check ERROR: {e}",
                                  flush=True)

                        # v13.4: Khôi phục CHUNK-SLOW-style window [0,30], [30,60], [60,90]...
                        # Window size configurable qua CLI (`--audio-speed-avg-window-seconds`,
                        # mặc định 30s). Khi 1 window có avg < threshold →
                        # _slow_flag=True → hook raise MidDownloadRotate NGAY →
                        # SmartDownloader._handle_mid_download_slow catch + force rotate.

                        # === on_chunk_progress (set _slow_flag) ===
                        try:
                            self._audio_ip_ctl.on_chunk_progress(
                                bytes_dl=bytes_now,
                                elapsed_s=elapsed,
                                speed_bps=speed_bps,
                            )
                        except Exception as e:
                            print(f"    [audio-ip] on_chunk_progress ERROR: {e}",
                                  flush=True)

                        # === v13.4: CHUNK-SLOW → RAISE MidDownloadRotate NGAY ===
                        # Ngay khi _slow_flag vừa chuyển False → True trong attempt
                        # này → raise 1 lần. Cờ _v13_chunk_slow_fired chỉ để
                        # tránh fire nhiều lần trong CÙNG attempt (reset bởi
                        # on_download_start_reset_slow_log khi attempt mới).
                        try:
                            if (self._audio_ip_ctl._slow_flag
                                    and not _dl_state.get("_v13_chunk_slow_fired", False)):
                                _dl_state["_v13_chunk_slow_fired"] = True
                                prev_bytes = _dl_state.get("_v13_prev_bytes", bytes_now)
                                speed_kbps_now = max(0, (bytes_now - prev_bytes) / 1024.0)
                                _dl_state["_v13_prev_bytes"] = bytes_now
                                print(
                                    f"    [v13-slow] 🐌 CHUNK-SLOW detected "
                                    f"(speed={speed_kbps_now:.1f} KB/s) "
                                    f"→ raise MidDownloadRotate NGAY để đổi IP",
                                    flush=True,
                                )
                                marker_path = ""
                                if write_mid_download_marker is not None:
                                    try:
                                        marker_path = write_mid_download_marker(
                                            _dl_state.get("_v13_video_id", ""),
                                            speed_kbps_now,
                                            float(self._audio_ip_ctl.speed_avg_window_seconds),
                                            bytes_now, elapsed,
                                        )
                                    except Exception:
                                        pass
                                raise MidDownloadRotate(
                                    avg_kbps=speed_kbps_now,
                                    window_seconds=float(
                                        self._audio_ip_ctl.speed_avg_window_seconds),
                                    bytes_dl=bytes_now,
                                    elapsed_s=elapsed,
                                    marker_file_path=marker_path or None,
                                )
                            # Reset _v13_chunk_slow_fired nếu _slow_flag đã False
                            elif not self._audio_ip_ctl._slow_flag:
                                _dl_state["_v13_chunk_slow_fired"] = False
                                _dl_state["_v13_prev_bytes"] = bytes_now
                        except MidDownloadRotate:
                            raise  # propagate ra ngoài hook scope
                        except Exception as e:
                            print(f"    [v13-slow] chunk-slow check ERROR: {e}",
                                  flush=True)

                    elif status == "finished":
                        downloaded_bytes = int(d.get("downloaded_bytes") or 0)
                        _dl_state["bytes_dl_max"] = max(
                            _dl_state["bytes_dl_max"],
                            downloaded_bytes,
                        )
                        _dl_state["downloaded_bytes"] = downloaded_bytes
                        _dl_state["elapsed_download"] = time.time() - _dl_state["t_start"]

                ydl_opts = {
                    'format': 'bestaudio/best',
                    'merge_output_format': audio_format,
                    'outtmpl': str(audio_dir / '%(id)s.%(ext)s'),
                    'quiet': True, 'js_runtimes': {'node': {}},
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'wav',
                        'preferredquality': '192',
                    }],
                    'postprocessor_args': ['-ar', '16000', '-ac', '1'],
                    'progress_hooks': [_audio_progress_hook],
                    # v7: GIỚI HẠN retry fragment thay vì default ∞. Khi IP
                    # chết, retry vô tận → tốn bandwidth vô ích. Giới hạn 5
                    # lần → sau 5 lần mà vẫn fail → throw exception → catch
                    # ở ngoài → cycle IP.
                    'retries': 5,
                    'fragment_retries': 5,
                    'retry_sleep_functions': {
                        'http': lambda n: min(2 ** n, 30),  # exponential backoff
                        'fragment': lambda n: min(2 ** n, 30),
                    },
                }
                self._apply_auth_skip(ydl_opts)
                self._apply_cookies(ydl_opts)
                self._apply_timeouts(ydl_opts, socket_timeout=25)
                # v15: Inject source_address để bind socket → policy routing
                if _source_address:
                    ydl_opts['source_address'] = _source_address
                try:
                    # === v5: IP routing qua AudioIPController ===
                    if using_real_ip:
                        # IP thật: KHÔNG acquire tunnel, KHÔNG set proxy.
                        # Traffic đi qua default route.
                        if self._smart_dl is not None:
                            # === v9: Smart retry — đổi IP NGAY khi timeout ===
                            _smart_result = self._smart_dl.download_with_smart_retry(
                                url=video.url,
                                ydl_opts=ydl_opts,
                                progress_hook=_audio_progress_hook,
                                # v13: mid-download slow-speed rotation
                                slow_speed_kbps=self._v13_slow_speed_kbps,
                                slow_window_seconds=self._v13_slow_window_seconds,
                                max_rotate_per_video=self._v13_max_rotate_per_video,
                            )
                            if not _smart_result['ok']:
                                # v13.1: Live not started → skip + ghi marker
                                if _smart_result.get('should_skip_video'):
                                    YouTubeResearcher._write_live_unavailable_marker(
                                        transcriptions_dir,
                                        video.video_id,
                                        reason=_smart_result.get(
                                            'skip_reason', 'live_not_started'),
                                    )
                                    # Ghi CSV/log skip + raise để caller skip
                                    print(
                                        f"  [SKIP-LIVE-NOT-STARTED] "
                                        f"{video.video_id} "
                                        f"(reason={_smart_result.get('skip_reason')})",
                                        flush=True,
                                    )
                                    raise RuntimeError(
                                        f"LIVE_NOT_STARTED: {_smart_result['last_error'][:200]}"
                                    )
                                raise RuntimeError(
                                    f"SmartDownloader fail sau {_smart_result['attempts']} attempts: "
                                    f"{_smart_result['last_error'][:200]}"
                                )
                            info = _smart_result['info']
                            filename = _smart_result['filename']
                        else:
                            # Fallback v7: chạy ydl.extract_info gốc
                            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                info = ydl.extract_info(video.url, download=True)
                                filename = ydl.prepare_filename(info)
                    elif (self._audio_rotator is not None
                          and self._audio_rotator is not self._rotator):
                        # IP fake qua audio_rotator riêng
                        with self._proxy_guard_for_audio():
                            if self._smart_dl is not None:
                                # === v9: Smart retry — đổi IP NGAY khi timeout ===
                                _smart_result = self._smart_dl.download_with_smart_retry(
                                    url=video.url,
                                    ydl_opts=ydl_opts,
                                    progress_hook=_audio_progress_hook,
                                )
                                if not _smart_result['ok']:
                                    if _smart_result.get('should_skip_video'):
                                        YouTubeResearcher._write_live_unavailable_marker(
                                            transcriptions_dir,
                                            video.video_id,
                                            reason=_smart_result.get(
                                                'skip_reason', 'live_not_started'),
                                        )
                                        print(
                                            f"  [SKIP-LIVE-NOT-STARTED] "
                                            f"{video.video_id} "
                                            f"(reason={_smart_result.get('skip_reason')})",
                                            flush=True,
                                        )
                                        raise RuntimeError(
                                            f"LIVE_NOT_STARTED: {_smart_result['last_error'][:200]}"
                                        )
                                    raise RuntimeError(
                                        f"SmartDownloader fail sau {_smart_result['attempts']} attempts: "
                                        f"{_smart_result['last_error'][:200]}"
                                    )
                                info = _smart_result['info']
                                filename = _smart_result['filename']
                            else:
                                # Fallback v7: chạy ydl.extract_info gốc
                                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                    info = ydl.extract_info(video.url, download=True)
                                    filename = ydl.prepare_filename(info)
                    else:
                        # Fallback: dùng _proxy_guard() (rotator dùng chung)
                        with self._proxy_guard():
                            if self._smart_dl is not None:
                                # === v9: Smart retry — đổi IP NGAY khi timeout ===
                                _smart_result = self._smart_dl.download_with_smart_retry(
                                    url=video.url,
                                    ydl_opts=ydl_opts,
                                    progress_hook=_audio_progress_hook,
                                )
                                if not _smart_result['ok']:
                                    if _smart_result.get('should_skip_video'):
                                        YouTubeResearcher._write_live_unavailable_marker(
                                            transcriptions_dir,
                                            video.video_id,
                                            reason=_smart_result.get(
                                                'skip_reason', 'live_not_started'),
                                        )
                                        print(
                                            f"  [SKIP-LIVE-NOT-STARTED] "
                                            f"{video.video_id} "
                                            f"(reason={_smart_result.get('skip_reason')})",
                                            flush=True,
                                        )
                                        raise RuntimeError(
                                            f"LIVE_NOT_STARTED: {_smart_result['last_error'][:200]}"
                                        )
                                    raise RuntimeError(
                                        f"SmartDownloader fail sau {_smart_result['attempts']} attempts: "
                                        f"{_smart_result['last_error'][:200]}"
                                    )
                                info = _smart_result['info']
                                filename = _smart_result['filename']
                            else:
                                # Fallback v7: chạy ydl.extract_info gốc
                                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                                    info = ydl.extract_info(video.url, download=True)
                                    filename = ydl.prepare_filename(info)
                    audio_path = Path(filename)
                    if not audio_path.exists() or audio_path.suffix not in (
                            ".wav", ".mp3", ".m4a", ".flac", ".opus", ".ogg",
                            ".webm", ".mp4"):
                        wav_candidate = audio_path.with_suffix(".wav")
                        if wav_candidate.exists():
                            audio_path = wav_candidate
                        else:
                            stem = audio_path.with_suffix("")
                            for ext in [".wav", ".m4a", ".mp3", ".flac",
                                        ".opus", ".ogg", ".webm", ".mp4"]:
                                cand = stem.with_suffix(ext)
                                if cand.exists():
                                    audio_path = cand
                                    break
                    # v6 BUG #2 FIX: KHÔNG trick bằng `with_suffix(".wav")` nữa.
                    # Bug cũ: chỉ đổi biến Python, file thật trên disk vẫn là
                    # raw `.webm`/`.m4a` (postprocess chưa chạy xong) → các block
                    # phía dưới (dùng audio_path.exists()) thấy False →
                    # resolve sai `target_ext = ".wav"`, sai `audio_filename`,
                    # cleanup xóa raw file → DATA LOSS + JSON pointer trỏ đến
                    # file không tồn tại.
                    #
                    # Fix: nếu extension != ".wav" mà file vẫn tồn tại → chỉ
                    # warning + GIỮ NGUYÊN extension thật của file trên disk.
                    if not audio_path.exists():
                        raise RuntimeError(
                            f"yt-dlp reported filename={filename} nhưng file "
                            f"không tồn tại trên disk ({audio_dir}). "
                            f"Có thể postprocess bị kill giữa chừng "
                            f"(SIGUSR1 audio-slow-rotate / OOM / crash). "
                            f"File raw có thể còn ở audio_dir.")
                    if audio_path.suffix != ".wav":
                        print(f"  [WARN] Audio postprocess chưa xong: "
                              f"file = {audio_path.name} (expected .wav). "
                              f"File sẽ được rename giữ nguyên extension "
                              f"{audio_path.suffix}.",
                              flush=True)
                        print(f"         Nếu downstream pipeline cần .wav, "
                              f"hãy chạy --force-redownload.",
                              flush=True)
                    info_cache = {
                        "subtitles": info.get("subtitles") or {},
                        "automatic_captions": info.get("automatic_captions") or {},
                    }
                    # v5: Report kết quả cho AudioIPController (đo tốc độ tổng)
                    bytes_dl = 0
                    if audio_path and audio_path.exists():
                        try:
                            bytes_dl = audio_path.stat().st_size
                        except OSError:
                            pass
                    if bytes_dl <= 0:
                        bytes_dl = _dl_state.get("downloaded_bytes", _dl_state["bytes_dl_max"])
                    # BUGFIX: luôn dùng elapsed thực tế từ t_start, KHÔNG dùng elapsed_download từ progress hook
                    # (hook có thể không set nó nếu status="finished" không trigger sau postprocessing).
                    # Nếu dùng 0.001s: speed sẽ GIGANTIC, luôn vượt ngưỡng → không bao giờ đổi IP!
                    elapsed_dl = max(time.time() - _dl_state["t_start"], 0.001)
                    self._audio_ip_ctl.on_download_complete(
                        bytes_dl=bytes_dl, elapsed_s=elapsed_dl, ok=True,
                    )
                    break
                except Exception as dl_err:
                    err_str_dl = str(dl_err)
                    print(f"    [audio-ip] Download ERROR (attempt {dl_attempt}/{dl_retries}): {err_str_dl}", flush=True)

                    # v7: Detect HTTP 500 + Read timed out trong exception message → cycle IP
                    is_http_500 = (
                        "HTTP Error 500" in err_str_dl
                        or "HTTP Error 503" in err_str_dl
                        or "Internal Server Error" in err_str_dl
                        or "Read timed out" in err_str_dl           # <-- MỚI: timeout
                        or "HTTPSConnectionPool" in err_str_dl      # <-- MỚI: timeout
                        or "ConnectionTimeout" in err_str_dl        # <-- MỚI: connect timeout
                        or "Connection reset" in err_str_dl         # <-- MỚI: reset
                        or "Connection aborted" in err_str_dl       # <-- MỚI: aborted
                        or "ConnectionRefusedError" in err_str_dl   # <-- MỚI: refused
                    )
                    # Nếu là timeout → tăng fragment_500_count (để trigger cycle IP)
                    if ("Read timed out" in err_str_dl
                        or "HTTPSConnectionPool" in err_str_dl
                        or "ConnectionTimeout" in err_str_dl):
                        # Trigger on_fragment_500 để tăng count + có thể trigger cycle IP
                        try:
                            self._http500_detector.on_fragment_500(
                                frag_idx=-1,  # timeout không có frag idx
                                total_frags=0,
                            )
                        except Exception:
                            pass
                    http_500_count = self._http500_detector.fragment_500_count()
                    print(f"    [v7-detector] HTTP 500 count = {http_500_count}, "
                          f"is_http_500={is_http_500}", flush=True)

                    # v5: Report fail cho AudioIPController (để nó quyết
                    # định state tiếp theo dựa trên speed/fail).
                    bytes_dl_at_fail = _dl_state.get("bytes_dl_max", 0)
                    elapsed_at_fail = max(
                        time.time() - _dl_state["t_start"], 0.001)
                    self._audio_ip_ctl.on_download_complete(
                        bytes_dl=bytes_dl_at_fail,
                        elapsed_s=elapsed_at_fail,
                        ok=False,
                    )

                    # v7: Nếu là HTTP 500 → KHÔNG retry ngay, mà cycle IP
                    # trước rồi retry. Lý do: nếu IP bị rate-limit, retry
                    # trên cùng IP sẽ tiếp tục fail.
                    if is_http_500 and dl_attempt < dl_retries:
                        print(f"      [v7] HTTP 500 detected → đã cycle IP qua "
                              f"AudioIPController. Sleep {3 * dl_attempt}s rồi retry...",
                              flush=True)
                        time.sleep(3 * dl_attempt)
                        continue

                    if self._is_youtube_blocked_error(dl_err) and dl_attempt < dl_retries:
                        # v5: KHÔNG gọi force_rotate thủ công — để
                        # AudioIPController.on_download_complete() đã xử lý
                        # state ở trên. Chỉ sleep rồi retry.
                        print(f"      (retrying after {3 * dl_attempt}s...)", flush=True)
                        time.sleep(3 * dl_attempt)
                        continue
                    raise
            if info is None:
                raise RuntimeError("Download failed after all retries")

            # Rename theo title
            target_ext = audio_path.suffix if audio_path and audio_path.exists() else ".wav"
            target_filename_new = f"{target_name}{target_ext}"
            target_path = audio_dir / target_filename_new
            if audio_path and audio_path.exists() and audio_path != target_path:
                if target_path.exists():
                    target_path = audio_dir / f"{target_name}_{video.video_id}{target_ext}"
                try:
                    audio_path.rename(target_path)
                    audio_path = target_path
                except Exception:
                    pass
            # v6 BUG #3 FIX: Sau khi rename xong, cleanup orphan .part/.ytdl
            # của video_id cũ. Ví dụ: rename ABC123.webm → Ten_video.webm, các
            # file ABC123.mp4.part, ABC123.mp4.ytdl, ABC123.mp4.part-Frag*.part
            # bị bỏ quên (chiếm disk, không bao giờ được dùng lại).
            for orphan in audio_dir.glob(f"{video.video_id}.*"):
                if orphan == audio_path:
                    continue
                if orphan.suffix.lower() in (".part", ".ytdl") or ".part-" in orphan.name:
                    try:
                        sz_kb = orphan.stat().st_size // 1024
                        print(f"  [CLEANUP] Xóa orphan sau rename: {orphan.name} "
                              f"({sz_kb}KB)", flush=True)
                        orphan.unlink()
                    except Exception:
                        pass
            audio_filename = audio_path.name if audio_path and audio_path.exists() else f"{target_name}.wav"
            # Update JSON path sau khi rename (extension có thể khác)
            new_json_stem = Path(audio_filename).stem
            new_json_path = transcriptions_dir / f"{new_json_stem}_transcription.json"
            video.audio_filename = audio_filename
            audio_downloaded_at = datetime.now().isoformat()

            # Xóa file gốc (.webm/.m4a/...) chỉ giữ .wav
            for leftover_ext in [".webm", ".m4a", ".mp4", ".opus", ".ogg"]:
                leftover = (audio_dir / video.video_id).with_suffix(leftover_ext)
                if leftover.exists() and leftover != audio_path:
                    try:
                        leftover.unlink()
                    except Exception:
                        pass

            # === audio-only mode: skip transcribe + save JSON ===
            if audio_only:
                print(f"  [AUDIO-ONLY] downloaded ({audio_filename}), "
                      f"skip transcribe + save JSON (--audio-only)")
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | AUDIO-ONLY DONE "
                     f"(audio: {audio_filename})", also_print=False)
                results.append({
                    "video_id": video.video_id, "title": video.title,
                    "status": "audio_downloaded",
                    "audio_filename": audio_filename,
                    "transcription_filename": None,
                    "transcript_language": None,
                    "transcript_is_auto": None,
                    "transcript_source": "audio_only_mode",
                    "audio_downloaded_at": audio_downloaded_at,
                })
                continue

            # === Transcribe ===
            result, status = self.transcribe_with_youtube(
                video_id=video.video_id, audio_path=audio_path,
                lang=["vi", "en"],
                max_sentence_duration=max_sentence_duration,
                min_sentence_words=min_sentence_words,
                info_cached=info_cache if info_cache else None,
                attempt=1,  # transcript_rotator riêng
            )
            if result:
                self._save_transcription(
                    output_path=new_json_path, segments=result["segments"],
                    video=video, audio_duration=result["audio_duration"],
                    audio_filename=audio_filename,
                    audio_downloaded_at=audio_downloaded_at,
                    extra_metadata={
                        "transcript_language": result.get("transcript_language", ""),
                        "transcript_is_auto": result.get("transcript_is_auto", False),
                        "transcript_source": result.get("transcript_source", ""),
                        "detected_languages": result.get("detected_languages", []),
                    },
                )
                results.append({
                    "video_id": video.video_id, "title": video.title,
                    "status": "success", "audio_filename": audio_filename,
                    "transcription_filename": new_json_path.name,
                    "transcript_language": result.get("transcript_language", ""),
                    "transcript_is_auto": result.get("transcript_is_auto", False),
                    "transcript_source": result.get("transcript_source", ""),
                    "audio_downloaded_at": audio_downloaded_at,
                    "transcribed_at": datetime.now().isoformat(),
                })
                print(f"  Done ({len(result['segments'])} segments, "
                      f"lang={result.get('transcript_language')}, "
                      f"auto={result.get('transcript_is_auto')})")
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | DONE "
                     f"({len(result['segments'])} seg, lang={result.get('transcript_language')}, "
                     f"audio: {audio_filename})", also_print=False)
            else:
                # v18: Chỉ mark .no_transcript khi THẬT SỰ không có sub.
                # Network/tunnel fail → KHÔNG mark, để run sau retry.
                if status == "no_subs":
                    results.append({
                        "video_id": video.video_id, "title": video.title,
                        "status": "transcript_unavailable",
                        "audio_filename": audio_filename,
                        "audio_downloaded_at": audio_downloaded_at,
                    })
                    print("  No YouTube transcript available (no Vi subs)")
                    _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | NO TRANSCRIPT "
                         f"(audio: {audio_filename})", also_print=False)
                    YouTubeResearcher._mark_no_transcript(video.video_id, transcriptions_dir)
                else:
                    results.append({
                        "video_id": video.video_id, "title": video.title,
                        "status": "transcript_error",
                        "audio_filename": audio_filename,
                        "audio_downloaded_at": audio_downloaded_at,
                        "error": f"network/tunnel fail: {status}",
                    })
                    print(f"  Transcript failed (network/tunnel, status={status}) — "
                          f"KHÔNG mark, sẽ retry run sau")
                    _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | TRANSCRIPT FAILED "
                         f"(status={status}, audio: {audio_filename})", also_print=False)
        except Exception as e:
            err_str = str(e)
            # v13.1: Phân biệt LIVE_NOT_STARTED với download fail thông thường
            # - LIVE_NOT_STARTED: marker đã được ghi ở inner block, không phải lỗi IP
            # - download_failed: lỗi IP/network thực sự, retry ở run sau có thể khác
            if "LIVE_NOT_STARTED" in err_str:
                print(f"  [SKIP-LIVE-NOT-STARTED] {err_str}")
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | SKIP (live not started)",
                     also_print=False)
                results.append({
                    "video_id": video.video_id, "title": video.title,
                    "status": "live_unavailable",
                    "audio_filename": None,
                    "audio_downloaded_at": None,
                    "skip_reason": "live_not_started",
                })
                # v13.1: Reset IP controller state để video sau dùng REAL
                try:
                    self._audio_ip_ctl.reset()
                except Exception:
                    pass
                continue
            print(f"  Download failed: {e}")
            _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | DOWNLOAD FAILED: {e}")
            results.append({
                "video_id": video.video_id, "title": video.title,
                "status": "download_failed",
                "audio_filename": f"{target_name}.wav",
                "audio_downloaded_at": datetime.now().isoformat(),
                "error": str(e),
            })
            continue

    success = sum(1 for r in results if r.get("status") in ("success", "skipped"))
    failed = [r for r in results if r.get("status") not in ("success", "skipped")]
    _log(f"\nPipeline channel: {success} success/skipped, {len(failed)} failed "
         f"(tong: {total})")
    if failed:
        for r in failed:
            _log(f"  - [{r.get('status')}] {r.get('video_id')} | "
                 f"{r.get('title', '')[:50]} | {r.get('error', '')}", also_print=False)
    return {"total": total, "success": success, "results": results}


# Bind pipeline method to class
YouTubeResearcher.process_videos_pipeline = _process_videos_pipeline


# ================= MAIN =================
def main():
    args = parse_args()

    # v17: Export BUCKET_B_WORKERS từ CLI arg để code Bucket B đọc qua os.environ
    if hasattr(args, 'bucket_b_workers') and args.bucket_b_workers:
        os.environ.setdefault("BUCKET_B_WORKERS", str(args.bucket_b_workers))

    print("=" * 80)
    print("YOUTUBE AUDIO + SUBS RESUMABLE — VPN BẮT BUỘC")
    print("=" * 80)

    # Instance ID
    # v11: Declare INSTANCE_ID as module-level global (khi gọi main() lần đầu)
    # để các module khác (atexit handler, error fallback) có thể truy cập.
    global INSTANCE_ID
    INSTANCE_ID = args.instance_id or f"pid{os.getpid()}_t{int(time.time())}"
    print(f"[Multi-instance] Instance ID: {INSTANCE_ID}")

    # v11→v14: LUÔN đăng ký atexit handler để cleanup tunnel khi thoát (Ctrl+C / SIGTERM / exit).
    # Không cần --cleanup-on-exit nữa.
    # Lưu ý: atexit KHÔNG chạy khi process bị kill -9 (SIGKILL).
    import atexit as _atexit

    def _v11_cleanup_tunnels():
        """atexit handler: kill tunnel + cleanup policy routes khi process thoát."""
        try:
            if not INSTANCE_ID:
                print(f"\n[v15-cleanup] INSTANCE_ID chưa set → skip cleanup",
                      flush=True)
                return
            print(f"\n[v15-cleanup] Auto-cleanup → kill tunnel + flush policy routes "
                  f"của instance={INSTANCE_ID}", flush=True)
            killed = kill_tunnel_by_instance(INSTANCE_ID)
            print(f"[v15-cleanup] Killed {killed} tunnel(s)", flush=True)

            # v15: Chỉ cleanup table mà instance NÀY đã dùng (không xóa table instance khác)
            import subprocess as _sp_cleanup
            _my_tables = list(IsolatedVPNRotator._used_table_ids)
            for table_id in _my_tables:
                _sp_cleanup.run(["sudo", "-n", "ip", "rule", "del", "table", str(table_id)],
                                capture_output=True, timeout=3)
                _sp_cleanup.run(["sudo", "-n", "ip", "route", "flush", "table", str(table_id)],
                                capture_output=True, timeout=3)
            if _my_tables:
                print(f"[v15-cleanup] Flushed policy routing tables {_my_tables}", flush=True)
            else:
                print(f"[v15-cleanup] No policy tables to flush", flush=True)
        except Exception as e:
            print(f"[v15-cleanup] error (ignored): {e}", flush=True)

    _atexit.register(_v11_cleanup_tunnels)
    print(f"[v15-cleanup] Registered atexit handler cho instance={INSTANCE_ID}")

    cache_root = Path(args.cache_dir) if args.cache_dir else (
        Path(__file__).parent / f".cache_{INSTANCE_ID}")
    cache_root.mkdir(parents=True, exist_ok=True)

    # v6: Load YouTubeKeyRotator từ env (rotate API key khi quota exceeded).
    # Phase 1 (playlistItems.list) cần rotator, KHÔNG dùng single api_key.
    key_rotator = _youtube_key_rotator_from_env()
    if key_rotator is None:
        print("WARN: YOUTUBE_API_KEY không có trong .env → Phase 1 sẽ FAIL.",
              file=sys.stderr)
        print("      Cần set YOUTUBE_API_KEY (primary) hoặc YOUTUBE_API_KEY_1.._7.",
              file=sys.stderr)
        # Fallback: dùng env var YOUTUBE_API_KEY nếu có
        single_key = os.environ.get("YOUTUBE_API_KEY", "").strip()
        if single_key:
            key_rotator = YouTubeKeyRotator([single_key])
    if key_rotator is not None:
        print(f"[v6] Loaded YouTubeKeyRotator với {len(key_rotator)} API key(s)")
        youtube_key = key_rotator.current_key() or "ytdlp"
    else:
        youtube_key = "ytdlp"

    # === 3 VPN rotator TÁCH BIỆT cho 3 nhóm việc ===
    # - metadata_rotator  : rotate theo --vpn-rotate-every, KHÔNG cycle (real_ip_cycle=0)
    # - audio_rotator     : cycle theo --vpn-real-ip-cycle, rotate_every=0
    # - transcript_rotator: rotate theo --vpn-rotate-every, KHÔNG cycle (real_ip_cycle=0)
    # Mỗi rotator có instance_id riêng → log file, PID file, openvpn process
    # hoàn toàn độc lập → 3 nhóm chạy SONG SONG không xung đột.
    metadata_rotator = None
    audio_rotator = None
    transcript_rotator = None

    # v15: mỗi rotator dùng tunnel_device riêng (max 15 chars cho Linux interface name)
    _dev_prefix = INSTANCE_ID[:8] if INSTANCE_ID else "v15"

    try:
        metadata_rotator = get_isolated_vpn_rotator_from_config(
            instance_id=f"{INSTANCE_ID}_meta",
            rotate_every=args.vpn_rotate_every,
            strategy=args.vpn_strategy,
            real_ip_cycle=0,  # TẮT cycle cho metadata
            tunnel_device=f"tun_{_dev_prefix}_m",
        )
        audio_rotator = get_isolated_vpn_rotator_from_config(
            instance_id=f"{INSTANCE_ID}_audio",
            rotate_every=0,  # để cycle điều khiển
            strategy=args.vpn_strategy,
            real_ip_cycle=args.vpn_real_ip_cycle,
            tunnel_device=f"tun_{_dev_prefix}_a",
        )
        transcript_rotator = get_isolated_vpn_rotator_from_config(
            instance_id=f"{INSTANCE_ID}_subs",
            rotate_every=0,  # v17: TẮT auto-rotate tầng thấp, để transcribe_with_youtube kiểm soát
            strategy=args.vpn_strategy,
            real_ip_cycle=0,  # TẮT cycle cho transcript
            tunnel_device=f"tun_{_dev_prefix}_s",
        )
    except (ImportError, Exception) as e:
        print(f"FATAL: Lỗi khởi tạo IsolatedVPNRotator ({e}). "
              f"Không fallback về IP thật.", file=sys.stderr)
        sys.exit(1)

    if metadata_rotator is None or audio_rotator is None or transcript_rotator is None:
        print("FATAL: Không tìm thấy file .ovpn trong ./proton_config/.\n"
              "       Bản này BẮT BUỘC phải có ProtonVPN config.\n"
              "       Không fallback về IP thật.", file=sys.stderr)
        sys.exit(1)

    print(f"\n=== 3 VPN ROTATOR ĐỘC LẬP ===")
    print(f"  • metadata_rotator  : instance '{INSTANCE_ID}_meta', "
          f"rotate_every={args.vpn_rotate_every}, real_ip_cycle=0 (TẮT cycle)")
    print(f"    → log: /tmp/openvpn-proton-{INSTANCE_ID}_meta.log")
    print(f"  • audio_rotator     : instance '{INSTANCE_ID}_audio', "
          f"rotate_every=0, real_ip_cycle={args.vpn_real_ip_cycle} "
          f"({'TẮT cycle' if args.vpn_real_ip_cycle <= 0 else f'BẬT {args.vpn_real_ip_cycle-1} fake + 1 real'})")
    print(f"    → log: /tmp/openvpn-proton-{INSTANCE_ID}_audio.log")
    print(f"  • transcript_rotator: instance '{INSTANCE_ID}_subs', "
          f"rotate_every={args.vpn_rotate_every}, real_ip_cycle=0 (TẮT cycle)")
    print(f"    → log: /tmp/openvpn-proton-{INSTANCE_ID}_subs.log")
    print(f"  • KHÔNG dùng pkill → an toàn chạy song song nhiều instance")
    print(f"\n=== v5 AUDIO IP CONTROLLER ===")
    print(f"  • Lần đầu LUÔN real IP (default route, KHÔNG qua VPN)")
    print(f"  • Đo tốc độ liên tục qua yt-dlp progress hook")
    print(f"  • Tốc độ < {args.audio_min_speed_mbps} MB/s → đổi IP")
    print(f"  • Cycle {args.audio_fake_before_real + 1}: "
          f"{args.audio_fake_before_real} fake chậm → 1 real")
    print(f"  • Min bytes for speed: {args.audio_min_bytes_for_speed:,}")
    print(f"  • Min window: {args.audio_min_window_seconds}s")
    print(f"  • Speed avg window: {args.audio_speed_avg_window_seconds}s (rolling average)")
    print(f"\n=== v7 HTTP 500 + STALL DETECTOR ===")
    print(f"  • HTTP 500 threshold: {args.audio_500_threshold} fragments "
          f"→ cycle IP (REAL ↔ FAKE ↔ REAL)")
    print(f"  • Stall detection: bytes không tăng trong {args.audio_stall_seconds}s "
          f"→ cycle IP (fire mỗi {args.audio_stall_seconds}s khi vẫn stuck)")
    print(f"\n=== v10 FORCE REAL AFTER N FAILS ===")
    print(f"  • Force REAL sau {args.audio_force_real_after_fails} lần fail "
          f"liên tiếp ở FAKE (ok=False) [0=tắt]")
    print(f"  • Giảm fragment_retries: ∞ → 5 (tránh retry vô tận trên IP chết)")
    print(f"  • Exponential backoff: 2^n giây, max 30s")
    print(f"=== v13 MID-DOWNLOAD SLOW-SPEED ROTATION (SmartDownloader) ===")
    print(f"  • Slow-speed threshold: {args.audio_slow_speed_kbps} KB/s")
    print(f"  • Window: {args.audio_slow_window_seconds}s")
    print(f"  • Max {args.audio_max_rotate_per_video} lần rotate do slow-speed/video")
    print(f"  • SmartDownloader force_rotate khi avg speed < threshold trong window")
    if args.audio_slow_speed_kbps <= 0:
        print(f"  ⚠️  audio_slow_speed_kbps <= 0 → TẮT mid-download slow-speed rotation")

    # Run logger
    output_root = Path(args.output)
    log_dir = output_root / "logs"
    log_path = log_dir / f"crawl_{INSTANCE_ID}.log"
    run_logger = RunLogger(log_path, script_path=str(Path(__file__).absolute()))

    # Load channels
    if args.channel:
        channels = [args.channel]
    else:
        channels = load_channels_from_file(args.channels_file)
    if not channels:
        print("Không có kênh nào để xử lý.")
        sys.exit(1)

    run_logger.log_batch_start(args.channels_file, len(channels),
                                command=" ".join(sys.argv))
    all_results = []
    for i, ch_url in enumerate(channels, 1):
        try:
            res = process_one_channel(
                ch_url, youtube_key=youtube_key, output_root=args.output,
                max_results=args.max_results, max_fetch=args.max_fetch,
                order=args.order, audio_format=args.audio_format,
                skip_existing=args.skip_existing,
                force_retranscribe=args.force_retranscribe,
                force_redownload=args.force_redownload,
                audio_only=args.audio_only,
                max_batches=args.max_batches, fetch_delay=args.fetch_delay,
                proxy_rotator=metadata_rotator,
                audio_proxy_rotator=audio_rotator,
                transcript_proxy_rotator=transcript_rotator,
                key_rotator=key_rotator,
                video_delay=args.video_delay,
                socket_timeout=args.socket_timeout, max_retries=args.max_retries,
                max_sentence_duration=args.max_sentence_duration,
                min_sentence_words=args.min_sentence_words,
                run_logger=run_logger, channel_idx=i, total_channels=len(channels),
                metadata_only=args.metadata_only,
                rebuild_from_transcripts=args.rebuild_from_transcripts,
                audio_min_speed_mbps=args.audio_min_speed_mbps,
                audio_fake_before_real=args.audio_fake_before_real,
                audio_min_bytes_for_speed=args.audio_min_bytes_for_speed,
                audio_min_window_seconds=args.audio_min_window_seconds,
                audio_speed_avg_window_seconds=args.audio_speed_avg_window_seconds,
                audio_500_threshold=args.audio_500_threshold,         # v7
                audio_stall_seconds=args.audio_stall_seconds,         # v7
                audio_force_real_after_fails=args.audio_force_real_after_fails,  # v10
                # v13: mid-download slow-speed rotation
                audio_slow_speed_kbps=args.audio_slow_speed_kbps,
                audio_slow_window_seconds=args.audio_slow_window_seconds,
                audio_max_rotate_per_video=args.audio_max_rotate_per_video,
                # v14: VIETSUB OPTIMIZATION
                vi_sub_priority=args.vi_sub_priority,
                no_marker_ttl_days=args.no_marker_ttl_days,
                respect_no_transcript_marker=args.respect_no_transcript_marker,
                retry_no_transcript=args.retry_no_transcript,
                retry_no_transcript_force=args.retry_no_transcript_force,
                no_api_fallback=args.no_api_fallback,
                api_fallback_langs=args.api_fallback_langs,
                player_client_rotate=not args.no_player_client_rotate,
                player_clients=args.player_clients,
            )
        except Exception as e:
            print(f"  ERROR processing {ch_url}: {e}")
            run_logger.log(f"ERROR: {ch_url}: {e}")
            res = {"channel": ch_url, "status": "error", "error": str(e)}
        all_results.append(res)
        # Ghi summary giữa các kênh (để crash giữa chừng vẫn còn dữ liệu)
        summary_path = output_root / f"_multi_channel_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_path, "w", encoding="utf_8") as f:
            json.dump({"results": all_results}, f, ensure_ascii=False, indent=2)

    success_n = sum(1 for r in all_results if r.get("status") in (
        "success", "rebuild_success", "metadata_only_success"))
    failed_n = len(all_results) - success_n
    run_logger.log_batch_end(len(channels), success_n, failed_n, all_results)

    # === Cleanup 3 VPN tunnels (độc lập, không ảnh hưởng nhau) ===
    print(f"\n[Cleanup] Disconnect 3 VPN tunnels...")
    for r, name in [(metadata_rotator, "meta"),
                    (audio_rotator, "audio"),
                    (transcript_rotator, "subs")]:
        if r is None:
            continue
        try:
            r.disconnect()
            print(f"  ✓ Tunnel '{INSTANCE_ID}_{name}' đã đóng")
        except Exception as e:
            print(f"  ⚠️  Lỗi disconnect {name}: {e}")


if __name__ == "__main__":
    main()
