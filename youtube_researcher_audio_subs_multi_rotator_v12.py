#!/usr/bin/env python3
"""
YouTube Researcher -- AUDIO + YOUTUBE SUBS (MULTI-ROTATOR) -- v11
================================================================================

v11 = v10 + Per-instance tunnel kill (mỗi tiến trình IP riêng, không giết
nhầm tunnel instance khác khi cycle IP).

Lịch sử:
  v10: SmartDownloader + 6 cải tiến cycle IP khi IP bị Google rate-limit.
  v11: Per-instance tunnel kill — fix vấn đề `kill_all_vpn_tunnels()` ở v10
       kill TẤT CẢ openvpn của user (kể cả instance khác).

VẤN ĐỀ CỦA V10 (khi chạy multi-instance):
  - `kill_all_vpn_tunnels()` ở đầu mỗi video (v10 line 4211) kill TẤT CẢ
    openvpn process của user `hientran`, kể cả tunnel của instance khác
    đang download. Nếu chạy `run_crawl_v10.sh` + `run_crawl_1.sh` cùng lúc
    → instance A cycle IP sẽ giết tunnel của instance B.
  - `kill_all_vpn_tunnels()` ở đầu shell script (line 168) tương tự.

V11 FIX:
  1) Pipeline (`_process_videos_pipeline`): thay `kill_all_vpn_tunnels()` bằng
     `audio_rotator._disconnect()` — chỉ kill PID của audio_rotator của
     instance này. KHÔNG động vào metadata_rotator, transcript_rotator, VÀ
     tunnel của instance khác.
  2) Thêm `kill_tunnel_by_instance(instance_id_prefix)`: kill CHỈ các tunnel
     có PID file prefix `/tmp/openvpn-proton-{instance_id_prefix}.*`. An toàn
     cho multi-instance cùng user.
  3) Thêm CLI flag `--cleanup-on-exit` (mặc định TẮT): khi instance thoát
     (atexit), auto gọi `kill_tunnel_by_instance(INSTANCE_ID)`.
  4) Shell script `run_crawl_v11.sh`: bỏ dòng kill_all ở đầu, thêm
     `--cleanup-on-exit` nếu user muốn.

GIỮ NGUYÊN TỪ V10:
  - SmartDownloader + 11 patterns + catch-all
  - HTTP500Detector stall fire mỗi 30s
  - AudioIPController force_real_after_2_fake_fails
  - Reset HTTP500Detector mỗi dl_attempt
  - Log [reason=...]
  - AudioIPController.force_rotate() đã per-rotator rồi (gọi _disconnect()
    trên rotator đó), KHÔNG cần sửa thêm.

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
    python youtube_researcher_audio_subs_multi_rotator_v11.py \\
        --channels-file ./channels_audio/channels_khoa_hoc_2.txt \\
        --output ./youtube_dataset_resumable \\
        --use-vpn --vpn-isolated \\
        --instance-id inst1 \\
        --video-delay 5 --skip-existing

    # Chạy 2 instances cùng lúc (MỖI CÁI có tunnel riêng)
    python ..._v11.py --channels-file ./channels_khoa_hoc_2.txt --instance-id inst1 --cleanup-on-exit &
    python ..._v11.py --channels-file ./channels_khoa_hoc_1.txt --instance-id inst2 --cleanup-on-exit &

    # Cleanup thủ công tunnel của 1 instance
    python -c "from youtube_researcher_audio_subs_multi_rotator_v11 import \\
        kill_tunnel_by_instance; kill_tunnel_by_instance('inst1')"

    # Rebuild CSV/summary từ JSON có sẵn (không gọi API, không tải audio)
    python youtube_researcher_audio_subs_multi_rotator_v11.py \\
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


# === v12: Smart downloader with stuck-IP detection + mid-download slow-speed rotation ===
# v12: clone v11 + thêm MidDownloadRotate exception để progress_hook có thể
# báo "đang tải giữa chừng nhưng avg speed < ngưỡng trong cửa sổ N giây"
# → outer loop catch → force_rotate NGAY, không đợi DownloadError.
V12_SMART_AVAILABLE = False
try:
    from v12_smart_downloader import (
        get_smart_downloader, classify_error, SmartDownloader,
        MidDownloadRotate,
    )
    V12_SMART_AVAILABLE = True
except Exception as _v12e:
    # Fallback: thử v11 nếu v12 chưa có (backward-compat)
    try:
        from v11_smart_downloader import (
            get_smart_downloader, classify_error, SmartDownloader,
        )
        MidDownloadRotate = None
        V12_SMART_AVAILABLE = True
        print(f"  [v12-warn] Dùng v11_smart_downloader (không có MidDownloadRotate). "
              f"v12 cần file v12_smart_downloader.py để bật slow-speed mid-download rotation.",
              flush=True)
    except Exception as _v11e:
        print(f"  [v12-warn] SmartDownloader không khả dụng: v12err={_v12e}, v11err={_v11e}",
              flush=True)
        MidDownloadRotate = None

load_dotenv(Path(__file__).parent / ".env")

# v11: Module-level global cho INSTANCE_ID, dùng bởi main() và atexit handler.
# Khởi tạo = None, sẽ được set trong main() khi parse args.
INSTANCE_ID: Optional[str] = None

# ================= VPN ROTATOR (BẮT BUỘC - OpenVPN) =================
# Chỉ dùng ProtonVPN OpenVPN tunnel để fake IP.
# - 5 server free (CA/MX/NL/SG/US/JP), rotate random theo --vpn-strategy
# - Auth: ./proton_config/auth.txt (chmod 600)
# - Cần: sudo setcap cap_net_admin+ep /usr/sbin/openvpn (chạy 1 lần)
try:
    from vpn_rotator_v4 import (
        get_vpn_rotator_from_config,
        VPNRotator,
        is_proxy_dead_error,
    )
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent))
    from vpn_rotator_v4 import (  # type: ignore
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

    Fix 3 vấn đề của VPNRotator gốc khi chạy nhiều process cùng lúc:
      1. OPENVPN_LOG constant → 2 instance ghi đè log của nhau.
      2. pkill fallback trong _disconnect() → kill nhầm tunnel instance khác.
      3. pgrep trong _is_connected() → thấy tunnel instance khác → tưởng mình đã connected.
    """

    def __init__(self, instance_id: str, **vpn_kwargs):
        self.instance_id = instance_id
        self._instance_log = f"/tmp/openvpn-proton-{instance_id}.log"
        self._instance_pid_prefix = f"/tmp/openvpn-proton-{instance_id}.pid"

        import vpn_rotator as _vr_mod
        _vr_mod.OPENVPN_LOG = self._instance_log
        self._vr_mod = _vr_mod

        self._inner = VPNRotator(**vpn_kwargs)
        self._patch_connect_server_pid()

    def _patch_connect_server_pid(self):
        instance_pid_prefix = self._instance_pid_prefix
        original_connect = self._inner._connect_server

        def _patched(idx: int, retry: int = 0) -> bool:
            import subprocess as _sp
            import time as _t
            import logging as _log

            ovpn = self._inner._ovpn_files[idx]
            _log.getLogger("vpn_rotator").info(
                "VPN[isolated=%s]: connecting to %s (attempt %d)",
                self.instance_id, ovpn.name, retry + 1,
            )
            self._inner._disconnect()
            prepared_config = self._inner._prepare_config(ovpn)
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

            for i in range(self._vr_mod.CONNECT_TIMEOUT):
                _t.sleep(1)
                if self._inner._has_tun0():
                    ip = self._inner._get_current_ip()
                    real_ip = self._inner._last_known_real_ip
                    if ip and ip != real_ip:
                        self._inner._current_ip = ip
                        self._inner._current_idx = idx
                        self._inner._usage_count[idx] = self._inner._usage_count.get(idx, 0) + 1
                        self._inner._request_count = 0
                        self._inner._last_connect_time = _t.time()
                        return True
                elif i >= 3:
                    ip = self._inner._get_current_ip()
                    real_ip = self._inner._last_known_real_ip
                    if ip and real_ip and ip != real_ip:
                        self._inner._current_ip = ip
                        self._inner._current_idx = idx
                        self._inner._usage_count[idx] = self._inner._usage_count.get(idx, 0) + 1
                        self._inner._request_count = 0
                        self._inner._last_connect_time = _t.time()
                        return True
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
        if name in ("_inner", "instance_id", "_instance_log", "_instance_pid_prefix"):
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
) -> Optional["IsolatedVPNRotator"]:
    try:
        return IsolatedVPNRotator(
            instance_id=instance_id,
            config_dir=Path(config_dir) if config_dir else None,
            rotate_every=rotate_every,
            strategy=strategy,
            real_ip_cycle=real_ip_cycle,
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
            Đánh dấu "đang chậm" nếu rolling average tốc độ < min.
            KHÔNG trigger rotate ngay; chỉ set cờ để on_download_complete xử lý.
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
        speed_avg_window_seconds: float = 10.0,
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
        self._slow_flag = False  # set bởi on_chunk_progress, consume bởi on_download_complete

        # === v5.4: Fixed window baseline (5s mỗi window, skip 5s handshake) ===
        # Lưu bytes lúc bắt đầu window hiện tại để tính speed khi window kết thúc.
        self._window_baseline_bytes = 0
        self._window_baseline_idx = -1
        # v5.1 (deprecated): rolling window samples - không dùng nữa
        self._speed_samples: list = []

    # ----- Public API -----

    def on_download_start(self) -> "str | None":
        """Trả proxy URL cho audio download hiện tại.

        Returns:
            None: dùng IP thật (default route). Caller KHÔNG set proxy
                  trong ydl_opts, KHÔNG acquire tunnel guard. Traffic sẽ
                  đi qua default route = IP thật của máy.
            str:  dùng IP fake qua VPN tunnel. Caller set proxy hoặc
                  acquire tunnel guard. Thường trả None ở VPNRotator
                  hiện tại (vì OpenVPN là system-level tunnel) → vẫn
                  None cho yt-dlp, nhưng cần acquire guard để giữ tunnel.
        """
        if self._state == self.STATE_REAL:
            self._ensure_vpn_disconnected()
            self._total_real_uses += 1
            print(f"    [audio-ip] STATE=REAL (IP thật, no VPN tunnel)", flush=True)
            return None
        else:
            # FAKE: cần VPN tunnel active
            next_result = None
            if hasattr(self.audio_rotator, "next"):
                try:
                    next_result = self.audio_rotator.next()
                except Exception as e:
                    print(f"    [audio-ip] next() error (ignored): {e}", flush=True)
            self._total_fake_uses += 1
            try:
                cur_idx = getattr(self.audio_rotator, "_current_idx", None)
                cur_ip = getattr(self.audio_rotator, "_current_ip", None)
                ovpn_files = getattr(self.audio_rotator, "_ovpn_files", [])
                server_name = ovpn_files[cur_idx].name if (cur_idx is not None and cur_idx < len(ovpn_files)) else "?"
                print(f"    [audio-ip] STATE=FAKE (server={server_name}, ip={cur_ip})", flush=True)
            except Exception:
                print(f"    [audio-ip] STATE=FAKE", flush=True)
            return next_result

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

        # Nếu chunk-progress hook đã flag "slow" (rolling avg < 1.0 MB/s)
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
            print(f"    [audio-ip] ❌ REAL SLOW/FAIL: {bytes_dl//1024}KB in {elapsed_s:.1f}s "
                  f"= {speed_mbps:.2f} MB/s (ok={ok}, slow_flag={self._slow_flag}) "
                  f"→ NHẢY sang FAKE (test VPN) [reason=rotate_to_fake]", flush=True)
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
            print(f"    [audio-ip] ❌ FAKE FAIL {self._consecutive_fake_slow + 1}/{self.force_real_after_n_fake_fails} lần "
                  f"liên tiếp (ok=False): {bytes_dl//1024}KB in {elapsed_s:.1f}s "
                  f"= {speed_mbps:.2f} MB/s → CYCLE về REAL [reason=force_real_after_{self.force_real_after_n_fake_fails}_fake_fails]",
                  flush=True)
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
            print(f"    [audio-ip] ❌ FAKE SLOW {self._consecutive_fake_slow}/{self.fake_before_real} lần "
                  f"liên tiếp: {bytes_dl//1024}KB in {elapsed_s:.1f}s "
                  f"= {speed_mbps:.2f} MB/s → CYCLE về REAL (reset rate-limit) [reason=fake_before_real_reached]",
                  flush=True)
            self._state = self.STATE_REAL
            self._consecutive_fake_slow = 0
            self._slow_flag = False
            self._total_rotates += 1
            self._ensure_vpn_disconnected()
            return

        # Chưa đủ ngưỡng → force_rotate sang VPN server khác
        # (chỉ áp dụng khi speed chậm nhưng OK, hoặc fail lần đầu tiên)
        print(f"    [audio-ip] ❌ FAKE SLOW/FAIL {self._consecutive_fake_slow}/{self.fake_before_real}: "
              f"{bytes_dl//1024}KB in {elapsed_s:.1f}s = {speed_mbps:.2f} MB/s "
              f"→ force_rotate sang VPN server khác [reason=force_rotate_via_smart_dl]",
              flush=True)
        self._slow_flag = False
        self._total_rotates += 1
        try:
            if hasattr(self.audio_rotator, "force_rotate"):
                self.audio_rotator.force_rotate(f"audio-slow-{self._consecutive_fake_slow}")
                print(f"      [audio-ip] force_rotate done", flush=True)
        except Exception as e:
            print(f"      [audio-ip] force_rotate ERROR: {e}", flush=True)

    def on_chunk_progress(self, bytes_dl: int, elapsed_s: float, speed_bps: float):
        """Callback mid-download từ yt-dlp progress hook.

        v5.4 FIX: Fixed window logic (thay vì rolling window).
        Bỏ qua 5s đầu (handshake). Sau đó chia thành các window cố định 5s.
        MỖI window đánh giá tốc độ 1 LẦN:
          - window 1: (5, 10]s  → tính bytes trong 5s / 5s
          - window 2: (10, 15]s → tính bytes trong 5s / 5s
          - ...
        Nếu speed < threshold trong BẤT KỲ window nào → set _slow_flag
        để on_download_complete() xử lý.
        """
        if bytes_dl < self.min_bytes_for_speed:
            return

        # SKIP 5s đầu (handshake YouTube)
        if elapsed_s < 5.0:
            return

        # === Fixed window logic ===
        # window_size = self.speed_avg_window_seconds (mặc định 5s)
        # window_idx 0 = (5, 10]s, window_idx 1 = (10, 15]s, ...
        # Sau khi skip 5s đầu, elapsed - 5 là thời gian đo được.
        window_size = self.speed_avg_window_seconds  # mặc định 5s
        measure_elapsed = elapsed_s - 5.0
        window_idx = int(measure_elapsed / window_size)

        # Lần đầu tiên vào window mới → set baseline
        if not hasattr(self, "_window_baseline_bytes"):
            self._window_baseline_bytes = 0
            self._window_baseline_idx = -1

        # Nếu đã ở window này rồi (cùng idx) → skip
        if window_idx == self._window_baseline_idx:
            return

        # Vào window MỚI → tính speed của window VỪA KẾT THÚC
        # (window vừa kết thúc là window_idx - 1, có baseline_bytes = _window_baseline_bytes)
        if self._window_baseline_idx >= 0:
            # Có baseline từ window trước → tính
            prev_window_idx = window_idx - 1
            bytes_in_prev_window = bytes_dl - self._window_baseline_bytes
            # Chỉ đánh giá nếu đủ bytes
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
        """Reset slow state khi bắt đầu download mới."""
        self._slow_logged_this_dl = False
        self._recovered_logged_this_dl = False  # v5.3: reset recovered log flag
        # v5.2: loại bỏ rolling window samples (KHÔNG dùng nữa)
        self._slow_flag = False
        # v5.4: reset fixed window baseline (cho chunk progress hook)
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
        self._window_baseline_bytes = 0  # v5.4 fixed window baseline
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
FILTER_MIN_DURATION = 0      # 30 phút
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
                 audio_speed_avg_window_seconds: float = 10.0,
                 audio_500_threshold: int = 5,
                 audio_stall_seconds: float = 30.0,
                 audio_force_real_after_fails: int = 2,    # v10
                 # === v12: mid-download slow-speed rotation ===
                 audio_slow_speed_kbps: float = 50.0,
                 audio_slow_window_seconds: float = 30.0,
                 audio_max_rotate_per_video: int = 3):
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
        # === v12: Mid-download slow-speed rotation config ===
        # Lưu vào self để _audio_progress_hook đọc và raise MidDownloadRotate.
        self._v12_slow_speed_kbps = audio_slow_speed_kbps
        self._v12_slow_window_seconds = audio_slow_window_seconds
        self._v12_max_rotate_per_video = audio_max_rotate_per_video
        # === v10: Smart downloader for stuck-IP detection ===
        self._smart_dl = get_smart_downloader(
            ip_controller=getattr(self, '_audio_ip_ctl', None),
            audio_rotator=getattr(self, '_audio_rotator', None),
            http500_detector=self._http500_detector,
        ) if V12_SMART_AVAILABLE else None

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
    def _apply_auth_skip(ydl_opts: dict) -> dict:
        ydl_opts.setdefault("extractor_args", {})
        if "youtube" not in ydl_opts["extractor_args"]:
            ydl_opts["extractor_args"]["youtube"] = {}
        yt_args = ydl_opts["extractor_args"]["youtube"]
        if "skip" not in yt_args:
            yt_args["skip"] = []
        if "authcheck" not in yt_args["skip"]:
            yt_args["skip"].append("authcheck")
        if "player_client" not in yt_args:
            yt_args["player_client"] = ["web_safari", "web"]
        if "js_runtimes" not in ydl_opts:
            node_path = "/home/hientran/.nvm/versions/node/v24.15.0/bin/node"
            if not Path(node_path).exists():
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
            ydl_opts["extractor_args"]["youtubepot-bgutilhttp"]["base_url"] = [
                "http://127.0.0.1:4416"]
        return ydl_opts

    @staticmethod
    def _apply_timeouts(ydl_opts: dict, socket_timeout: int = 60) -> dict:
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
        """Tìm file audio đã có cho video. Skip file < min_size_bytes (corrupt)."""
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
        subdirs = sorted([d for d in root.iterdir() if d.is_dir()], reverse=True)
        for sub in subdirs:
            for name in candidates:
                p = sub / name
                if p.exists():
                    try:
                        if p.stat().st_size >= min_size_bytes:
                            return p
                    except OSError:
                        continue
        return None

    @staticmethod
    def _build_audio_index(audio_root, min_size_bytes: int = 50 * 1024) -> dict:
        """Build index {stem: Path} cho TẤT CẢ subfolder audio/, lấy file mới nhất.

        v6 fix:
          - CHỈ nhận file `.wav` (đã postprocess xong), KHÔNG nhận raw
            `.webm`/`.m4a`/`.mp4`/`.opus`/`.ogg`. Lý do: yt-dlp config
            (line ~4312-4316) dùng `FFmpegExtractAudio preferredcodec: wav`
            → audio HOÀN THIỆN cuối cùng LUÔN là `.wav`. File raw chỉ là
            intermediate. Nếu tồn tại mà KHÔNG có `.wav` tương ứng →
            postprocess đã bị KILL → cần re-download.
          - Verify WAV header + data integrity (không tin size >= 50KB).

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
        subdirs = sorted([d for d in root.iterdir() if d.is_dir()], reverse=True)
        for sub in subdirs:
            try:
                for f in sub.iterdir():
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
        """Build index {stem: Path} cho TẤT CẢ subfolder transcriptions/, lấy file mới nhất.

        Skip file JSON: size < min_size_bytes, parse fail, hoặc thiếu video_id/segments.
        Mặc định 100 bytes — JSON transcription thật > 1KB.
        """
        index: dict = {}
        if not transcriptions_root:
            return index
        root = Path(transcriptions_root)
        if not root.exists():
            return index
        suffix = "_transcription.json"
        subdirs = sorted([d for d in root.iterdir() if d.is_dir()], reverse=True)
        for sub in subdirs:
            try:
                for f in sub.iterdir():
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
                              socket_timeout: int = 60, fetch_delay: int = 5,
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
                if any(kw in err for kw in ["ssl", "timeout", "connection", "handshake"]):
                    time.sleep(3 * page_count)
                    continue
                raise

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

    # ================== yt-dlp subtitles downloader ==================
    def _get_youtube_transcript_via_ytdlp(self, video_id: str,
                                            proxy_url: Optional[str] = None,
                                            info_cached: Optional[dict] = None) -> dict | None:
        """
        Lấy phụ đề qua yt-dlp bằng 2 bước:
          Bước 1: extract_info lấy sub URLs
          Bước 2: download sub file → parse json3/vtt

        Engine DUY NHẤT để lấy transcript. Dùng cookies + yt-dlp player_client
        để bypass IP-block của YouTube.
        """
        try:
            import yt_dlp
        except ImportError:
            return None

        # === Bước 1: lấy info (sub URLs) ===
        info = None
        # FIX v2 Skip #1: Nếu info_cached là dict rỗng → Phase 2 đã xác nhận
        # video KHÔNG có subs → skip luôn, không gọi yt-dlp extract_info() lại.
        # An toàn vì Phase 2 chỉ set dict rỗng khi extract_info() thật sự thành
        # công (không phải do cookies/VPN fail).
        if info_cached is not None:
            has_subs = bool(info_cached.get("subtitles")) or bool(info_cached.get("automatic_captions"))
            if not has_subs:
                print(f"  [ytdlp-subs] SKIP: Phase 2 confirmed no subs for {video_id}")
                return None
            info = {
                "subtitles": info_cached.get("subtitles") or {},
                "automatic_captions": info_cached.get("automatic_captions") or {},
            }
            print(f"  [ytdlp-subs] using cached sub URLs (skip yt-dlp extract)")

        if info is None:
            ydl_opts = {
                "quiet": True, "no_warnings": True,
                "skip_download": True, "ignoreerrors": True,
                "js_runtimes": {"node": {}}, "age_limit": None,
            }
            self._apply_auth_skip(ydl_opts)
            self._apply_cookies(ydl_opts)
            self._apply_timeouts(ydl_opts, socket_timeout=60)
            if proxy_url:
                ydl_opts["proxy"] = proxy_url

            # Bound timeout 25s (kể cả khi socket_timeout 60s fail)
            import concurrent.futures
            try:
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(
                        lambda: yt_dlp.YoutubeDL(ydl_opts).extract_info(
                            f"https://www.youtube.com/watch?v={video_id}", download=False
                        )
                    )
                    try:
                        info = future.result(timeout=25)
                    except concurrent.futures.TimeoutError:
                        print(f"  [ytdlp-subs] extract_info timeout 25s, killing")
                        if proxy_url:
                            # Dùng transcript_rotator riêng để mark dead
                            self._mark_transcript_proxy_dead(proxy_url)
                        return None
            except Exception as e:
                err_str = str(e)
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
                return None

        if not info:
            print(f"  [ytdlp-subs] no info returned for {video_id}")
            return None

        subtitles = info.get("subtitles") or {}
        auto_captions = info.get("automatic_captions") or {}

        # Tìm key bắt đầu bằng 'vi' trong dict subtitles
        def _find_vi_lang(d):
            if not d:
                return None
            for k in d.keys():
                if k.lower().startswith("vi"):
                    return k
            return None

        # Thử manual trước, rồi auto
        vi_key = _find_vi_lang(subtitles)
        source_type = "manual"
        lang_code = vi_key or "vi"
        chosen = subtitles.get(vi_key) if vi_key else None

        if not chosen:
            vi_key = _find_vi_lang(auto_captions)
            source_type = "auto"
            lang_code = vi_key or "vi"
            chosen = auto_captions.get(vi_key) if vi_key else None

        if not chosen:
            all_langs = list(subtitles.keys()) + list(auto_captions.keys())
            print(f"  [ytdlp-subs] no Vi sub. Available: {all_langs[:10]}")
            return None

        # Tìm format json3 hoặc vtt
        sub_url = None
        sub_format = None
        for fmt in ["json3", "vtt", "ttml", "srv3", "srv2", "srv1"]:
            for entry in chosen:
                if entry.get("ext") == fmt or fmt in (entry.get("url") or ""):
                    sub_url = entry["url"]
                    sub_format = fmt
                    break
            if sub_url:
                break

        if not sub_url and chosen:
            sub_url = chosen[0].get("url")
            sub_format = chosen[0].get("ext", "vtt")

        if not sub_url:
            print(f"  [ytdlp-subs] no sub URL for {video_id}")
            return None

        print(f"  [ytdlp-subs] found {source_type} sub '{lang_code}' "
              f"format={sub_format}, downloading...")

        # === Bước 2: download sub file ===
        try:
            import requests
            proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
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
                    print(f"  [ytdlp-subs] warn: load cookies failed: {ce}")
            resp = requests.get(sub_url, proxies=proxies, cookies=cookies_dict,
                                timeout=(10, 20))
            if resp.status_code == 429:
                print(f"  [ytdlp-subs] HTTP 429 (rate limited)")
                if proxy_url:
                    self._mark_transcript_proxy_failed(proxy_url)
                return None
            if resp.status_code == 403:
                print(f"  [ytdlp-subs] HTTP 403 (forbidden)")
                if proxy_url:
                    self._mark_transcript_proxy_failed(proxy_url)
                return None
            resp.raise_for_status()
            content = resp.text
        except Exception as e:
            err_str = str(e)
            print(f"  [ytdlp-subs] download sub failed: {type(e).__name__}: {err_str[:200]}")
            # v4: captcha detection trong download sub
            if self._transcript_rotator and hasattr(self._transcript_rotator, 'is_captcha_error'):
                try:
                    if self._transcript_rotator.is_captcha_error(e):
                        print(f"  [ytdlp-subs] CAPTCHA DETECTED (download) -> force rotate")
                        self._transcript_rotator.increment_captcha_hit()
                        self._transcript_rotator.force_rotate("captcha-download")
                except Exception:
                    pass
            if proxy_url and is_proxy_dead_error(e):
                self._mark_transcript_proxy_dead(proxy_url)
            elif proxy_url:
                self._mark_transcript_proxy_failed(proxy_url)
            return None

        # === Bước 3: parse ===
        segs = []
        is_auto = (source_type == "auto")

        if sub_format == "json3":
            try:
                data = json.loads(content)
                segs = self._parse_json3_subtitle(data)
            except Exception as e:
                print(f"  [ytdlp-subs] json3 parse error: {e}")
        elif sub_format in ("vtt", "ttml", "srv1", "srv2", "srv3"):
            segs = self._parse_vtt_subtitle(content)

        if not segs:
            print(f"  [ytdlp-subs] parse returned 0 segments "
                  f"(format={sub_format}, len={len(content)} bytes)")
            return None

        return {
            "segments": segs,
            "language": lang_code,
            "is_auto": is_auto,
            "source": f"yt-dlp-{sub_format}-{source_type}",
        }

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
                                 attempt: int = 1) -> dict | None:
        """
        Lấy phụ đề sẵn có của YouTube theo cách của youtube_researcher_youtube_subs_multi_only_vpn.py.

        Priority:
          1. yt-dlp subtitles download (retry 2 lần với proxy khác từ transcript_rotator).
             KHÔNG còn fallback nào khác.

        Args:
            attempt: KHÔNG còn dùng (giữ để tương thích signature cũ).
                Retry logic đã được tích hợp bên trong (range(2)).
        """
        if lang is None:
            lang = ["vi"]

        print(f"  Fetching YouTube transcript via yt-dlp (langs={lang})...")

        # v4: Retry toi da 3 lan voi force_rotate moi khi fail
        # - Attempt 1: proxy binh thuong (hoac IP that neu _direct_blocked)
        # - Attempt 2: force_rotate (captcha/bot check) + proxy moi
        # - Attempt 3: them 1 lan force_rotate + proxy moi (last chance)
        result = None
        for a in range(1):
            if a == 0:
                yt_proxy = self._proxy_for_transcript_fallback()
            else:
                # v4: FORCE rotate truoc moi retry (de tranh dung server bi flag)
                if self._transcript_rotator:
                    try:
                        self._transcript_rotator.force_rotate(
                            f"transcript-fail-attempt-{a+1}")
                    except Exception:
                        pass
                yt_proxy = self._proxy_for_transcript_fallback()
            print(f"  [transcript-ytdlp] attempt {a+1}/3 via "
                  f"{self._short_proxy(yt_proxy) if yt_proxy else 'DIRECT'} "
                  f"(transcript_rotator={'ON' if self._transcript_rotator and self._transcript_rotator is not self._rotator else 'shared'})")
            cached = info_cached if a == 0 else None
            result = self._get_youtube_transcript_via_ytdlp(
                video_id, proxy_url=yt_proxy, info_cached=cached)
            if result:
                break

        if not result:
            print("  No YouTube transcript available (yt-dlp fail sau 2 attempts)")
            return None

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
            return None
        segments = self._merge_youtube_segments_to_sentences(
            raw_parsed, max_duration=max_sentence_duration,
            min_words=min_sentence_words)
        if not segments:
            return None
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
        }

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
    # === v12: Mid-download slow-speed rotation ===
    p.add_argument("--audio-slow-speed-kbps", type=float, default=50.0,
                   help="v12: Ngưỡng tốc độ tối thiểu (KB/s) trong cửa sổ "
                        "slow-window-seconds để coi là OK. Nếu rolling avg < "
                        "ngưỡng này TRONG KHI ĐANG TẢI (không phải chờ "
                        "DownloadError) → force_rotate IP NGAY và resume "
                        "file .part. Mặc định: 50.0 KB/s. Set 0 = tắt.")
    p.add_argument("--audio-slow-window-seconds", type=float, default=30.0,
                   help="v12: Cửa sổ thời gian (giây) để tính rolling avg "
                        "speed cho slow-speed detection. Nếu avg speed trong "
                        "cửa sổ này < audio-slow-speed-kbps → rotate. "
                        "Mặc định: 30.0s. Phải > 2s.")
    p.add_argument("--audio-max-rotate-per-video", type=int, default=3,
                   help="v12: Số lần rotate tối đa DO SLOW-SPEED cho mỗi video "
                        "(không tính rotate do DownloadError). Nếu vượt → "
                        "stop video đó. Mặc định: 3. Set 0 = không giới hạn.")
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
    audio_speed_avg_window_seconds: float = 10.0,
    audio_500_threshold: int = 5,           # v7
    audio_stall_seconds: float = 30.0,      # v7
    audio_force_real_after_fails: int = 2, # v10
    # === v12: mid-download slow-speed rotation ===
    audio_slow_speed_kbps: float = 50.0,
    audio_slow_window_seconds: float = 30.0,
    audio_max_rotate_per_video: int = 3,
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
    # ============================================================
    for i, (video, audio_path, audio_filename) in enumerate(bucket_b, 1):
        # === audio-only mode: skip hoàn toàn, audio đã có sẵn rồi ===
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
        # FIX v2 Skip #2: nếu video đã được đánh dấu no_transcript ở run trước
        # → skip luôn, không tốn thời gian gọi yt-dlp / download sub URL.
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

        print(f"\n[B-{i}/{len(bucket_b)}] {video.title[:60]}")
        print(f"  [SKIP-DOWNLOAD] audio có sẵn ở "
              f"{audio_path.parent.name}/{audio_filename}, lấy transcript YouTube...")
        _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | {video.title[:50]} "
             f"-> transcribe-only (audio: {audio_filename})", also_print=False)

        # Tên file JSON cũng theo tên audio (đồng nhất với audio + CSV)
        json_stem = Path(audio_filename).stem
        new_json_path = transcriptions_dir / f"{json_stem}_transcription.json"

        # Truyền info_cached từ Phase 2 yt-dlp metadata (nếu có) để tránh
        # gọi yt-dlp extract_info() lần 2 → giảm rate limit "Sign in".
        info_cached_b: dict = {}
        try:
            if getattr(video, "subtitles", None):
                info_cached_b["subtitles"] = video.subtitles
            if getattr(video, "automatic_captions", None):
                info_cached_b["automatic_captions"] = video.automatic_captions
        except Exception:
            pass
        try:
            result = self.transcribe_with_youtube(
                video_id=video.video_id, audio_path=audio_path,
                lang=["vi", "en"],
                max_sentence_duration=max_sentence_duration,
                min_sentence_words=min_sentence_words,
                info_cached=info_cached_b if info_cached_b else None,
                attempt=1,  # transcript_rotator riêng
            )
        except Exception as e:
            _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | transcript error: {e}")
            results.append({
                "video_id": video.video_id, "title": video.title,
                "status": "transcript_error",
                "audio_filename": audio_filename,
                "audio_downloaded_at": None,
                "error": str(e),
            })
            continue

        if result:
            video.audio_filename = audio_filename
            self._save_transcription(
                output_path=new_json_path, segments=result["segments"],
                video=video, audio_duration=result["audio_duration"],
                audio_filename=audio_filename or "",
                audio_downloaded_at=None,
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
                "audio_downloaded_at": None,
                "transcribed_at": datetime.now().isoformat(),
            })
            print(f"  Done ({len(result['segments'])} segments, "
                  f"lang={result.get('transcript_language')}, "
                  f"auto={result.get('transcript_is_auto')})")
            _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | DONE "
                 f"({len(result['segments'])} seg, lang={result.get('transcript_language')}, "
                 f"audio: {audio_filename})", also_print=False)
        else:
            results.append({
                "video_id": video.video_id, "title": video.title,
                "status": "transcript_unavailable",
                "audio_filename": audio_filename,
                "audio_downloaded_at": None,
            })
            print("  No YouTube transcript available")
            _log(f"[B-{i}/{len(bucket_b)}] {video.video_id} | NO TRANSCRIPT "
                 f"(audio: {audio_filename})", also_print=False)
            # FIX v2 Skip #2: ghi marker file để lần sau skip luôn không gọi yt-dlp
            self._mark_no_transcript(video.video_id, transcriptions_dir)

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
                audio_proxy = self._audio_ip_ctl.on_download_start()
                self._audio_ip_ctl.on_download_start_reset_slow_log()
                using_real_ip = (audio_proxy is None
                                 and self._audio_ip_ctl.get_state() == AudioIPController.STATE_REAL)

                # Biến track cho progress hook
                _dl_state = {
                    "t_start": time.time(),
                    "last_chunk_bytes": 0,
                    "last_chunk_t": time.time(),
                    "bytes_dl_max": 0,
                    "downloaded_bytes": 0,
                    "elapsed_download": 0.0,
                    # === v12: slow-speed rolling window ===
                    "_v12_speed_samples": [],
                    "_v12_last_fire_t": None,
                    "_v12_fire_cooldown": 5.0,
                }

                def _audio_progress_hook(d):
                    """yt-dlp progress hook. v7: stall. v12: slow-speed mid-dl rotate.
                    """
                    try:
                        status = d.get("status", "")
                        if status == "downloading":
                            bytes_now = int(d.get("downloaded_bytes") or 0)
                            speed_bps = float(d.get("speed") or 0)
                            elapsed = time.time() - _dl_state["t_start"]
                            _dl_state["bytes_dl_max"] = max(
                                _dl_state["bytes_dl_max"], bytes_now)
                            _dl_state["downloaded_bytes"] = bytes_now
                            _dl_state["elapsed_download"] = elapsed
                            self._audio_ip_ctl.on_chunk_progress(
                                bytes_dl=bytes_now,
                                elapsed_s=elapsed,
                                speed_bps=speed_bps,
                            )
                            try:
                                self._http500_detector.on_progress_check_stall(
                                    bytes_dl=bytes_now, now=time.time(),
                                )
                            except Exception:
                                pass

                            # === v12: slow-speed rolling window check ===
                            if MidDownloadRotate is not None:
                                try:
                                    now = time.time()
                                    samples = _dl_state["_v12_speed_samples"]
                                    samples.append((now, bytes_now))
                                    win = self._v12_slow_window_seconds
                                    while samples and (now - samples[0][0]) > win:
                                        samples.pop(0)
                                    if len(samples) >= 3 and elapsed >= win:
                                        t_old, b_old = samples[0]
                                        t_new, b_new = samples[-1]
                                        dt = t_new - t_old
                                        db = max(0, b_new - b_old)
                                        if dt > 0:
                                            avg_kbps = (db / dt) / 1024.0
                                            last_fire = _dl_state["_v12_last_fire_t"]
                                            cooldown_ok = (
                                                last_fire is None
                                                or (now - last_fire) >= _dl_state["_v12_fire_cooldown"]
                                            )
                                            if avg_kbps < self._v12_slow_speed_kbps and cooldown_ok:
                                                _dl_state["_v12_last_fire_t"] = now
                                                print(
                                                    f"    [v12-slow] 🐌 rolling avg "
                                                    f"{avg_kbps:.1f} KB/s over "
                                                    f"{dt:.1f}s "
                                                    f"(< threshold {self._v12_slow_speed_kbps} KB/s) "
                                                    f"bytes_dl={bytes_now//1024}KB "
                                                    f"elapsed={elapsed:.1f}s → raise MidDownloadRotate",
                                                    flush=True,
                                                )
                                                raise MidDownloadRotate(
                                                    avg_kbps=avg_kbps,
                                                    window_seconds=dt,
                                                    bytes_dl=bytes_now,
                                                    elapsed_s=elapsed,
                                                )
                                except MidDownloadRotate:
                                    raise
                                except Exception:
                                    pass
                        elif status == "finished":
                            downloaded_bytes = int(d.get("downloaded_bytes") or 0)
                            _dl_state["bytes_dl_max"] = max(
                                _dl_state["bytes_dl_max"],
                                downloaded_bytes,
                            )
                            _dl_state["downloaded_bytes"] = downloaded_bytes
                            _dl_state["elapsed_download"] = time.time() - _dl_state["t_start"]
                    except Exception as hook_err:
                        print(f"    [audio-ip-hook] ERROR: {hook_err}", flush=True)

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
                self._apply_timeouts(ydl_opts, socket_timeout=30)
                # v5: Nếu AudioIPController trả proxy URL (chỉ khi rotator là
                # HTTP proxy thật) thì set. Với OpenVPN tunnel thì audio_proxy
                # luôn = None và IP được route qua system tunnel.
                if audio_proxy and not isinstance(audio_proxy, type(None)):
                    if isinstance(audio_proxy, str) and audio_proxy:
                        ydl_opts['proxy'] = audio_proxy
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
                                # v12: mid-download slow-speed rotation
                                slow_speed_kbps=self._v12_slow_speed_kbps,
                                slow_window_seconds=self._v12_slow_window_seconds,
                                max_rotate_per_video=self._v12_max_rotate_per_video,
                            )
                            if not _smart_result['ok']:
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
            result = self.transcribe_with_youtube(
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
                results.append({
                    "video_id": video.video_id, "title": video.title,
                    "status": "transcript_unavailable",
                    "audio_filename": audio_filename,
                    "audio_downloaded_at": audio_downloaded_at,
                })
                print("  No YouTube transcript available")
                _log(f"[C-{i}/{len(bucket_c)}] {video.video_id} | NO TRANSCRIPT "
                     f"(audio: {audio_filename})", also_print=False)
                # FIX v2 Skip #2: ghi marker để lần sau skip luôn (cả download lẫn yt-dlp)
                YouTubeResearcher._mark_no_transcript(video.video_id, transcriptions_dir)
        except Exception as e:
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
    print("=" * 80)
    print("YOUTUBE AUDIO + SUBS RESUMABLE — VPN BẮT BUỘC")
    print("=" * 80)

    # Instance ID
    # v11: Declare INSTANCE_ID as module-level global (khi gọi main() lần đầu)
    # để các module khác (atexit handler, error fallback) có thể truy cập.
    global INSTANCE_ID
    INSTANCE_ID = args.instance_id or f"pid{os.getpid()}_t{int(time.time())}"
    print(f"[Multi-instance] Instance ID: {INSTANCE_ID}")

    # v11: Đăng ký atexit handler để cleanup tunnel của instance này khi thoát.
    # Chỉ hoạt động nếu user pass --cleanup-on-exit.
    # Lưu ý: atexit KHÔNG chạy khi process bị kill -9 (SIGKILL), nhưng chạy
    # với SIGTERM, SIGINT, exception, hoặc return bình thường.
    if getattr(args, 'cleanup_on_exit', False):
        import atexit as _atexit

        def _v11_cleanup_tunnels():
            """atexit handler: kill tunnel của INSTANCE_ID khi process thoát."""
            try:
                if not INSTANCE_ID:
                    print(f"\n[v11-cleanup] INSTANCE_ID chưa set → skip cleanup",
                          flush=True)
                    return
                print(f"\n[v11-cleanup] --cleanup-on-exit enabled → kill tunnel "
                      f"của instance={INSTANCE_ID}", flush=True)
                killed = kill_tunnel_by_instance(INSTANCE_ID)
                print(f"[v11-cleanup] Killed {killed} tunnel(s) on exit", flush=True)
            except Exception as e:
                print(f"[v11-cleanup] error (ignored): {e}", flush=True)

        _atexit.register(_v11_cleanup_tunnels)
        print(f"[v11-cleanup] Registered atexit handler cho instance={INSTANCE_ID}")

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

    try:
        metadata_rotator = get_isolated_vpn_rotator_from_config(
            instance_id=f"{INSTANCE_ID}_meta",
            rotate_every=args.vpn_rotate_every,
            strategy=args.vpn_strategy,
            real_ip_cycle=0,  # TẮT cycle cho metadata
        )
        audio_rotator = get_isolated_vpn_rotator_from_config(
            instance_id=f"{INSTANCE_ID}_audio",
            rotate_every=0,  # để cycle điều khiển
            strategy=args.vpn_strategy,
            real_ip_cycle=args.vpn_real_ip_cycle,
        )
        transcript_rotator = get_isolated_vpn_rotator_from_config(
            instance_id=f"{INSTANCE_ID}_subs",
            rotate_every=args.vpn_rotate_every,
            strategy=args.vpn_strategy,
            real_ip_cycle=0,  # TẮT cycle cho transcript
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
    print(f"\n=== v12 MID-DOWNLOAD SLOW-SPEED ROTATION ===")
    print(f"  • Nếu rolling avg speed < {args.audio_slow_speed_kbps} KB/s "
          f"trong cửa sổ {args.audio_slow_window_seconds}s "
          f"TRONG KHI ĐANG TẢI → force_rotate NGAY")
    print(f"  • Max {args.audio_max_rotate_per_video} lần rotate do slow-speed "
          f"trên mỗi video (không tính rotate do DownloadError)")
    print(f"  • yt-dlp tự resume file .part nhờ continuedl=True (HTTP Range)")
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
                # v12: mid-download slow-speed rotation
                audio_slow_speed_kbps=args.audio_slow_speed_kbps,
                audio_slow_window_seconds=args.audio_slow_window_seconds,
                audio_max_rotate_per_video=args.audio_max_rotate_per_video,
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
