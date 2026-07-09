# =============================================================================
# v13: Smart timeout/IP detection — clone v12 + FIX bug "MidDownloadRotate bị nuốt".
# =============================================================================
# Khác biệt so với v12 (v12_smart_downloader.py):
#
#   *** BUG ĐÃ FIX TRONG V12 ***
#   Trong v12, hook raise MidDownloadRotate ngay giữa chunk download.
#   Inner `except MidDownloadRotate: raise` re-raise đúng, NHƯNG outer
#   `except Exception as hook_err:` ở line 4539 catch lại, chỉ in log,
#   nuốt mất exception. Kết quả: yt-dlp KHÔNG nhận được exception ->
#   SmartDownloader KHÔNG BAO GIỜ catch MidDownloadRotate -> KHÔNG BAO GIỜ
#   rotate mid-download. Hook chỉ log "sẽ rotate" rồi thôi.
#
#   *** FIX V13 ***
#   - MidDownloadRotate giờ là subclass của yt_dlp.utils.DownloadError
#     -> yt-dlp không nuốt, mà propagate lên như DownloadError thông thường.
#   - Để fix triệt để kể cả khi yt-dlp vẫn nuốt (future-proof), hook còn
#     ghi một "marker file" /tmp/v13_mid_download_rotate.<pid>.<video_id>
#     chứa JSON {avg_kbps, window_seconds, bytes_dl, elapsed_s, ts}.
#     Sau khi yt_dlp.extract_info return (kể cả OK hay fail), main code
#     kiểm tra marker file tồn tại -> consume -> xử lý như MidDownloadRotate.
#   - SmartDownloader có nhánh catch MidDownloadRotate (hoạt động nếu
#     exception propagate được) + nhánh classify_error match marker
#     "MID_DOWNLOAD_SLOW" trong DownloadError message (fallback).
#   - Khi yt-dlp catch exception từ hook, nó wrap thành DownloadError với
#     message dạng "ERROR: <hook error message>". MidDownloadRotate.__str__
#     có prefix marker "MID_DOWNLOAD_SLOW:" để classify dễ.
#
#   - Tham số `slow_speed_kbps` (KB/s) và `slow_window_seconds` (s) giữ
#     nguyên từ v12. Khi progress_hook phát hiện rolling avg < ngưỡng
#     trong cửa sổ -> raise MidDownloadRotate -> outer loop catch -> rotate + retry
#     (yt-dlp tự resume file `.part` nhờ continuedl=True mặc định).
# =============================================================================

import re
import time
import logging
import json
import os
import glob
from typing import Optional, Callable, Any

# QUAN TRỌNG: import DownloadError TRƯỚC để MidDownloadRotate kế thừa từ đó.
try:
    from yt_dlp.utils import DownloadError
except ImportError:  # fallback nếu yt_dlp chưa import
    class DownloadError(Exception):  # type: ignore[no-redef]
        pass

log = logging.getLogger(__name__)


# === v13: MidDownloadRotate giờ LÀ MỘT DownloadError ===
# Lý do: yt-dlp wrap exception từ hook thành DownloadError. Nếu hook raise
# exception KHÔNG phải DownloadError, yt-dlp vẫn wrap nhưng thông tin gốc
# bị mất. Bằng cách kế thừa DownloadError, ta đảm bảo:
#   1. yt-dlp propagate đúng (như 1 DownloadError hợp lệ)
#   2. Message gốc của MidDownloadRotate được giữ nguyên trong DownloadError
#   3. SmartDownloader có thể detect qua isinstance(e, MidDownloadRotate) HOẶC
#      classify_error() match "MID_DOWNLOAD_SLOW" marker trong message.
class MidDownloadRotate(DownloadError):
    """Raise từ progress_hook khi tốc độ trung bình trong cửa sổ N giây
    thấp hơn ngưỡng slow_speed_kbps.

    SmartDownloader.download_with_smart_retry catch exception này -> force_rotate
    IP NGAY (không đợi DownloadError) -> retry với IP mới. yt-dlp mặc định có
    continuedl=True nên sẽ tự resume file `.part` (HTTP Range).

    v13: kế thừa DownloadError để yt-dlp propagate đúng (v12 bug: hook raise
    Exception nhưng outer try/except trong main code nuốt mất exception).
    """

    # Marker dùng để SmartDownloader phát hiện message trong DownloadError
    # (kể cả khi hook exception bị yt-dlp wrap và re-raise).
    MARKER = "MID_DOWNLOAD_SLOW"

    def __init__(self, avg_kbps: float, window_seconds: float,
                 bytes_dl: int, elapsed_s: float,
                 marker_file_path: Optional[str] = None):
        self.avg_kbps = avg_kbps
        self.window_seconds = window_seconds
        self.bytes_dl = bytes_dl
        self.elapsed_s = elapsed_s
        self.marker_file_path = marker_file_path
        # Prefix message với marker để dễ detect khi bị wrap.
        super().__init__(
            f"{self.MARKER}: avg_speed={avg_kbps:.1f} KB/s "
            f"over {window_seconds:.1f}s (window) < threshold"
        )


def write_mid_download_marker(video_id: str, avg_kbps: float,
                              window_seconds: float, bytes_dl: int,
                              elapsed_s: float) -> str:
    """v13: Ghi marker file khi hook phát hiện slow-speed.

    File path: /tmp/v13_mid_download_rotate.<pid>.<video_id>

    Marker file này là FALLBACK cuối cùng: kể cả khi yt-dlp nuốt exception
    của hook, main code vẫn phát hiện được bằng cách kiểm tra marker file
    sau khi extract_info return.

    Returns:
        Đường dẫn marker file (cũng được lưu trong MidDownloadRotate.marker_file_path).
    """
    pid = os.getpid()
    safe_video_id = re.sub(r"[^A-Za-z0-9_-]", "_", str(video_id or "unknown"))[:32]
    path = f"/tmp/v13_mid_download_rotate.{pid}.{safe_video_id}"
    try:
        payload = {
            "avg_kbps": avg_kbps,
            "window_seconds": window_seconds,
            "bytes_dl": bytes_dl,
            "elapsed_s": elapsed_s,
            "ts": time.time(),
        }
        # Write atomically: write to .tmp then rename
        tmp_path = path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(payload, f)
        os.replace(tmp_path, path)
        return path
    except Exception as e:
        log.warning("v13: write_mid_download_marker failed: %s", e)
        return ""


def consume_mid_download_marker(video_id: str) -> Optional[dict]:
    """v13: Đọc + xóa marker file (atomic).

    Trả về dict với các trường avg_kbps/window_seconds/bytes_dl/elapsed_s/ts
    hoặc None nếu không có marker.
    """
    pid = os.getpid()
    safe_video_id = re.sub(r"[^A-Za-z0-9_-]", "_", str(video_id or "unknown"))[:32]
    path = f"/tmp/v13_mid_download_rotate.{pid}.{safe_video_id}"
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r") as f:
            data = json.load(f)
        try:
            os.remove(path)
        except OSError:
            pass
        return data
    except Exception as e:
        log.warning("v13: consume_mid_download_marker failed: %s", e)
        # Best-effort cleanup
        try:
            os.remove(path)
        except OSError:
            pass
        return None


def consume_any_mid_download_marker() -> Optional[dict]:
    """v13: Fallback - consume bất kỳ marker nào của pid hiện tại.

    Dùng khi không biết video_id chính xác (vd: error path trước khi extract_info).
    """
    pid = os.getpid()
    pattern = f"/tmp/v13_mid_download_rotate.{pid}.*"
    matches = sorted(glob.glob(pattern))
    for path in matches:
        try:
            with open(path, "r") as f:
                data = json.load(f)
            try:
                os.remove(path)
            except OSError:
                pass
            return data
        except Exception:
            continue
    return None


# --- Pattern detection -------------------------------------------------------

# Host Google CDN hay bị "đóng băng" route — đổi IP NGAY khi thấy
STUCK_HOST_PATTERNS = [
    r"rr\d+---sn-\w+\.googlevideo\.com",
    r"redirector\.googlevideo\.com",
    r"manifest\.googlevideo\.com",
]

# Các keyword timeout/connection error — bắt buộc phải rotate IP
NETWORK_FATAL_PATTERNS = [
    r"Read timed out",
    r"read timeout=\d+",
    r"Connection timed out",
    r"ConnectionResetError",
    r"RemoteDisconnected",
    r"NewConnectionError",
    r"MaxRetryError",
    r"ChunkedEncodingError",
    r"ssl\.SSLError.*timed out",
    r"HTTPSConnectionPool",
    r"ConnectionTimeout",
    r"Connection refused",
    r"Connection aborted",
    # v10: SSL EOF mid-read — Google CDN đóng connection giữa chừng,
    # thường do IP bị rate-limit hoặc captive portal/firewall chèn.
    r"SSL: UNEXPECTED_EOF_WHILE_READING",
    r"UNEXPECTED_EOF_WHILE_READING",
    r"EOF occurred in violation of protocol",
    r"ssl\.SSLEOFError",
    r"SSLEOFError",
    r"BAD_EOF",
    r"WRONG_VERSION_NUMBER",
]

# Lỗi HTTP có thể do IP bị block (giống v7)
HTTP_BLOCK_PATTERNS = [
    r"HTTP Error 403",
    r"HTTP Error 429",
    r"HTTP Error 500",
    r"HTTP Error 503",
    r"Forbidden",
    r"Your client",
    r"Sign in to confirm",
]

# yt-dlp "giving up" / final-fail messages — yt-dlp raise DownloadError với
# các message này khi hết retry. PHẢI rotate IP vì IP hiện tại đã bị stuck/
# blocked (kết hợp với "[download] Got error" trước đó đã đủ bằng chứng).
# v10: bổ sung patterns cho "Got error: N bytes read" (partial read timeout)
# và các biến thể connection/timeout mà yt-dlp hay gặp khi IP bị stuck.
GIVING_UP_PATTERNS = [
    r"giving up after \d+ retries",
    r"Giving up after \d+ retries",
    r"fragment download failed",
    r"unable to download",
    r"unable to extract",
    r"No video formats found",
    r"This video is unavailable",
    r"Video unavailable",
    r"Got error:.*HTTPSConnectionPool",
    r"Got error:.*HTTP Error [45]\d\d",
    # === v10: patterns mới bắt partial read timeout + network fatal ===
    r"Got error: \d+ bytes read",          # "Got error: 593124 bytes read"
    r"Got error:.*timed out",              # "Got error: ... Read timed out"
    r"Got error:.*HTTP Error 5\d\d",       # HTTP 500/502/503/504 wrap
    r"Got error:.*Connection.*reset",      # "Got error: ... Connection reset"
    r"Got error:.*Connection.*refused",     # "Got error: ... Connection refused"
    r"Got error:.*ConnectionTimeout",      # "Got error: ... ConnectionTimeout"
    r"Got error:.*RemoteDisconnected",     # "Got error: ... Remote end closed"
    r"Got error:.*NewConnectionError",     # "Got error: ... Failed to establish"
    r"Got error:.*MaxRetryError",          # "Got error: ... Max retries exceeded"
    r"Got error:.*ChunkedEncodingError",   # "Got error: ... ChunkedEncodingError"
    r"Got error:.*ssl\..*timed out",       # SSL timeout
]

# v13: marker pattern cho mid-download slow-speed detection
MID_DOWNLOAD_SLOW_PATTERNS = [
    r"MID_DOWNLOAD_SLOW",
]

# === v13.1: Live stream chưa bắt đầu / không khả dụng ===
# Lỗi này KHÔNG liên quan IP — video chưa stream hoặc đã kết thúc.
# Trước đây rơi vào catch-all "Got error:" → force rotate IP vô ích
# → 3 attempts đều fail vì cùng 1 lý do.
# Fix: trả should_rotate_ip=False + should_skip_video=True để caller
# skip hẳn video này (không re-rotate, không re-extract).
LIVE_NOT_STARTED_PATTERNS = [
    r"This live event will begin",
    r"This live stream will begin",
    r"This live stream is not yet started",
    r"Premiere will begin",                   # YouTube Premiere upcoming
    r"upcoming premiere",
    r"Scheduled for .* UTC",                  # yt-dlp "Scheduled for ..."
    r"upcoming live",
    r"live_event will start",
    r"upcoming live stream",
    r"is not currently streaming",
]


# === v13.1: Custom exception để caller skip hẳn video (không re-rotate) ===
class LiveNotStartedError(DownloadError):
    """Raise khi YouTube trả về lỗi 'live event will begin' / 'upcoming live'.

    Lý do tách class riêng (KHÔNG dùng chung DownloadError):
      - Caller (youtube_researcher_v13.py) có thể catch riêng để GHI marker
        'live_unavailable' cho video, tránh retry ở các lần chạy sau.
      - SmartDownloader.download_with_smart_retry() KHÔNG rotate IP, trả
        về ok=False + should_skip_video=True để loop ngoài skip.
    """
    MARKER = "LIVE_NOT_STARTED"

    def __init__(self, message: str = ""):
        self.video_skip_reason = self.MARKER
        super().__init__(f"{self.MARKER}: {message}" if message else self.MARKER)


def classify_error(err: Any, host: str = "") -> dict:
    """
    Phân loại lỗi yt-dlp trả về.

    Returns:
        {
            "category": "stuck_host" | "tcp_timeout" | "connection_error"
                        | "ssl_error" | "http_blocked" | "mid_download_slow"
                        | "live_not_started" | "unknown",
            "should_rotate_ip": bool,
            "should_retry_same_ip": bool,
            "should_skip_video": bool,    # v13.1: skip hẳn, không retry
            "urgency": int,        # 0..10
            "host": str,
            "matched_pattern": str,
        }
    """
    msg = str(err) if err else ""
    matched = None
    category = "unknown"
    urgency = 0

    # 0. v13: MidDownloadRotate marker (có thể bị yt-dlp wrap thành DownloadError).
    #    Phải check TRƯỚC các pattern khác vì marker có urgency cao nhất.
    if MidDownloadRotate.MARKER in msg:
        # Parse lại avg_kbps/window từ message (best-effort)
        avg_kbps = 0.0
        window_seconds = 0.0
        m = re.search(r"avg_speed=([\d.]+) KB/s over ([\d.]+)s", msg)
        if m:
            try:
                avg_kbps = float(m.group(1))
                window_seconds = float(m.group(2))
            except (ValueError, TypeError):
                pass
        return {
            "category": "mid_download_slow",
            "should_rotate_ip": True,
            "should_retry_same_ip": False,
            "should_skip_video": False,
            "urgency": 9,
            "host": _extract_host(msg),
            "matched_pattern": MidDownloadRotate.MARKER,
            "avg_kbps": avg_kbps,
            "window_seconds": window_seconds,
        }

    # 1. Host bị stuck (quan trọng nhất — đổi IP NGAY)
    #    Check host truyền vào + fallback tìm trong msg
    host_to_check = host or _extract_host(msg)
    if not host_to_check:
        # Thử match trực tiếp pattern trong msg (khi không có host param)
        for pat in STUCK_HOST_PATTERNS:
            m = re.search(pat, msg)
            if m:
                return {
                    "category": "stuck_host",
                    "should_rotate_ip": True,
                    "should_retry_same_ip": False,
                    "should_skip_video": False,
                    "urgency": 10,
                    "host": m.group(0),
                    "matched_pattern": pat,
                }
    elif re.search(STUCK_HOST_PATTERNS[0], host_to_check):
        return {
            "category": "stuck_host",
            "should_rotate_ip": True,
            "should_retry_same_ip": False,
            "should_skip_video": False,
            "urgency": 10,
            "host": host_to_check,
            "matched_pattern": STUCK_HOST_PATTERNS[0],
        }

    # 2. TCP read timeout -> IP route hỏng
    if "Read timed out" in msg or re.search(r"read timeout=\d+", msg):
        h = _extract_host(msg)
        return {
            "category": "tcp_timeout",
            "should_rotate_ip": True,
            "should_retry_same_ip": False,
            "should_skip_video": False,
            "urgency": 9,
            "host": h,
            "matched_pattern": "Read timed out",
        }

    # 3. Connection-level error (reset / refused / disconnected)
    if any(p in msg for p in ["ConnectionResetError", "RemoteDisconnected",
                               "NewConnectionError", "MaxRetryError",
                               "Connection refused", "Connection aborted",
                               "ConnectionTimeout"]):
        h = _extract_host(msg)
        return {
            "category": "connection_error",
            "should_rotate_ip": True,
            "should_retry_same_ip": False,
            "should_skip_video": False,
            "urgency": 8,
            "host": h,
            "matched_pattern": "connection_error",
        }

    # 4. SSL / chunked encoding error -> có thể do MITM hoặc IP bị chặn
    # v10: Mở rộng để bắt SSL UNEXPECTED_EOF (Google CDN đóng connection
    # giữa chừng do IP rate-limit / captive portal) — rất hay gặp.
    if any(p in msg for p in [
        "ChunkedEncodingError",
        "SSLError",
        "SSL: UNEXPECTED_EOF_WHILE_READING",
        "UNEXPECTED_EOF_WHILE_READING",
        "EOF occurred in violation of protocol",
        "SSLEOFError",
        "ssl.SSLEOFError",
        "WRONG_VERSION_NUMBER",
    ]):
        # Tìm pattern cụ thể để log rõ lý do
        matched = "ssl_error"
        for p in [
            "SSL: UNEXPECTED_EOF_WHILE_READING",
            "UNEXPECTED_EOF_WHILE_READING",
            "EOF occurred in violation of protocol",
            "SSLEOFError",
            "WRONG_VERSION_NUMBER",
            "ChunkedEncodingError",
            "SSLError",
        ]:
            if p in msg:
                matched = p
                break
        return {
            "category": "ssl_error",
            "should_rotate_ip": True,
            "should_retry_same_ip": False,
            "should_skip_video": False,
            "urgency": 8,  # v10: bump lên 8 (cùng urgency với connection_error)
            "host": _extract_host(msg),
            "matched_pattern": matched,
        }

    # 5. HTTP blocked (giống v7)
    for pat in HTTP_BLOCK_PATTERNS:
        if re.search(pat, msg):
            return {
                "category": "http_blocked",
                "should_rotate_ip": True,
                "should_retry_same_ip": False,
                "should_skip_video": False,
                "urgency": 6,
                "host": _extract_host(msg),
                "matched_pattern": pat,
            }

    # 6. yt-dlp "giving up" — IP chết, PHẢI rotate
    #    Match DownloadError message có dạng:
    #      - "ERROR: giving up after N retries"
    #      - "ERROR: fragment download failed: ..."
    #      - "ERROR: unable to download ..."
    #      - "[download] Got error: HTTPSConnectionPool(...)" (từ stderr msg
    #        mà yt-dlp wrap vào DownloadError)
    for pat in GIVING_UP_PATTERNS:
        if re.search(pat, msg, re.IGNORECASE):
            # Kết hợp thông tin từ msg gốc (nếu có)
            extra = ""
            h = _extract_host(msg)
            if not h:
                # Thử match host trong msg
                m = re.search(r"host='([^']+)'", msg)
                if m:
                    h = m.group(1)
            return {
                "category": "giving_up",
                "should_rotate_ip": True,
                "should_retry_same_ip": False,
                "should_skip_video": False,
                "urgency": 9,
                "host": h,
                "matched_pattern": pat,
            }

    # 6b. v13.1: Live stream chưa bắt đầu / Premiere upcoming / scheduled
    #    → KHÔNG rotate IP (lỗi không liên quan IP), KHÔNG retry cùng IP
    #    → SKIP hẳn video này (ghi marker live_unavailable, tiết kiệm retry).
    for pat in LIVE_NOT_STARTED_PATTERNS:
        if re.search(pat, msg, re.IGNORECASE):
            return {
                "category": "live_not_started",
                "should_rotate_ip": False,
                "should_retry_same_ip": False,
                "should_skip_video": True,
                "urgency": 0,
                "host": "",
                "matched_pattern": pat,
            }

    # 7. v10: Catch-all cho "Got error:" / "ERROR:" — bất kỳ lỗi nào mà
    #    yt-dlp wrap vào DownloadError đều có thể do IP bị stuck. Trước đây
    #    những case này rơi vào "unknown" -> KHÔNG rotate -> retry trên IP chết.
    if "Got error:" in msg or "ERROR:" in msg:
        h = _extract_host(msg)
        if not h:
            m = re.search(r"host='([^']+)'", msg)
            if m:
                h = m.group(1)
        return {
            "category": "giving_up",
            "should_rotate_ip": True,
            "should_retry_same_ip": False,
            "should_skip_video": False,
            "urgency": 7,
            "host": h,
            "matched_pattern": "catch-all Got error/ERROR",
        }

    return {
        "category": "unknown",
        "should_rotate_ip": False,
        "should_retry_same_ip": True,
        "should_skip_video": False,
        "urgency": 0,
        "host": _extract_host(msg),
        "matched_pattern": None,
    }


def _extract_host(err_msg: str) -> str:
    """Trích host từ HTTPSConnectionPool error message."""
    m = re.search(r"host='([^']+)'", err_msg)
    return m.group(1) if m else ""


# --- Smart retry wrapper -----------------------------------------------------

class SmartDownloader:
    """
    Wrapper quanh yt-dlp download với logic retry thông minh.

    Thay vì để yt-dlp tự retry 5 lần × 30s = 150s trên cùng 1 IP,
    SmartDownloader catch DownloadError sớm hơn và đổi IP ngay.

    Usage (trong v7 thay thế đoạn `with yt_dlp.YoutubeDL(ydl_opts) as ydl:`):

        sm = SmartDownloader(
            ip_controller=self._audio_ip_ctl,
            audio_rotator=self._audio_rotator,
            http500_detector=self._http500_detector,
        )
        info = sm.download_with_smart_retry(
            url=video.url,
            ydl_opts=ydl_opts,
            progress_hook=_audio_progress_hook,
            max_attempts=3,
        )
    """

    def __init__(
        self,
        ip_controller=None,         # AudioIPController
        audio_rotator=None,         # VPNRotator / IsolatedVPNRotator / proxy pool
        http500_detector=None,      # HTTP500Detector (giữ tương thích v7)
        socket_timeout: int = 15,   # giảm từ 30 -> 15 để fail nhanh
        max_attempts: int = 3,
    ):
        self.ip_ctl = ip_controller
        self.rotator = audio_rotator
        self.detector = http500_detector
        self.socket_timeout = socket_timeout
        self.max_attempts = max_attempts

    def _force_rotate(self, reason: str) -> Optional[str]:
        """Force rotate IP — gọi rotator trước, fallback về ip_controller.

        v13.1: Log đầy đủ thông tin IP fake (old IP, new IP, server name, idx)
        ra CẢ terminal (print) VÀ file log (log.info). Lý do: user cần thấy
        ngay server VPN nào đang active để debug khi speed không phục hồi.
        """
        old = None
        # DEBUG v9: log đầy đủ state trước khi rotate
        rotator_type = type(self.rotator).__name__ if self.rotator else "None"
        ip_ctl_type = type(self.ip_ctl).__name__ if self.ip_ctl else "None"
        current_ip = "?"
        current_idx = None
        current_server_name = "?"
        current_country = "?"
        if self.rotator and hasattr(self.rotator, "_current_ip"):
            current_ip = self.rotator._current_ip
        if self.rotator and hasattr(self.rotator, "_current_idx"):
            current_idx = self.rotator._current_idx
        if self.rotator and hasattr(self.rotator, "_ovpn_files") and current_idx is not None:
            try:
                current_server_name = self.rotator._ovpn_files[current_idx].name
                if hasattr(self.rotator, "_extract_country"):
                    current_country = self.rotator._extract_country(current_server_name)
            except Exception:
                pass
        elif self.rotator and hasattr(self.rotator, "_current_idx"):
            current_ip = f"idx={self.rotator._current_idx}"

        print(
            f"  [v13-smart] ⚡ FORCE ROTATE reason={reason} "
            f"current_ip={current_ip} current_server=[{current_idx}]"
            f"{current_server_name}({current_country}) rotator={rotator_type} "
            f"ip_ctl={ip_ctl_type}",
            flush=True,
        )
        log.warning(
            "[smart-dl] FORCE ROTATE reason=%s current_ip=%s "
            "current_server=[%s]%s(%s) rotator=%s ip_ctl=%s",
            reason, current_ip, current_idx, current_server_name,
            current_country, rotator_type, ip_ctl_type,
        )
        try:
            if self.rotator and hasattr(self.rotator, "force_rotate"):
                self.rotator.force_rotate(reason)
                new_ip = getattr(self.rotator, "_current_ip", "?")
                new_idx = getattr(self.rotator, "_current_idx", "?")
                new_server_name = "?"
                new_country = "?"
                if hasattr(self.rotator, "_ovpn_files") and isinstance(new_idx, int):
                    try:
                        new_server_name = self.rotator._ovpn_files[new_idx].name
                        if hasattr(self.rotator, "_extract_country"):
                            new_country = self.rotator._extract_country(new_server_name)
                    except Exception:
                        pass
                print(
                    f"  [v13-smart] ✅ ROTATED IP-FAKE: "
                    f"[{current_idx}]{current_server_name}({current_country})@{current_ip} "
                    f"→ [{new_idx}]{new_server_name}({new_country})@{new_ip} "
                    f"[reason={reason}]",
                    flush=True,
                )
                log.info(
                    "[smart-dl] ✅ ROTATED IP-FAKE: [%s]%s(%s)@%s → "
                    "[%s]%s(%s)@%s [reason=%s]",
                    current_idx, current_server_name, current_country, current_ip,
                    new_idx, new_server_name, new_country, new_ip, reason,
                )
            elif self.ip_ctl and hasattr(self.ip_ctl, "force_rotate"):
                self.ip_ctl.force_rotate(reason)
                print(f"  [v13-smart] ✅ Rotated via ip_controller [reason={reason}]", flush=True)
                log.info("[smart-dl] Rotated via ip_controller [reason=%s]", reason)
            else:
                print(
                    f"  [v13-smart] ❌ NO ROTATOR METHOD: rotator={rotator_type} "
                    f"has force_rotate={hasattr(self.rotator, 'force_rotate') if self.rotator else 'N/A'}",
                    flush=True,
                )
        except Exception as e:
            print(f"  [v13-smart] ❌ force_rotate FAILED: {type(e).__name__}: {e}", flush=True)
            log.warning("force_rotate failed: %s", e)
        return old

    def _handle_mid_download_slow(self, slow_exc, attempt, slow_speed_kbps,
                                  max_rotate_per_video, slow_rotate_count,
                                  rotate_history, t_start):
        """Xử lý khi phát hiện mid-download slow-speed. Trả về tuple:
        (should_break, new_slow_rotate_count, should_continue).

        should_break=True: đạt max_rotate_per_video, dừng retry video này.
        should_continue=True: đã rotate, tiếp tục retry attempt tiếp theo.
        """
        avg_kbps = getattr(slow_exc, "avg_kbps", 0.0)
        window_seconds = getattr(slow_exc, "window_seconds", 0.0)
        bytes_dl = getattr(slow_exc, "bytes_dl", 0)
        elapsed_s_exc = getattr(slow_exc, "elapsed_s", 0.0)

        elapsed = time.time() - t_start
        log.warning(
            "[smart-dl] MID-DOWNLOAD SLOW attempt=%d avg=%.1f KB/s "
            "window=%.1fs bytes_dl=%d elapsed=%.1fs",
            attempt, avg_kbps, window_seconds,
            bytes_dl, elapsed_s_exc,
        )
        print(
            f"  [v13-smart] 🐌 MID-DOWNLOAD SLOW attempt={attempt} "
            f"avg={avg_kbps:.1f} KB/s over "
            f"{window_seconds:.1f}s "
            f"(threshold={slow_speed_kbps} KB/s, "
            f"bytes_dl={bytes_dl//1024}KB, "
            f"elapsed={elapsed_s_exc:.1f}s) → FORCE ROTATE",
            flush=True,
        )

        # Check max_rotate_per_video
        # Quy ước: None hoặc 0 = không giới hạn (theo argparse help).
        cap_reached = (
            max_rotate_per_video is not None
            and max_rotate_per_video > 0
            and slow_rotate_count >= max_rotate_per_video
        )
        if cap_reached:
            print(
                f"  [v13-smart] ⛔ Reached max_rotate_per_video="
                f"{max_rotate_per_video} → STOP retrying this video",
                flush=True,
            )
            rotate_history.append({
                "attempt": attempt,
                "reason": "slow_speed_max_reached",
                "avg_kbps": avg_kbps,
                "window_s": window_seconds,
                "elapsed_s": elapsed,
            })
            return True, slow_rotate_count, False

        # Force rotate IP NGAY
        reason = f"slow_speed_mid_download_{slow_rotate_count+1}"
        old_ip = self._force_rotate(reason)
        new_count = slow_rotate_count + 1
        rotate_history.append({
            "attempt": attempt,
            "reason": reason,
            "avg_kbps": avg_kbps,
            "window_s": window_seconds,
            "elapsed_s": elapsed,
        })

        # Sleep ngắn để VPN settle + DNS re-resolve (chỉ sleep nếu còn attempt)
        return False, new_count, True

    def _report_attempt_failure_to_ip_ctl(self, attempt: int, rotate_history: list):
        """v13 BUG #3 FIX: Gọi ip_ctl.on_download_complete(ok=False) để
        AudioIPController state machine REAL↔FAKE nhận biết mỗi attempt fail.

        Trước đây SmartDownloader chỉ rotate giữa các VPN server qua
        self.rotator.force_rotate(). Khi TẤT CẢ VPN servers exhausted
        (5 servers, đã rotate hết), SmartDownloader vẫn xoay vòng trong
        nhóm đã fail → AudioIPController KHÔNG BAO GIỜ nhận biết → state
        FAKE không đổi → REAL cũng không được cycle lại.

        Fix: SAU MỖI attempt thất bại (DownloadError / MidDownloadRotate /
        generic Exception), gọi on_download_complete(ok=False) để controller
        tăng counter `_consecutive_fake_slow` → sau 2 lần fail ở FAKE →
        cycle về REAL.

        Args:
            attempt: attempt number (1-based) để log.
            rotate_history: history để check xem attempt này đã trigger
                on_download_complete chưa (tránh double-call).
        """
        if self.ip_ctl is None:
            return
        # Chỉ gọi khi attempt thực sự đã rotate IP (không gọi cho non-IP errors)
        # Check rotate_history có entry cho attempt này
        rotated_this_attempt = any(
            e.get("attempt") == attempt for e in rotate_history
        )
        if not rotated_this_attempt:
            return  # Không rotate IP → không cần notify IP controller

        try:
            current_state = self.ip_ctl.get_state() if hasattr(self.ip_ctl, "get_state") else "unknown"
            # Bytes = 0, elapsed = 0 → controller coi như fail toàn phần
            # → cycle REAL↔FAKE logic chạy đúng
            self.ip_ctl.on_download_complete(bytes_dl=0, elapsed_s=0.001, ok=False)
            print(
                f"  [v13-smart] 📊 Reported attempt {attempt} fail to "
                f"AudioIPController (state was {current_state}) → "
                f"cycle counter +1",
                flush=True,
            )
        except Exception as e:
            print(
                f"  [v13-smart] ❌ _report_attempt_failure_to_ip_ctl ERROR: "
                f"{type(e).__name__}: {e}",
                flush=True,
            )

    def _consume_marker_and_handle_slow(
        self, video_id: str, attempt: int,
        slow_speed_kbps, max_rotate_per_video,
        slow_rotate_count, rotate_history, t_start,
    ):
        """v13 BUG #1 FIX: Consume marker file NGAY TRONG except handler.

        Trước đây consume_mid_download_marker nằm ngoài tất cả except
        (line 825 cũ) → dead code. Fix: gọi helper này ở đầu MỖI except.

        Trả về tuple (slow_exc, new_slow_rotate_count, action) trong đó:
        - slow_exc: MidDownloadRotate object nếu marker có + chưa xử lý,
          None nếu không có marker hoặc đã xử lý rồi.
        - new_slow_rotate_count: counter sau khi xử lý.
        - action: 'continue', 'break', hoặc None (không có marker).
        """
        marker_data = consume_mid_download_marker(video_id)
        if not marker_data:
            return None, slow_rotate_count, None

        # Check marker này đã được xử lý chưa (qua exception path khác)
        already_handled = any(
            e.get("reason", "").startswith("slow_speed_mid_download_")
            for e in rotate_history
            if e.get("attempt") == attempt
        )
        if already_handled:
            print(
                f"  [v13-smart] 🐌 marker consumed (already handled by "
                f"exception path)",
                flush=True,
            )
            return None, slow_rotate_count, None

        print(
            f"  [v13-smart] 🐌 MARKER FILE FOUND (mid-download slow) "
            f"avg={marker_data.get('avg_kbps', 0):.1f} KB/s "
            f"window={marker_data.get('window_seconds', 0):.1f}s "
            f"(threshold={slow_speed_kbps} KB/s) → FORCE ROTATE",
            flush=True,
        )
        synthetic = MidDownloadRotate(
            avg_kbps=float(marker_data.get("avg_kbps", 0)),
            window_seconds=float(marker_data.get("window_seconds", 0)),
            bytes_dl=int(marker_data.get("bytes_dl", 0)),
            elapsed_s=float(marker_data.get("elapsed_s", 0)),
        )
        should_break, new_count, should_continue = self._handle_mid_download_slow(
            synthetic, attempt, slow_speed_kbps,
            max_rotate_per_video, slow_rotate_count,
            rotate_history, t_start,
        )
        action = "break" if should_break else ("continue" if should_continue else None)
        return synthetic, new_count, action

    def download_with_smart_retry(
        self,
        url: str,
        ydl_opts: dict,
        progress_hook: Optional[Callable] = None,
        max_attempts: Optional[int] = None,
        # === v12: mid-download slow-speed rotation ===
        slow_speed_kbps: Optional[float] = None,
        slow_window_seconds: Optional[float] = None,
        max_rotate_per_video: Optional[int] = None,
    ) -> dict:
        """
        Download với smart retry.

        Returns:
            {"ok": bool, "info": dict|None, "attempts": int,
             "last_error": str, "rotate_history": list}

        v12:
            - Nếu progress_hook raise `MidDownloadRotate` (avg_speed trong
              slow_window_seconds < slow_speed_kbps KB/s) -> catch, force_rotate,
              retry trên IP mới. yt-dlp sẽ tự resume file `.part` (HTTP Range).
            - max_rotate_per_video: số lần rotate tối đa do slow-speed (không
              tính rotate do DownloadError). None = không giới hạn.

        v13 FIX:
            - MidDownloadRotate giờ kế thừa DownloadError -> yt-dlp propagate
              đúng (v12 bug đã fix).
            - Ngoài ra, hook còn ghi marker file /tmp/v13_mid_download_rotate.*
              làm fallback cuối cùng. Nếu marker tồn tại sau khi extract_info
              return (kể cả OK hay DownloadError), sẽ treat như slow-speed.
        """
        import yt_dlp  # local import để tránh circular

        attempts = max_attempts or self.max_attempts
        rotate_history = []
        last_err = None
        slow_rotate_count = 0  # v12: đếm số lần rotate do slow-speed

        # Patch socket_timeout mỗi lần (giảm từ 30s mặc định v7 -> 15s)
        opts = dict(ydl_opts)
        opts["socket_timeout"] = self.socket_timeout
        opts["retries"] = 2          # giảm từ 5 -> 2 (smart sẽ lo phần còn lại)
        opts["fragment_retries"] = 2
        # v12: BẬT continue + Part để resume file .part khi IP đổi giữa chừng
        opts["continuedl"] = True
        opts["nopart"] = False
        if progress_hook:
            opts["progress_hooks"] = [progress_hook]

        # v13: Trích video_id từ url để quản lý marker file theo video.
        video_id_for_marker = ""
        m = re.search(r"(?:v=|/)([A-Za-z0-9_-]{11})(?:[?&]|$)", url or "")
        if m:
            video_id_for_marker = m.group(1)

        for attempt in range(1, attempts + 1):
            t_start = time.time()
            print(
                f"  [v13-smart] 🚀 attempt {attempt}/{attempts} url={url[:80]}",
                flush=True,
            )
            # v13: Consume marker cũ của attempt trước (nếu có) trước khi start
            consume_mid_download_marker(video_id_for_marker)
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    filename = ydl.prepare_filename(info)
                print(f"  [v13-smart] ✅ attempt {attempt} OK in {time.time()-t_start:.1f}s", flush=True)
                return {
                    "ok": True,
                    "info": info,
                    "filename": filename,
                    "attempts": attempt,
                    "last_error": None,
                    "rotate_history": rotate_history,
                }

            except MidDownloadRotate as slow_exc:
                # === v12/v13: MidDownloadRotate propagate thành công ===
                last_err = str(slow_exc)
                # v13 BUG #3 FIX: report fail to ip_ctl ngay để controller
                # biết attempt này thất bại (cycle REAL↔FAKE counter).
                self._report_attempt_failure_to_ip_ctl(attempt, rotate_history)
                should_break, slow_rotate_count, should_continue = \
                    self._handle_mid_download_slow(
                        slow_exc, attempt, slow_speed_kbps,
                        max_rotate_per_video, slow_rotate_count,
                        rotate_history, t_start,
                    )
                if should_break:
                    break
                if attempt >= attempts:
                    break
                if should_continue:
                    time.sleep(min(2 + attempt, 5))
                    continue

            except DownloadError as e:
                # === v13 FALLBACK 1: DownloadError có marker MID_DOWNLOAD_SLOW ===
                # (xảy ra khi yt-dlp wrap MidDownloadRotate nhưng marker trong msg)
                last_err = str(e)
                # v13 BUG #1 FIX: consume marker file NGAY TRONG except (không
                # chờ fall-through vốn dead code).
                _, slow_rotate_count, action = self._consume_marker_and_handle_slow(
                    video_id_for_marker, attempt,
                    slow_speed_kbps, max_rotate_per_video,
                    slow_rotate_count, rotate_history, t_start,
                )
                if action == "break":
                    break
                if action == "continue" and attempt < attempts:
                    time.sleep(min(2 + attempt, 5))
                    continue

                verdict = classify_error(last_err)
                if verdict["category"] == "mid_download_slow":
                    # Recreate MidDownloadRotate từ verdict để dùng handler thống nhất
                    synthetic = MidDownloadRotate(
                        avg_kbps=verdict.get("avg_kbps", 0.0),
                        window_seconds=verdict.get("window_seconds", 0.0),
                        bytes_dl=0,
                        elapsed_s=time.time() - t_start,
                    )
                    print(
                        f"  [v13-smart] 🔁 MidDownloadRotate detected via wrapped "
                        f"DownloadError message (cat=mid_download_slow)",
                        flush=True,
                    )
                    should_break, slow_rotate_count, should_continue = \
                        self._handle_mid_download_slow(
                            synthetic, attempt, slow_speed_kbps,
                            max_rotate_per_video, slow_rotate_count,
                            rotate_history, t_start,
                        )
                    if should_break:
                        break
                    if attempt >= attempts:
                        break
                    if should_continue:
                        time.sleep(min(2 + attempt, 5))
                        continue

                elapsed = time.time() - t_start
                log.warning(
                    "[smart-dl] attempt=%d/%d elapsed=%.1fs category=%s "
                    "host=%s matched=%s",
                    attempt, attempts, elapsed, verdict["category"],
                    verdict["host"], verdict["matched_pattern"],
                )

                # v13.1: SKIP hẳn video (không retry, không rotate IP).
                # Dùng cho live_not_started / scheduled / premiere upcoming
                # — lỗi không liên quan IP, không có ý nghĩa retry 3 lần.
                if verdict.get("should_skip_video", False):
                    print(
                        f"  [v13-smart] ⏭️  SKIP VIDEO (reason={verdict['category']}, "
                        f"matched='{verdict['matched_pattern']}') "
                        f"→ không retry, không rotate IP",
                        flush=True,
                    )
                    last_err = (
                        f"{LiveNotStartedError.MARKER}: {verdict['matched_pattern']}"
                    )
                    rotate_history.append({
                        "attempt": attempt,
                        "reason": verdict["category"],
                        "host": verdict["host"],
                        "elapsed_s": elapsed,
                        "skipped": True,
                    })
                    return {
                        "ok": False,
                        "info": None,
                        "filename": None,
                        "attempts": attempt,
                        "last_error": last_err,
                        "rotate_history": rotate_history,
                        "should_skip_video": True,
                        "skip_reason": verdict["category"],
                    }

                if not verdict["should_rotate_ip"]:
                    # Lỗi không liên quan IP -> retry nhanh trên cùng IP
                    time.sleep(min(2 ** attempt, 5))
                    continue

                # Cần rotate IP
                reason = verdict["category"]
                old_ip = self._force_rotate(reason)
                rotate_history.append({
                    "attempt": attempt,
                    "reason": reason,
                    "host": verdict["host"],
                    "elapsed_s": elapsed,
                })

                # v13 BUG #3 FIX: report fail to ip_ctl để cycle counter
                self._report_attempt_failure_to_ip_ctl(attempt, rotate_history)

                if attempt >= attempts:
                    break

                # v7-style: tăng fragment_500_count cho tương thích detector
                if self.detector:
                    try:
                        self.detector.on_fragment_500(frag_idx=-1, total_frags=0)
                    except Exception:
                        pass

                # Sleep ngắn để VPN/proxy settle
                time.sleep(min(1 + attempt, 3))
                continue

            except Exception as e:
                # === v13 FALLBACK 2: Bất kỳ exception nào khác ===
                # 2a: Nếu message có marker MID_DOWNLOAD_SLOW
                last_err = str(e)
                # v13 BUG #1 FIX: consume marker file NGAY TRONG except
                _, slow_rotate_count, action = self._consume_marker_and_handle_slow(
                    video_id_for_marker, attempt,
                    slow_speed_kbps, max_rotate_per_video,
                    slow_rotate_count, rotate_history, t_start,
                )
                if action == "break":
                    break
                if action == "continue" and attempt < attempts:
                    time.sleep(min(2 + attempt, 5))
                    continue

                verdict = classify_error(last_err)
                if verdict["category"] == "mid_download_slow":
                    synthetic = MidDownloadRotate(
                        avg_kbps=verdict.get("avg_kbps", 0.0),
                        window_seconds=verdict.get("window_seconds", 0.0),
                        bytes_dl=0,
                        elapsed_s=time.time() - t_start,
                    )
                    print(
                        f"  [v13-smart] 🔁 MidDownloadRotate detected via "
                        f"generic Exception (cat=mid_download_slow)",
                        flush=True,
                    )
                    should_break, slow_rotate_count, should_continue = \
                        self._handle_mid_download_slow(
                            synthetic, attempt, slow_speed_kbps,
                            max_rotate_per_video, slow_rotate_count,
                            rotate_history, t_start,
                        )
                    if should_break:
                        break
                    if attempt >= attempts:
                        break
                    if should_continue:
                        time.sleep(min(2 + attempt, 5))
                        continue

                # 2b: Generic exception - vẫn classify theo cách cũ
                log.warning(
                    "[smart-dl] non-DownloadError attempt=%d category=%s err=%s",
                    attempt, verdict["category"], last_err[:200],
                )

                # v13.1: SKIP hẳn video (live not started / premiere / scheduled)
                if verdict.get("should_skip_video", False):
                    print(
                        f"  [v13-smart] ⏭️  SKIP VIDEO via generic exc "
                        f"(reason={verdict['category']}, "
                        f"matched='{verdict['matched_pattern']}')",
                        flush=True,
                    )
                    last_err = (
                        f"{LiveNotStartedError.MARKER}: {verdict['matched_pattern']}"
                    )
                    rotate_history.append({
                        "attempt": attempt,
                        "reason": verdict["category"],
                        "host": verdict["host"],
                        "skipped": True,
                    })
                    return {
                        "ok": False,
                        "info": None,
                        "filename": None,
                        "attempts": attempt,
                        "last_error": last_err,
                        "rotate_history": rotate_history,
                        "should_skip_video": True,
                        "skip_reason": verdict["category"],
                    }

                if verdict["should_rotate_ip"]:
                    old_ip = self._force_rotate(verdict["category"])
                    rotate_history.append({
                        "attempt": attempt,
                        "reason": verdict["category"],
                        "host": verdict["host"],
                    })
                    # v13 BUG #3 FIX: report fail to ip_ctl
                    self._report_attempt_failure_to_ip_ctl(attempt, rotate_history)
                else:
                    # v13 BUG #3 FIX: cũng report nếu không rotate IP nhưng
                    # đã có rotate_history entry (vd: from marker)
                    if any(e.get("attempt") == attempt for e in rotate_history):
                        self._report_attempt_failure_to_ip_ctl(attempt, rotate_history)

                if attempt >= attempts:
                    break
                time.sleep(min(2 ** attempt, 5))

        # === v13 BUG FIX: KHÔNG CÒN FALLBACK 3 ở đây ===
        # v13 ban đầu có FALLBACK 3 (consume marker file) đặt NGOÀI tất cả
        # except blocks. Bug: mọi except đều kết thúc bằng break/continue nên
        # FALLBACK 3 dead code (chỉ chạy khi try body return OK).
        # Marker file đã được consume NGAY ĐẦU mỗi except handler (xem
        # self._consume_marker_in_except ở các nhánh except).

        return {
            "ok": False,
            "info": None,
            "filename": None,
            "attempts": attempts,
            "last_error": last_err,
            "rotate_history": rotate_history,
        }


# --- Singleton factory -------------------------------------------------------

_smart_dl_instance: Optional[SmartDownloader] = None


def get_smart_downloader(
    ip_controller=None,
    audio_rotator=None,
    http500_detector=None,
) -> SmartDownloader:
    """Lazy-init singleton để dùng xuyên suốt class YouTubeResearcher."""
    global _smart_dl_instance
    if _smart_dl_instance is None:
        _smart_dl_instance = SmartDownloader(
            ip_controller=ip_controller,
            audio_rotator=audio_rotator,
            http500_detector=http500_detector,
        )
    return _smart_dl_instance