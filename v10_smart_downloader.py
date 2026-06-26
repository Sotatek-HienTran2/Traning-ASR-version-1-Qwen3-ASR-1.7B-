# =============================================================================
# v10: Smart timeout/IP detection — file riêng để refactor từ v9
# =============================================================================
# Copy phần này vào cuối file v10 (youtube_researcher_audio_subs_multi_rotator_v10.py)
# hoặc save thành file riêng rồi import.
#
# Sự khác biệt so với v9 (v9_smart_downloader.py):
#   - v9: yt-dlp retries=5 × socket_timeout=30 → 150s TRÊN CÙNG 1 IP trước
#         khi văng exception → cycle IP. Lãng phí ~150s cho mỗi IP chết.
#   - v9: catch DownloadError sớm hơn (1 lần timeout ~15s), detect stuck-IP,
#         rotate NGAY lần đầu. Tổng thời gian fail: ~15-30s, thử được 3 IP.
#
# v10 FIX so với v9:
#   - Thêm 11 patterns GIVING_UP cho "Got error: N bytes read",
#     "Got error: timed out", "Got error: Connection reset/refused/Timeout",
#     "Got error: RemoteDisconnected/NewConnectionError/MaxRetryError/
#     ChunkedEncodingError". Trước đây nhiều case bị classify "unknown"
#     → KHÔNG rotate.
#   - Catch-all trong classify_error: nếu có "Got error:" hoặc "ERROR:" →
#     treat as giving_up với urgency=7, should_rotate_ip=True.
#   - Branch riêng cho SSL errors (ssl_error category, urgency=8): bắt
#     "SSL: UNEXPECTED_EOF_WHILE_READING", "SSLEOFError", "WRONG_VERSION_NUMBER",
#     "EOF occurred in violation of protocol" — Google CDN hay đóng connection
#     giữa chừng khi IP bị rate-limit.
# =============================================================================

import re
import time
import logging
from typing import Optional, Callable, Any

try:
    from yt_dlp.utils import DownloadError
except ImportError:  # fallback nếu yt_dlp chưa import
    DownloadError = Exception

log = logging.getLogger(__name__)


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


def classify_error(err: Any, host: str = "") -> dict:
    """
    Phân loại lỗi yt-dlp trả về.

    Returns:
        {
            "category": "stuck_host" | "tcp_timeout" | "connection_error"
                        | "ssl_error" | "http_blocked" | "unknown",
            "should_rotate_ip": bool,
            "should_retry_same_ip": bool,
            "urgency": int,        # 0..10
            "host": str,
            "matched_pattern": str,
        }
    """
    msg = str(err) if err else ""
    matched = None
    category = "unknown"
    urgency = 0

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
                    "urgency": 10,
                    "host": m.group(0),
                    "matched_pattern": pat,
                }
    elif re.search(STUCK_HOST_PATTERNS[0], host_to_check):
        return {
            "category": "stuck_host",
            "should_rotate_ip": True,
            "should_retry_same_ip": False,
            "urgency": 10,
            "host": host_to_check,
            "matched_pattern": STUCK_HOST_PATTERNS[0],
        }

    # 2. TCP read timeout → IP route hỏng
    if "Read timed out" in msg or re.search(r"read timeout=\d+", msg):
        h = _extract_host(msg)
        return {
            "category": "tcp_timeout",
            "should_rotate_ip": True,
            "should_retry_same_ip": False,
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
            "urgency": 8,
            "host": h,
            "matched_pattern": "connection_error",
        }

    # 4. SSL / chunked encoding error → có thể do MITM hoặc IP bị chặn
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
                "urgency": 9,
                "host": h,
                "matched_pattern": pat,
            }

    # 7. v10: Catch-all cho "Got error:" / "ERROR:" — bất kỳ lỗi nào mà
    #    yt-dlp wrap vào DownloadError đều có thể do IP bị stuck. Trước đây
    #    những case này rơi vào "unknown" → KHÔNG rotate → retry trên IP chết.
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
            "urgency": 7,
            "host": h,
            "matched_pattern": "catch-all Got error/ERROR",
        }

    return {
        "category": "unknown",
        "should_rotate_ip": False,
        "should_retry_same_ip": True,
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
        socket_timeout: int = 15,   # giảm từ 30 → 15 để fail nhanh
        max_attempts: int = 3,
    ):
        self.ip_ctl = ip_controller
        self.rotator = audio_rotator
        self.detector = http500_detector
        self.socket_timeout = socket_timeout
        self.max_attempts = max_attempts

    def _force_rotate(self, reason: str) -> Optional[str]:
        """Force rotate IP — gọi rotator trước, fallback về ip_controller."""
        old = None
        # DEBUG v9: log đầy đủ state trước khi rotate
        rotator_type = type(self.rotator).__name__ if self.rotator else "None"
        ip_ctl_type = type(self.ip_ctl).__name__ if self.ip_ctl else "None"
        current_ip = "?"
        if self.rotator and hasattr(self.rotator, "_current_ip"):
            current_ip = self.rotator._current_ip
        elif self.rotator and hasattr(self.rotator, "_current_idx"):
            current_ip = f"idx={self.rotator._current_idx}"

        print(
            f"  [v9-smart] ⚡ FORCE ROTATE reason={reason} "
            f"current_ip={current_ip} rotator={rotator_type} "
            f"ip_ctl={ip_ctl_type}",
            flush=True,
        )
        try:
            if self.rotator and hasattr(self.rotator, "force_rotate"):
                self.rotator.force_rotate(reason)
                new_ip = getattr(self.rotator, "_current_ip", "?")
                print(f"  [v9-smart] ✅ Rotated rotator IP → {new_ip}", flush=True)
                log.info("🔄 Rotated rotator IP: %s → %s [reason=%s]", current_ip, new_ip, reason)
            elif self.ip_ctl and hasattr(self.ip_ctl, "force_rotate"):
                self.ip_ctl.force_rotate(reason)
                print(f"  [v9-smart] ✅ Rotated via ip_controller", flush=True)
                log.info("🔄 Rotated via ip_controller [reason=%s]", reason)
            else:
                print(
                    f"  [v9-smart] ❌ NO ROTATOR METHOD: rotator={rotator_type} "
                    f"has force_rotate={hasattr(self.rotator, 'force_rotate') if self.rotator else 'N/A'}",
                    flush=True,
                )
        except Exception as e:
            print(f"  [v9-smart] ❌ force_rotate FAILED: {type(e).__name__}: {e}", flush=True)
            log.warning("force_rotate failed: %s", e)
        return old

    def download_with_smart_retry(
        self,
        url: str,
        ydl_opts: dict,
        progress_hook: Optional[Callable] = None,
        max_attempts: Optional[int] = None,
    ) -> dict:
        """
        Download với smart retry.

        Returns:
            {"ok": bool, "info": dict|None, "attempts": int,
             "last_error": str, "rotate_history": list}
        """
        import yt_dlp  # local import để tránh circular

        attempts = max_attempts or self.max_attempts
        rotate_history = []
        last_err = None

        # Patch socket_timeout mỗi lần (giảm từ 30s mặc định v7 → 15s)
        opts = dict(ydl_opts)
        opts["socket_timeout"] = self.socket_timeout
        opts["retries"] = 2          # giảm từ 5 → 2 (smart sẽ lo phần còn lại)
        opts["fragment_retries"] = 2
        if progress_hook:
            opts["progress_hooks"] = [progress_hook]

        for attempt in range(1, attempts + 1):
            t_start = time.time()
            print(
                f"  [v9-smart] 🚀 attempt {attempt}/{attempts} url={url[:80]}",
                flush=True,
            )
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=True)
                    filename = ydl.prepare_filename(info)
                print(f"  [v9-smart] ✅ attempt {attempt} OK in {time.time()-t_start:.1f}s", flush=True)
                return {
                    "ok": True,
                    "info": info,
                    "filename": filename,
                    "attempts": attempt,
                    "last_error": None,
                    "rotate_history": rotate_history,
                }

            except DownloadError as e:
                last_err = str(e)
                verdict = classify_error(last_err)
                elapsed = time.time() - t_start
                log.warning(
                    "[smart-dl] attempt=%d/%d elapsed=%.1fs category=%s "
                    "host=%s matched=%s",
                    attempt, attempts, elapsed, verdict["category"],
                    verdict["host"], verdict["matched_pattern"],
                )

                if not verdict["should_rotate_ip"]:
                    # Lỗi không liên quan IP → retry nhanh trên cùng IP
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
                # Lỗi khác (không phải DownloadError) — vẫn classify
                last_err = str(e)
                verdict = classify_error(last_err)
                log.warning(
                    "[smart-dl] non-DownloadError attempt=%d category=%s err=%s",
                    attempt, verdict["category"], last_err[:200],
                )

                if verdict["should_rotate_ip"]:
                    old_ip = self._force_rotate(verdict["category"])
                    rotate_history.append({
                        "attempt": attempt,
                        "reason": verdict["category"],
                        "host": verdict["host"],
                    })

                if attempt >= attempts:
                    break
                time.sleep(min(2 ** attempt, 5))

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