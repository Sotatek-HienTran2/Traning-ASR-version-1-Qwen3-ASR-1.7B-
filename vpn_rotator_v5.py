#!/usr/bin/env python3
"""
VPN Rotator - Rotate IP qua ProtonVPN OpenVPN tunnel.

Hoạt động:
    - Quản lý 5 file .ovpn của ProtonVPN Free plan (5 server ở 5 nước)
    - Connect qua `openvpn --daemon`, đợi tunnel lên, verify IP
    - Rotate bằng cách disconnect → reconnect với file .ovpn khác
    - Tương thích với `ProxyRotator` interface (next/mark_failed) để
      `youtube_researcher_youtube_subs.py` không cần đổi code nhiều

Cài đặt 1 lần (cần sudo):
    sudo setcap cap_net_admin+ep /usr/sbin/openvpn
    → Sau đó user thường chạy openvpn mà KHÔNG cần sudo mỗi lần rotate.

Public API:
    VPNRotator(config_dir, rotate_every, strategy)
        .next()              -> str proxy URL hoặc None
        .mark_failed(url)    -> báo proxy fail
        .mark_success(url)   -> báo proxy OK (no-op)
        .remove_proxy(url)   -> bỏ qua (no-op vì VPN server không thể "xóa")
        .__len__()           -> số server Free
        .stats()             -> list dict stats

Caveats:
    - Mỗi lần rotate tốn 5-10s (reconnect OpenVPN)
    - Cần `setcap cap_net_admin+ep /usr/sbin/openvpn` (chạy 1 lần với sudo)
      để user thường start openvpn mà không cần sudo mỗi lần
    - Auth file phải chmod 600 chứa user + pass ProtonVPN
    - Free plan: 5 server, IP nằm trong blacklist Google/YouTube thường xuyên
"""

import os
import sys
import time
import random
import logging
import subprocess
import threading
from pathlib import Path
from typing import Optional, List, Dict

log = logging.getLogger("vpn_rotator")

# ================= CONFIG =================

PROTON_CONFIG_DIR = Path(__file__).resolve().parent / "proton_config"
PROTON_AUTH_FILE = PROTON_CONFIG_DIR / "auth.txt"
OPENVPN_BIN = "/usr/sbin/openvpn"
# File log để debug khi tunnel không lên
OPENVPN_LOG = "/tmp/openvpn-proton.log"
# Đợi tối đa bao lâu để tunnel sẵn sàng (giây)
# Giam tu 120s -> 40s: ProtonVPN free server thuong mat 10-30s de handshake.
# 40s du cho 95% case. Neu timeout, retry 2 lan voi backoff 5s.
# Tang len 60-80 neu mang rat cham (tuy theo vi tri server).
CONNECT_TIMEOUT = 40
# Đợi bao lâu giữa disconnect và connect mới
# Giam tu 5s -> 2s: moi tien trinh (metadata_rotator, audio_rotator) co
# openvpn process RIÊNG → tun0 RIÊNG → kill 1 process chi giat 1 tun0,
# khong ảnh hưởng process kia. 2s du de kernel release route/netdev.
# Tang len 3-5s neu gap loi "address already in use" hoac "tun0: device busy".
ROTATE_DELAY = 2
# Đợi bao nhiêu giay de kill process cu chet han (sau pkill)
KILL_TIMEOUT = 10
# IP check service (không dùng ipinfo vì free tier giới hạn)
IP_CHECK_URL = "https://ifconfig.me"


# ================= v15: DETECT DEFAULT GATEWAY (wifi/ethernet interface) =================
def get_default_interface_and_ip():
    """Lấy interface gốc (wifi/ethernet) và IP local từ default route.

    Returns:
        (interface_name, local_ip) hoặc (None, None) nếu không detect được.
        Ví dụ: ("wlp3s0", "192.168.1.100")
    """
    try:
        result = subprocess.run(
            ["ip", "route", "show", "default"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None, None
        # Parse: "default via 192.168.1.1 dev wlp3s0 proto dhcp metric 600"
        parts = result.stdout.strip().split('\n')[0].split()
        dev = None
        for i, p in enumerate(parts):
            if p == "dev" and i + 1 < len(parts):
                dev = parts[i + 1]
                break
        if not dev:
            return None, None
        # Lấy IP local của interface
        result2 = subprocess.run(
            ["ip", "-4", "addr", "show", "dev", dev],
            capture_output=True, text=True, timeout=5,
        )
        local_ip = None
        if result2.returncode == 0:
            for line in result2.stdout.split('\n'):
                line = line.strip()
                if line.startswith("inet "):
                    local_ip = line.split()[1].split('/')[0]
                    break
        return dev, local_ip
    except Exception:
        return None, None


# ================= VPN ROTATOR =================

class VPNRotator:
    """
    Rotate IP qua ProtonVPN OpenVPN tunnel.

    Args:
        config_dir: thư mục chứa file .ovpn + auth.txt
        rotate_every: số request trước khi tự rotate (0 = chỉ rotate khi fail)
        strategy: 'random' | 'sequential' | 'least_used'
    """

    def __init__(
        self,
        config_dir: Optional[Path] = None,
        rotate_every: int = 0,
        strategy: str = "random",
        real_ip_cycle: int = 0,
        tunnel_device: str = "tun0",
    ):
        """
        Args:
            config_dir: thư mục chứa file .ovpn + auth.txt
            rotate_every: số request trước khi tự rotate (0 = chỉ rotate khi fail)
            strategy: 'random' | 'sequential' | 'least_used'
            real_ip_cycle: chu kỳ cycle "N fake VPN → 1 IP thật".
                - 0 (mặc định): TẮT cycle, dùng VPN cho MỌI request (giống cũ).
                - N > 0: cứ N request (gồm N-1 fake VPN + 1 IP thật) thì lặp lại.
                  Vd: N=11 → 10 fake VPN, 1 IP thật, lặp lại. Dùng để reset
                  rate-limit counter của YouTube cho IP fake tiếp theo.
            tunnel_device: tên tun device riêng cho rotator này (vd: "tun_audio",
                "tun_subs", "tun_meta"). Mỗi rotator PHẢI có device riêng để khi
                disconnect/rotate không ảnh hưởng tunnel của rotator khác.
        """
        self.config_dir = Path(config_dir) if config_dir else PROTON_CONFIG_DIR
        self.rotate_every = rotate_every
        self.strategy = strategy
        self.real_ip_cycle = real_ip_cycle
        self._tunnel_device = tunnel_device

        # Tìm tất cả file .ovpn trong config_dir
        # (Bỏ qua file ẩn bắt đầu bằng '.': là file tạm do _prepare_config sinh ra)
        self._ovpn_files: List[Path] = sorted(
            p for p in self.config_dir.glob("*.ovpn") if not p.name.startswith(".")
        )
        if not self._ovpn_files:
            raise FileNotFoundError(
                f"Không tìm thấy file .ovpn nào trong {self.config_dir}. "
                f"Download từ https://account.protonvpn.com/downloads"
            )

        # Auth file
        self._auth_file = self.config_dir / "auth.txt"
        if not self._auth_file.exists():
            raise FileNotFoundError(
                f"Thiếu auth file: {self._auth_file}. "
                f"Tạo bằng: printf 'USER\\nPASS\\n' > {self._auth_file} && chmod 600 {self._auth_file}"
            )

        # State
        self._lock = threading.Lock()
        self._current_idx: Optional[int] = None
        self._current_ip: Optional[str] = None
        self._request_count: int = 0
        self._usage_count: Dict[int, int] = {i: 0 for i in range(len(self._ovpn_files))}
        self._fail_count: Dict[int, int] = {i: 0 for i in range(len(self._ovpn_files))}
        self._last_connect_time: float = 0

        # === Multi-instance: instance_id được set từ IsolatedVPNRotator ===
        # (KHÔNG có ở class VPNRotator base — chỉ là attribute optional)
        # Dùng để log rõ rotator nào rotate/cycle khi 2 rotator chạy song song.
        self._instance_id: Optional[str] = None

        # === Cycle "N fake VPN → 1 IP thật" ===
        # Khi real_ip_cycle > 0, sau mỗi N request thì disconnect VPN,
        # dùng IP thật (default route) cho 1 request, rồi reconnect VPN
        # cho cycle kế tiếp. Pattern giúp reset rate-limit counter của
        # YouTube cho IP fake tiếp theo.
        # - _use_real_ip: flag = True nếu request hiện tại đang dùng IP thật
        # - _use_real_ip được SET bởi next() ở request thứ N (khi cycle đạt)
        # - _use_real_ip được RESET bởi next() ở request thứ N+1 (IP thật)
        # - _use_real_ip được CONSUME ở request thứ N+2 (reconnect VPN)
        self._use_real_ip: bool = False

        # === Track active workers đang dùng tunnel ===
        # Mỗi request qua tunnel gọi acquire() (atomic +1) trước khi vào,
        # release() (atomic -1) sau khi xong. Rotate CHỈ xảy ra khi
        # active_workers == 0, nếu không sẽ làm hỏng request đang chạy.
        self._active_workers: int = 0
        # Event signal khi active_workers về 0 (cho rotate-blocking)
        self._idle_event = threading.Event()
        self._idle_event.set()  # ban đầu idle (chưa có worker)
        # Rotate lock riêng: chỉ 1 thread rotate tại 1 thời điểm,
        # thread khác gọi next() sẽ block cho tới khi rotate xong.
        self._rotate_lock = threading.Lock()

        log.info(
            "VPNRotator: %d server files trong %s, strategy=%s, "
            "real_ip_cycle=%d (%s), tunnel_device=%s",
            len(self._ovpn_files), self.config_dir, self.strategy,
            self.real_ip_cycle,
            "TẮT cycle (chỉ fake IP)" if self.real_ip_cycle <= 0
            else f"BẬT cycle {self.real_ip_cycle} request (= {self.real_ip_cycle - 1} fake + 1 real)",
            self._tunnel_device,
        )

    # ----- Public API (compatible với ProxyRotator) -----

    def __len__(self) -> int:
        """Số server có thể rotate (interface giống ProxyRotator)."""
        return len(self._ovpn_files)

    def __bool__(self) -> bool:
        return len(self._ovpn_files) > 0

    # === Cycle "N fake → 1 real" ===

    def is_using_real_ip(self) -> bool:
        """
        True nếu request hiện tại đang ở phase "dùng IP thật" (VPN đã disconnect).

        Caller (YouTubeResearcher) đọc flag này ĐỂ:
        - KHÔNG set `ydl_opts["proxy"]` (traffic sẽ đi qua default route = IP thật).
        - KHÔNG acquire tunnel guard (vì không có tunnel để bảo vệ).
        - Log rõ "[proxy] → IP thật (cycle mode)" cho user biết.

        Flag chỉ True ở DUY NHẤT 1 request giữa cycle (request thứ N+1),
        sau đó `next()` ở request thứ N+2 sẽ reset flag và reconnect VPN.
        """
        return self._use_real_ip

    # === NEW: Worker tracking ===

    def acquire(self):
        """
        Worker PHẢI gọi method này TRƯỚC khi bắt đầu dùng tunnel
        (trước khi gọi requests.get() / yt-dlp extract).

        Return: context manager. Ví dụ:
            with rotator.acquire():
                resp = requests.get(url)

        Hoặc dùng acquire()/release() thủ công (KHÔNG khuyến khích):
            rotator.acquire()
            try:
                resp = requests.get(url)
            finally:
                rotator.release()

        Tăng active_workers lên 1. Nếu rotate đang chờ idle, worker mới
        sẽ block cho tới khi rotate xong.
        """
        return _WorkerGuard(self)

    def _acquire_inc(self):
        """Internal: tăng active_workers."""
        with self._lock:
            self._active_workers += 1
            # Có worker mới → KHÔNG idle nữa
            self._idle_event.clear()

    def _acquire_dec(self):
        """Internal: giảm active_workers."""
        with self._lock:
            self._active_workers -= 1
            if self._active_workers <= 0:
                self._active_workers = 0
                # Idle rồi → signal cho rotate chờ
                self._idle_event.set()

    def _wait_idle(self, timeout: Optional[float] = None) -> bool:
        """
        Chờ cho tất cả worker hiện tại xong việc (active_workers == 0).

        Args:
            timeout: tối đa bao nhiêu giây. None = chờ vô hạn.

        Returns:
            True nếu idle trong timeout, False nếu timeout.
        """
        if self._active_workers == 0:
            return True
        return self._idle_event.wait(timeout=timeout)

    def _active_workers_count(self) -> int:
        """Số worker hiện đang dùng tunnel (cho debug/log)."""
        with self._lock:
            return self._active_workers

    def next(self) -> Optional[str]:
        """
        Lấy proxy URL tiếp theo.

        Returns:
            - None: VPN đang route traffic qua system tunnel (tun0).
              Caller KHÔNG truyền proxies=... vì sẽ conflict với tunnel.
              Đây là behavior đúng vì OpenVPN là system-level tunnel, không phải HTTP proxy.

        Note: Hàm này chỉ dùng để:
            1. Trigger connect lần đầu (nếu chưa connected)
            2. Trigger rotate (nếu rotate_every > 0 hoặc mark_failed được gọi)
            3. Update _request_count internal

        Code gọi (yt-dlp / requests) chỉ cần check `proxy_url = rotator.next()` rồi
        truyền vào `ydl_opts["proxy"]` / `requests.get(proxies=...)`. Nếu None → dùng
        default route (đã đi qua VPN tunnel).

        QUAN TRỌNG: Caller PHẢI dùng `with rotator.acquire(): ...` quanh đoạn code
        thực sự dùng tunnel, nếu không rotate có thể kill tunnel giữa chừng làm
        hỏng request đang chạy.

        Multi-instance: log có kèm `self._instance_id` (set từ IsolatedVPNRotator)
        để phân biệt rotator nào đang rotate/cycle khi 2 rotator chạy song song.
        """
        # Tag cho log: dùng để phân biệt 2 rotator khi chạy multi-instance
        _idtag = (
            f"[{self._instance_id}]" if getattr(self, "_instance_id", None)
            else "[VPN]"
        )

        # === Logic rotate: chờ idle nếu có worker, KHÔNG defer vô tận ===
        # (Defer vô tận sẽ spam 1 IP → Google block → "Sign in to confirm")

        # Bước 1: chờ idle nếu có worker (giữ _rotate_lock để chỉ 1 thread rotate)
        # KHÔNG giữ _lock khi chờ, để worker có thể update _active_workers
        while True:
            with self._lock:
                if self._active_workers == 0:
                    break  # idle, có thể rotate
                workers_idle = self._active_workers
            log.info(
                "VPN %s: %d worker đang dùng tunnel, chờ idle để rotate",
                _idtag, workers_idle,
            )
            self._idle_event.wait(timeout=30)
            # Kiểm tra timeout: nếu vẫn không idle sau 30s → quay lại chờ tiếp
            # (KHÔNG skip rotate, vì skip sẽ spam IP)

        # Bước 2: giờ idle rồi, kiểm tra điều kiện rotate / cycle
        with self._rotate_lock:
            with self._lock:
                # === CASE A: Đã connected (có tunnel) ===
                if self._current_idx is not None:
                    if self.real_ip_cycle > 0:
                        # === CYCLE MODE: N fake → 1 real ===
                        # Tăng counter TRƯỚC (đếm request fake hiện tại)
                        self._request_count += 1
                        if self._request_count >= self.real_ip_cycle:
                            # Đến lượt IP thật: disconnect VPN tunnel
                            # để traffic đi qua default route (= IP thật)
                            log.info(
                                "VPN %s: cycle đạt %d request (>= real_ip_cycle=%d) "
                                "→ DISCONNECT VPN, dùng IP thật cho request kế tiếp "
                                "(CHỈ ảnh hưởng tunnel của instance này — rotator khác "
                                "KHÔNG bị kill PID)",
                                _idtag, self._request_count, self.real_ip_cycle,
                            )
                            self._disconnect()  # kill openvpn PID (chỉ PID của mình)
                            self._current_idx = None
                            self._current_ip = None
                            # SET flag: request KẾ TIẾP sẽ check flag
                            # này để biết đang dùng IP thật (KHÔNG acquire tunnel)
                            self._use_real_ip = True
                            self._request_count = 0  # reset cho cycle mới
                            return None
                        # Chưa đến lượt IP thật: check rotate_every như cũ
                        if self.rotate_every > 0 and self._request_count >= self.rotate_every:
                            log.info(
                                "VPN %s: rotate sang server khác sau %d request (idle) "
                                "(CHỈ kill tunnel của instance này)",
                                _idtag, self._request_count,
                            )
                            self._rotate_locked()
                        return None
                    else:
                        # === KHÔNG CYCLE: logic cũ ===
                        if self.rotate_every > 0 and self._request_count >= self.rotate_every:
                            log.info(
                                "VPN %s: rotate sau %d request (idle) "
                                "(CHỈ kill tunnel của instance này)",
                                _idtag, self._request_count,
                            )
                            self._rotate_locked()
                        else:
                            self._request_count += 1
                        return None

                # === CASE B: Chưa connected (đã disconnect ở cycle hoặc lần đầu) ===
                if self.real_ip_cycle > 0 and self._use_real_ip:
                    # Đang ở phase IP thật: KHÔNG connect VPN.
                    # Caller sẽ dùng default route (= IP thật).
                    # RESET flag để request KẾ TIẾP sẽ reconnect VPN
                    # (đây là 1 request duy nhất dùng IP thật giữa cycle).
                    log.debug(
                        "VPN %s: cycle mode — request này dùng IP thật "
                        "(default route, không qua tunnel)",
                        _idtag,
                    )
                    self._use_real_ip = False  # reset cho cycle fake tiếp theo
                    return None

                # === CASE C: Chưa connected, cần reconnect VPN (cycle fake mới) ===
                if not self._connect_locked():
                    return None

                self._request_count = 1
                return None  # tunnel vừa lên, return None (system route qua tunnel)

    def mark_failed(self, proxy_url: Optional[str]):
        """
        Báo proxy fail → trigger rotate.
        (Không giống ProxyRotator thật sự xóa proxy, VPN chỉ rotate sang server khác.)

        Lưu ý: VPN.next() giờ return None (không phải vpn://tunnel-N như trước),
        nên hàm này phải chấp nhận proxy_url=None và vẫn trigger rotate.
        Caller có thể truyền None hoặc vpn://... (backward-compat).

        QUAN TRỌNG: rotate CHỈ xảy ra khi không có worker nào đang dùng tunnel,
        nếu không sẽ làm hỏng request của họ.
        """
        # Chấp nhận cả None (mới) và vpn://... (cũ, backward-compat)
        if proxy_url is not None and not proxy_url.startswith("vpn://"):
            return  # không phải marker của VPN, bỏ qua

        # Tag cho log: phân biệt rotator khi 2 rotator chạy song song
        _idtag = (
            f"[{self._instance_id}]" if getattr(self, "_instance_id", None)
            else "[VPN]"
        )

        with self._rotate_lock:
            with self._lock:
                if self._active_workers > 0:
                    # Có worker đang dùng tunnel → DEFER rotate.
                    # mark_failed chỉ log warning, KHÔNG rotate ngay.
                    log.warning(
                        "VPN %s: mark_failed(%s) được yêu cầu NHƯNG %d worker "
                        "đang dùng tunnel → SKIP rotate, sẽ retry ở lần next() "
                        "tiếp theo khi worker idle",
                        _idtag, proxy_url, self._active_workers,
                    )
                    return

                # === CYCLE MODE: nếu đang ở phase IP thật ===
                # KHÔNG rotate (không có tunnel). Nếu request IP thật fail
                # (vd: cũng bị block), reset flag để next() kế tiếp reconnect VPN.
                if self.real_ip_cycle > 0 and self._use_real_ip:
                    log.warning(
                        "VPN %s: mark_failed(%s) trong phase IP thật (cycle mode) "
                        "→ reset _use_real_ip=False để next() kế tiếp reconnect VPN",
                        _idtag, proxy_url,
                    )
                    self._use_real_ip = False
                    return

                log.warning(
                    "VPN %s: mark_failed(%s) → rotate "
                    "(CHỈ kill tunnel của instance này)",
                    _idtag, proxy_url,
                )
                if self._current_idx is not None:
                    self._fail_count[self._current_idx] += 1
                self._rotate_locked()

    def mark_success(self, proxy_url: Optional[str]):
        """Báo proxy OK (no-op cho VPN)."""
        pass

    def remove_proxy(self, proxy_url: Optional[str]):
        """Bỏ proxy (no-op cho VPN)."""
        pass

    def stats(self) -> List[Dict]:
        """Trả về list dict stats per server (interface giống ProxyRotator)."""
        result = []
        for i, ovpn in enumerate(self._ovpn_files):
            result.append({
                "url": f"vpn://{ovpn.name}",
                "uses": self._usage_count.get(i, 0),
                "fails": self._fail_count.get(i, 0),
                "country": self._extract_country(ovpn.name),
            })
        return result

    def print_stats(self):
        """Print stats table (giống ProxyRotator.print_stats)."""
        print(f"\n=== VPN Stats ({len(self._ovpn_files)} servers) ===")
        print(f"{'Idx':<4} {'Country':<10} {'Uses':<6} {'Fails':<6} {'File'}")
        stats_list = self.stats()
        max_uses = max((x["uses"] for x in stats_list), default=0)
        for i, s in enumerate(stats_list):
            marker = " *" if s["uses"] == max_uses and max_uses > 0 else ""
            print(f"{i:<4} {s['country']:<10} {s['uses']:<6} {s['fails']:<6} "
                  f"{self._ovpn_files[i].name}{marker}")
        print(f"Current IP: {self._current_ip or 'N/A'}")
        print()

    # ----- Internal VPN control -----

    def _extract_country(self, filename: str) -> str:
        """Trích country code từ tên file (vd 'us-free-5' → 'US')."""
        parts = filename.split("-")
        return parts[0].upper() if parts else "??"

    def _is_connected(self) -> bool:
        """Check xem có openvpn process đang chạy không."""
        try:
            result = subprocess.run(
                ["pgrep", "-f", "openvpn.*proton_config"],
                capture_output=True, text=True, timeout=5,
            )
            return result.returncode == 0
        except Exception:
            return False

    def _get_current_ip(self) -> Optional[str]:
        """Lấy IP public hiện tại qua tunnel (nếu connected).

        Dùng --interface để force curl đi qua tunnel device thay vì default route
        (tránh conflict khi có VPN khác giữ default route, vd FortiClient).
        """
        dev = self._tunnel_device
        # Thử qua tunnel interface trước (chính xác hơn)
        for use_iface in (True, False):
            try:
                cmd = ["curl", "-s", "--max-time", "10"]
                if use_iface:
                    cmd += ["--interface", dev]
                cmd.append(IP_CHECK_URL)
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=15,
                )
                if result.returncode == 0 and result.stdout.strip():
                    return result.stdout.strip()
            except Exception as e:
                if use_iface:
                    log.debug("IP check qua %s failed: %s, thử không chỉ định interface", dev, e)
                else:
                    log.warning("Không lấy được IP: %s", e)
        return None

    def _disconnect(self):
        """
        Kill openvpn process (user thường có thể pkill process của mình).

        Chú ý quan trọng:
        - KHÔNG dùng `pkill -f "openvpn.*proton_config"` vì sẽ kill NHẦM process cũ
          vẫn còn đang chạy khi process mới start lên (race condition).
        - Phải track PID của từng openvpn instance qua self._current_pid, chỉ kill
          đúng PID đó.
        - Sau khi pkill, phải `wait` cho process chết hẳn (tối đa KILL_TIMEOUT giây)
          trước khi start cái mới, nếu không 2 openvpn cùng chạy sẽ gây:
              + 2 tunnel lên cùng lúc → 2 default route → packet đi lung tung
              + DNS leak
              + 1 trong 2 process chiếm port 1194 → process mới fail
        """
        # Xóa route VPN TRƯỚC khi kill process → traffic về wifi ngay lập tức
        self._cleanup_routes()

        pid = getattr(self, "_current_pid", None)
        if pid is None:
            # Backward-compat: nếu self._current_pid chưa được set (process start
            # bởi phiên trước, hoặc ai đó start openvpn ngoài rotator), dùng
            # pkill nhưng với filter CHẶT hơn để giảm nhầm.
            try:
                # Chỉ kill process openvpn CỦA USER HIỆN TẠI, tránh kill process
                # của user khác (vd: root-owned openvpn từ session khác)
                result = subprocess.run(
                    ["pkill", "-9", "-u", str(os.getuid()),
                     "-f", "openvpn.*proton_config.*\\.ovpn"],
                    capture_output=True, timeout=5,
                )
                if result.returncode == 0:
                    log.info("VPN: pkill fallback kill %s process openvpn cu",
                             result.returncode)
                    time.sleep(ROTATE_DELAY)
                return
            except Exception as e:
                log.warning("Lỗi pkill fallback: %s", e)
                return

        # Có PID chính xác → kill chỉ PID đó, chờ chết hẳn
        try:
            # Bước 1: SIGTERM trước (graceful, openvpn sẽ cleanup tun/route)
            try:
                os.kill(pid, 15)  # SIGTERM
            except ProcessLookupError:
                # Process đã chết rồi
                self._current_pid = None
                return
            except PermissionError:
                # Process thuộc user khác (vd: root) → fallback pkill có filter
                log.warning("VPN: PID %s thuộc user khác, fallback pkill", pid)
                self._disconnect_fallback_pkill()
                return

            # Bước 2: đợi process chết, check mỗi 0.5s, tối đa KILL_TIMEOUT
            deadline = time.time() + KILL_TIMEOUT
            while time.time() < deadline:
                try:
                    # Check process còn sống không: kill -0 chỉ check, không kill
                    os.kill(pid, 0)
                    time.sleep(0.5)
                except ProcessLookupError:
                    # Process đã chết
                    self._current_pid = None
                    time.sleep(ROTATE_DELAY)
                    return
                except PermissionError:
                    # Race: process chuyển owner trước khi chết
                    pass

            # Bước 3: timeout mà vẫn sống → SIGKILL
            log.warning("VPN: PID %s không chết sau %ds, dùng SIGKILL",
                        pid, KILL_TIMEOUT)
            try:
                os.kill(pid, 9)  # SIGKILL
                time.sleep(0.5)  # đợi kernel thu hồi process
            except ProcessLookupError:
                pass
            except PermissionError:
                pass

            self._current_pid = None
            time.sleep(ROTATE_DELAY)

        except Exception as e:
            log.warning("Lỗi khi kill PID %s: %s", pid, e)
            # Thử fallback pkill cuối cùng
            self._disconnect_fallback_pkill()

    def _disconnect_fallback_pkill(self):
        """Fallback cuối cùng: pkill với filter chặt (chỉ user hiện tại)."""
        try:
            subprocess.run(
                ["pkill", "-9", "-u", str(os.getuid()),
                 "-f", "openvpn.*proton_config.*\\.ovpn"],
                capture_output=True, timeout=5,
            )
            time.sleep(ROTATE_DELAY)
        except Exception as e:
            log.warning("Lỗi pkill fallback: %s", e)

    def _prepare_config(self, ovpn_path: Path) -> Path:
        """
        Tạo temp .ovpn file:
        - KHÔNG có `up`/`down` DNS update script (cần root).
        - Dùng `route 0.0.0.0 128.0.0.0` + `route 128.0.0.0 128.0.0.0` để OpenVPN
          tự add route split-tunnel bằng cap_net_admin (KHÔNG cần Python có quyền).
        - Khi disconnect (kill openvpn), kernel tự xóa route vì tun device biến mất.

        Returns:
            Path tới temp .ovpn (đã patch)
        """
        original = ovpn_path.read_text(encoding="utf-8", errors="replace")

        cleaned_lines = []
        for line in original.splitlines():
            stripped = line.strip()
            if (
                stripped.startswith("up ")
                or stripped.startswith("down ")
            ) and "/etc/openvpn/update-resolv-conf" in stripped:
                cleaned_lines.append(f"# {line}  # disabled by vpn_rotator")
            elif stripped.startswith("redirect-gateway"):
                cleaned_lines.append(f"# {line}  # replaced by explicit routes below")
            elif stripped == "route-nopull":
                cleaned_lines.append(f"# {line}  # disabled: let openvpn manage routes")
            else:
                cleaned_lines.append(line)

        # Chặn server push route/redirect nhưng cho phép mọi thứ khác
        # Rồi add route riêng: 0.0.0.0/1 + 128.0.0.0/1 qua VPN gateway
        cleaned_lines.append("")
        cleaned_lines.append("# === vpn_rotator: openvpn-managed split routes ===")
        cleaned_lines.append("route-nopull")
        cleaned_lines.append('pull-filter ignore "redirect-gateway"')
        cleaned_lines.append('pull-filter ignore "redirect-private"')
        cleaned_lines.append('pull-filter ignore "route "')
        cleaned_lines.append('pull-filter ignore "dhcp-option"')
        cleaned_lines.append('pull-filter accept ""')
        # OpenVPN tự add route 0.0.0.0/1 + 128.0.0.0/1 qua tunnel gateway
        # vpn_gateway = biến nội bộ OpenVPN, tự resolve từ server push
        cleaned_lines.append("route 0.0.0.0 128.0.0.0 vpn_gateway")
        cleaned_lines.append("route 128.0.0.0 128.0.0.0 vpn_gateway")

        cleaned = "\n".join(cleaned_lines) + "\n"

        temp_path = ovpn_path.parent / f".{ovpn_path.stem}.no-dns.ovpn"
        temp_path.write_text(cleaned, encoding="utf-8")
        return temp_path

    def _add_vpn_routes(self):
        """No-op: OpenVPN tự add route qua redirect-gateway def1 (cap_net_admin)."""
        log.debug("VPN: _add_vpn_routes no-op (openvpn manages routes via redirect-gateway def1)")

    def _cleanup_routes(self):
        """No-op: OpenVPN tự cleanup route khi process bị kill."""
        log.debug("VPN: _cleanup_routes no-op (openvpn cleans up on exit)")

    def _has_tunnel_device(self) -> bool:
        """Check xem tunnel interface (self._tunnel_device) đã lên chưa."""
        dev = self._tunnel_device
        try:
            result = subprocess.run(
                ["ip", "link", "show", dev],
                capture_output=True, text=True, timeout=3,
            )
            # Có "state UP" hoặc "state UNKNOWN" (point-to-point) là OK
            if result.returncode != 0:
                return False
            output = result.stdout
            return "state " in output and "DOWN" not in output.split("state ")[1].split("\n")[0]
        except Exception:
            return False

    # Backward-compat alias
    _has_tun0 = _has_tunnel_device

    def _connect_server(self, idx: int, retry: int = 0) -> bool:
        """
        Connect server thứ idx trong list.
        Returns True nếu tunnel lên và IP verify OK.

        Yêu cầu: `sudo setcap cap_net_admin+ep /usr/sbin/openvpn` đã chạy 1 lần.
        Nếu chưa chạy setcap, openvpn sẽ fail với permission denied.

        Multi-instance: log có _idtag để biết rotator nào đang connect.
        Lưu ý: IsolatedVPNRotator._patch_connect_server_pid() wrap method này
        và thay thế hoàn toàn → khi dùng IsolatedVPNRotator, code trong đây
        KHÔNG được gọi (wrapper gọi self._inner._disconnect + openvpn command).

        Args:
            idx: index của .ovpn file
            retry: số lần đã retry (để backoff)
        """
        ovpn = self._ovpn_files[idx]
        _idtag = (
            f"[{self._instance_id}]" if getattr(self, "_instance_id", None)
            else "[VPN]"
        )
        log.info("VPN %s: connecting to %s (attempt %d) ...", _idtag, ovpn.name, retry + 1)

        # Disconnect trước nếu có connection cũ
        self._disconnect()

        # Tạo config đã strip DNS update script
        prepared_config = self._prepare_config(ovpn)

        # Ghi nhận PID openvpn cũ (nếu còn) để so sánh sau
        old_pid = getattr(self, "_current_pid", None)

        # Start openvpn daemon (KHÔNG cần sudo nếu đã setcap)
        new_pid = None
        try:
            # Dùng log riêng cho từng lần thử để debug
            log_path = OPENVPN_LOG + f".{idx}.{retry}"
            # Thay `--daemon` (rẽ nhánh, mất kiểm soát PID) bằng `popen` để bắt PID
            # rồi sau đó `detach` bằng cách đợi process thật sự start xong.
            #
            # Cách an toàn hơn: dùng `writepid` để openvpn tự ghi PID ra file.
            pid_file = Path("/tmp") / f"openvpn-proton.pid.{idx}.{retry}"
            try:
                pid_file.unlink(missing_ok=True)
            except Exception:
                pass

            proc = subprocess.Popen(
                [
                    OPENVPN_BIN,
                    "--config", str(prepared_config),
                    "--dev", self._tunnel_device,
                    "--auth-user-pass", str(self._auth_file),
                    "--auth-retry", "nointeract",
                    "--auth-nocache",
                    "--daemon",
                    "--log", log_path,
                    "--writepid", str(pid_file),
                    # Bỏ qua update-resolv-conf (cần sudo, fail trên user thường):
                    # ProtonVPN config có --up /etc/openvpn/update-resolv-conf
                    # để set DNS, nhưng script này KHÔNG CẦN DNS của VPN (chỉ cần route qua tunnel).
                    # Override bằng /bin/true để bypass.
                    "--script-security", "2",
                    "--up", "/bin/true",
                    "--down", "/bin/true",
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,  # tách khỏi process group hiện tại
            )
            # Đợi openvpn fork xong (parent exit) — openvpn --daemon sẽ fork 1 lần
            # rồi parent thoát, ta đợi parent thoát
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                log.warning("VPN: openvpn parent không thoát sau 10s, kill")
                proc.kill()
                proc.wait()
                return False

            # Đọc PID file để biết PID openvpn thật sự
            for _ in range(20):  # đợi tối đa 2s để openvpn ghi pid file
                time.sleep(0.1)
                if pid_file.exists():
                    try:
                        new_pid = int(pid_file.read_text().strip())
                        break
                    except Exception:
                        pass
            if new_pid is None:
                log.warning("VPN: openvpn start nhưng không tạo pid file")
                return False

            # Sanity check: PID mới phải khác PID cũ
            if old_pid is not None and new_pid == old_pid:
                log.warning("VPN: PID mới == PID cũ (%s) — có gì đó sai, kill và retry",
                            new_pid)
                try:
                    os.kill(new_pid, 9)
                except Exception:
                    pass
                return False

            self._current_pid = new_pid
            log.info("VPN %s: started openvpn PID=%s, file=%s", _idtag, new_pid, ovpn.name)
        except Exception as e:
            log.error("VPN %s: không start được openvpn: %s", _idtag, e)
            return False

        # Đợi tunnel lên — check tunnel device interface VÀ IP change
        # (vì IP change là dấu hiệu traffic đã route qua tunnel)
        routes_added = False
        for i in range(CONNECT_TIMEOUT):
            time.sleep(1)
            # Ưu tiên check tunnel device (chính xác hơn)
            if self._has_tunnel_device():
                # tunnel device lên → thêm route (chỉ 1 lần)
                if not routes_added:
                    self._add_vpn_routes()
                    routes_added = True
                # Verify IP đã đổi
                ip = self._get_current_ip()
                real_ip = self._last_known_real_ip
                if ip and ip != real_ip:
                    self._current_ip = ip
                    self._current_idx = idx
                    self._usage_count[idx] = self._usage_count.get(idx, 0) + 1
                    self._request_count = 0
                    self._last_connect_time = time.time()
                    log.info(
                        "VPN %s: ✓ connected to %s, IP=%s, PID=%s, dev=%s (sau %ds, attempt %d)",
                        _idtag, ovpn.name, ip, new_pid, self._tunnel_device, i + 1, retry + 1,
                    )
                    return True
                # tunnel device lên nhưng IP chưa đổi → route có thể chưa add xong
                # tiếp tục đợi
            # Fallback: check IP change (cho trường hợp device chưa detect được)
            elif i >= 3:
                ip = self._get_current_ip()
                real_ip = self._last_known_real_ip
                if ip and real_ip and ip != real_ip:
                    if not routes_added:
                        self._add_vpn_routes()
                        routes_added = True
                    self._current_ip = ip
                    self._current_idx = idx
                    self._usage_count[idx] = self._usage_count.get(idx, 0) + 1
                    self._request_count = 0
                    self._last_connect_time = time.time()
                    log.info(
                        "VPN %s: ✓ connected (no dev check) to %s, IP=%s, PID=%s, dev=%s "
                        "(sau %ds, attempt %d)",
                        _idtag, ovpn.name, ip, new_pid, self._tunnel_device, i + 1, retry + 1,
                    )
                    return True

        log.warning("VPN %s: timeout %ds, tunnel không lên tới %s. Log:\n%s",
                    _idtag, CONNECT_TIMEOUT, ovpn.name, self._tail_log(20))
        self._disconnect()
        return False

    def _rotate_locked(self):
        """Rotate sang server khác (phải giữ lock). Có retry/backoff.

        Multi-instance: log có _idtag để biết rotator nào rotate (khi 2 rotator
        chạy song song, mỗi cái rotate server riêng, độc lập).
        """
        _idtag = (
            f"[{self._instance_id}]" if getattr(self, "_instance_id", None)
            else "[VPN]"
        )
        if self._current_idx is not None:
            old_idx = self._current_idx
            # Chọn server khác
            candidates = [i for i in range(len(self._ovpn_files)) if i != old_idx]
            if not candidates:
                return
            if self.strategy == "random":
                new_idx = random.choice(candidates)
            elif self.strategy == "least_used":
                new_idx = min(candidates, key=lambda i: self._usage_count.get(i, 0))
            else:  # sequential
                new_idx = (old_idx + 1) % len(self._ovpn_files)
        else:
            new_idx = 0 if self.strategy != "random" else random.randrange(len(self._ovpn_files))

        log.info(
            "VPN %s: rotating %s → %s "
            "(CHỈ kill tunnel của instance này, rotator khác KHÔNG bị ảnh hưởng)",
            _idtag,
            self._ovpn_files[old_idx].name if self._current_idx is not None else "(none)",
            self._ovpn_files[new_idx].name,
        )

        # Retry logic: thử 2 lần với backoff 5s
        for attempt in range(2):
            if self._connect_server(new_idx, retry=attempt):
                return True
            if attempt == 0:
                log.info("VPN %s: retry %s sau 5s ...", _idtag, self._ovpn_files[new_idx].name)
                time.sleep(5)
        log.error("VPN %s: Rotate thất bại sau 2 lần thử, giữ connection cũ (nếu có)", _idtag)
        return False

    def force_rotate(self, reason: str = "force"):
        """Force rotate sang VPN server khác ngay lập tức (public API).

        Gọi từ caller khi bị 429/captcha cần đổi IP ngay.
        Raises RuntimeError nếu rotate thất bại (tunnel không lên).
        """
        _idtag = (
            f"[{self._instance_id}]" if getattr(self, "_instance_id", None)
            else "[VPN]"
        )
        log.warning("VPN %s: FORCE ROTATE (%s)", _idtag, reason)
        with self._rotate_lock:
            with self._lock:
                success = self._rotate_locked()
        if not success:
            raise RuntimeError(
                f"VPN rotate failed (reason={reason}): tunnel không lên sau 2 lần thử"
            )

    def _connect_locked(self) -> bool:
        """Connect lần đầu (phải giữ lock). Có retry/backoff."""
        idx = 0 if self.strategy != "random" else random.randrange(len(self._ovpn_files))
        _idtag = (
            f"[{self._instance_id}]" if getattr(self, "_instance_id", None)
            else "[VPN]"
        )
        for attempt in range(2):
            if self._connect_server(idx, retry=attempt):
                return True
            if attempt == 0:
                log.info("VPN %s: retry %s sau 5s ...", _idtag, self._ovpn_files[idx].name)
                time.sleep(5)
        log.error("VPN %s: Connect lần đầu thất bại sau 2 lần thử", _idtag)
        return False

    def _tail_log(self, n: int = 20) -> str:
        """Đọc N dòng cuối của openvpn log (tìm file mới nhất theo glob OPENVPN_LOG*)."""
        import glob
        try:
            # Tìm log file mới nhất (OPENVPN_LOG có thể có suffix .idx.retry)
            log_files = sorted(
                glob.glob(OPENVPN_LOG + "*"),
                key=lambda p: Path(p).stat().st_mtime,
                reverse=True,
            )
            if not log_files:
                return "(không tìm thấy log file)"
            latest = log_files[0]
            return f"[{Path(latest).name}]\n" + "\n".join(
                Path(latest).read_text(errors="replace").splitlines()[-n:]
            )
        except Exception as e:
            return f"(không đọc được log: {e})"

    @property
    def _last_known_real_ip(self) -> Optional[str]:
        """IP thật của máy (không qua VPN) — dùng để detect tunnel đã lên."""
        # Cache 1 lần, không gọi nhiều vì tốn 1s
        if not hasattr(self, "_cached_real_ip"):
            try:
                result = subprocess.run(
                    ["curl", "-s", "--max-time", "5", IP_CHECK_URL],
                    capture_output=True, text=True, timeout=8,
                )
                self._cached_real_ip = result.stdout.strip() if result.returncode == 0 else None
            except Exception:
                self._cached_real_ip = None
        return self._cached_real_ip

    def disconnect(self):
        """Disconnect VPN thủ công (gọi khi cleanup)."""
        self._disconnect()
        self._current_idx = None
        self._current_ip = None
        self._use_real_ip = False
        self._request_count = 0


# ================= FACTORY (giống proxy_helper) =================

def get_vpn_rotator_from_config(
    config_dir: Optional[str] = None,
    rotate_every: int = 0,
    strategy: str = "random",
) -> Optional[VPNRotator]:
    """
    Tạo VPNRotator từ config dir.

    Returns None nếu không có file .ovpn (để caller fallback sang proxy thường).
    """
    try:
        return VPNRotator(
            config_dir=Path(config_dir) if config_dir else None,
            rotate_every=rotate_every,
            strategy=strategy,
        )
    except FileNotFoundError as e:
        log.warning("VPN rotator không khả dụng: %s", e)
        return None


# ================= ERROR CLASSIFIER =================

def is_proxy_dead_error(err: Exception | str) -> bool:
    """
    Phân biệt lỗi proxy chết thật vs YouTube rate limit.

    True = proxy chết thật (nên gọi remove_proxy - xóa vĩnh viễn):
        - Connect timeout, read timeout
        - SSL error, Connection reset, Broken pipe
        - HTTP 5xx (proxy server lỗi)
        - "Sign in to confirm you're not a bot" (IP bị block vĩnh viễn)
        - "Proxy error", "Unable to connect to proxy"

    False = YouTube rate limit (chỉ nên gọi mark_failed - cooldown):
        - HTTP 429, 403 (rate limit)
        - "Too Many Requests", "Forbidden"
        - "Rate limit", "Quota exceeded"

    Mặc định False (cooldown) để an toàn - tránh xóa nhầm proxy vẫn dùng được.
    """
    err_str = str(err).lower()

    # Rate limit keywords (CHỈ cooldown)
    rate_limit_kw = [
        '429', 'too many', 'rate limit', 'ratelimited',
        'forbidden', 'quota exceeded',
    ]
    for kw in rate_limit_kw:
        if kw in err_str:
            return False

    # Proxy dead keywords (XÓA vĩnh viễn)
    dead_kw = [
        'connect timeout', 'connection timeout', 'read timeout',
        'timed out', 'ssl', 'connection reset', 'broken pipe',
        'proxy error', 'unable to connect', 'connection refused',
        'remote end closed', 'eof occurred',
        '502', '503', '504', 'bad gateway', 'gateway timeout',
        'service unavailable', 'sign in to confirm', 'not a bot',
    ]
    for kw in dead_kw:
        if kw in err_str:
            return True

    # Không rõ → an toàn: cooldown
    return False


# ================= WORKER GUARD =================

class _WorkerGuard:
    """
    Context manager cho worker acquire/release tunnel.

    QUAN TRỌNG — dùng đúng cách:
        with rotator.acquire():
            # Mọi code trong block này ĐƯỢC PHÉP chạy khi tunnel đang rotate
            # (rotate sẽ chờ block này xong mới kill tunnel)
            response = requests.get("https://youtube.com/...")
            ydl.extract_info(...)

    KHÔNG dùng:
        proxy = rotator.next()       # ← sai: không acquire
        requests.get(...)            # ← tunnel có thể rotate giữa chừng

    Nếu exception xảy ra trong block, release() vẫn được gọi (đảm bảo
    active_workers về 0 cho rotate tiếp theo).
    """

    def __init__(self, rotator: "VPNRotator"):
        self._rotator = rotator

    def __enter__(self):
        self._rotator._acquire_inc()
        return self._rotator

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._rotator._acquire_dec()
        return False  # không nuốt exception


# ================= CLI TEST =================

if __name__ == "__main__":
    # Test rotate 2 lần để xem IP đổi
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    rotator = VPNRotator(rotate_every=3, strategy="random")

    print("\n=== Test 1: next() lần 1 ===")
    print(f"Proxy: {rotator.next()}, IP: {rotator._current_ip}")
    rotator.print_stats()

    print("\n=== Test 2: next() thêm vài lần (sẽ trigger rotate) ===")
    for i in range(5):
        print(f"  next() #{i+2}: {rotator.next()}")
    rotator.print_stats()

    print("\n=== Cleanup ===")
    rotator.disconnect()
    print("Done")
