# VPN Routing trong YouTube Crawler — Tài liệu tổng hợp

> **Mục đích:** Giải thích đầy đủ vì sao cần `setup_vpn_uidrange.sh`, sự khác biệt giữa v13 (proxy-level) và v15+/v16 (system-level routing), chi tiết từng địa chỉ IP, từng lệnh, và tại sao phải dùng đúng như vậy.
>
> **Đối tượng đọc:** Người maintain crawler, muốn hiểu sâu về cách VPN được route và vì sao server có rule admin cứng là cả một "thiết kế có chủ đích".

---

## MỤC LỤC

### PHẦN A — So sánh v13 vs v15/v16

- **A1.** Bối cảnh vấn đề IP thật bị lộ
- **A2.** Hai cách route traffic tránh IP thật
- **A3.** Cách v13 router traffic qua VPN (không cần uidrange)
  - A3.1. Code thực tế
  - A3.2. Flow chi tiết
  - A3.3. Tại sao KHÔNG cần uidrange
  - A3.4. Nhược điểm (cách cũ)
- **A4.** Cách v15/v16 router traffic qua VPN (cần uidrange)
  - A4.1. Code thực tế
  - A4.2. Flow chi tiết
  - A4.3. Tại sao CẦN uidrange
  - A4.4. Ưu điểm (cách mới)
- **A5.** Setup cần thiết cho từng cách
  - A5.1. v13 (proxy-level) — chỉ cần 1 lệnh
  - A5.2. v15/v16 (system-level) — cần 2 thứ
- **A6.** Giải thích từng lệnh — tại sao phải dùng đúng như vậy
  - A6.1. `sudo setcap cap_net_admin+ep /usr/sbin/openvpn`
  - A6.2. `sudo setcap cap_net_admin+ep /bin/ip`
  - A6.3. `sudo ip rule add uidrange <uid>-<uid> priority 100 lookup main`
  - A6.4. `lookup main` — tại sao không phải `lookup 100` hay custom
- **A7.** So sánh trực tiếp trên cùng 1 server
- **A8.** Khi nào dùng cách nào?
- **A9.** Tóm tắt lệnh + lý do

### PHẦN B — Bối cảnh & Từng IP trong `setup_vpn_uidrange.sh`

- **B1.** Bối cảnh tổng thể (server, mục tiêu, vấn đề, vì sao cần uidrange)
- **B2.** Vì sao cần `setup_vpn_uidrange.sh` (không tự fix được trong Python)
- **B3.** Từng địa chỉ IP xuất hiện trong script
  - B3.1. `172.16.198.60` — IP local của server
  - B3.2. `100` (table ID admin)
  - B3.3. `172.16.198.1` — Gateway datacenter
  - B3.4. `1001` (UID user = `${USER_UID}`)
  - B3.5. `100` (priority)
  - B3.6. `main` (lookup table)
  - B3.7. `0.0.0.0/1` và `128.0.0.0/1` — OpenVPN trick
  - B3.8. `10.96.0.1` — Gateway tunnel
  - B3.9. `117.4.246.88` — IP public thật (kết quả xấu)
  - B3.10. `185.180.13.41` — IP VPN điển hình (kết quả mong muốn)
- **B4.** So sánh Selector cho rule (tại sao `uidrange`)
- **B5.** Setup thực tế đã chạy trên server (verified)
- **B6.** Flow IP khi đi đúng qua VPN (sau setup)
- **B7.** IP KHÔNG xuất hiện trong flow khi OK
- **B8.** Prerequisite bắt buộc cho toàn hệ thống
- **B9.** Cách dùng script
- **B10.** Liên kết với các file khác

---

# PHẦN A — SO SÁNH v13 vs v15/v16

> **Tóm tắt một câu:** v13 dùng **in-app proxy** (chỉ HTTP request của yt-dlp đi qua SOCKS/HTTP proxy của VPN), v15/v16 dùng **system-level routing** (toàn bộ traffic của user đi qua `tun0`). Cách mới mạnh hơn nhưng cần fix `uidrange rule` để bypass "policy routing rule" do admin server cài sẵn.

---

## A1. Bối cảnh: Vấn đề IP thật bị lộ

Server này có sẵn 1 rule cứng do admin cài đặt để bảo vệ SSH/health-check:

```bash
$ ip rule show
0:      from all lookup local
32760:  from 172.16.198.60 lookup 100   ← rule admin
32761:  from 172.16.198.60 lookup 100
32762:  from 172.16.198.60 lookup 100
32763:  from 172.16.198.60 lookup 100
32764:  from 172.16.198.60 lookup 100
32765:  from 172.16.198.60 lookup 100
```

**Cơ chế:** Packet có `src=172.16.198.60` → kernel tra bảng rule theo priority tăng dần → tới priority 32760 → MATCH → ép đi **table 100** → table 100 chỉ có 1 default route qua `enp3s0` (NIC vật lý) → **BẮT BUỘC traffic ra IP thật `172.16.198.60`, bypass hoàn toàn VPN `tun0`**.

**Hậu quả nếu không fix:** OpenVPN tunnel vẫn lên (có `tun0`, có IP VPN), `curl --interface tun0` ra IP VPN OK, nhưng `curl` thường (không chỉ định interface) vẫn ra IP thật → YouTube thấy IP thật → block captcha.

```bash
# Khi rule admin còn, VPN "vô hiệu" cho system traffic:
$ curl --interface tun0 https://ifconfig.me
185.180.13.41   ← IP VPN ✓

$ curl https://ifconfig.me                   # ← không chỉ định interface
172.16.198.60   ← IP thật ✗

$ ip route get 142.250.198.142
142.250.198.142 via 10.0.0.1 dev enp3s0 src 172.16.198.60 uid 1000
                  ↑↑↑ vẫn qua NIC vật lý, không qua tun0
```

---

## A2. Hai cách route traffic tránh IP thật

| Cách tiếp cận | v13 | v15/v16 |
|---|---|---|
| Tên gọi | **Proxy-level routing** (in-app proxy) | **System-level routing** (default route qua tun0) |
| Cơ chế | Mỗi HTTP client (yt-dlp, requests) chỉ định proxy rõ ràng | Toàn bộ traffic của UID user đi qua `tun0` |
| Setup cần | `setcap cap_net_admin+ep /usr/sbin/openvpn` | `setcap` cho openvpn + **`setcap cap_net_admin+ep /bin/ip`** + **`uidrange rule`** |
| Cách bypass rule admin | Không cần (proxy không qua `ip rule`) | Cần uidrange rule priority 100 |

---

## A3. Cách v13 router traffic qua VPN (không cần uidrange)

### A3.1. Code thực tế

Trong v13, mỗi lần crawler cần request tới YouTube, code set proxy vào `ydl_opts`:

```python
# File: youtube_researcher_audio_subs_multi_rotator_v13.py
# (line ~1640)

ydl_opts = {
    "quiet": True,
    "no_warnings": True,
    # ... các option khác
}
if proxy_url:                              # ← proxy_url = "socks5://10.8.0.1:1080"
    ydl_opts["proxy"] = proxy_url          # ← CHỈ HTTP request này đi qua proxy

with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(url, download=False)
```

```python
# VPN rotator trả về URL proxy (KHÔNG phải None):
def next(self):
    """Trả về proxy URL để caller tự set vào ydl_opts['proxy']."""
    # v13 (cách cũ): next() trả về "socks5://10.8.0.1:1080"
    return proxy_url
```

### A3.2. Flow chi tiết

```
yt-dlp request
    ↓
HTTP client đọc ydl_opts["proxy"] = "socks5://10.8.0.1:1080"
    ↓
Connect trực tiếp tới SOCKS proxy server (10.8.0.1:1080)
    ↓
Proxy server (chạy trong openvpn) đẩy request qua tun0
    ↓
Packet ra internet qua tun0 → IP VPN
```

### A3.3. Tại sao KHÔNG cần uidrange

**Proxy chỉ định rõ ràng endpoint** → kernel không cần tra `ip rule`/`ip route` cho packet này:

- Packet đi tới `10.8.0.1:1080` (SOCKS server trong tunnel) → kernel dùng **local table** (priority 0) → không match rule admin `from 172.16.198.60` (vì destination khác 172.16.198.60, packet này đi tới IP của tunnel gateway) → route OK.
- HTTP request thật (tới youtube.com) **bị SOCKS server đóng gói lại** → bên ngoài chỉ thấy packet đi từ SOCKS proxy → IP VPN.

→ Toàn bộ traffic kernel-level vẫn đi `enp3s0` (IP thật), nhưng **payload** là proxy tunnel → YouTube chỉ thấy IP cuối cùng là IP VPN.

### A3.4. Nhược điểm (cách cũ)

| Vấn đề | Giải thích |
|---|---|
| **ffmpeg subprocess leak IP** | yt-dlp spawn `ffmpeg` để merge audio/video → ffmpeg KHÔNG có `HTTP_PROXY` env → gọi thẳng `googlevideo.com` qua system route → lộ IP thật |
| **DNS leak** | `curl --proxy socks5://...` vẫn resolve DNS qua system resolver (trừ khi dùng `--proxy-insecure`) → ISP biết domain |
| **Library bypass proxy** | `youtube-transcript-api` (engine khác của yt-dlp) có thể không tôn trọng `ydl_opts["proxy"]` → request đi IP thật |
| **Overhead SOCKS** | Tốn 1 lớp encapsulation, throughput chậm hơn 5-15% |

---

## A4. Cách v15/v16 router traffic qua VPN (cần uidrange)

### A4.1. Code thực tế

Trong v15+, `vpn_rotator_v4.next()` đã được **refactor** để trả về `None`:

```python
# File: vpn_rotator_v4.py docstring:
"""
Returns:
    - None: VPN đang route traffic qua system tunnel (tun0).
      Caller KHÔNG truyền proxies=... vì sẽ conflict với tunnel.
"""
```

```python
# File: youtube_researcher_audio_subs_multi_rotator_v16.py (line ~1080-1083)
None: dùng IP thật (default route). Caller KHÔNG set proxy
      trong ydl_opts, KHÔNG acquire tunnel guard. Traffic sẽ
      đi qua default route (đã route qua VPN nhờ uidrange rule).

str:  dùng IP fake qua VPN tunnel. Caller set proxy hoặc
      ... (vẫn hỗ trợ fallback cho một số engine cũ)
```

```python
# v15/v16 (cách mới): KHÔNG set proxy vào ydl_opts
ydl_opts = {
    "quiet": True,
    "no_warnings": True,
    # ... KHÔNG có "proxy" key
    # Để yt-dlp dùng default route
}
with yt_dlp.YoutubeDL(ydl_opts) as ydl:
    info = ydl.extract_info(url, download=False)
    # → yt-dlp connect thẳng tới youtube.com
    # → kernel tra ip rule → đi main table (nhờ uidrange)
    # → main table có "0.0.0.0/1 via tun0" → qua VPN ✓
```

### A4.2. Flow chi tiết

```
yt-dlp request không set proxy
    ↓
yt-dlp connect thẳng tới youtube.com:443
    ↓
Kernel tạo packet có src=172.16.198.60 (IP user)
    ↓
Kernel tra ip rule theo priority tăng dần:
    ├─ Priority 0:    from all lookup local       → không match (không phải 127.x.x.x)
    ├─ Priority 100:  uidrange 1000-1000 lookup main  ← user đã add (CẦN CÓ)
    │                  → MATCH → đi main table
    │
    └─ (không xét tiếp)
    ↓
Main table có:
    - 0.0.0.0/1 via 10.8.0.1 dev tun0          ← VPN default route
    - 128.0.0.0/1 via 10.8.0.1 dev tun0        ← VPN default route
    - default via 10.0.0.1 dev enp3s0          ← route thật (không dùng)
    ↓
Packet đi qua tun0 → ra internet qua VPN server → IP VPN
```

### A4.3. Tại sao CẦN uidrange

**System routing BẮT BUỘC kernel tra `ip rule`** (không có cách nào bypass nếu rule admin ở priority thấp số cao):

- Không set proxy trong app → kernel phải tự route
- Packet có `src=172.16.198.60` → nếu KHÔNG có rule uidrange priority 100 → kernel xét tới priority 32760 (admin) → MATCH → đi table 100 → chỉ có default qua enp3s0 → ra IP thật ✗

→ **uidrange rule là điều kiện TIÊN QUYẾT** cho cách này.

### A4.4. Ưu điểm (cách mới)

| Ưu điểm | Giải thích |
|---|---|
| **Bắt được 100% traffic** | Mọi syscall `connect()`, mọi subprocess (ffmpeg, aria2c, ...), mọi DNS query đều qua tun0 |
| **Không DNS leak** | DNS query cũng qua tun0 → không lộ domain cho ISP |
| **Throughput cao hơn** | Raw TCP qua tun0, không overhead SOCKS |
| **Subs engine nào cũng OK** | `youtube-transcript-api`, `yt-dlp`, `requests` đều đi qua cùng 1 tunnel — không cần support proxy |
| **Robust với library mới** | Thêm bất kỳ HTTP client nào cũng auto đi VPN |

---

## A5. Setup cần thiết cho từng cách

### A5.1. v13 (proxy-level) — chỉ cần 1 lệnh

```bash
# Cho openvpn chạy được mà không cần sudo (user-level tunnel)
sudo setcap cap_net_admin+ep /usr/sbin/openvpn
```

**KHÔNG cần:**
- `setcap` cho `/bin/ip` (vì không xóa bypass routes)
- `uidrange rule` (vì không dùng system routing)

### A5.2. v15/v16 (system-level) — cần 2 thứ

```bash
# (1) Cho openvpn chạy được
sudo setcap cap_net_admin+ep /usr/sbin/openvpn

# (2a) Cho phép user xóa bypass host routes mà không cần sudo mỗi lần rotate
sudo setcap cap_net_admin+ep /bin/ip

# (2b) Thêm rule priority 100 để packet đi main table (bypass rule admin 32760)
sudo ip rule add uidrange <uid>-<uid> priority 100 lookup main
```

**Hoặc gọn hơn:**

```bash
sudo bash setup_vpn_uidrange.sh add    # ← script wrap cả (2a) + (2b)
```

---

## A6. Giải thích từng lệnh — tại sao phải dùng đúng như vậy

### A6.1. `sudo setcap cap_net_admin+ep /usr/sbin/openvpn`

| Thành phần | Ý nghĩa |
|---|---|
| `setcap` | Set Linux capability (thay vì setuid bit) |
| `cap_net_admin` | Quyền thay đổi network config (route, interface, ...) |
| `+ep` | **E**ffective + **P**ermitted — process có thể dùng cap này |
| `/usr/sbin/openvpn` | Binary cần setcap |

**Tại sao cần:**
- OpenVPN cần tạo tunnel interface (`tun0`), add route (`0.0.0.0/1 via tun0`), modify routing table
- Mặc định chỉ root mới có `cap_net_admin` → user thường không start openvpn được
- **KHÔNG dùng `sudo chmod u+s`** (setuid) vì openvpn sẽ chạy với quyền root → bảo mật kém, log/credential lẫn lộn
- `setcap` chỉ cấp đúng 1 capability → nếu openvpn bị exploit, attacker chỉ có quyền network, không có quyền root full

### A6.2. `sudo setcap cap_net_admin+ep /bin/ip`

| Thành phần | Ý nghĩa |
|---|---|
| `/bin/ip` | Tool quản lý routing table của iproute2 |
| `cap_net_admin` | Cho phép `ip route add/del`, `ip rule add/del` |

**Tại sao cần:**
- `vpn_rotator_v4._force_traffic_via_tun0()` cần `ip route del 142.250.0.0/15 via 10.0.0.1 dev enp3s0` để xóa bypass routes của server
- Mặc định user thường KHÔNG có quyền `ip route del` (cần sudo)
- Nếu KHÔNG setcap → mỗi lần rotate phải gọi sudo → phức tạp, dễ lộ password trong script

**Tại sao KHÔNG dùng sudo mỗi lần:**
- Script crawler chạy tới hàng giờ, có thể rotate hàng trăm lần
- `sudo` yêu cầu password (trong cron/headless mode không có)
- `setcap` một lần → dùng mãi mãi, không cần tương tác

### A6.3. `sudo ip rule add uidrange <uid>-<uid> priority 100 lookup main`

| Thành phần | Ý nghĩa |
|---|---|
| `ip rule` | Quản lý policy routing database của kernel |
| `add` | Thêm rule mới |
| `uidrange <uid>-<uid>` | Match packet từ process có UID = `<uid>` (ví dụ `1000-1000`) |
| `priority 100` | Xét rule này TRƯỚC (số nhỏ = ưu tiên cao) |
| `lookup main` | Nếu match → tra bảng routing "main" (default routing table của hệ thống) |

**Tại sao priority = 100 (không phải 1000 hay 32764):**

Kernel xét rule theo **priority tăng dần** (số nhỏ xét trước), gặp rule **MATCH ĐẦU TIÊN** thì dừng:
- Priority 0:    `lookup local`        — kiểu match loopback
- Priority 100:  **`uidrange 1000-1000` lookup main** ← MỚI (user)
- ...
- Priority 32760-32765: `from 172.16.198.60 lookup 100` (admin)

Nếu chọn priority 32764 (sát rule admin) → vẫn có khả năng rule admin match trước do kernel xử lý tie-break không deterministic giữa các priority bằng nhau → race condition.

Chọn priority **100** (cách xa 32760) → luôn match trước, không bao giờ có tie-break.

**Tại sao dùng `uidrange` thay vì `from <IP>` hay `iif lo`:**

| Selector | Match khi... | Vấn đề |
|---|---|---|
| `from 172.16.198.60` | Packet có src IP | IP có thể đổi (DHCP); match cả root → ảnh hưởng SSH |
| `from all iif lo` | Loopback only | Packet YouTube không qua loopback |
| **`uidrange 1000-1000`** | **Process có UID = 1000** | **Cố định theo user, không match root** |

`uidrange` an toàn vì:
- UID cố định suốt đời user → không phụ thuộc IP/DHCP
- UID = 0 (root) **KHÔNG match** → SSH/monitoring/admin tools vẫn đi rule admin → không mất kết nối
- Chỉ UID user (1000) mới bị "đè" → mọi process do user chạy đều qua main table

### A6.4. `lookup main` — tại sao không phải `lookup 100` hay `lookup <custom>`:

- `main` là routing table mặc định của hệ thống, chứa **MỌI route do openvpn vừa add**: `0.0.0.0/1 via tun0`, `128.0.0.0/1 via tun0`, v.v.
- `lookup 100` là **table admin** (chỉ có default via enp3s0) → đó là route admin muốn giữ cho SSH
- `lookup <custom>` sẽ cần maintain riêng → phức tạp

→ `lookup main` là tận dụng route VPN có sẵn do openvpn add tự động.

---

## A7. So sánh trực tiếp trên cùng 1 server

### Test 1: Packet có đi qua VPN không?

```bash
# v13 (proxy-level)
$ curl --interface tun0 https://ifconfig.me     # CHỈ ĐỊNH tun0
185.180.13.41   ← IP VPN ✓ (vì ép interface)
$ curl https://ifconfig.me                       # KHÔNG chỉ định
172.16.198.60   ← IP thật ✗
```

```bash
# v16 (system-level, có uidrange rule)
$ curl https://ifconfig.me                       # default
185.180.13.41   ← IP VPN ✓ (vì rule uidrange priority 100 → main → tun0)
$ curl --interface tun0 https://ifconfig.me      # cũng qua tun0
185.180.13.41
```

### Test 2: ffmpeg subprocess có lộ IP không?

```bash
# v13
$ yt-dlp --proxy socks5://... URL_VIEO
$ ps aux | grep ffmpeg
ffmpeg ... googlevideo.com                   # ← KHÔNG có proxy env
                                              # Lộ IP thật (172.16.198.60) ✗

# v16
$ yt-dlp URL_VIDEO                            # không cần proxy
$ ps aux | grep ffmpeg
ffmpeg ... googlevideo.com
# Nhưng: kernel route ưu tiên uidrange → packet đi tun0 → IP VPN ✓
```

### Test 3: DNS resolution có leak không?

```bash
# v13
$ nslookup youtube.com
Server: 8.8.8.8                               # ← DNS leak ra ISP ✗
Address: 142.250.198.142

# v16
$ nslookup youtube.com
Server: 10.8.0.1                              # ← DNS qua tun0 ✓
Address: 142.250.198.142
```

---

## A8. Khi nào dùng cách nào?

| Tiêu chí | v13 (proxy) | v15/v16 (system) |
|---|---|---|
| Audio-only crawl (chỉ cần audio) | ✅ Đủ dùng | ✅ OK |
| Cần subs chất lượng cao | ⚠️ Hay fail (engine bypass proxy) | ✅ Tốt (mọi engine qua tun0) |
| Cần metadata ổn định | ⚠️ Captcha nhiều (IP thật 1 số request) | ✅ Sạch hơn |
| Multi-process (ffmpeg, aria2, ...) | ❌ Subprocess leak IP | ✅ An toàn |
| Setup đơn giản | ✅ 1 lệnh setcap | ⚠️ 3 lệnh setcap + rule |
| Server không có rule admin cứng | ✅ Vẫn chạy OK | ✅ Chạy OK |
| Server CÓ rule admin `from <IP> lookup 100` | ✅ Vẫn chạy OK (proxy bypass) | ⚠️ CẦN uidrange rule |
| Thay đổi ít code (backward-compat) | ✅ | ❌ (đã refactor rotator) |
| Debug đơn giản | ✅ (`curl --proxy` dễ test) | ⚠️ (`ip rule` + `ip route` phức tạp hơn) |

---

## A9. Tóm tắt lệnh + lý do

### Lệnh 1: `sudo setcap cap_net_admin+ep /usr/sbin/openvpn`

```bash
sudo setcap cap_net_admin+ep /usr/sbin/openvpn
```

**Lý do:** Cho phép user thường start/kill openvpn daemon (tạo tun0, add route VPN) mà không cần sudo mỗi lần. `cap_net_admin` là capability tối thiểu cần cho tunnel, an toàn hơn setuid root.

### Lệnh 2: `sudo setcap cap_net_admin+ep /bin/ip`

```bash
sudo setcap cap_net_admin+ep /bin/ip
```

**Lý do:** Cho phép user xóa bypass host routes (VD: `142.250.0.0/15 via enp3s0`) mà không cần sudo mỗi lần crawler rotate. Cần thiết vì `vpn_rotator_v4._force_traffic_via_tun0()` gọi `ip route del` liên tục.

### Lệnh 3: `sudo ip rule add uidrange <uid>-<uid> priority 100 lookup main`

```bash
UID=$(id -u)
sudo ip rule add uidrange ${UID}-${UID} priority 100 lookup main
```

**Lý do:**
- `uidrange <UID>-<UID>` → match packet từ process UID user (CỐ ĐỊNH, không phụ thuộc IP)
- `priority 100` → xét TRƯỚC rule admin (priority 32760) do số priority nhỏ hơn = ưu tiên cao hơn
- `lookup main` → đi tới main table (có `0.0.0.0/1 via tun0` do openvpn vừa add)
- Không match UID 0 (root) → SSH từ root vẫn đi rule admin → an toàn

### Lệnh 4: `curl --proxy socks5://10.8.0.1:1080 https://...` (chỉ v13)

```bash
curl --proxy socks5://10.8.0.1:1080 https://ifconfig.me
```

**Lý do:** Trong cách proxy-level (v13), mỗi HTTP client phải tự chỉ định proxy. Đây là cách bypass hoàn toàn `ip rule`: kernel chỉ route packet tới SOCKS server (trong tunnel), không bao giờ đụng tới rule admin.

---

# PHẦN B — BỐI CẢNH & TỪNG IP TRONG `setup_vpn_uidrange.sh`

> **File:** `setup_vpn_uidrange.sh` (wrapper thân thiện cho admin chạy 1 lần với sudo).
> **Mục đích:** Thêm rule `uidrange <UID>-<UID> priority 100 lookup main` + setcap `/bin/ip` để user-level process (crawler) dùng được VPN, trong khi không phá rule admin cứng `from 172.16.198.60 lookup 100` (priority 32760).

---

## B1. Bối cảnh tổng thể

### B1.1. Cấu hình server (verified)

```
SERVER: 172.16.198.60 (datacenter Việt Nam, NIC enp3s0)
IP public thật: 117.4.246.88 (qua NAT gateway 172.16.198.1)
User: UID 1001 (hientran)
Tunnel VPN: 10.96.0.30 (tun0, ProtonVPN openvpn)
Tunnel gateway: 10.96.0.1 (server-side gateway của tunnel)
OpenVPN entry server (ProtonVN): vd 89.238.156.242 (cho file nl-free-16.protonvpn.udp.ovpn)
OpenVPN exit server (ProtonVN): vd 185.180.13.41 (IP public mà Google thấy)
```

### B1.2. Mục tiêu

Crawl **video YouTube + vietsub** để train model ASR tiếng Việt.

| Pipeline | Endpoint | Cần gì |
|---|---|---|
| Metadata | innertube API (`youtubei/v1/...`) | Fake IP vì datacenter IP bị captcha |
| Subs | timedtext API (`/api/timedtext?lang=vi`) | Fake IP vì rate-limit |
| Audio | googlevideo CDN (`*.googlevideo.com`) | Fake IP vì throttle |

→ Bắt buộc phải đi qua VPN để Google không block.

### B1.3. Vấn đề cốt lõi

Server có **rule admin cứng** (KHÔNG do user tạo):

```bash
# Đặt bởi admin datacenter để bảo vệ SSH/health check:
32760: from 172.16.198.60 lookup 100
32761: from 172.16.198.60 lookup 100
32762: from 172.16.198.60 lookup 100
32763: from 172.16.198.60 lookup 100
32764: from 172.16.198.60 lookup 100
32765: from 172.16.198.60 lookup 100
```

**Logic:**
- Packet có `src=172.16.198.60`
- → kernel match rule này (priority 32760-32765)
- → đi **table 100**
- → table 100 chỉ chứa `default via 172.16.198.1 dev enp3s0`
- → packet ra NIC vật lý → NAT qua gateway → IP public `117.4.246.88`
- → **BYPASS hoàn toàn VPN tunnel `tun0`** dù openvpn đang chạy

**Hậu quả khi KHÔNG fix:**
```bash
# OpenVPN tunnel vẫn lên OK
$ ip route show dev tun0
10.96.0.0/16 dev tun0 proto kernel scope link src 10.96.0.30

# Nhưng traffic user-level vẫn ra IP thật:
$ curl https://ifconfig.me
117.4.246.88            ← IP THẬT, YouTube sẽ block

$ ip route get 142.250.198.142
142.250.198.142 via 10.0.0.1 dev enp3s0 src 172.16.198.60 uid 1001
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                  Kernel vẫn chọn enp3s0 (không qua tun0)
```

→ VPN "chạy trên giấy", toàn bộ effort rotate qua ProtonVPN vô nghĩa.

### B1.4. Vì sao admin set rule cứng

| Lý do | Giải thích |
|---|---|
| **Bảo vệ SSH** | Nếu user start VPN sai cách crash tunnel → mặc định vẫn có route ra internet để SSH hoạt động |
| **Health monitoring** | Datacenter monitor server qua IP thật (ICMP, port check). Nếu server đột ngột không trả lời qua IP thật → alarm |
| **Chống abuse** | Server chia sẻ có nhiều user; mỗi user có "lối thoát" về IP riêng nếu cần debug |

→ Rule cứng này là **thiết kế có chủ đích**, không phải lỗi.

### B1.5. Giải pháp (không phá rule admin): `uidrange` priority cao

Thay vì xóa rule admin (nguy hiểm), thêm 1 rule **priority CAO HƠN** (số priority THẤP hơn) chỉ match UID user:

```bash
100: from all uidrange 1001-1001 lookup main
```

**Kernel xét priority tăng dần** (0 → 100 → ... → 32760):
- Priority 100 match → lookup main → packet qua VPN
- Priority 32760 (admin) bị skip vì packet đã match trước
- **SSH từ root (UID 0)** → KHÔNG match rule 1001 → đi rule admin → SSH không mất

---

## B2. Vì sao cần `setup_vpn_uidrange.sh` (không tự fix được trong code Python)

`vpn_rotator_v4.py` chỉ có thể **check** rule qua `_check_uidrange_rule()`:

```python
# File vpn_rotator_v4.py:
def _check_uidrange_rule(uid=None):
    """Kiểm tra rule 'uidrange <uid>-<uid> lookup main' đã có chưa."""
    # Nếu CHƯA có → in cảnh báo + return False
    # NHƯNG KHÔNG TỰ ADD được vì:
    # - Lệnh `ip rule add` cần CAP_NET_ADMIN
    # - User thường (UID 1001) KHÔNG có capability này
    # - Cần `sudo` → cần nhập password → không auto trong script
```

→ Tách ra script riêng cho admin chạy 1 lần với sudo.

| Approach | Đánh giá |
|---|---|
| Auto-add mỗi lần VPN rotate | ❌ Vi phạm phân quyền, có thể xung đột với rule admin rollback |
| Viết daemon tự check + add | ❌ Over-engineering; setup 1 lần là đủ |
| **Script setup 1 lần với sudo** | **✅ Đúng cách — đây là `setup_vpn_uidrange.sh`** |

---

## B3. Từng địa chỉ IP xuất hiện trong script

### B3.1. `172.16.198.60` — IP local của server

| Thuộc tính | Giá trị |
|---|---|
| Loại | IP private (RFC 1918, class B) |
| Gán cho | Card mạng vật lý `enp3s0` |
| Source IP | Mọi packet user-space tạo ra đều có `src=172.16.198.60` |
| NAT | Router datacenter NAT thành `117.4.246.88` khi ra internet |

**Xuất hiện trong script** ở các dòng 6, 52, 107, 171 — trong warning/giải thích, KHÔNG phải lệnh thao tác:

```bash
# Dòng 6:
"Server có sẵn rule 'from 172.16.198.60 lookup 100' (priority 32760-32765)"

# Dòng 52:
"→ Packet từ UID ${USER_UID} sẽ bị rule 'from 172.16.198.60' bắt → bypass VPN."

# Dòng 107 (test instruction):
"# phải ra IP VPN (không phải 172.16.198.60)"

# Dòng 171:
"Server có sẵn 6 rules 'from 172.16.198.60 lookup 100' (priority 32760-32765)"
```

**Vai trò:** Tham chiếu để user hiểu "đây là IP bị rule admin bắt". Script KHÔNG sửa IP này.

### B3.2. `100` (lookup table 100 — table của rule admin)

| Thuộc tính | Giá trị |
|---|---|
| Loại | Routing table ID (số nguyên 1-255) |
| Tạo bởi | Admin (không phải user) |
| Nội dung | `default via 172.16.198.1 dev enp3s0` |
| Mục đích | Ép traffic đi qua gateway datacenter → ra IP `117.4.246.88` |

**Vai trò trong script:** nhắc đến trong comment giải thích. User không thể sửa table này từ account thường.

### B3.3. `172.16.198.1` — Gateway datacenter (KHÔNG xuất hiện literal)

- Default gateway của server trong LAN datacenter
- NAT `172.16.198.60` → `117.4.246.88` khi ra internet
- Là đích đến trong `default via 172.16.198.1 dev enp3s0` của table 100
- Script nhắc đến ngầm qua "table 100" và "enp3s0"

### B3.4. `1001` (UID của user = `${USER_UID}`)

```bash
# Dòng 27:
USER_UID="${SUDO_UID:-$(id -u)}"  # UID của user gốc (khi chạy qua sudo)
```

| Thuộc tính | Giá trị |
|---|---|
| Loại | Linux user ID (số nguyên) |
| Cách lấy | `$(id -u)` (không qua sudo) hoặc `$SUDO_UID` (qua sudo) |
| Mục đích | Để rule `uidrange 1001-1001` match đúng user |

**Tại sao dùng UID thay vì IP:**

| Selector | Ưu | Nhược |
|---|---|---|
| `from 172.16.198.60` | Đơn giản | IP có thể đổi (DHCP/NAT); match cả root → mất SSH |
| `iif lo` | Match loopback | Không match traffic ra internet |
| **`uidrange 1001-1001`** | **Cố định theo user; KHÔNG match root** | |

→ `uidrange` chỉ match UID = 1001 → chỉ user `hientran` mới đi VPN; root vẫn đi rule admin → SSH an toàn.

### B3.5. `100` (priority của rule user)

```bash
# Dòng 11:
"→ packet từ UID hiện tại match rule uidrange TRƯỚC (priority 100 < 32760)"

# Dòng 76 (lệnh thực tế):
ip rule add uidrange "${USER_UID}-${USER_UID}" priority 100 lookup main
```

**Tại sao chọn priority 100 (không phải 200, 1000, 32764):**

| Priority | Phân tích |
|---|---|
| **100** | ✅ Cách xa admin (32760), luôn match trước, không có tie-break, đủ thấp cho rule future cao hơn đều thua |
| 1000 | ⚠️ Vẫn thắng admin NHƯNG nếu admin thêm rule priority < 1000 → user thua |
| 32764 (sát admin) | ❌ Kernel tie-break giữa rule cùng priority không deterministic → race condition |

**Cơ chế kernel:** priority càng THẤP = xét TRƯỚC. Rule nào MATCH ĐẦU TIÊN thì DỪNG (first-match wins).

### B3.6. `main` (lookup table)

```bash
ip rule add uidrange "${USER_UID}-${USER_UID}" priority 100 lookup main
                                                      ^^^^^
```

**Tại sao `lookup main` (không phải `lookup 100` hay custom):**

| Table | Nội dung | Vai trò |
|---|---|---|
| `100` | `default via 172.16.198.1 dev enp3s0` | Admin — SSH qua IP thật |
| **`main`** | `0.0.0.0/1 via 10.96.0.1 dev tun0` + `128.0.0.0/1 via 10.96.0.1 dev tun0` + `default via 172.16.198.1 dev enp3s0 metric 100` | **User — qua VPN** |
| custom | Phải tự maintain | Không cần |

→ `main` có sẵn `0.0.0.0/1 via 10.96.0.1 dev tun0` do OpenVPN tự add khi tunnel lên → tận dụng là tốt nhất.

### B3.7. `0.0.0.0/1` và `128.0.0.0/1` — OpenVPN redirect-gateway trick (nhắc đến trong comment)

```bash
# Dòng 12:
"→ đi main table → dùng `0.0.0.0/1 + 128.0.0.0/1 via tun0` → qua VPN ✓"

# Dòng 178-179:
"match TRƯỚC rule 'from 172.16.198.60' (priority 32760) → packet đi main table → có
'0.0.0.0/1 + 128.0.0.0/1 via tun0' → qua VPN."
```

| Thuộc tính | Giá trị |
|---|---|
| Phạm vi | `0.0.0.0/1` = 0.x.x.x → 127.x.x.x (nửa thấp IPv4); `128.0.0.0/1` = 128.x.x.x → 255.x.x.x (nửa cao) |
| Đặc điểm | Hai CIDR `/1` cộng lại = toàn bộ IPv4, match **chồng lấn** |
| Metric | 0 (mặc định OpenVPN) → thắng `default` metric 100 |

**Tại sao OpenVPN chia làm `/1` thay vì dùng `default`:**
- `default via ... metric 100` đã có sẵn (do admin)
- OpenVPN KHÔNG thể xóa route admin (cần root)
- → OpenVPN thêm 2 route `/1` mới (metric 0)
- → Longest-prefix match: `/1` thắng `/0` → traffic đi `tun0`

**Vai trò trong script:** không trực tiếp xuất hiện trong lệnh, nhưng nhắc đến để giải thích "vì sao đi main table là qua VPN".

### B3.8. `10.96.0.1` — Gateway tunnel (KHÔNG trong script)

- Local gateway của tunnel ProtonVPN
- Packet match `0.0.0.0/1 via 10.96.0.1 dev tun0` → forward tới gateway tunnel
- OpenVPN process xử lý tiếp → ra entry server → exit server

### B3.9. `117.4.246.88` — IP public thật (xuất hiện qua comment ngầm)

```bash
# Dòng 107:
"# phải ra IP VPN (không phải 172.16.198.60)"
# (ý là: phải ra IP VPN, không phải IP thật 117.4.246.88)
```

| Thuộc tính | Giá trị |
|---|---|
| Loại | IP public (routeable trên internet) |
| Gán bởi | Datacenter/ISP cho server |
| Hiển thị | `curl https://ifconfig.me` khi VPN KHÔNG hoạt động đúng |

→ IP này là kết quả xấu mà setup CẦN tránh (Google sẽ block IP datacenter này).

### B3.10. `185.180.13.41` (IP VPN điển hình) — không có trong script nhưng là kết quả mong muốn

- IP public mà Google thấy KHI VPN hoạt động đúng
- Thay đổi theo server ProtonVPN (NL, US, JP, ...)

| Server file | Entry IP | Exit IP (đại diện) |
|---|---|---|
| `nl-free-16.protonvpn.udp.ovpn` | 89.39.107.185 | IP NL nào đó (vd 185.x.x.x) |
| `ca-free-16.protonvpn.udp.ovpn` | 89.238.156.242 | IP CA nào đó |
| `jp-free-6.protonvpn.udp.ovpn` | 138.199.22.103 | IP JP nào đó |

→ Mỗi lần rotate, exit IP thay đổi → Google không nhận diện pattern → bypass rate-limit.

---

## B4. So sánh Selector cho rule (tại sao `uidrange` thay vì `from IP`)

| Selector | Match | Vấn đề |
|---|---|---|
| `from 172.16.198.60` | Packet có src IP server | IP thay đổi (DHCP); match cả root → SSH bị ảnh hưởng |
| `from all iif lo` | Loopback only | YouTube request không đi qua loopback |
| `uidrange 0-0` (root) | Root process only | SSH từ root đi tunnel → mất kết nối nếu tunnel chết |
| **`uidrange 1001-1001` (user)** | **User-level process only** | **Cố định, an toàn, đúng mục tiêu** |

**Rule admin dùng `from 172.16.198.60` → match cả root → SSH an toàn.**
**Rule user dùng `uidrange 1001-1001` → chỉ match user → an toàn cho root.**

→ Hai rule cùng tồn tại, mỗi cái match đúng đối tượng của nó.

---

## B5. Setup thực tế đã chạy trên server (verified)

### B5.1. Trước khi setup

```bash
$ ip rule show
0:    from all lookup local
32760: from 172.16.198.60 lookup 100
32761: from 172.16.198.60 lookup 100
32762: from 172.16.198.60 lookup 100
32763: from 172.16.198.60 lookup 100
32764: from 172.16.198.60 lookup 100
32765: from 172.16.198.60 lookup 100
32766: from all lookup main
32767: from all lookup default
```

→ KHÔNG CÓ rule uidrange. Mọi packet từ UID 1001 sẽ đi table 100 → IP thật.

### B5.2. Chạy setup

```bash
$ sudo bash setup_vpn_uidrange.sh add
[STEP 1/3] Add rule: ip rule add uidrange 1001-1001 priority 100 lookup main
[STEP 2/3] Verify (rule đã có) ✓
[STEP 3/3] Setcap cap_net_admin+ep /bin/ip
          ✓ Done
```

### B5.3. Sau khi setup

```bash
$ ip rule show
0:    from all lookup local
100:  from all uidrange 1001-1001 lookup main    ← MỚI ✓
32760: from 172.16.198.60 lookup 100             ← admin (vẫn còn)
...
32767: from all lookup default
```

### B5.4. Test crawler qua VPN

```bash
$ curl https://ifconfig.me
185.180.13.41     ← IP VPN ✓ (đã đổi từ IP thật 117.4.246.88)

$ ip route get 142.250.198.142
142.250.198.142 via 10.96.0.1 dev tun0 src 10.96.0.30 uid 1001
                  ^^^^^^^^^^^^^^^^^^^^^^^^
                  ĐÃ ĐI QUA tun0 ✓
```

### B5.5. Test SSH từ root vẫn đi IP thật

```bash
$ sudo -i
# UID 0 không match rule 1001 → vẫn đi rule admin
$ curl --interface 172.16.198.60 https://ifconfig.me
117.4.246.88     ← IP thật ✓ (SSH an toàn)
```

---

## B6. Flow IP khi đi đúng qua VPN (sau setup)

```
USER PROCESS (uid=1001)
    │ connect(142.250.198.142)  [Google IP]
    ↓
PACKET src=172.16.198.60 uid=1001
    ↓
KERNEL tra ip rule theo priority tăng dần:
    ├─ 0:    lookup local → SKIP (dst không phải local IP)
    ├─ 100:  uidrange 1001-1001 lookup main → MATCH ✓
    │       → LOAD MAIN TABLE
    ↓
MAIN TABLE:
    ├─ 172.16.198.0/24 dev enp3s0 → không match dst
    ├─ 10.96.0.0/16 dev tun0 → không match
    ├─ Bypass routes 142.250.0.0/15 → đã bị XÓA bởi rotator
    ├─ 0.0.0.0/1 via 10.96.0.1 dev tun0 → MATCH ✓ (longest prefix)
    ↓
PACKET gửi qua tun0
    │ src cập nhật thành 10.96.0.30 (IP trong tunnel)
    ↓
OPENVPN PROCESS forward
    ↓
PROTONVPN entry server (vd 89.39.107.185 cho nl-free-16)
    ↓
PROTONVPN exit server (vd 185.180.13.41)
    │ src IP public ra internet
    ↓
GOOGLE thấy IP 185.180.13.41 (IP VPN) → match VPN reputation ✓ → không block
```

---

## B7. IP KHÔNG xuất hiện trong flow khi OK

| IP | Khi nào xuất hiện | Vai trò |
|---|---|---|
| ~~`117.4.246.88`~~ | Khi rule uidrange THIẾU hoặc rotator fail | IP thật bị lộ → Google block |
| ~~`172.16.198.1`~~ | Khi traffic bypass VPN (không mong muốn) | Gateway datacenter, chỉ nên dùng cho SSH |

---

## B8. Prerequisite bắt buộc cho toàn hệ thống

| Yếu tố | Bắt buộc? | Nếu thiếu |
|---|---|---|
| OpenVPN `.ovpn` file (vd `nl-free-16.protonvpn.udp.ovpn`) | ✅ | Không có gì để connect |
| Auth ProtonVPN (`./proton_config/auth.txt`) | ✅ | Handshake fail |
| `setcap cap_net_admin+ep /usr/sbin/openvpn` | ✅ | User start openvpn không được |
| **`setcap cap_net_admin+ep /bin/ip`** | ✅ (v15+) | Không xóa được bypass routes → vẫn lộ IP |
| **`uidrange` rule priority 100** | ✅ (v15+) | Traffic đi rule admin → bypass VPN |
| IP ProtonVPN trong `.ovpn` (vd `89.39.107.185`) | ✅ | OpenVPN connect fail |

→ `setup_vpn_uidrange.sh` giải quyết **2 prerequisite cuối**:
1. **Rule `uidrange 1001-1001 priority 100 lookup main`** — bypass rule admin
2. **`setcap cap_net_admin+ep /bin/ip`** — cho phép user xóa bypass routes

Không có 1 trong 2 → traffic ra IP thật → Google block → crawler fail thầm lặng.

---

## B9. Cách dùng script

```bash
# Add rule (chạy 1 lần duy nhất với sudo):
sudo bash setup_vpn_uidrange.sh add

# Check trạng thái (không cần sudo):
bash setup_vpn_uidrange.sh check

# Remove rule (rollback nếu cần):
sudo bash setup_vpn_uidrange.sh remove

# In hướng dẫn:
bash setup_vpn_uidrange.sh help
```

**Sau khi `add`, verify:**

```bash
# Rule đã có?
ip rule show | grep "uidrange 1001-1001"

# IP đã đổi qua VPN chưa?
curl https://ifconfig.me    # phải ra IP ProtonVPN (vd 185.x.x.x), KHÔNG phải 117.4.246.88

# SSH từ root còn OK?
sudo -i
curl --interface 172.16.198.60 https://ifconfig.me  # vẫn phải ra IP thật 117.4.246.88
```

---

## B10. Liên kết với các file khác

| File | Vai trò với setup này |
|---|---|
| `setup_vpn_uidrange.sh` | Script setup hiện tại (Phần B giải thích file này) |
| `vpn_rotator_v4.py` | Rotate VPN + check rule uidrange có không (qua `_check_uidrange_rule()`) |
| `youtube_researcher_audio_subs_multi_rotator_v16.py` | Crawler gọi rotator, KHÔNG tự sửa rule |
| `youtube_researcher_audio_subs_multi_rotator_v13.py` | Dùng cách cũ (in-app proxy) — KHÔNG cần uidrange rule (Phần A so sánh) |
| `run_crawl_v16.sh` | Check rule + cảnh báo user trước khi crawler chạy |

→ `setup_vpn_uidrange.sh` là prerequisite cho v15+ (system routing). V13 (proxy routing) vẫn chạy được nếu thiếu rule này vì proxy không qua `ip rule`.

---

# KẾT LUẬN

| | v13 | v15/v16 |
|---|---|---|
| **Cần setup_vpn_uidrange.sh?** | ❌ Không | ✅ Có |
| **Cần setcap /bin/ip?** | ❌ Không | ✅ Có |
| **Cần setcap openvpn?** | ✅ Có | ✅ Có |
| **Cách bypass rule admin `from <IP> lookup 100`?** | Dùng in-app proxy (kernel không đụng rule) | Cần uidrange rule priority cao hơn admin |
| **Robust cho subs/metadata engine?** | ⚠️ Hay fail | ✅ Tốt |
| **Robust cho ffmpeg subprocess?** | ❌ Lộ IP | ✅ An toàn |

→ **V13 dùng khi:** server không có rule admin cứng, hoặc bạn chấp nhận subs/metadata chất lượng thấp hơn, hoặc không muốn chạm vào admin setup.

→ **V15/v16 dùng khi:** muốn crawl chất lượng cao nhất (subs + metadata + audio đều ổn định), cần handle subprocess (ffmpeg, aria2c), và đã có quyền admin để chạy `setup_vpn_uidrange.sh` 1 lần.

→ **Script `setup_vpn_uidrange.sh` chính là prerequisite cho v15+/v16** — nó giải quyết 2 vấn đề: bypass rule admin cứng (`uidrange priority 100`) và cho phép user xóa bypass routes (`setcap /bin/ip`). Không có script này → traffic ra IP thật → Google block → crawler fail.
