#!/usr/bin/env python3
"""
Test cycle mode logic KHÔNG cần VPN thật (mock _connect_server/_disconnect).
"""
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

# Tắt network calls
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("test")

from vpn_rotator import VPNRotator


def make_mock_rotator(real_ip_cycle: int = 0, rotate_every: int = 0):
    """Tạo VPNRotator + monkey-patch _connect_server và _disconnect để KHÔNG gọi openvpn."""

    rotator = VPNRotator(
        config_dir="/home/hientran/sythetic_crawl_data/proton_config",
        rotate_every=rotate_every,
        strategy="sequential",
        real_ip_cycle=real_ip_cycle,
    )

    # Track idx để simulate IP change
    state = {"connect_count": 0, "disconnect_count": 0, "real_ip_requests": 0}

    def mock_connect(idx: int, retry: int = 0) -> bool:
        """Mock: chỉ set state, KHÔNG start openvpn."""
        rotator._current_idx = idx
        rotator._current_ip = f"1.2.3.{idx+1}"  # fake IP
        rotator._current_pid = 12345 + idx
        rotator._request_count = 0
        rotator._usage_count[idx] = rotator._usage_count.get(idx, 0) + 1
        state["connect_count"] += 1
        log.info(f"  [mock] connect #{state['connect_count']} → idx={idx} (IP={rotator._current_ip})")
        return True

    def mock_disconnect():
        """Mock: chỉ set state, KHÔNG kill openvpn."""
        if rotator._current_pid is not None:
            state["disconnect_count"] += 1
            log.info(f"  [mock] disconnect #{state['disconnect_count']} (was idx={rotator._current_idx})")
            rotator._current_pid = None
        rotator._current_idx = None
        rotator._current_ip = None

    rotator._connect_server = mock_connect
    rotator._disconnect = mock_disconnect

    return rotator, state


def print_state(rotator, label):
    """Print trạng thái rotator sau 1 next()."""
    is_real = rotator.is_using_real_ip()
    print(f"  [{label:18}] idx={rotator._current_idx}, ip={rotator._current_ip}, "
          f"req_count={rotator._request_count}, use_real_ip={is_real}")


def test_no_cycle():
    """Test default (real_ip_cycle=0) - KHÔNG cycle, behavior cũ."""
    print("\n=== TEST 1: KHÔNG CYCLE (real_ip_cycle=0) ===")
    rotator, state = make_mock_rotator(real_ip_cycle=0)
    assert rotator.is_using_real_ip() is False
    print(f"Initial state: is_using_real_ip={rotator.is_using_real_ip()}")

    for i in range(1, 6):
        rotator.next()
        print_state(rotator, f"next #{i}")

    assert state["connect_count"] == 1, f"Chỉ nên connect 1 lần, got {state['connect_count']}"
    assert state["disconnect_count"] == 0, f"KHÔNG nên disconnect, got {state['disconnect_count']}"
    assert rotator.is_using_real_ip() is False
    print("✓ TEST 1 OK: KHÔNG cycle, 1 connect, 0 disconnect, no real_ip flag")


def test_cycle_3():
    """Test cycle 3 (= 2 fake + 1 real, lặp lại)."""
    print("\n=== TEST 2: CYCLE=3 (2 fake → 1 real) ===")
    rotator, state = make_mock_rotator(real_ip_cycle=3, rotate_every=0)
    assert rotator.is_using_real_ip() is False

    # Request 1: first connect → idx=0, req_count=1
    rotator.next()
    print_state(rotator, "req #1 (fake)")
    assert rotator._current_idx == 0 and rotator._request_count == 1
    assert not rotator.is_using_real_ip()

    # Request 2: req_count=2 (still fake, < cycle=3)
    rotator.next()
    print_state(rotator, "req #2 (fake)")
    assert rotator._current_idx == 0 and rotator._request_count == 2
    assert not rotator.is_using_real_ip()

    # Request 3: req_count=3 >= cycle=3 → DISCONNECT, set _use_real_ip=True
    rotator.next()
    print_state(rotator, "req #3 (END cycle)")
    assert rotator._current_idx is None, f"VPN phải disconnect ở cycle boundary, got idx={rotator._current_idx}"
    assert rotator._use_real_ip is True
    assert rotator._request_count == 0
    assert state["disconnect_count"] == 1

    # Request 4: đang ở phase real IP → KHÔNG reconnect, trả None, reset _use_real_ip
    rotator.next()
    print_state(rotator, "req #4 (REAL IP)")
    assert rotator._current_idx is None, "Real IP request không được có tunnel"
    assert rotator._use_real_ip is False, "Flag phải reset sau khi consume"
    assert state["disconnect_count"] == 1, "KHÔNG disconnect thêm"

    # Request 5: reconnect VPN (cycle mới)
    rotator.next()
    print_state(rotator, "req #5 (fake again)")
    assert rotator._current_idx is not None, "Phải reconnect VPN cho cycle mới"
    assert rotator._request_count == 1
    assert state["connect_count"] == 2

    # Request 6: req_count=2
    rotator.next()
    print_state(rotator, "req #6 (fake)")
    assert rotator._current_idx is not None and rotator._request_count == 2

    # Request 7: cycle đạt → disconnect
    rotator.next()
    print_state(rotator, "req #7 (END cycle)")
    assert rotator._current_idx is None
    assert rotator._use_real_ip is True
    assert state["disconnect_count"] == 2

    # Request 8: real IP
    rotator.next()
    print_state(rotator, "req #8 (REAL IP)")
    assert rotator._use_real_ip is False
    assert rotator._current_idx is None

    # Request 9: reconnect
    rotator.next()
    print_state(rotator, "req #9 (fake)")
    assert rotator._current_idx is not None
    assert state["connect_count"] == 3

    print(f"\nFinal: connect_count={state['connect_count']}, "
          f"disconnect_count={state['disconnect_count']}")
    print("✓ TEST 2 OK: CYCLE=3 hoạt động đúng pattern 2 fake + 1 real, lặp lại")


def test_cycle_with_rotate():
    """Test cycle 11 + rotate_every=3 → rotate trước khi cycle đạt."""
    print("\n=== TEST 3: CYCLE=11 + rotate_every=3 ===")
    rotator, state = make_mock_rotator(real_ip_cycle=11, rotate_every=3)
    rotator.next()  # req #1, idx=0, req_count=1
    print_state(rotator, "req #1 (fake)")
    rotator.next()  # req #2, idx=0, req_count=2
    print_state(rotator, "req #2 (fake)")
    rotator.next()  # req #3, idx=0, req_count=3 >= rotate_every=3 → ROTATE sang idx=1
    print_state(rotator, "req #3 (rotate)")
    assert rotator._current_idx == 1, f"rotate phải sang idx=1, got {rotator._current_idx}"
    assert rotator._request_count == 0, "request_count reset sau rotate"
    rotator.next()  # req #4
    print_state(rotator, "req #4 (fake)")
    assert rotator._current_idx == 1 and rotator._request_count == 1
    print("✓ TEST 3 OK: rotate_every vẫn hoạt động bên trong cycle")


def test_mark_failed_in_real_phase():
    """Test mark_failed khi đang ở phase real IP → reset flag, KHÔNG rotate."""
    print("\n=== TEST 4: mark_failed trong phase IP thật ===")
    rotator, state = make_mock_rotator(real_ip_cycle=3)
    rotator.next()  # fake
    rotator.next()  # fake
    rotator.next()  # cycle end → disconnect, _use_real_ip=True
    assert rotator._use_real_ip is True
    rotator.next()  # consume flag → _use_real_ip=False
    assert rotator._use_real_ip is False

    # Tạo state cycle mới
    rotator.next()  # fake, idx=?
    rotator.next()  # fake
    rotator.next()  # cycle end → disconnect
    assert rotator._use_real_ip is True

    # Bây giờ trong phase real IP, gọi mark_failed
    rotator.mark_failed(None)
    assert rotator._use_real_ip is False, \
        "mark_failed trong phase real IP phải reset flag (KHÔNG rotate vì không có tunnel)"
    print("✓ TEST 4 OK: mark_failed trong phase real IP reset flag, không rotate")


if __name__ == "__main__":
    try:
        test_no_cycle()
        test_cycle_3()
        test_cycle_with_rotate()
        test_mark_failed_in_real_phase()
        print("\n🎉 ALL TESTS PASSED")
        sys.exit(0)
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        log.exception("Unexpected error")
        sys.exit(1)