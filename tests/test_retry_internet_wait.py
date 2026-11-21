# tests/test_retry_internet_wait.py -- TMT-MDL-US-0006
# core.retry.call_with_retry: when the internet is really down, wait until it is back and retry the SAME call
# without using up an attempt; a failure while the internet is fine is counted as before.
# No network: the probe and sleeps are faked. Run: python tests/test_retry_internet_wait.py (or pytest)

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core import retry  # noqa: E402


class _Patch:
    def __init__(self, **kw):
        self.kw, self.old = kw, {}

    def __enter__(self):
        for k, v in self.kw.items():
            self.old[k] = getattr(retry, k)
            setattr(retry, k, v)
        return self

    def __exit__(self, *a):
        for k, v in self.old.items():
            setattr(retry, k, v)


class _FakeTime:
    """time.sleep / time.time / time.strftime without waiting."""
    def __init__(self):
        self.now = 1000.0

    def sleep(self, s):
        self.now += s

    def time(self):
        return self.now

    def strftime(self, fmt):
        return "09:54:00"


DNS = ConnectionError("curl: (6) Could not resolve host: guce.yahoo.com")


def test_r1_outage_waits_then_retries_same_call_without_using_attempts():
    calls, probes, logs = [], iter([False, False, False, True]), []
    # call 1: DNS error, probe says down -> wait (probe: down, down, up) -> call 2 succeeds

    def fn():
        calls.append(1)
        if len(calls) == 1:
            raise DNS
        return "data"
    ft = _FakeTime()
    with _Patch(internet_available=lambda: next(probes), time=ft):
        out = retry.call_with_retry(fn)
    assert out == "data" and len(calls) == 2, calls


def test_r2_wait_logs_down_and_back():
    logs = []
    probes = iter([False, False, True])
    with _Patch(internet_available=lambda: next(probes), time=_FakeTime()):
        waited = retry.wait_until_online(log=logs.append)
    assert waited == 60, waited
    assert logs[0].strip().startswith("[INTERNET DOWN]") and logs[-1].strip().startswith("[INTERNET BACK]"), logs


def test_r3_site_failure_with_internet_up_counts_attempts_as_before():
    calls = []

    def fn():
        calls.append(1)
        raise ConnectionError("curl: (28) Connection timed out after 10000 ms")
    with _Patch(internet_available=lambda: True, time=_FakeTime()):
        try:
            retry.call_with_retry(fn)
            assert False, "should raise"
        except ConnectionError:
            pass
    assert len(calls) == retry.MAX_ATTEMPTS, calls


def test_r4_bad_data_is_never_a_pause():
    calls = []

    def fn():
        calls.append(1)
        raise RuntimeError("GetQuoteApi 404: Not Found")

    def probe():
        raise AssertionError("must not probe for a 404")
    with _Patch(internet_available=probe, time=_FakeTime()):
        try:
            retry.call_with_retry(fn)
        except RuntimeError:
            pass
    assert len(calls) == retry.MAX_ATTEMPTS


def test_r5_what_counts_as_connectivity():
    yes = [DNS, OSError("[Errno 101] Network is unreachable"),
           ConnectionError("HTTPSConnectionPool: Max retries exceeded (Caused by NameResolutionError(...))"),
           ConnectionError("Failed to establish a new connection: [Errno 111] Connection refused")]
    no = [RuntimeError("404: Not Found"), ValueError("No data found, symbol may be delisted"), KeyError("sector")]
    assert all(retry.looks_like_connectivity_error(e) for e in yes)
    assert not any(retry.looks_like_connectivity_error(e) for e in no)
    wrapped = RuntimeError("fetch failed")
    wrapped.__cause__ = OSError("Temporary failure in name resolution")
    assert retry.looks_like_connectivity_error(wrapped)


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"[PASS] {name}")
            except Exception as e:  # noqa: BLE001
                failures += 1
                print(f"[FAIL] {name}: {e!r}")
    print("ALL PASSED" if not failures else f"{failures} FAILED")
    sys.exit(1 if failures else 0)
