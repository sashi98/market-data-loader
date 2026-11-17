# tests/test_login_wait_for_backend.py -- TMT-MDL-BUG-0010
# After a stack restart the backend comes up a minute or two after the scheduler: an unreachable login is retried
# every 30 s (~5 min) instead of waiting the whole 15-minute cycle retry; a real login failure is not retried.
# No network: auth_client.login and requests are faked. Run: python tests/test_login_wait_for_backend.py (or pytest)

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests  # noqa: E402
from core import auth_client  # noqa: E402
import bhavcopy_scheduler_main as sched  # noqa: E402

ENV = {"TMT_APP_BASE_URL": "http://tmt:8080", "TMT_ADMIN_USER_ID": "admin", "TMT_ADMIN_PASSWORD": "x"}


def _with_login(outcomes):
    """Replaces auth_client.login: each call takes the next outcome (an exception to raise, or a token)."""
    calls = []

    def fake(url, user, pw):
        calls.append(1)
        o = outcomes[len(calls) - 1]
        if isinstance(o, Exception):
            raise o
        return o
    return fake, calls


def _run(outcomes, attempts=10):
    original = auth_client.login
    fake, calls = _with_login(outcomes)
    sleeps = []
    auth_client.login = fake
    try:
        try:
            return sched._login_waiting_for_backend(ENV, attempts=attempts, delay_seconds=30, sleep=sleeps.append), calls, sleeps
        except auth_client.AuthError as e:
            return e, calls, sleeps
    finally:
        auth_client.login = original


def test_w1_backend_comes_up_on_the_third_try():
    down = auth_client.BackendUnreachableError("connection refused")
    result, calls, sleeps = _run([down, down, "TOKEN"])
    assert result == "TOKEN" and len(calls) == 3 and sleeps == [30, 30]


def test_w2_real_login_failure_is_not_retried():
    result, calls, sleeps = _run([auth_client.AuthError("Login failed: Scheduled maintenance is underway")])
    assert isinstance(result, auth_client.AuthError) and len(calls) == 1 and sleeps == []


def test_w3_still_down_after_all_attempts_gives_up_to_the_15_minute_retry():
    down = auth_client.BackendUnreachableError("connection refused")
    result, calls, sleeps = _run([down] * 4, attempts=4)
    assert isinstance(result, auth_client.BackendUnreachableError) and len(calls) == 4 and len(sleeps) == 3


class _Resp:
    def __init__(self, status, body=None):
        self.status_code, self._body = status, body

    def json(self):
        if self._body is None:
            raise ValueError("no json")
        return self._body


def _login_with(response_or_exc):
    original = requests.post

    def fake_post(*a, **k):
        if isinstance(response_or_exc, Exception):
            raise response_or_exc
        return response_or_exc
    requests.post = fake_post
    try:
        auth_client.login("http://tmt:8080", "admin", "x")
    except auth_client.AuthError as e:
        return e
    finally:
        requests.post = original


def test_w4_what_counts_as_unreachable():
    assert isinstance(_login_with(requests.exceptions.ConnectionError("refused")), auth_client.BackendUnreachableError)
    assert isinstance(_login_with(_Resp(502)), auth_client.BackendUnreachableError)
    assert isinstance(_login_with(_Resp(503)), auth_client.BackendUnreachableError)
    # a JSON answer is the backend talking: maintenance / bad credentials are real failures, not "unreachable"
    e = _login_with(_Resp(503, {"success": False, "message": "Scheduled maintenance is underway"}))
    assert isinstance(e, auth_client.AuthError) and not isinstance(e, auth_client.BackendUnreachableError)
    e = _login_with(_Resp(500))
    assert isinstance(e, auth_client.AuthError) and not isinstance(e, auth_client.BackendUnreachableError)


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
