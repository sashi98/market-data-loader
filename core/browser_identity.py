# core/browser_identity.py -- TMT-MDL-BUG-0008
#
# The one browser User-Agent every NSE / BSE download sends. BSE's firewall (Akamai) answers an outdated browser
# version with HTTP 403 "Access Denied": the scheduler sent Chrome/120 and every BSE corporate actions request
# failed from at least 04-Oct-2026 until 17-Nov-2026, while the same request as Chrome/130 returned the CSV.
# When BSE / NSE start answering 403 "Access Denied" again, raise the Chrome version here (one place) -- use a
# version current browsers really send; tests/probe_bse_corpact_403_READONLY.py checks it against BSE.
#
# 18-Nov-2026: the User-Agent alone is not enough from the prod container (Linux, Python 3.11, OpenSSL 3.5) -- BSE's
# api.bseindia.com still answered 403 to plain `requests` with any User-Agent, because the firewall also scores the
# TLS handshake. curl_cffi impersonating Chrome got the CSV every time (probe run in tmt-prod-mdl-scheduler). So
# browser_get() below goes through curl_cffi when it is installed (it comes with yfinance; also listed in
# requirements.txt) and falls back to `requests` otherwise.

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)


def is_firewall_block(response):
    """True for the exchange firewall's 403 'Access Denied' page (not an API error of the exchange's own)."""
    if response is None or response.status_code != 403:
        return False
    try:
        return "access denied" in response.text[:500].lower()
    except Exception:  # noqa: BLE001
        return False


FIREWALL_HINT = ("the exchange's firewall answered 'Access Denied' -- it rejects outdated browser versions; raise "
                 "the Chrome version in core/browser_identity.py (BROWSER_USER_AGENT)")


IMPERSONATE = "chrome"  # curl_cffi's newest Chrome profile (TLS fingerprint + matching User-Agent / headers)


def _curl_cffi():
    try:
        from curl_cffi import requests as curl_requests
        return curl_requests
    except ImportError:
        return None


def browser_get(url, headers=None, params=None, timeout=(10, 60), _curl=None):
    """
    GET that looks like a real Chrome to an exchange firewall: curl_cffi impersonating Chrome when installed (the
    User-Agent header is left to curl_cffi so it matches the TLS fingerprint), else requests.get with
    BROWSER_USER_AGENT. Returns a response with .status_code / .content / .text / .headers. Any network failure is
    raised as requests.exceptions.RequestException, so callers keep one except clause either way.
    """
    import requests
    curl = _curl if _curl is not None else _curl_cffi()
    if curl is None:
        return requests.get(url, headers={**(headers or {}), "User-Agent": BROWSER_USER_AGENT}, params=params,
                            timeout=timeout)
    clean = {k: v for k, v in (headers or {}).items() if k.lower() != "user-agent"}
    total = sum(timeout) if isinstance(timeout, (tuple, list)) else timeout
    try:
        return curl.get(url, headers=clean, params=params, timeout=total, impersonate=IMPERSONATE)
    except Exception as e:  # noqa: BLE001 -- curl_cffi's own error classes vary by version
        raise requests.exceptions.ConnectionError(f"{type(e).__name__}: {e}") from e
