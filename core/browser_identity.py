# core/browser_identity.py -- TMT-MDL-BUG-0008
#
# The one browser User-Agent every NSE / BSE download sends. BSE's firewall (Akamai) answers an outdated browser
# version with HTTP 403 "Access Denied": the scheduler sent Chrome/120 and every BSE corporate actions request
# failed from at least 04-Oct-2026 until 17-Nov-2026, while the same request as Chrome/130 returned the CSV.
# When BSE / NSE start answering 403 "Access Denied" again, raise the Chrome version here (one place) -- use a
# version current browsers really send; tests/probe_bse_corpact_403_READONLY.py checks it against BSE.

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
