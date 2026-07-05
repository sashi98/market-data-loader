# core/health_client.py
#
# Pre-flight check -- calls TMT's aggregate Actuator health endpoint before
# any credential prompt or network work begins. This endpoint is public
# (no JWT required -- see SecurityConfig PUBLIC_PATTERNS) and aggregates
# TMT's own health (db, diskSpace, ping, ssl) plus stock-py-services
# (via the custom PyServicesHealthIndicator). If either is down, the
# overall status is DOWN.

import requests

HEALTH_PATH = "/actuator/health"
REQUEST_TIMEOUT_SECONDS = 10


class HealthCheckError(Exception):
    """Raised when TMT or one of its dependencies (e.g. stock-py-services) is down."""
    pass


def check_health(tmt_app_base_url):
    """
    Calls GET {tmt_app_base_url}/actuator/health.
    Raises HealthCheckError with a clear message if:
      - the request fails outright (connection error, timeout)
      - the overall status is not "UP"
    Returns the parsed JSON body on success.
    """
    url = tmt_app_base_url.rstrip("/") + HEALTH_PATH

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.exceptions.RequestException as e:
        raise HealthCheckError(
            f"Could not reach TMT at {url} -- is the app running? Error: {e}"
        )

    try:
        body = response.json()
    except ValueError:
        raise HealthCheckError(
            f"TMT health endpoint at {url} returned a non-JSON response "
            f"(HTTP {response.status_code})."
        )

    overall_status = body.get("status")
    if overall_status != "UP":
        down_components = []
        components = body.get("components", {})
        for name, detail in components.items():
            if detail.get("status") != "UP":
                down_components.append(f"{name} ({detail.get('status')})")

        detail_msg = ", ".join(down_components) if down_components else "no component detail available"
        raise HealthCheckError(
            f"TMT health check failed -- overall status: {overall_status}. "
            f"Down component(s): {detail_msg}"
        )

    return body
