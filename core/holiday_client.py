# core/holiday_client.py
#
# Calls TMT's holiday sync endpoint and parses the response into a set of
# python date objects for a given year. Requires ADMIN JWT (see auth_client).
#
# GET {tmt_app_base_url}/api/holidays/sync/{year}
# Headers: Authorization: Bearer <jwt_token>
# Response: List[HolidayDTO] -- date field format is "MMMM dd, yyyy"
#   (e.g. "January 26, 2026" -- confirmed against HolidayDTO.createNewObj(),
#   which formats with DateProvider "MMMM dd, yyyy" then applies a no-op
#   camelCase() transform on an already-capitalized month name).

from datetime import datetime

import requests

REQUEST_TIMEOUT_SECONDS = 15
HOLIDAY_DATE_FORMAT = "%B %d, %Y"  # matches "MMMM dd, yyyy"

# In-run cache -- avoids re-fetching the same year twice in one script run.
_year_cache = {}


class HolidayFetchError(Exception):
    """Raised when the holiday sync call fails or returns unparseable data."""
    pass


def get_holiday_dates_for_year(tmt_app_base_url, jwt_token, year):
    """
    Returns a set of datetime.date objects for all holidays in the given year.
    Cached per year within a single script run.
    Raises HolidayFetchError on any failure (network, auth, parse).
    """
    if year in _year_cache:
        return _year_cache[year]

    url = tmt_app_base_url.rstrip("/") + f"/api/holidays/sync/{year}"
    headers = {"Authorization": f"Bearer {jwt_token}"}

    try:
        response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.exceptions.RequestException as e:
        raise HolidayFetchError(f"Could not reach {url}. Error: {e}")

    if response.status_code == 401:
        raise HolidayFetchError(
            f"401 Unauthorized calling {url} -- JWT may have expired or lacks ADMIN role."
        )
    if response.status_code != 200:
        raise HolidayFetchError(
            f"Unexpected HTTP {response.status_code} calling {url}. Body: {response.text[:300]}"
        )

    try:
        holidays = response.json()
    except ValueError:
        raise HolidayFetchError(f"Non-JSON response from {url}: {response.text[:300]}")

    dates = set()
    for holiday in holidays:
        date_str = holiday.get("date")
        if not date_str:
            continue
        try:
            parsed = datetime.strptime(date_str, HOLIDAY_DATE_FORMAT).date()
        except ValueError:
            raise HolidayFetchError(
                f"Could not parse holiday date '{date_str}' using format "
                f"'{HOLIDAY_DATE_FORMAT}'. Response from {url} may have changed shape."
            )
        dates.add(parsed)

    _year_cache[year] = dates
    return dates
