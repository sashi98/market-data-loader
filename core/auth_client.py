# core/auth_client.py
#
# Prompts for admin credentials and authenticates against TMT's login
# endpoint to obtain a JWT. The JWT is needed for /api/holidays/sync/{year}
# (ROLE_ADMIN only -- see SecurityConfig).
#
# Matches the exact contract of LoginController / LoginRequest / LoginResponse:
#   POST /api/auth/login
#   Body:    {"userId": "...", "password": "..."}
#   200 -->  {success: true,  userId, fullName, roles, token, message, loggedInAt}
#   401 -->  {success: false, message}

import getpass
import requests

LOGIN_PATH = "/api/auth/login"
REQUEST_TIMEOUT_SECONDS = 10


class AuthError(Exception):
    """Raised when login fails -- wrong credentials, non-ADMIN role, or connection error."""
    pass


def prompt_credentials():
    """Prompts for admin userId and password (password input masked)."""
    print("\nAdmin authentication required (needed for /api/holidays/sync/{year}).")
    user_id = input("  Admin userId: ").strip()
    password = getpass.getpass("  Admin password: ")
    return user_id, password


def login(tmt_app_base_url, user_id, password):
    """
    Calls POST {tmt_app_base_url}/api/auth/login.
    Raises AuthError with a clear message on any failure:
      - connection error
      - HTTP 401 (bad credentials)
      - success=true but ADMIN role missing
    Returns the JWT token string on success.
    """
    url = tmt_app_base_url.rstrip("/") + LOGIN_PATH
    payload = {"userId": user_id, "password": password}

    try:
        response = requests.post(url, json=payload, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.exceptions.RequestException as e:
        raise AuthError(f"Could not reach TMT login endpoint at {url}. Error: {e}")

    try:
        body = response.json()
    except ValueError:
        raise AuthError(
            f"Login endpoint at {url} returned a non-JSON response "
            f"(HTTP {response.status_code})."
        )

    if not body.get("success"):
        raise AuthError(f"Login failed: {body.get('message', 'unknown reason')}")

    roles = body.get("roles") or []
    if "ROLE_ADMIN" not in roles and "ADMIN" not in roles:
        raise AuthError(
            f"Login succeeded but user '{user_id}' does not have ADMIN role "
            f"(roles: {roles}). /api/holidays/sync/{{year}} requires ROLE_ADMIN."
        )

    token = body.get("token")
    if not token:
        raise AuthError("Login response marked success=true but no token was returned.")

    return token


def authenticate(tmt_app_base_url):
    """
    Full flow: prompt for credentials, attempt login.
    Returns the JWT token string on success. Raises AuthError on failure.
    """
    user_id, password = prompt_credentials()
    token = login(tmt_app_base_url, user_id, password)
    print(f"  [OK] Authenticated as '{user_id}' with ADMIN role.")
    return token
