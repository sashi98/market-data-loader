# core/db_client.py
#
# Direct Postgres connection for market-data-loader. Parses the JDBC-style
# SPRING_DATASOURCE_URL (e.g. "jdbc:postgresql://localhost:5432/tmt_dev")
# from the shared .env into host/port/dbname, since psycopg2 doesn't
# understand the "jdbc:" prefix that Java's driver expects.
#
# Connections are opened with autocommit=False -- callers (Step 3.3/3.4)
# are expected to explicitly commit() on success or rollback() on failure,
# since NSE and BSE persistence for a given date must each be an
# independent transaction.

from urllib.parse import urlparse
import warnings

import psycopg2

JDBC_PREFIX = "jdbc:"

# Every pd.read_sql(query, conn, ...) call across this project (rollup,
# RSI, MA, adjusted-series persistence, etc.) is handed a raw psycopg2
# connection from get_connection() below rather than a SQLAlchemy engine --
# deliberately, since this project only ever talks to one Postgres database
# directly and doesn't otherwise need SQLAlchemy as a dependency. pandas
# reacts to that on every single read_sql() call with:
#   UserWarning: pandas only supports SQLAlchemy connectable (engine/
#   connection) or database string URI or sqlite3 DBAPI2 connection...
# which is harmless (psycopg2 is a fully-supported DBAPI2 connection).
#
# TMT-MDL-BUG-0003 (2026-09-24) first tried action="once" here (and,
# separately/redundantly, in core/logging_setup.py) to print this exactly
# once per process instead of flooding the log. That did NOT fully work
# (Sashikant, 2026-09-26 -- still saw 4+ repeats in one run) because
# _run_indicator() is fan-out across concurrent worker threads via
# ThreadPoolExecutor(max_workers=2) in bhavcopy_scheduler_main.py
# (TMT-MDL-US-0001) -- CPython's warnings "once" bookkeeping is a plain,
# unlocked dict (the global onceregistry), so two threads hitting
# read_sql() at nearly the same moment can each see "not yet warned" and
# both print before either updates the registry. That's exactly the
# pattern in the reported log (multiple copies of the identical warning,
# including duplicates from the SAME call site).
#
# Fixed here by switching to action="ignore": the filter list itself is
# only written once, at import time, before any indicator threads exist,
# so every later read (from any thread) is a race-free lookup against an
# already-settled filter list -- no shared mutable "have I warned yet?"
# state to race on. This fully and permanently silences the warning
# (rather than surfacing it once) -- if a "prints once per process" notice
# is wanted instead, it needs its own explicit one-time log line (with a
# lock) rather than relying on warnings' own "once" action, which isn't
# safe under this codebase's concurrent indicator execution.
warnings.filterwarnings(
    "ignore",
    message=r"pandas only supports SQLAlchemy connectable.*",
    category=UserWarning,
)


class DbConnectionError(Exception):
    """Raised when the .env datasource URL can't be parsed, or the DB connection fails."""
    pass


def _parse_jdbc_url(jdbc_url):
    """
    "jdbc:postgresql://localhost:5432/tmt_dev" -> (host, port, dbname)
    """
    if not jdbc_url.startswith(JDBC_PREFIX):
        raise DbConnectionError(
            f"Expected SPRING_DATASOURCE_URL to start with '{JDBC_PREFIX}', got: {jdbc_url}"
        )

    stripped = jdbc_url[len(JDBC_PREFIX):]  # "postgresql://localhost:5432/tmt_dev"
    parsed = urlparse(stripped)

    if not parsed.hostname or not parsed.port or not parsed.path:
        raise DbConnectionError(
            f"Could not parse host/port/dbname from SPRING_DATASOURCE_URL: {jdbc_url}"
        )

    dbname = parsed.path.lstrip("/")
    return parsed.hostname, parsed.port, dbname


def get_connection(env_values):
    """
    Opens a new psycopg2 connection using SPRING_DATASOURCE_URL,
    POSTGRES_USER, POSTGRES_PASSWORD from env_values (as returned by
    env_validator.load_and_validate_env()).

    autocommit is False -- caller must commit()/rollback() explicitly.

    Raises DbConnectionError on any failure (bad URL, connection refused,
    auth failure, etc.).
    """
    jdbc_url = env_values["SPRING_DATASOURCE_URL"]
    user = env_values["POSTGRES_USER"]
    password = env_values["POSTGRES_PASSWORD"]

    host, port, dbname = _parse_jdbc_url(jdbc_url)

    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        raise DbConnectionError(
            f"Could not connect to Postgres at {host}:{port}/{dbname} as user '{user}': {e}"
        )


def test_connection(env_values):
    """
    Opens and immediately closes a connection, to verify DB connectivity
    without holding a connection open. Used as a Step 3.1 pre-flight check.

    Raises DbConnectionError on failure. Returns True on success.
    """
    conn = get_connection(env_values)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    finally:
        conn.close()
    return True
