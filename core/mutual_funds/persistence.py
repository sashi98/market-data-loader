# core/mutual_funds/persistence.py -- TMT-MDL-US-0007
#
# Writes one parsed AMFI file into mf_amc / mf_scheme / mf_nav in ONE transaction (all or nothing: a power cut or a
# dropped connection never leaves half a day), and records each scheduler step in mf_scheduler_run.
# Re-running the same file changes nothing (upserts; NAV rows ON CONFLICT DO NOTHING).

from datetime import datetime

from psycopg2.extras import execute_values

from core.mutual_funds.amfi_nav import is_active


class MfPersistenceError(Exception):
    pass


def start_run(conn, step):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO mf_scheduler_run (step, started_at, status) VALUES (%s, %s, 'RUNNING') RETURNING id",
                    (step, datetime.now()))
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def finish_run(conn, run_id, status, message=None, **counts):
    cols = ["finished_at = %(finished_at)s", "status = %(status)s", "message = %(message)s"]
    params = {"finished_at": datetime.now(), "status": status, "message": (message or "")[:2000], "id": run_id}
    for k in ("latest_nav_date", "amcs", "schemes", "new_schemes", "inactive_schemes", "nav_rows", "file_bytes",
              "file_sha256"):
        if k in counts:
            cols.append(f"{k} = %({k})s")
            params[k] = counts[k]
    with conn.cursor() as cur:
        cur.execute(f"UPDATE mf_scheduler_run SET {', '.join(cols)} WHERE id = %(id)s", params)
    conn.commit()


def close_abandoned_runs(conn):
    """A RUNNING row left by a process killed mid-run (power cut, restart) is marked ABANDONED at startup."""
    with conn.cursor() as cur:
        cur.execute("UPDATE mf_scheduler_run SET status = 'ABANDONED', finished_at = %s, "
                    "message = 'process stopped before the run finished; redone by the next run' "
                    "WHERE status = 'RUNNING'", (datetime.now(),))
        n = cur.rowcount
    conn.commit()
    return n


def last_success(conn, step):
    """(finished_at, latest_nav_date) of the latest SUCCESS run of a step, or (None, None)."""
    with conn.cursor() as cur:
        cur.execute("SELECT finished_at, latest_nav_date FROM mf_scheduler_run WHERE step = %s AND status = 'SUCCESS' "
                    "ORDER BY finished_at DESC LIMIT 1", (step,))
        row = cur.fetchone()
    return (row[0], row[1]) if row else (None, None)


def stored_nav_date(conn):
    """NAV date of the latest successfully loaded file (the file's common date -- not MAX over schemes: overnight /
    liquid funds publish NAVs for weekend days in advance, which would make an incomplete weekday look loaded)."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(latest_nav_date) FROM mf_scheduler_run WHERE step = 'AMFI_NAV' AND status = 'SUCCESS'")
        row = cur.fetchone()
    return row[0] if row else None


def save_nav_file(conn, parsed, run_date):
    """
    Upserts fund houses, schemes and NAVs of one parsed file in one transaction. Returns counts. A scheme missing
    from today's file is not touched (kept as it was); one present but stale / without NAV is marked inactive.
    """
    rows = parsed.rows
    latest = parsed.latest_nav_date
    now = datetime.now()
    try:
        with conn.cursor() as cur:
            amcs = sorted({r.amc for r in rows})
            execute_values(cur, "INSERT INTO mf_amc (name, created_at) VALUES %s ON CONFLICT (name) DO NOTHING",
                           [(a, now) for a in amcs])
            cur.execute("SELECT name, id FROM mf_amc")
            amc_id = dict(cur.fetchall())

            scheme_values = []
            for r in rows:
                active = is_active(r, latest)
                scheme_values.append((r.scheme_code, amc_id[r.amc], r.scheme_name, r.scheme_type, r.category,
                                      r.asset_class, r.plan, r.option_name, r.isin_growth, r.isin_reinvest,
                                      r.nav, r.nav_date, active, run_date, run_date, now))
            inserted = execute_values(cur, """
                INSERT INTO mf_scheme (scheme_code, amc_id, scheme_name, scheme_type, category, asset_class, plan,
                                       option_name, isin_growth, isin_reinvest, latest_nav, latest_nav_date, active,
                                       first_seen, last_seen, updated_at)
                VALUES %s
                ON CONFLICT (scheme_code) DO UPDATE SET
                    amc_id = EXCLUDED.amc_id, scheme_name = EXCLUDED.scheme_name, scheme_type = EXCLUDED.scheme_type,
                    category = EXCLUDED.category, asset_class = EXCLUDED.asset_class, plan = EXCLUDED.plan,
                    option_name = EXCLUDED.option_name,
                    isin_growth = COALESCE(EXCLUDED.isin_growth, mf_scheme.isin_growth),
                    isin_reinvest = COALESCE(EXCLUDED.isin_reinvest, mf_scheme.isin_reinvest),
                    latest_nav = COALESCE(EXCLUDED.latest_nav, mf_scheme.latest_nav),
                    latest_nav_date = COALESCE(EXCLUDED.latest_nav_date, mf_scheme.latest_nav_date),
                    active = EXCLUDED.active, last_seen = EXCLUDED.last_seen, updated_at = EXCLUDED.updated_at
                RETURNING (xmax = 0)
            """, scheme_values, page_size=1000, fetch=True)
            new_schemes = sum(1 for (is_new,) in inserted if is_new)

            nav_values = [(r.scheme_code, r.nav_date, r.nav) for r in rows if r.nav is not None and r.nav_date]
            added = execute_values(cur, "INSERT INTO mf_nav (scheme_code, nav_date, nav) VALUES %s "
                                        "ON CONFLICT (scheme_code, nav_date) DO NOTHING RETURNING 1",
                                   nav_values, page_size=2000, fetch=True)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise MfPersistenceError(f"Saving the AMFI NAV file failed (nothing was written): {e}") from e
    inactive = sum(1 for v in scheme_values if not v[12])
    return {"amcs": len(amcs), "schemes": len(rows), "new_schemes": new_schemes, "inactive_schemes": inactive,
            "nav_rows": len(added), "latest_nav_date": latest}


# ── NAV history back-fill (TMT-MDL-US-0007 item 5) ─────────────────────────────────────────────────────────────────

STEP_HISTORY = "AMFI_NAV_HISTORY"


def done_history_chunks(conn):
    """(from_date, to_date) of every back-fill chunk already loaded -- a restarted back-fill skips them."""
    with conn.cursor() as cur:
        cur.execute("SELECT message, latest_nav_date FROM mf_scheduler_run WHERE step = %s AND status = 'SUCCESS'",
                    (STEP_HISTORY,))
        out = set()
        for message, to_date in cur.fetchall():
            if message and message.startswith("from=") and to_date:
                out.add((message[5:15], to_date.isoformat()))
    return out


def save_nav_history(conn, rows):
    """NAVs of a history chunk for schemes TMT already knows (others -- wound-up schemes -- are skipped). One
    transaction; existing (scheme, date) rows are kept. Returns the number of NAV rows added."""
    values = [(r.scheme_code, r.nav_date, r.nav) for r in rows if r.nav is not None and r.nav_date]
    if not values:
        return 0
    try:
        with conn.cursor() as cur:
            added = execute_values(cur, """
                INSERT INTO mf_nav (scheme_code, nav_date, nav)
                SELECT v.c, v.d, v.n FROM (VALUES %s) AS v (c, d, n)
                  JOIN mf_scheme s ON s.scheme_code = v.c
                ON CONFLICT (scheme_code, nav_date) DO NOTHING RETURNING 1
            """, values, template="(%s::integer, %s::date, %s::numeric)", page_size=5000, fetch=True)
        conn.commit()
        return len(added)
    except Exception as e:
        conn.rollback()
        raise MfPersistenceError(f"Saving NAV history failed (nothing written for this chunk): {e}") from e


# ── Scheme portfolios (TMT-MDL-US-0008) ─────────────────────────────────────────────────────────────────────────────

STEP_PORTFOLIO = "MF_PORTFOLIO"


def portfolio_file_unchanged(conn, amc, month_end, url, sha):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM mf_portfolio_file WHERE amc_name = %s AND month_end = %s AND source_url = %s "
                    "AND status = 'SUCCESS' AND file_sha256 = %s", (amc, month_end, url[:1000], sha))
        return cur.fetchone() is not None


def save_portfolio_file(conn, amc, month_end, url, sha, schemes):
    """One file's schemes and equity holdings in one transaction (a reloaded file replaces its schemes' holdings).
    Returns (schemes saved, holdings saved)."""
    now = datetime.now()
    n_hold = 0
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO mf_portfolio_file (amc_name, month_end, source_url, file_sha256, status, schemes, holdings, message, loaded_at)
                VALUES (%s, %s, %s, %s, 'SUCCESS', %s, %s, NULL, %s)
                ON CONFLICT (amc_name, month_end, source_url) DO UPDATE SET file_sha256 = EXCLUDED.file_sha256,
                    status = 'SUCCESS', schemes = EXCLUDED.schemes, holdings = EXCLUDED.holdings, message = NULL,
                    loaded_at = EXCLUDED.loaded_at
                RETURNING id
            """, (amc, month_end, url[:1000], sha, len(schemes), sum(len(s.holdings) for s in schemes), now))
            file_id = cur.fetchone()[0]
            for sp in schemes:
                cur.execute("""
                    INSERT INTO mf_portfolio_scheme (amc_name, month_end, scheme_name, scheme_type, file_id, holdings, pct_total)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (amc_name, month_end, scheme_name) DO UPDATE SET scheme_type = EXCLUDED.scheme_type,
                        file_id = EXCLUDED.file_id, holdings = EXCLUDED.holdings, pct_total = EXCLUDED.pct_total
                    RETURNING id
                """, (amc, month_end, sp.scheme_name, sp.scheme_type, file_id, len(sp.holdings), sp.pct_total))
                scheme_id = cur.fetchone()[0]
                cur.execute("DELETE FROM mf_holding WHERE scheme_id = %s", (scheme_id,))
                if sp.holdings:
                    execute_values(cur, """
                        INSERT INTO mf_holding (scheme_id, month_end, isin, instrument_name, industry, quantity,
                                                market_value_lakhs, pct_of_aum) VALUES %s
                    """, [(scheme_id, month_end, h.isin, h.name, h.industry, h.quantity, h.market_value_lakhs, h.pct_of_aum)
                          for h in sp.holdings], page_size=1000)
                    n_hold += len(sp.holdings)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise MfPersistenceError(f"Saving the portfolio file failed (nothing written for it): {e}") from e
    return len(schemes), n_hold


def record_portfolio_failure(conn, amc, month_end, url, message):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO mf_portfolio_file (amc_name, month_end, source_url, status, message, loaded_at)
            VALUES (%s, %s, %s, 'FAILED', %s, %s)
            ON CONFLICT (amc_name, month_end, source_url) DO UPDATE SET status = CASE WHEN mf_portfolio_file.status = 'SUCCESS'
                THEN 'SUCCESS' ELSE 'FAILED' END, message = EXCLUDED.message, loaded_at = EXCLUDED.loaded_at
        """, (amc, month_end, url[:1000], message[:2000], datetime.now()))
    conn.commit()


PORTFOLIO_TABLES = ("mf_portfolio_file", "mf_portfolio_scheme", "mf_holding")


def missing_portfolio_tables(conn):
    """The portfolio tables (Liquibase 035, created by the tmt backend on start) not in the database yet."""
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = current_schema() "
                    "AND table_name = ANY(%s)", (list(PORTFOLIO_TABLES),))
        found = {r[0] for r in cur.fetchall()}
    return [t for t in PORTFOLIO_TABLES if t not in found]


def portfolio_amcs_loaded(conn, month_end):
    """Fund houses with at least one successfully loaded file for the month."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT amc_name FROM mf_portfolio_file WHERE month_end = %s AND status = 'SUCCESS'", (month_end,))
        return {r[0] for r in cur.fetchall()}
