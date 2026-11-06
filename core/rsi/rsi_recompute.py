# core/rsi/rsi_recompute.py -- TMT-MDL-US-0005-003
#
# RSI14 (daily / weekly / monthly) with mode_of_recompute, shared by rsi14d_runner, rsi14w_runner and
# rsi14m_runner (each passes its own CONFIG: source table, workbook table, recompute_queue target, and
# whether gaps are checked against the trading calendar -- daily only, as before).
#
#   FULL         every stock, whole history (the behaviour before TMT-MDL-US-0005): compute_rsi14_all().
#   INCREMENTAL  per exchange:
#                  since  = the workbook's latest stored trade_date (the last day, or the still-open week /
#                           month, which the rollup has just rewritten) -- recomputed again, plus anything newer.
#                  anchor = each stock's last stored row BEFORE since, with its avg_gain / avg_loss.
#                  list-A: Wilder's step() continues from the anchor over the stock's source rows after the
#                          anchor date -- exactly what FULL does on those rows, minus the walk through the
#                          years before them.
#                  list-B: stocks queued in recompute_queue for this target (history rebuilt upstream) get a
#                          whole-history recompute, those stocks only.
#                  Fallback to a whole-history recompute of that one stock (logged as A_FALLBACK) when the
#                  short walk could differ from FULL: no stored row yet (new stock), anchor still before the
#                  RSI seed, anchor averages too small for the stored decimals (a stock flat for months), a missing
#                  prev_close, or (daily) a trading-calendar gap that FULL would reseed at.
#                Queue ALL (whole exchange) or an empty workbook -> FULL for that exchange.
#   SHADOW       FULL is written; the INCREMENTAL result is computed first, in memory, and compared with the
#                rows FULL wrote -> recompute_shadow_check + a verdict line.
#
# Crash safety: at the start of every run (any mode) the step queues ALL / RUN_INCOMPLETE for its target and
# deletes it, together with the list-B rows it read, only after both exchanges are written. A run that dies
# midway (part of the rows upserted) therefore makes the next run rebuild everything for this step.
#
# Precision: avg_gain / avg_loss are stored NUMERIC(30, 10) since tmt changelog 026.02.00, so continuing
# from the stored value gives the same RSI as FULL's unrounded walk (at 4 decimals a few-paise stock could
# drift by ~0.25 RSI points). Rows written by FULL before that changelog are still 4-decimal values until
# the next FULL / SHADOW run rewrites them -- so the FIRST SHADOW run after 026.02.00 may report small
# mismatches on very low-priced stocks; from the second run on it must be clean.

import pandas as pd

from core.date_format import fmt_date
from core.recompute import mode as recompute_mode
from core.recompute import queue as recompute_queue
from core.recompute import shadow
from core.rsi.rsi_calculator import compute_rsi14_all
from core.rsi.rsi_continuity import fetch_trading_calendar, build_calendar_index, is_gap
from core.rsi.rsi_math import step, compute_rsi14
from core.rsi.rsi_persistence import upsert_rsi_workbook

EXCHANGES = ["NSE", "BSE"]
A_FALLBACK = "A_FALLBACK"
# Below this the stored (10-decimal) averages may no longer carry FULL's ratio -> whole-history recompute.
TINY_AVERAGE = 1e-6
FULL_HISTORY_BATCH = 300

OUTPUT_COLUMNS = ["security_id", "isin", "exchange", "series", "symbol", "trade_date",
                  "gain", "loss", "avg_gain", "avg_loss", "rsi14"]
SHADOW_VALUE_COLUMNS = ["gain", "loss", "avg_gain", "avg_loss", "rsi14"]
SHADOW_TOLERANCES = {"gain": 0.0001, "loss": 0.0001, "avg_gain": 0.0001, "avg_loss": 0.0001,
                     # RSI14 itself is rounded to 2 decimals (rsi_math._round2): allow one rounding step
                     "rsi14": 0.0101}


class RsiRecomputeError(Exception):
    pass


def _isin_expr(source_table):
    return "source_isin AS isin" if source_table == "bhav_copy_adjusted" else "isin"


def _latest_stored_date(conn, target_table, exchange):
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(trade_date) FROM {target_table} WHERE exchange = %s", (exchange,))
        row = cur.fetchone()
    return row[0] if row else None


def _fetch_anchored_rows(conn, cfg, exchange, since):
    """
    One row per (stock with source rows on/after `since`, source row after its anchor), carrying the
    anchor (last stored workbook row before `since`). A stock without an anchor gets its whole history.
    Both lookups go through the (security_id, exchange, trade_date) unique indexes -- no big sort.
    """
    src, wb = cfg["source_table"], cfg["target_table"]
    sql = f"""
        WITH stocks AS (
            SELECT DISTINCT security_id FROM {src} WHERE exchange = %(exchange)s AND trade_date >= %(since)s
        ),
        anchors AS (
            SELECT s.security_id, a.trade_date AS anchor_date, a.avg_gain AS anchor_avg_gain,
                   a.avg_loss AS anchor_avg_loss
              FROM stocks s
              LEFT JOIN LATERAL (
                    SELECT w.trade_date, w.avg_gain, w.avg_loss
                      FROM {wb} w
                     WHERE w.security_id = s.security_id AND w.exchange = %(exchange)s AND w.trade_date < %(since)s
                     ORDER BY w.trade_date DESC
                     LIMIT 1
              ) a ON TRUE
        )
        SELECT a.anchor_date, a.anchor_avg_gain, a.anchor_avg_loss,
               x.security_id, x.isin, x.exchange, x.series, x.symbol, x.trade_date, x.close, x.prev_close
          FROM anchors a
          CROSS JOIN LATERAL (
                SELECT t.security_id, {_isin_expr(src)}, t.exchange, t.series, t.symbol, t.trade_date,
                       t.close, t.prev_close
                  FROM {src} t
                 WHERE t.security_id = a.security_id AND t.exchange = %(exchange)s
                   AND (a.anchor_date IS NULL OR t.trade_date > a.anchor_date)
          ) x
         ORDER BY x.security_id, x.trade_date
    """
    return pd.read_sql(sql, conn, params={"exchange": exchange, "since": since})


def _fetch_full_history(conn, cfg, exchange, security_ids):
    src = cfg["source_table"]
    sids = sorted(security_ids)
    frames = []
    for i in range(0, len(sids), FULL_HISTORY_BATCH):
        frames.append(pd.read_sql(
            f"""
            SELECT security_id, {_isin_expr(src)}, exchange, series, symbol, trade_date, close, prev_close
              FROM {src}
             WHERE exchange = %(exchange)s AND security_id = ANY(%(sids)s::text[])
             ORDER BY security_id, trade_date
            """, conn, params={"exchange": exchange, "sids": sids[i:i + FULL_HISTORY_BATCH]}))
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _fetch_exchange(conn, cfg, exchange):
    src = cfg["source_table"]
    return pd.read_sql(
        f"""
        SELECT security_id, {_isin_expr(src)}, exchange, series, symbol, trade_date, close, prev_close
          FROM {src}
         WHERE exchange = %(exchange)s
         ORDER BY security_id, trade_date ASC
        """, conn, params={"exchange": exchange})


def walk_from_anchor(group, calendar_index):
    """
    Continue Wilder's average from the stock's anchor over its new rows. Returns the output rows, or None
    when the result could differ from a FULL recompute (caller then recomputes the stock's whole history).
    `group`: one stock's rows from _fetch_anchored_rows(), sorted by trade_date.
    """
    first = group.iloc[0]
    if pd.isna(first["anchor_date"]) or pd.isna(first["anchor_avg_gain"]) or pd.isna(first["anchor_avg_loss"]):
        return None
    avg_gain, avg_loss = float(first["anchor_avg_gain"]), float(first["anchor_avg_loss"])
    if avg_gain + avg_loss < TINY_AVERAGE:
        # A stock whose price has not moved for months: Wilder's averages decay by 13/14 a day until they are
        # below the stored 10 decimals and come back as 0 / 0 (RSI NULL), while FULL still holds the tiny
        # unrounded values and their ratio (e.g. RSI 41.94). Recompute such a stock in full. (06-Nov-2026,
        # INE965H01011 on BSE.)
        return None
    prev_date = first["anchor_date"]
    rows = []
    for r in group.itertuples(index=False):
        if pd.isna(r.close) or pd.isna(r.prev_close):
            return None
        if calendar_index is not None and is_gap(calendar_index, prev_date, r.trade_date):
            return None
        delta = float(r.close) - float(r.prev_close)
        gain, loss = max(delta, 0.0), max(-delta, 0.0)
        avg_gain, avg_loss = step(avg_gain, avg_loss, gain, loss)
        rows.append({"security_id": r.security_id, "isin": r.isin, "exchange": r.exchange, "series": r.series,
                     "symbol": r.symbol, "trade_date": r.trade_date, "gain": gain, "loss": loss,
                     "avg_gain": avg_gain, "avg_loss": avg_loss, "rsi14": compute_rsi14(avg_gain, avg_loss)})
        prev_date = r.trade_date
    return rows


def compute_incremental(conn, cfg, exchange, since, list_b, calendar_index):
    """INCREMENTAL result for one exchange (no writes): (DataFrame, list_a_count, fallback_count)."""
    anchored = _fetch_anchored_rows(conn, cfg, exchange, since)
    walked, full_sids, whole_history_frames = [], set(list_b), []
    list_a_count = 0
    if not anchored.empty:
        for sid, group in anchored.groupby("security_id", sort=False):
            if sid in full_sids:
                continue
            if pd.isna(group.iloc[0]["anchor_date"]):
                # No stored row yet: the fetched rows already ARE its whole history.
                whole_history_frames.append(group.drop(columns=["anchor_date", "anchor_avg_gain", "anchor_avg_loss"]))
                continue
            rows = walk_from_anchor(group, calendar_index)
            if rows is None:
                full_sids.add(sid)
            else:
                walked.extend(rows)
                list_a_count += 1
    fallback_count = len(full_sids) - len(set(list_b)) + len(whole_history_frames)
    if full_sids:
        whole_history_frames.append(_fetch_full_history(conn, cfg, exchange, full_sids))
    frames = [pd.DataFrame(walked, columns=OUTPUT_COLUMNS)] if walked else []
    history = pd.concat([f for f in whole_history_frames if not f.empty], ignore_index=True) \
        if any(not f.empty for f in whole_history_frames) else pd.DataFrame()
    if not history.empty:
        frames.append(compute_rsi14_all(history, calendar_index=calendar_index)[OUTPUT_COLUMNS])
    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=OUTPUT_COLUMNS)
    return result, list_a_count, fallback_count


def _compare(conn, cfg, exchange, candidate, cycle_trade_date, list_a_count, list_b_count):
    actual = pd.DataFrame()
    if not candidate.empty:
        actual = pd.read_sql(
            f"""
            SELECT w.security_id, w.trade_date, w.gain, w.loss, w.avg_gain, w.avg_loss, w.rsi14
              FROM {cfg['target_table']} w
              JOIN unnest(%(sids)s::text[], %(dates)s::date[]) AS k(security_id, trade_date)
                ON k.security_id = w.security_id AND k.trade_date = w.trade_date
             WHERE w.exchange = %(exchange)s
            """, conn, params={"exchange": exchange, "sids": candidate["security_id"].astype(str).tolist(),
                               "dates": candidate["trade_date"].tolist()})
    result = shadow.compare(candidate, actual, ["security_id", "trade_date"], SHADOW_VALUE_COLUMNS, SHADOW_TOLERANCES)
    print(f"  {shadow.record(conn, cycle_trade_date, cfg['target'], exchange, result, list_a_count, list_b_count)}")


def run(conn, cfg, indicator_id, start_date, end_date, mode_of_recompute):
    """
    Recomputes cfg's RSI workbook for both exchanges in the given mode. Raises on failure (the runner
    records it); returns the number of rows written.
    """
    mode = recompute_mode.parse_mode(mode_of_recompute)
    target = cfg["target"]
    calendar_index = build_calendar_index(fetch_trading_calendar(conn)) if cfg["use_calendar"] else None

    plans = {}
    for exchange in EXCHANGES:
        queued = recompute_queue.fetch_queued(conn, exchange, target)
        since = _latest_stored_date(conn, cfg["target_table"], exchange)
        plans[exchange] = (queued, since)
        recompute_queue.enqueue(conn, exchange, [target], {recompute_queue.ALL: recompute_queue.RUN_INCOMPLETE},
                                detail=f"{indicator_id} {fmt_date(end_date)}")

    total_written = 0
    for exchange in EXCHANGES:
        queued, since = plans[exchange]
        whole_exchange = recompute_queue.ALL in queued or since is None
        list_b = set() if whole_exchange else set(queued)
        reason = queued.get(recompute_queue.ALL, "workbook empty") if whole_exchange else None

        candidate, list_a_count, fallback_count = pd.DataFrame(), 0, 0
        if mode == recompute_mode.SHADOW and not whole_exchange:
            candidate, list_a_count, fallback_count = compute_incremental(conn, cfg, exchange, since, list_b, calendar_index)
            print(f"  [{indicator_id}/{exchange}] SHADOW: INCREMENTAL computed in memory from {fmt_date(since)} -- "
                  f"list-A {list_a_count:,} | list-B {len(list_b):,} | {A_FALLBACK} {fallback_count:,} "
                  f"({len(candidate):,} rows).")

        if mode in (recompute_mode.FULL, recompute_mode.SHADOW) or whole_exchange:
            if mode == recompute_mode.INCREMENTAL:
                print(f"  [{indicator_id}/{exchange}] INCREMENTAL fell back to FULL ({reason}).")
            df = _fetch_exchange(conn, cfg, exchange)
            if df.empty:
                continue
            total_written += upsert_rsi_workbook(conn, cfg["target_table"], compute_rsi14_all(df, calendar_index=calendar_index))
        else:
            result, list_a_count, fallback_count = compute_incremental(conn, cfg, exchange, since, list_b, calendar_index)
            written = upsert_rsi_workbook(conn, cfg["target_table"], result) if not result.empty else 0
            total_written += written
            print(f"  [{indicator_id}/{exchange}] INCREMENTAL from {fmt_date(since)} -- list-A {list_a_count:,} | "
                  f"list-B {len(list_b):,} | {A_FALLBACK} {fallback_count:,} -- {written:,} rows written.")

        if mode == recompute_mode.SHADOW and not whole_exchange:
            _compare(conn, cfg, exchange, candidate, end_date, list_a_count, len(list_b))
        elif mode == recompute_mode.SHADOW:
            print(f"  [{indicator_id}/{exchange}] SHADOW: nothing to compare ({reason}) -- INCREMENTAL would rebuild everything too.")

    for exchange in EXCHANGES:
        queued, _ = plans[exchange]
        recompute_queue.clear(conn, exchange, target, set(queued) | {recompute_queue.ALL})
    return total_written
