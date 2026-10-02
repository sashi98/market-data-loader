# -----------------------------------------------------------
# core/stock_universe/taxonomy.py
# Sector / industry master data for stock_universe (TMT-US-0010).
#
# Sources spell the same sector differently ("Oil Gas & Consumable Fuels"
# from NSE, "Oil, Gas & Consumable Fuels" from BSE, "SERVICES" vs
# "Services"). Every name is reduced to a KEY; the sector / industry tables
# (Liquibase 019.01.00) hold one display name per key, and stock_universe
# gets the keys plus that display name, so every screen shows one name.
#
# Key rule -- identical to the taxonomy_key() SQL function in 019.01.00:
#   '&' -> ' AND ', every run of characters other than A-Z a-z 0-9 -> one
#   space, trim, spaces -> '_', upper case.
#   "Oil, Gas & Consumable Fuels" -> OIL_GAS_AND_CONSUMABLE_FUELS
# Different letters give different keys ("Healthcare" vs "Health
# Services" stay separate).
#
# Flow per enrichment run (TaxonomyMaps):
#   0. At start, load the sector map {sector_key: name} and the industry map
#      {(sector_key, industry_key): name} from the tables.
#   For every stock's fetched details (SD):
#   1-3. Read sector / industry from SD and build the keys. A key already in
#      the map keeps its display name; a new key is added to the map with
#      SD's spelling. No sector -> OTHERS / "Others"; no industry -> OTHERS /
#      "Others" under the stock's sector.
#   4. SD gets sector_key / industry_key and the map's display names.
#   5. A new key is inserted into sector / industry at once (own autocommit
#      connection), so it exists before any stock row points at it.
#   6. The caller upserts SD into stock_universe.
# A display name, once stored, is never changed by enrichment.
# -----------------------------------------------------------

import re
import threading

from core.db_client import get_connection
from core.stock_universe.persistence import StockUniversePersistenceError

OTHERS_KEY = "OTHERS"
OTHERS_NAME = "Others"

# Which tier supplied a stock's sector. Used in memory only, during a run (the listener's BSE -> NSE sector
# sharing for the same ISIN); never stored in the sector / industry tables.
SOURCE_BSE = "BSE"
SOURCE_NSE = "NSE"
SOURCE_TRADINGVIEW = "TRADINGVIEW"
SOURCE_YFINANCE = "YFINANCE"
OFFICIAL_SOURCES = {SOURCE_BSE, SOURCE_NSE}

_NON_ALNUM = re.compile(r"[^A-Za-z0-9]+")
_SPACES = re.compile(r"\s+")
_NAME_MAX = 100


def taxonomy_key(name):
    """Normalised key for a sector / industry name, or None when it has no letters or digits."""
    if name is None:
        return None
    words = _NON_ALNUM.sub(" ", str(name).replace("&", " AND ")).strip()
    return words.replace(" ", "_").upper() or None


def clean_name(name):
    """Display form of a source name: trimmed, repeated spaces collapsed; None when blank."""
    if name is None:
        return None
    cleaned = _SPACES.sub(" ", str(name)).strip()
    return cleaned[:_NAME_MAX] if cleaned else None


class TaxonomyMaps:
    """
    The sector and industry maps for one enrichment run, shared by all worker threads.
    Thread-safe: one lock guards both maps and the inserts of new keys.
    """

    def __init__(self, env_values):
        self._lock = threading.Lock()
        try:
            self._conn = get_connection(env_values)
            self._conn.autocommit = True
            with self._conn.cursor() as cur:
                cur.execute("SELECT sector_key, name FROM sector")
                self.sectors = {k: n for k, n in cur.fetchall()}
                cur.execute("SELECT sector_key, industry_key, name FROM industry")
                self.industries = {(s, i): n for s, i, n in cur.fetchall()}
        except Exception as e:
            raise StockUniversePersistenceError(f"Failed to load sector / industry maps: {e}")
        self.new_sectors = 0
        self.new_industries = 0

    def close(self):
        try:
            self._conn.close()
        except Exception:
            pass

    def _sector(self, cur, key, name):
        if key not in self.sectors:
            cur.execute("INSERT INTO sector (sector_key, name) VALUES (%s, %s) ON CONFLICT (sector_key) DO NOTHING",
                        [key, name])
            cur.execute("SELECT name FROM sector WHERE sector_key = %s", [key])
            self.sectors[key] = cur.fetchone()[0]
            self.new_sectors += 1
        return self.sectors[key]

    def _industry(self, cur, sector_key, key, name):
        if (sector_key, key) not in self.industries:
            cur.execute("INSERT INTO industry (sector_key, industry_key, name) VALUES (%s, %s, %s) "
                        "ON CONFLICT (sector_key, industry_key) DO NOTHING", [sector_key, key, name])
            cur.execute("SELECT name FROM industry WHERE sector_key = %s AND industry_key = %s", [sector_key, key])
            self.industries[(sector_key, key)] = cur.fetchone()[0]
            self.new_industries += 1
        return self.industries[(sector_key, key)]

    def resolve(self, sector, industry):
        """
        Steps 1-5 for one stock: returns {sector, sector_key, industry, industry_key} with the map's display
        names, adding (and inserting) new keys. Missing sector / industry -> OTHERS / "Others".
        """
        sector_name = clean_name(sector)
        sector_key = taxonomy_key(sector_name)
        if sector_key is None:
            sector_key, sector_name = OTHERS_KEY, OTHERS_NAME
        industry_name = clean_name(industry)
        industry_key = taxonomy_key(industry_name)
        if industry_key is None:
            industry_key, industry_name = OTHERS_KEY, OTHERS_NAME
        try:
            with self._lock, self._conn.cursor() as cur:
                return {
                    "sector": self._sector(cur, sector_key, sector_name),
                    "sector_key": sector_key,
                    "industry": self._industry(cur, sector_key, industry_key, industry_name),
                    "industry_key": industry_key,
                }
        except Exception as e:
            raise StockUniversePersistenceError(f"Failed to add sector/industry {sector_name!r}/{industry_name!r}: {e}")

    def apply(self, conn, isin_number, exchange, fields):
        """
        Returns a copy of SD (fields) with sector / industry replaced by the map's display names and the keys
        added. When SD has no sector but the stock row already has one, SD is returned without sector /
        industry so the existing values stay (a source gap must not turn a known sector into Others).
        """
        out = dict(fields)
        if taxonomy_key(fields.get("sector")) is None:
            with conn.cursor() as cur:
                cur.execute("SELECT sector_key FROM stock_universe WHERE isin_number = %s AND exchange = %s",
                            [isin_number, exchange])
                row = cur.fetchone()
            if row is not None and row[0] is not None:
                out.pop("sector", None)
                out.pop("industry", None)
                return out
        out.update(self.resolve(fields.get("sector"), fields.get("industry")))
        return out


def taxonomy_fields(fields):
    """The sector / industry part of written fields, for sharing to another row of the same ISIN."""
    return {k: fields[k] for k in ("sector", "sector_key", "industry", "industry_key") if k in fields}
