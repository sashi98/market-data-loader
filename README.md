# market-data-loader

Standalone Python tool for bulk historical BhavCopy backfill (NSE + BSE)
into the TMT (Track My Trade) database.

## Why this exists

The TMT UI processes BhavCopy one date at a time, driven by an admin user --
fine for the daily post-market-hours routine, but impractical for a bulk
backfill spanning weeks or months (would mean sitting through hundreds of
manual clicks, or keeping a Java/Spring Boot process alive in a loop for
hours). This repo runs as a standalone script with direct Postgres access,
fully decoupled from the `tmt` app stack during a long unattended run,
while still reusing `tmt`'s existing holiday-sync endpoint (rather than
duplicating that logic) and mirroring `tmt`'s exact BhavCopy CSV parsing
and persistence rules.

## Requirements

- Python 3.9+
- `tmt` (Spring Boot) running and reachable, e.g. `http://localhost:8080`
- `stock-py-services` (Flask) running and reachable -- checked transitively
  via `tmt`'s `/actuator/health`
- An admin user account on `tmt` (ROLE_ADMIN) -- needed to call
  `/api/holidays/sync/{year}`
- Shared `.env` at `../../config/.env` relative to this repo (the same file
  `tmt` reads via `springboot3-dotenv`) with these keys present:
  - `SPRING_DATASOURCE_URL`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
  - `TMT_APP_BASE_URL` (e.g. `http://localhost:8080`)
  - `MARKET_DATA_LOADER_DOWNLOAD_DIR` (absolute path where downloaded
    BhavCopy files are saved)
- `psycopg2-binary` (direct Postgres access) -- installed via
  `requirements.txt`, no separate setup needed

## Setup

```
cd market-data-loader
pip install -r requirements.txt
```

## Usage

```
python main.py
```

This lists all available loaders and prompts for a choice:

```
List Of Loaders:
1) bhavcopy_loader

Enter your choice: 1
```

Selecting a loader clears the screen and runs it.

### bhavcopy_loader

Backfills historical BhavCopy data for a range of trading days ending on a
given date.

**Step 0 -- Environment validation, health check, auth, DB connectivity**
- Validates required `.env` keys are present.
- Pre-flight check against `{TMT_APP_BASE_URL}/actuator/health` -- fails
  fast if `tmt` or `stock-py-services` is down, before prompting for
  credentials.
- Prompts for admin `userId` / password (masked), logs in against
  `POST /api/auth/login`, verifies the ADMIN role, and holds the JWT in
  memory for the rest of the run.
- Opens a direct Postgres connection and runs `SELECT 1` -- fails fast
  before any download/parse work starts if the DB is unreachable.

**Step 1 -- Build the trading date list**
- Prompts for `to_date` (`DDMMYYYY`) and number of trading days.
- Walks backward from `to_date`, calling `/api/holidays/sync/{year}` (JWT
  from Step 0) as needed, skipping weekends and holidays, until the
  requested number of trading days is collected.
- Reports the derived `from_date`, weekends excluded, holidays excluded,
  and total calendar days spanned, then asks for confirmation before
  continuing.

**Step 2 -- Download BhavCopy files**
- Downloads NSE (ZIP, extracted) and BSE (direct CSV) BhavCopy files for
  every date in the trading date list, into `MARKET_DATA_LOADER_DOWNLOAD_DIR`.
- HTTP 404 is treated as a soft "not published" signal, not a hard error.
- 1.5s rate-limit pause between each request (NSE and BSE separately).

**Step 3 -- Parse and persist**
- NSE: parsed and batch-inserted fresh into `bhav_copy`, then
  `bhav_copy_metadata` is written on success -- all in one transaction.
- BSE: parsed, then upserted -- rows matching an existing NSE row by
  `(isin, trade_date)` get `", BSE"` appended to their `exchange` field;
  rows with no match are inserted fresh with `exchange = "BSE"`. Also one
  transaction, independent of NSE's.
- Mirrors `tmt`'s `BhavCopyCSVParser` and `BhavCopyPersistenceServiceHandler`
  exactly (same columns, same upsert rule, same metadata contract).

**Step 4 -- Summary report**
- Prints a full run summary: date range, how many dates succeeded on both
  exchanges / one exchange only / neither, total rows persisted per
  exchange, total elapsed time, and a list of every error encountered
  with its exact message.

## Repo structure

```
market-data-loader/
  main.py                 -- lists loaders, prompts, clears screen, dispatches
  requirements.txt
  core/
    env_validator.py       -- loads + validates ../../config/.env
    health_client.py         -- pre-flight check against /actuator/health
    auth_client.py             -- admin login, JWT
    db_client.py                 -- direct Postgres connection (Step 0.4 check + Step 3 writes)
    holiday_client.py              -- calls /api/holidays/sync/{year}
    trading_calendar.py             -- builds the trading date list
    bhavcopy_downloader.py           -- NSE/BSE download logic
    bhavcopy_parser.py                -- CSV parsing (mirrors BhavCopyCSVParser.java)
    bhavcopy_persistence.py            -- NSE insert + BSE upsert (mirrors BhavCopyPersistenceServiceHandler.java)
  loaders/
    bhavcopy_loader.py                  -- BhavCopy backfill loader (all 4 steps implemented)
```

To add a new loader: create `loaders/<name>_loader.py` exposing a `run()`
function, then register it in the `LOADERS` list in `main.py`.

## Conventions

- No non-ASCII characters in code or comments (currency symbols excepted).
- Use `--` for dashes and `->` for arrows in comments, not Unicode
  equivalents.
- This repo does not use Spring profiles/config -- it reads the same
  shared `.env` file `tmt` uses, so environment values never drift
  between the two.
