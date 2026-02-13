#!/usr/bin/env python3
"""
Scrapper Cascade - Descarga OHLCV con cascada de fuentes y mínimo 5 años.

Prioridad de fuentes: ByMA → IOL → Yahoo → analisistecnico
Para cada ticker, descarga desde la fuente de mayor prioridad y complementa
con fuentes adicionales hasta alcanzar al menos 5 años de historia.

Uso:
    python scraper_cascade.py                    # Todos los tickers
    python scraper_cascade.py --force            # Re-descargar todo
    python scraper_cascade.py --ticker GGAL      # Un solo ticker
    python scraper_cascade.py --ticker GGAL -v   # Verbose
    python scraper_cascade.py --min-years 3      # Mínimo 3 años
    python scraper_cascade.py --skip-iol         # Sin IOL (sin Playwright)

Gestión de tickers:
    python scraper_cascade.py --export-csv tickers.csv  # Exportar DB a CSV
    python scraper_cascade.py --sync                     # Sync tickers.csv -> DB + descarga
    python scraper_cascade.py --sync --csv mi_lista.csv  # Sync desde CSV personalizado
"""

import argparse
import csv
import json
import logging
import os
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone, timedelta

# ─── Constants ───────────────────────────────────────────────────────────────

# ByMA
BYMA_BASE = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free"
BYMA_HISTORY = f"{BYMA_BASE}/chart/historical-series/history"
SOURCE_BYMA = "byma"

# analisistecnico
AT_BASE_URL = "https://analisistecnico.com.ar/services/datafeed"
SOURCE_AT = "analisistecnico"

# Yahoo Finance
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
SOURCE_YAHOO = "yahoo"

# IOL
SOURCE_IOL = "iol"

# General
MIN_YEARS = 5
DEFAULT_DELAY = 0.5
DB_FILENAME = "historial.db"
DATA_DIR = "data"
MAX_RETRIES = 3
RETRY_BACKOFF = [3, 10, 30]

log = logging.getLogger("scraper_cascade")

# SSL context for self-signed certs (ByMA, analisistecnico)
_ssl_ctx = ssl.create_default_context()
_ssl_ctx.check_hostname = False
_ssl_ctx.verify_mode = ssl.CERT_NONE

# ─── Database ────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tickers (
    id              INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    ticker          TEXT NOT NULL UNIQUE,
    trading_type    TEXT NOT NULL,
    settlement_type TEXT,
    enabled         BOOLEAN NOT NULL,
    description     TEXT,
    url_icon        TEXT,
    downloaded_at   TIMESTAMP,
    bars_count      INTEGER DEFAULT 0,
    data_source     TEXT
);

CREATE TABLE IF NOT EXISTS ohlcv (
    ticker      TEXT NOT NULL,
    date        TEXT NOT NULL,
    timestamp   INTEGER NOT NULL,
    open        REAL NOT NULL,
    high        REAL NOT NULL,
    low         REAL NOT NULL,
    close       REAL NOT NULL,
    volume      INTEGER NOT NULL,
    row_index   INTEGER NOT NULL DEFAULT 0,
    data_source TEXT DEFAULT 'analisistecnico',
    PRIMARY KEY (ticker, date, row_index)
);

CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker ON ohlcv(ticker);
CREATE INDEX IF NOT EXISTS idx_ohlcv_date ON ohlcv(date);

CREATE TABLE IF NOT EXISTS download_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker      TEXT NOT NULL,
    started_at  TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    status      TEXT NOT NULL,
    bars_found  INTEGER DEFAULT 0,
    bars_stored INTEGER DEFAULT 0,
    error_msg   TEXT,
    data_source TEXT
);
"""

MIGRATION_SQL = [
    "ALTER TABLE ohlcv ADD COLUMN data_source TEXT DEFAULT 'analisistecnico'",
    "ALTER TABLE tickers ADD COLUMN data_source TEXT",
    "ALTER TABLE download_log ADD COLUMN data_source TEXT",
]


def init_db(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA_SQL)
    for sql in MIGRATION_SQL:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass
    conn.commit()
    return conn


def store_ohlcv(conn: sqlite3.Connection, rows: list) -> int:
    if not rows:
        return 0
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT OR REPLACE INTO ohlcv (ticker, date, timestamp, open, high, low, close, volume, row_index, data_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return cursor.rowcount


def mark_downloaded(conn: sqlite3.Connection, ticker: str, bars_count: int, source: str):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE tickers SET downloaded_at = ?, bars_count = ?, data_source = ? WHERE ticker = ?",
        (now, bars_count, source, ticker),
    )
    conn.commit()


def log_download(conn, ticker, status, bars_found, bars_stored, error_msg=None, source=None):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO download_log (ticker, started_at, finished_at, status, bars_found, bars_stored, error_msg, data_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (ticker, now, now, status, bars_found, bars_stored, error_msg, source),
    )
    conn.commit()


def get_enabled_tickers(conn: sqlite3.Connection) -> list:
    rows = conn.execute(
        "SELECT ticker, trading_type FROM tickers WHERE enabled = 1 ORDER BY ticker"
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


def get_ohlcv_date_range(conn: sqlite3.Connection, ticker: str) -> tuple:
    """Returns (min_date, max_date, count) for a ticker's OHLCV data."""
    row = conn.execute(
        "SELECT MIN(date), MAX(date), COUNT(*) FROM ohlcv WHERE ticker = ? AND row_index = 0",
        (ticker,),
    ).fetchone()
    return row[0], row[1], row[2]


def years_of_data(min_date: str, max_date: str) -> float:
    """Calculate years between two ISO date strings."""
    if not min_date or not max_date:
        return 0.0
    d1 = datetime.strptime(min_date, "%Y-%m-%d")
    d2 = datetime.strptime(max_date, "%Y-%m-%d")
    return (d2 - d1).days / 365.25


# ─── HTTP helpers ────────────────────────────────────────────────────────────


def _http_get(url: str, ssl_ctx=None, headers=None, timeout: int = 30) -> bytes | None:
    """HTTP GET with retries."""
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)

    ctx = ssl_ctx or _ssl_ctx
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code in (400, 404):
                return None
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF[attempt]
                log.debug(f"    Retry {attempt + 1}: HTTP {e.code}, wait {wait}s")
                time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF[attempt]
                log.debug(f"    Retry {attempt + 1}: {e}, wait {wait}s")
                time.sleep(wait)

    if last_error:
        raise last_error
    return None


def unix_to_date(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d")


# ─── Source: ByMA ────────────────────────────────────────────────────────────


def fetch_byma(ticker: str) -> list:
    """Fetch OHLCV from ByMA. Returns list of row tuples or empty list."""
    now = int(time.time())
    symbol = f"{ticker}+24HS"
    url = f"{BYMA_HISTORY}?symbol={symbol}&resolution=D&from=946684800&to={now}"

    headers = {
        "Accept": "application/json",
        "Origin": "https://open.bymadata.com.ar",
        "Referer": "https://open.bymadata.com.ar/",
    }

    data_bytes = _http_get(url, ssl_ctx=_ssl_ctx, headers=headers)
    if not data_bytes:
        return []

    try:
        data = json.loads(data_bytes)
    except json.JSONDecodeError:
        return []

    if data.get("s") != "ok":
        return []

    timestamps = data.get("t", [])
    opens = data.get("o", [])
    highs = data.get("h", [])
    lows = data.get("l", [])
    closes = data.get("c", [])
    volumes = data.get("v", [])

    rows = []
    for i in range(len(timestamps)):
        date_str = unix_to_date(timestamps[i])
        rows.append((
            ticker, date_str, timestamps[i],
            opens[i], highs[i], lows[i], closes[i],
            int(volumes[i]), 0, SOURCE_BYMA,
        ))
    return rows


# ─── Source: analisistecnico ─────────────────────────────────────────────────


def fetch_analisistecnico(ticker: str) -> list:
    """Fetch OHLCV from analisistecnico.com.ar (TradingView UDF)."""
    url = f"{AT_BASE_URL}/history?symbol={ticker}&resolution=D"

    data_bytes = _http_get(url, ssl_ctx=_ssl_ctx)
    if not data_bytes:
        return []

    try:
        data = json.loads(data_bytes)
    except json.JSONDecodeError:
        return []

    if not isinstance(data, dict) or data.get("s") != "ok":
        return []

    timestamps = data.get("t", [])
    opens = data.get("o", [])
    highs = data.get("h", [])
    lows = data.get("l", [])
    closes = data.get("c", [])
    volumes = data.get("v", [])

    rows = []
    date_count = {}
    for i in range(len(timestamps)):
        date_str = unix_to_date(timestamps[i])
        row_idx = date_count.get(date_str, 0)
        date_count[date_str] = row_idx + 1
        rows.append((
            ticker, date_str, timestamps[i],
            opens[i], highs[i], lows[i], closes[i],
            int(volumes[i]), row_idx, SOURCE_AT,
        ))
    return rows


# ─── Source: Yahoo Finance ───────────────────────────────────────────────────


def fetch_yahoo(ticker: str) -> list:
    """Fetch OHLCV from Yahoo Finance (.BA suffix)."""
    yahoo_symbol = f"{ticker}.BA"
    url = f"{YAHOO_URL}/{yahoo_symbol}?period1=0&period2=9999999999&interval=1d"

    # Yahoo uses standard SSL (no self-signed)
    std_ctx = ssl.create_default_context()

    try:
        data_bytes = _http_get(url, ssl_ctx=std_ctx)
    except Exception:
        # Fallback to no-verify if standard fails
        data_bytes = _http_get(url, ssl_ctx=_ssl_ctx)

    if not data_bytes:
        return []

    try:
        data = json.loads(data_bytes)
    except json.JSONDecodeError:
        return []

    chart = data.get("chart", {})
    if chart.get("error") or not chart.get("result"):
        return []

    result = chart["result"][0]
    timestamps = result.get("timestamp", [])
    if not timestamps:
        return []

    quote = result.get("indicators", {}).get("quote", [{}])[0]
    opens = quote.get("open", [])
    highs = quote.get("high", [])
    lows = quote.get("low", [])
    closes = quote.get("close", [])
    volumes = quote.get("volume", [])

    rows = []
    for i in range(len(timestamps)):
        o = opens[i] if i < len(opens) else None
        h = highs[i] if i < len(highs) else None
        l = lows[i] if i < len(lows) else None
        c = closes[i] if i < len(closes) else None
        v = volumes[i] if i < len(volumes) else 0

        if o is None or h is None or l is None or c is None:
            continue

        date_str = unix_to_date(timestamps[i])
        rows.append((
            ticker, date_str, timestamps[i],
            o, h, l, c,
            int(v or 0), 0, SOURCE_YAHOO,
        ))
    return rows


# ─── Source: IOL (Playwright) ────────────────────────────────────────────────

_iol_scraper = None


def get_iol_scraper(headless: bool = True):
    """Lazy-initialize IOL scraper (Playwright is heavy)."""
    global _iol_scraper
    if _iol_scraper is None:
        try:
            from scraper_iol import IOLScraper
            _iol_scraper = IOLScraper(headless=headless)
            _iol_scraper.start()
            log.info("IOL scraper (Playwright) inicializado")
        except ImportError:
            log.warning("Playwright no disponible, IOL deshabilitado")
            return None
        except Exception as e:
            log.warning(f"Error iniciando IOL scraper: {e}")
            return None
    return _iol_scraper


def stop_iol_scraper():
    """Clean up IOL scraper."""
    global _iol_scraper
    if _iol_scraper:
        _iol_scraper.stop()
        _iol_scraper = None


def fetch_iol(ticker: str) -> list:
    """Fetch OHLCV from IOL via Playwright scraper."""
    scraper = get_iol_scraper()
    if scraper is None:
        return []
    try:
        return scraper.scrape_ticker(ticker)
    except Exception as e:
        log.warning(f"    IOL error for {ticker}: {e}")
        return []


# ─── Cascade Logic ───────────────────────────────────────────────────────────


SOURCES = [
    ("byma", fetch_byma),
    ("iol", fetch_iol),
    ("yahoo", fetch_yahoo),
    ("analisistecnico", fetch_analisistecnico),
]


def cascade_download(
    ticker: str,
    trading_type: str,
    conn: sqlite3.Connection,
    min_years: float,
    delay: float,
    skip_sources: set = None,
) -> tuple:
    """
    Download OHLCV for a ticker using cascade of sources.
    Returns (total_bars_stored, primary_source, sources_used).
    """
    skip_sources = skip_sources or set()
    total_stored = 0
    primary_source = None
    sources_used = []

    for source_name, fetch_fn in SOURCES:
        if source_name in skip_sources:
            continue

        # Check if we already have enough data
        min_date, max_date, count = get_ohlcv_date_range(conn, ticker)
        yrs = years_of_data(min_date, max_date)

        if count > 0 and yrs >= min_years:
            log.debug(f"    {ticker}: {yrs:.1f} years ({count} bars), >= {min_years}yr minimum — done")
            break

        log.debug(f"    {ticker}: trying {source_name}... (current: {count} bars, {yrs:.1f}yr)")

        try:
            rows = fetch_fn(ticker)
        except Exception as e:
            log.warning(f"    {ticker}: {source_name} failed: {e}")
            time.sleep(delay)
            continue

        if rows:
            stored = store_ohlcv(conn, rows)
            total_stored += stored
            sources_used.append(f"{source_name}({stored})")
            if primary_source is None:
                primary_source = source_name
            log.debug(f"    {ticker}: {source_name} -> {stored} bars stored")
        else:
            log.debug(f"    {ticker}: {source_name} -> no data")

        time.sleep(delay)

    return total_stored, primary_source, sources_used


# ─── Download Orchestration ──────────────────────────────────────────────────


class CascadeStats:
    def __init__(self):
        self.success = 0
        self.partial = 0  # Got data but < min_years
        self.empty = 0
        self.errors = 0
        self.skipped = 0
        self.total_bars = 0
        self.detail = []


def download_all(
    tickers: list,
    conn: sqlite3.Connection,
    min_years: float = MIN_YEARS,
    force: bool = False,
    delay: float = DEFAULT_DELAY,
    skip_sources: set = None,
) -> CascadeStats:
    stats = CascadeStats()
    total = len(tickers)

    for i, (ticker, trading_type) in enumerate(tickers, 1):
        # Check existing data
        min_date, max_date, existing_count = get_ohlcv_date_range(conn, ticker)
        existing_years = years_of_data(min_date, max_date)

        if not force and existing_count > 0 and existing_years >= min_years:
            log.debug(f"[{i}/{total}] {ticker} - {existing_years:.1f}yr ({existing_count} bars), OK")
            stats.skipped += 1
            continue

        if existing_count > 0 and not force:
            log.info(f"[{i}/{total}] {ticker} ({trading_type}) - {existing_years:.1f}yr, need {min_years}yr...")
        else:
            log.info(f"[{i}/{total}] {ticker} ({trading_type})...")

        try:
            stored, primary, sources = cascade_download(
                ticker, trading_type, conn, min_years, delay, skip_sources
            )

            # Final check
            min_d, max_d, final_count = get_ohlcv_date_range(conn, ticker)
            final_years = years_of_data(min_d, max_d)

            if final_count == 0:
                log.warning(f"[{i}/{total}] {ticker} - sin datos en ninguna fuente")
                log_download(conn, ticker, "empty", 0, 0, source="cascade")
                stats.empty += 1
                stats.detail.append((ticker, 0, 0, "none", []))
                continue

            # Mark with primary source
            mark_downloaded(conn, ticker, final_count, primary)
            log_download(conn, ticker, "ok", stored, final_count, source="cascade")

            if final_years >= min_years:
                log.info(
                    f"[{i}/{total}] {ticker} - {final_count} bars, {final_years:.1f}yr "
                    f"[{', '.join(sources)}]"
                )
                stats.success += 1
            else:
                log.info(
                    f"[{i}/{total}] {ticker} - {final_count} bars, {final_years:.1f}yr "
                    f"(< {min_years}yr) [{', '.join(sources)}]"
                )
                stats.partial += 1

            stats.total_bars += stored
            stats.detail.append((ticker, final_count, final_years, primary, sources))

        except KeyboardInterrupt:
            log.warning("Interrumpido por el usuario")
            break
        except Exception as e:
            log.error(f"[{i}/{total}] {ticker} CASCADE ERROR: {e}")
            log_download(conn, ticker, "error", 0, 0, str(e), source="cascade")
            stats.errors += 1
            stats.detail.append((ticker, 0, 0, "error", [str(e)]))

    return stats


def print_summary(stats: CascadeStats, total: int, min_years: float):
    print("\n" + "=" * 70)
    print("RESUMEN CASCADE")
    print("=" * 70)
    print(f"  Total tickers:         {total}")
    print(f"  Completos (>={min_years}yr):  {stats.success}")
    print(f"  Parciales (<{min_years}yr):   {stats.partial}")
    print(f"  Sin datos:             {stats.empty}")
    print(f"  Errores:               {stats.errors}")
    print(f"  Saltados (ya OK):      {stats.skipped}")
    print(f"  Barras nuevas:         {stats.total_bars:,}")

    if stats.partial > 0:
        print(f"\nTickers con < {min_years} años:")
        for ticker, bars, yrs, src, sources in stats.detail:
            if 0 < yrs < min_years:
                print(f"  {ticker:10s} {bars:5d} bars, {yrs:.1f}yr ({src})")

    if stats.empty > 0:
        print(f"\nTickers sin datos:")
        for ticker, bars, yrs, src, sources in stats.detail:
            if bars == 0 and src == "none":
                print(f"  - {ticker}")

    print("=" * 70)


# ─── CSV Ticker Management ───────────────────────────────────────────────────

CSV_COLUMNS = ["id", "name", "ticker", "trading_type", "settlement_type", "enabled", "description"]


def export_tickers_to_csv(conn: sqlite3.Connection, csv_path: str):
    """Export all tickers from DB to a CSV file."""
    rows = conn.execute(
        "SELECT id, name, ticker, trading_type, "
        "COALESCE(settlement_type, ''), enabled, COALESCE(description, '') "
        "FROM tickers ORDER BY "
        "CASE trading_type WHEN 'STOCK' THEN 1 WHEN 'CEDEARS' THEN 2 WHEN 'BOND' THEN 3 END, "
        "ticker"
    ).fetchall()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "id": r[0],
                "name": r[1],
                "ticker": r[2],
                "trading_type": r[3],
                "settlement_type": r[4],
                "enabled": "true" if r[5] else "false",
                "description": r[6],
            })

    log.info(f"Exportados {len(rows)} tickers a {csv_path}")
    return len(rows)


def load_tickers_from_csv(csv_path: str, conn: sqlite3.Connection) -> dict:
    """
    Sync CSV → DB. Returns dict with sync results:
    {"new": [...], "updated": [...], "disabled": [...], "unchanged": N, "total_csv": N}
    """
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        csv_rows = list(reader)

    # Get existing tickers from DB
    existing = {}
    for row in conn.execute("SELECT ticker, name, trading_type, settlement_type, enabled, description FROM tickers"):
        existing[row[0]] = {
            "name": row[1], "trading_type": row[2], "settlement_type": row[3] or "",
            "enabled": bool(row[4]), "description": row[5] or "",
        }

    result = {"new": [], "updated": [], "disabled": [], "unchanged": 0, "total_csv": len(csv_rows)}

    for row in csv_rows:
        ticker = row["ticker"].strip()
        name = row["name"].strip()
        trading_type = row["trading_type"].strip()
        settlement_type = row.get("settlement_type", "").strip()
        enabled_str = row.get("enabled", "true").strip().lower()
        enabled = enabled_str in ("true", "1", "yes")
        description = row.get("description", "").strip()

        # Auto-assign ID if missing or 0
        row_id = int(row.get("id", 0) or 0)
        if row_id == 0:
            max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tickers").fetchone()[0]
            row_id = max_id + 1

        # UPSERT into DB
        conn.execute(
            "INSERT INTO tickers (id, name, ticker, trading_type, settlement_type, enabled, description) "
            "VALUES (?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(ticker) DO UPDATE SET "
            "name=excluded.name, trading_type=excluded.trading_type, "
            "settlement_type=excluded.settlement_type, enabled=excluded.enabled, "
            "description=excluded.description",
            (row_id, name, ticker, trading_type, settlement_type, enabled, description),
        )

        # Classify change
        if ticker not in existing:
            result["new"].append({"ticker": ticker, "trading_type": trading_type, "name": name})
        else:
            old = existing[ticker]
            changes = []
            if old["name"] != name:
                changes.append("name")
            if old["trading_type"] != trading_type:
                changes.append("trading_type")
            if old["enabled"] and not enabled:
                result["disabled"].append(ticker)
                continue
            if old["enabled"] != enabled:
                changes.append("enabled")
            if old["description"] != description:
                changes.append("description")
            if changes:
                result["updated"].append({"ticker": ticker, "changes": changes})
            else:
                result["unchanged"] += 1

    conn.commit()
    return result


def print_sync_report(sync_result: dict):
    """Print sync report to console."""
    print("\n" + "=" * 60)
    print("SYNC REPORT: tickers.csv → DB")
    print("=" * 60)
    print(f"  Tickers en CSV:        {sync_result['total_csv']}")
    print(f"  Nuevos:                {len(sync_result['new'])}")
    print(f"  Modificados:           {len(sync_result['updated'])}")
    print(f"  Deshabilitados:        {len(sync_result['disabled'])}")
    print(f"  Sin cambios:           {sync_result['unchanged']}")

    if sync_result["new"]:
        print(f"\n  Tickers nuevos:")
        for t in sync_result["new"]:
            print(f"    + {t['ticker']:10s} {t['trading_type']:10s} {t['name']}")

    if sync_result["updated"]:
        print(f"\n  Tickers modificados:")
        for t in sync_result["updated"]:
            print(f"    ~ {t['ticker']:10s} ({', '.join(t['changes'])})")

    if sync_result["disabled"]:
        print(f"\n  Tickers deshabilitados:")
        for t in sync_result["disabled"]:
            print(f"    - {t}")

    print("=" * 60 + "\n")


def get_tickers_needing_data(conn: sqlite3.Connection, min_years: float) -> list:
    """Get enabled tickers that have insufficient data (< min_years or no data)."""
    all_tickers = get_enabled_tickers(conn)
    needing = []
    for ticker, trading_type in all_tickers:
        min_date, max_date, count = get_ohlcv_date_range(conn, ticker)
        yrs = years_of_data(min_date, max_date)
        if count == 0 or yrs < min_years:
            needing.append((ticker, trading_type))
    return needing


# ─── CLI ─────────────────────────────────────────────────────────────────────


def parse_args():
    parser = argparse.ArgumentParser(
        description="Descarga OHLCV con cascada de fuentes (ByMA → IOL → Yahoo → analisistecnico)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-descargar todos los tickers desde cero",
    )
    parser.add_argument(
        "--min-years",
        type=float,
        default=MIN_YEARS,
        help=f"Mínimo de años de historia requerido (default: {MIN_YEARS})",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Segundos entre requests (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help=f"Ruta a la base de datos SQLite (default: {DATA_DIR}/{DB_FILENAME})",
    )
    parser.add_argument(
        "--ticker",
        help="Descargar solo un ticker específico",
    )
    parser.add_argument(
        "--skip-iol",
        action="store_true",
        help="No usar IOL (evita necesitar Playwright)",
    )
    parser.add_argument(
        "--skip-yahoo",
        action="store_true",
        help="No usar Yahoo Finance",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Logging verbose"
    )

    # Ticker management
    parser.add_argument(
        "--export-csv",
        metavar="FILE",
        help="Exportar tickers de DB a archivo CSV",
    )
    parser.add_argument(
        "--sync",
        action="store_true",
        help="Sync tickers.csv → DB y descarga nuevos/desactualizados",
    )
    parser.add_argument(
        "--csv",
        default=None,
        help="Ruta al CSV de tickers (default: tickers.csv en raíz del repo)",
    )

    return parser.parse_args()


def setup_logging(verbose: bool):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )


def main():
    args = parse_args()
    setup_logging(args.verbose)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = args.db_path or os.path.join(script_dir, DATA_DIR, DB_FILENAME)
    default_csv = os.path.join(script_dir, "tickers.csv")

    conn = init_db(db_path)

    # ─── Mode: export DB to CSV ───────────────────────────────────────────
    if args.export_csv:
        count = export_tickers_to_csv(conn, args.export_csv)
        print(f"\n✓ Exportados {count} tickers a {args.export_csv}")
        conn.close()
        return

    # ─── Mode: sync CSV → DB + download ──────────────────────────────────
    if args.sync:
        csv_path = args.csv or default_csv
        if not os.path.exists(csv_path):
            print(f"\n✗ CSV no encontrado: {csv_path}")
            print(f"  Generá uno con: python scraper_cascade.py --export-csv {csv_path}")
            conn.close()
            return

        log.info(f"Sync: {csv_path} → DB")
        sync_result = load_tickers_from_csv(csv_path, conn)
        print_sync_report(sync_result)

        # Find tickers needing data (new or insufficient history)
        needing = get_tickers_needing_data(conn, args.min_years)
        if not needing:
            print("✓ Todos los tickers tienen datos suficientes.")
            conn.close()
            return

        print(f"Descargando datos para {len(needing)} tickers que necesitan datos...")
        log.info(f"Fuentes: ByMA → IOL → Yahoo → analisistecnico")
        log.info(f"Mínimo de historia: {args.min_years} años")

        skip_sources = set()
        if args.skip_iol:
            skip_sources.add("iol")
            log.info("IOL deshabilitado (--skip-iol)")
        if args.skip_yahoo:
            skip_sources.add("yahoo")
            log.info("Yahoo deshabilitado (--skip-yahoo)")

        try:
            stats = download_all(
                needing, conn,
                min_years=args.min_years,
                force=True,  # Force download for tickers needing data
                delay=args.delay,
                skip_sources=skip_sources,
            )
        except KeyboardInterrupt:
            log.warning("Interrumpido")
            stats = CascadeStats()
        finally:
            stop_iol_scraper()

        conn.close()
        print_summary(stats, len(needing), args.min_years)
        return

    # ─── Mode: normal download (existing behavior) ───────────────────────
    log.info(f"Base de datos: {db_path}")
    log.info(f"Minimo de historia: {args.min_years} años")
    log.info(f"Fuentes: ByMA → IOL → Yahoo → analisistecnico")

    all_tickers = get_enabled_tickers(conn)
    log.info(f"Tickers habilitados: {len(all_tickers)}")

    if args.ticker:
        match = [(t, tt) for t, tt in all_tickers if t == args.ticker]
        if not match:
            log.warning(f"Ticker {args.ticker} no encontrado en DB, intentando igual")
            match = [(args.ticker, "UNKNOWN")]
        all_tickers = match

    skip_sources = set()
    if args.skip_iol:
        skip_sources.add("iol")
        log.info("IOL deshabilitado (--skip-iol)")
    if args.skip_yahoo:
        skip_sources.add("yahoo")
        log.info("Yahoo deshabilitado (--skip-yahoo)")

    total = len(all_tickers)

    try:
        stats = download_all(
            all_tickers, conn,
            min_years=args.min_years,
            force=args.force,
            delay=args.delay,
            skip_sources=skip_sources,
        )
    except KeyboardInterrupt:
        log.warning("Interrumpido")
        stats = CascadeStats()
    finally:
        stop_iol_scraper()

    conn.close()
    print_summary(stats, total, args.min_years)


if __name__ == "__main__":
    main()
