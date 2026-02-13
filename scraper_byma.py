#!/usr/bin/env python3
"""
Scrapper ByMA Data - Descarga datos históricos OHLCV desde open.bymadata.com.ar

Usa la API pública gratuita de ByMA (protocolo TradingView UDF) para descargar
datos históricos OHLCV. No requiere autenticación.

Nota: ByMA solo tiene ~2 años de historia (desde 2024-01-02).
Para obtener más historial, usar scraper_cascade.py.

Uso:
    python scraper_byma.py                    # Descargar tickers sin datos
    python scraper_byma.py --force            # Re-descargar todo
    python scraper_byma.py --ticker GGAL      # Un solo ticker
    python scraper_byma.py --delay 0.5        # Delay entre requests
    python scraper_byma.py -v                 # Verbose
"""

import argparse
import json
import logging
import os
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

# ─── Constants ───────────────────────────────────────────────────────────────

BYMA_BASE = "https://open.bymadata.com.ar/vanoms-be-core/rest/api/bymadata/free"
BYMA_HISTORY = f"{BYMA_BASE}/chart/historical-series/history"
BYMA_SYMBOLS = f"{BYMA_BASE}/chart/historical-series/symbols"

SOURCE_BYMA = "byma"
DEFAULT_DELAY = 0.5
DB_FILENAME = "historial.db"
DATA_DIR = "data"
MAX_RETRIES = 3
RETRY_BACKOFF = [3, 10, 30]

log = logging.getLogger("scraper_byma")

# ─── SSL Context ─────────────────────────────────────────────────────────────

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


def has_ohlcv_data(conn: sqlite3.Connection, ticker: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM ohlcv WHERE ticker = ?", (ticker,)
    ).fetchone()
    return row[0] > 0


def mark_downloaded(conn: sqlite3.Connection, ticker: str, bars_count: int, source: str):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE tickers SET downloaded_at = ?, bars_count = ?, data_source = ? WHERE ticker = ?",
        (now, bars_count, source, ticker),
    )
    conn.commit()


def log_download(conn, ticker, status, bars_found, bars_stored, error_msg=None):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO download_log (ticker, started_at, finished_at, status, bars_found, bars_stored, error_msg, data_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (ticker, now, now, status, bars_found, bars_stored, error_msg, SOURCE_BYMA),
    )
    conn.commit()


def store_ohlcv(conn: sqlite3.Connection, rows: list) -> int:
    cursor = conn.cursor()
    cursor.executemany(
        "INSERT OR REPLACE INTO ohlcv (ticker, date, timestamp, open, high, low, close, volume, row_index, data_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.commit()
    return cursor.rowcount


def get_enabled_tickers(conn: sqlite3.Connection) -> list:
    """Get all enabled tickers from the database."""
    rows = conn.execute(
        "SELECT ticker, trading_type FROM tickers WHERE enabled = 1 ORDER BY ticker"
    ).fetchall()
    return [(r[0], r[1]) for r in rows]


# ─── ByMA API Client ────────────────────────────────────────────────────────


def _byma_request(url: str, timeout: int = 30) -> bytes | None:
    """Make a GET request to ByMA with required headers and retries."""
    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("Origin", "https://open.bymadata.com.ar")
    req.add_header("Referer", "https://open.bymadata.com.ar/")
    req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")

    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, context=_ssl_ctx, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 400:
                # ByMA returns 400 for invalid symbols — not retryable
                return None
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF[attempt]
                log.warning(f"  Retry {attempt + 1}/{MAX_RETRIES} in {wait}s: HTTP {e.code}")
                time.sleep(wait)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_BACKOFF[attempt]
                log.warning(f"  Retry {attempt + 1}/{MAX_RETRIES} in {wait}s: {e}")
                time.sleep(wait)

    if last_error:
        raise last_error
    return None


def fetch_history(ticker: str, from_ts: int = 946684800, to_ts: int = None) -> list:
    """Fetch OHLCV history from ByMA for a ticker.

    Args:
        ticker: Ticker symbol (e.g. 'GGAL', 'AL30', 'AAPL')
        from_ts: Start timestamp (default: 2000-01-01)
        to_ts: End timestamp (default: now)

    Returns:
        List of (ticker, date, timestamp, open, high, low, close, volume, row_index, data_source) tuples,
        or empty list if no data.
    """
    if to_ts is None:
        to_ts = int(time.time())

    # ByMA requires +24HS suffix
    symbol = f"{ticker}+24HS"
    url = f"{BYMA_HISTORY}?symbol={symbol}&resolution=D&from={from_ts}&to={to_ts}"

    data_bytes = _byma_request(url)
    if data_bytes is None:
        return []

    try:
        data = json.loads(data_bytes)
    except json.JSONDecodeError:
        log.warning(f"  {ticker}: invalid JSON from ByMA")
        return []

    status = data.get("s")
    if status != "ok":
        if status == "no_data":
            log.debug(f"  {ticker}: no_data from ByMA")
        elif status == "error":
            log.debug(f"  {ticker}: error from ByMA: {data.get('errmsg', '')}")
        return []

    timestamps = data.get("t", [])
    opens = data.get("o", [])
    highs = data.get("h", [])
    lows = data.get("l", [])
    closes = data.get("c", [])
    volumes = data.get("v", [])

    n = len(timestamps)
    if n == 0:
        return []

    rows = []
    for i in range(n):
        ts = timestamps[i]
        date_str = datetime.fromtimestamp(ts).strftime("%Y-%m-%d")
        rows.append((
            ticker,
            date_str,
            ts,
            opens[i],
            highs[i],
            lows[i],
            closes[i],
            int(volumes[i]),
            0,  # row_index
            SOURCE_BYMA,
        ))

    return rows


def fetch_symbols(ticker: str) -> dict | None:
    """Fetch symbol metadata from ByMA (optional, for diagnostics)."""
    url = f"{BYMA_SYMBOLS}?symbol={ticker}"
    data_bytes = _byma_request(url)
    if data_bytes is None:
        return None
    try:
        return json.loads(data_bytes)
    except json.JSONDecodeError:
        return None


# ─── Download Orchestration ──────────────────────────────────────────────────


class DownloadStats:
    def __init__(self):
        self.success = 0
        self.empty = 0
        self.errors = 0
        self.skipped = 0
        self.total_bars = 0
        self.empty_tickers = []
        self.error_tickers = []


def download_all(
    tickers: list,
    conn: sqlite3.Connection,
    force: bool = False,
    delay: float = DEFAULT_DELAY,
) -> DownloadStats:
    """Download historical data from ByMA for all specified tickers."""
    stats = DownloadStats()
    total = len(tickers)

    for i, (ticker, trading_type) in enumerate(tickers, 1):
        # Skip tickers that already have data (unless --force)
        if not force and has_ohlcv_data(conn, ticker):
            log.debug(f"[{i}/{total}] {ticker} - ya tiene datos, saltando")
            stats.skipped += 1
            continue

        log.info(f"[{i}/{total}] ByMA: {ticker} ({trading_type})...")

        try:
            rows = fetch_history(ticker)

            if not rows:
                log.info(f"[{i}/{total}] {ticker} - sin datos en ByMA")
                log_download(conn, ticker, "empty", 0, 0)
                stats.empty += 1
                stats.empty_tickers.append(ticker)
                time.sleep(delay * 0.3)
                continue

            stored = store_ohlcv(conn, rows)
            mark_downloaded(conn, ticker, stored, SOURCE_BYMA)
            log_download(conn, ticker, "ok", len(rows), stored)

            log.info(f"[{i}/{total}] {ticker} - {stored:,} barras guardadas (ByMA)")
            stats.success += 1
            stats.total_bars += stored

        except KeyboardInterrupt:
            log.warning("Interrumpido por el usuario")
            break
        except Exception as e:
            log.error(f"[{i}/{total}] {ticker} ByMA ERROR: {e}")
            log_download(conn, ticker, "error", 0, 0, str(e))
            stats.errors += 1
            stats.error_tickers.append((ticker, str(e)))

        time.sleep(delay)

    return stats


def print_summary(stats: DownloadStats, total: int):
    print("\n" + "=" * 60)
    print("RESUMEN ByMA DATA")
    print("=" * 60)
    print(f"  Total tickers:         {total}")
    print(f"  Descargados OK:        {stats.success}")
    print(f"  Sin datos en ByMA:     {stats.empty}")
    print(f"  Errores:               {stats.errors}")
    print(f"  Saltados (ya OK):      {stats.skipped}")
    print(f"  Total barras:          {stats.total_bars:,}")

    if stats.empty_tickers:
        print(f"\nTickers sin datos en ByMA ({len(stats.empty_tickers)}):")
        for t in stats.empty_tickers:
            print(f"  - {t}")

    if stats.error_tickers:
        print(f"\nTickers con error ({len(stats.error_tickers)}):")
        for t, err in stats.error_tickers:
            print(f"  - {t}: {err}")

    print("=" * 60)


# ─── CLI ─────────────────────────────────────────────────────────────────────


def parse_args():
    parser = argparse.ArgumentParser(
        description="Descarga datos históricos OHLCV desde open.bymadata.com.ar (API pública)"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-descargar tickers aunque ya tengan datos",
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
        "-v", "--verbose", action="store_true", help="Logging verbose"
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

    log.info(f"Base de datos: {db_path}")
    conn = init_db(db_path)

    # Load tickers from database (not CSV)
    all_tickers = get_enabled_tickers(conn)
    log.info(f"Tickers habilitados en DB: {len(all_tickers)}")

    if args.ticker:
        match = [(t, tt) for t, tt in all_tickers if t == args.ticker]
        if not match:
            log.warning(f"Ticker {args.ticker} no encontrado en DB, intentando igual")
            match = [(args.ticker, "UNKNOWN")]
        all_tickers = match

    total = len(all_tickers)
    log.info(f"Descargando {total} tickers desde ByMA | delay={args.delay}s")

    try:
        stats = download_all(all_tickers, conn, force=args.force, delay=args.delay)
    except KeyboardInterrupt:
        log.warning("Interrumpido")
        stats = DownloadStats()

    conn.close()
    print_summary(stats, total)


if __name__ == "__main__":
    main()
