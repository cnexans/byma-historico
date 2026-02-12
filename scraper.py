#!/usr/bin/env python3
"""
Scrapper Histórico - Descarga datos OHLCV desde analisistecnico.com.ar y Yahoo Finance

Descarga el historial completo de precios para todos los tickers de un CSV
y los persiste en una base de datos SQLite. Usa analisistecnico.com.ar como
fuente primaria y Yahoo Finance como fallback para tickers sin datos.

Uso:
    python scraper.py tickers.csv                    # Descargar todos los tickers
    python scraper.py tickers.csv --force            # Re-descargar todo
    python scraper.py tickers.csv --ticker YPFD      # Un solo ticker
    python scraper.py tickers.csv --no-yahoo         # Sin fallback Yahoo
    python scraper.py tickers.csv --delay 2.0        # Delay custom entre requests
    python scraper.py tickers.csv -v                 # Logging verbose
"""

import argparse
import csv
import json
import logging
import os
import signal
import sqlite3
import ssl
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ─── Constants ───────────────────────────────────────────────────────────────

BASE_URL = "https://analisistecnico.com.ar/services/datafeed"
YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart"
DEFAULT_RESOLUTION = "D"
DEFAULT_DELAY = 1.5
DB_FILENAME = "historial.db"
DATA_DIR = "data"
MAX_RETRIES = 3
RETRY_BACKOFF = [5, 15, 45]  # seconds

# Data source identifiers
SOURCE_ANALISISTECNICO = "analisistecnico"
SOURCE_YAHOO = "yahoo"

log = logging.getLogger("scraper")

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
    # Run migrations for existing databases
    for sql in MIGRATION_SQL:
        try:
            conn.execute(sql)
        except sqlite3.OperationalError:
            pass  # Column already exists
    conn.commit()
    return conn


def is_already_downloaded(conn: sqlite3.Connection, ticker: str) -> bool:
    row = conn.execute(
        "SELECT downloaded_at FROM tickers WHERE ticker = ?", (ticker,)
    ).fetchone()
    return row is not None and row[0] is not None


def has_ohlcv_data(conn: sqlite3.Connection, ticker: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) FROM ohlcv WHERE ticker = ?", (ticker,)
    ).fetchone()
    return row[0] > 0


def mark_downloaded(conn: sqlite3.Connection, ticker: str, bars_count: int, source: str = None):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE tickers SET downloaded_at = ?, bars_count = ?, data_source = ? WHERE ticker = ?",
        (now, bars_count, source, ticker),
    )
    conn.commit()


def log_download(
    conn: sqlite3.Connection,
    ticker: str,
    status: str,
    bars_found: int,
    bars_stored: int,
    error_msg: str = None,
    source: str = None,
):
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO download_log (ticker, started_at, finished_at, status, bars_found, bars_stored, error_msg, data_source) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (ticker, now, now, status, bars_found, bars_stored, error_msg, source),
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


# ─── CSV Loading ─────────────────────────────────────────────────────────────


def load_tickers(csv_path: str, conn: sqlite3.Connection) -> dict:
    """Load tickers from CSV into database. Returns {ticker: trading_type}."""
    trading_types = {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    for row in rows:
        ticker = row["ticker"].strip()
        trading_type = row["trading_type"].strip()
        enabled = row["enabled"].strip().lower() == "true"
        trading_types[ticker] = trading_type

        conn.execute(
            "INSERT INTO tickers (id, name, ticker, trading_type, settlement_type, enabled, description, url_icon) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(ticker) DO UPDATE SET "
            "name=excluded.name, trading_type=excluded.trading_type, "
            "settlement_type=excluded.settlement_type, enabled=excluded.enabled, "
            "description=excluded.description, url_icon=excluded.url_icon",
            (
                int(row["id"]),
                row["name"].strip(),
                ticker,
                trading_type,
                row["settlement_type"].strip(),
                enabled,
                row["description"].strip(),
                row["url_icon"].strip(),
            ),
        )

    conn.commit()

    count_by_type = {}
    for t in trading_types.values():
        count_by_type[t] = count_by_type.get(t, 0) + 1

    log.info(
        f"Loaded {len(rows)} tickers: "
        + ", ".join(f"{v} {k}" for k, v in sorted(count_by_type.items()))
    )

    return trading_types


# ─── API Client: analisistecnico.com.ar ──────────────────────────────────────


class APIClient:
    def __init__(self, base_url: str, delay: float = DEFAULT_DELAY):
        self.base_url = base_url
        self.delay = delay
        self.ssl_ctx = ssl.create_default_context()
        self.ssl_ctx.check_hostname = False
        self.ssl_ctx.verify_mode = ssl.CERT_NONE
        self._last_request_time = 0.0

    def _throttle(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    def _request(self, url: str, ssl_ctx=None) -> bytes:
        self._throttle()
        self._last_request_time = time.time()

        req = urllib.request.Request(url)
        req.add_header("User-Agent", "ScrapperHistorico/1.0")

        ctx = ssl_ctx or self.ssl_ctx

        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                with urllib.request.urlopen(req, context=ctx, timeout=30) as resp:
                    return resp.read()
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF[attempt]
                    log.warning(f"  Retry {attempt + 1}/{MAX_RETRIES} in {wait}s: {e}")
                    time.sleep(wait)

        raise last_error

    def get_history(self, symbol: str, resolution: str = DEFAULT_RESOLUTION) -> dict | list:
        url = f"{self.base_url}/history?symbol={symbol}&resolution={resolution}"
        data = self._request(url)
        return json.loads(data)


# ─── API Client: Yahoo Finance ───────────────────────────────────────────────


class YahooClient:
    """Yahoo Finance v8 chart API client for BYMA tickers."""

    def __init__(self, delay: float = 0.5):
        self.delay = delay
        self._last_request_time = 0.0

    def _throttle(self):
        elapsed = time.time() - self._last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)

    def _request(self, url: str) -> bytes:
        self._throttle()
        self._last_request_time = time.time()

        req = urllib.request.Request(url)
        req.add_header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")

        last_error = None
        for attempt in range(MAX_RETRIES):
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    return resp.read()
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    return None  # Symbol not found on Yahoo
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF[attempt]
                    log.warning(f"  Yahoo retry {attempt + 1}/{MAX_RETRIES} in {wait}s: {e}")
                    time.sleep(wait)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                last_error = e
                if attempt < MAX_RETRIES - 1:
                    wait = RETRY_BACKOFF[attempt]
                    log.warning(f"  Yahoo retry {attempt + 1}/{MAX_RETRIES} in {wait}s: {e}")
                    time.sleep(wait)

        if last_error:
            raise last_error
        return None

    def get_history(self, ticker: str) -> dict | None:
        """Fetch full daily history from Yahoo Finance.

        Uses period1=0&period2=9999999999 (NOT range=max) to get true daily data.
        Appends .BA suffix for BYMA-listed instruments.
        Returns parsed chart data or None if not found.
        """
        yahoo_symbol = f"{ticker}.BA"
        url = (
            f"{YAHOO_URL}/{yahoo_symbol}"
            f"?period1=0&period2=9999999999&interval=1d"
        )
        raw = self._request(url)
        if raw is None:
            return None

        data = json.loads(raw)
        chart = data.get("chart", {})

        if chart.get("error") or not chart.get("result"):
            return None

        return chart["result"][0]


# ─── Data Processing ─────────────────────────────────────────────────────────


def unix_to_date(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")


def process_history(ticker: str, data, trading_type: str, source: str = SOURCE_ANALISISTECNICO) -> list:
    """Convert analisistecnico API response to list of row tuples for SQLite insertion.

    For bonds with duplicate timestamps, both entries are kept with row_index 0 and 1.

    Returns: list of (ticker, date, timestamp, open, high, low, close, volume, row_index, data_source)
    """
    if not data or isinstance(data, list):
        return []

    if data.get("s") != "ok":
        return []

    timestamps = data["t"]
    opens = data["o"]
    highs = data["h"]
    lows = data["l"]
    closes = data["c"]
    volumes = data["v"]

    rows = []
    date_count = {}  # track how many times each date appears

    for i in range(len(timestamps)):
        date_str = unix_to_date(timestamps[i])
        row_idx = date_count.get(date_str, 0)
        date_count[date_str] = row_idx + 1

        rows.append((
            ticker,
            date_str,
            timestamps[i],
            opens[i],
            highs[i],
            lows[i],
            closes[i],
            volumes[i],
            row_idx,
            source,
        ))

    return rows


def process_yahoo_history(ticker: str, chart_data: dict) -> list:
    """Convert Yahoo Finance chart response to list of row tuples for SQLite insertion.

    Filters out null OHLC entries (non-trading days).

    Returns: list of (ticker, date, timestamp, open, high, low, close, volume, row_index, data_source)
    """
    if not chart_data:
        return []

    timestamps = chart_data.get("timestamp", [])
    if not timestamps:
        return []

    quote = chart_data.get("indicators", {}).get("quote", [{}])[0]
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
        v = volumes[i] if i < len(volumes) else None

        # Skip null entries (non-trading days)
        if o is None or h is None or l is None or c is None:
            continue

        date_str = unix_to_date(timestamps[i])
        rows.append((
            ticker,
            date_str,
            timestamps[i],
            o,
            h,
            l,
            c,
            v or 0,
            0,  # row_index always 0 for Yahoo
            SOURCE_YAHOO,
        ))

    return rows


# ─── Download Orchestration ──────────────────────────────────────────────────


class DownloadStats:
    def __init__(self):
        self.success = 0
        self.empty = 0
        self.errors = 0
        self.skipped = 0
        self.yahoo_success = 0
        self.yahoo_empty = 0
        self.yahoo_errors = 0
        self.empty_tickers = []
        self.error_tickers = []


def download_all(
    tickers: list,
    conn: sqlite3.Connection,
    client: APIClient,
    trading_types: dict,
    resolution: str = DEFAULT_RESOLUTION,
    force: bool = False,
    use_yahoo: bool = True,
) -> DownloadStats:
    stats = DownloadStats()
    total = len(tickers)
    yahoo_client = YahooClient(delay=0.5) if use_yahoo else None
    yahoo_pending = []  # tickers to try on Yahoo

    # ── Phase 1: analisistecnico.com.ar ──
    log.info("═══ Fase 1: analisistecnico.com.ar ═══")
    for i, ticker in enumerate(tickers, 1):
        if not force and is_already_downloaded(conn, ticker):
            # If ticker was downloaded but has no data, queue for Yahoo
            if use_yahoo and not has_ohlcv_data(conn, ticker):
                trading_type = trading_types.get(ticker, "UNKNOWN")
                if trading_type != "BOND":
                    yahoo_pending.append(ticker)
                    log.debug(f"[{i}/{total}] {ticker} - sin datos, encolado para Yahoo")
                    continue
            log.debug(f"[{i}/{total}] {ticker} - ya descargado, saltando")
            stats.skipped += 1
            continue

        trading_type = trading_types.get(ticker, "UNKNOWN")
        log.info(f"[{i}/{total}] Descargando {ticker} ({trading_type})...")

        try:
            data = client.get_history(ticker, resolution)
            rows = process_history(ticker, data, trading_type, SOURCE_ANALISISTECNICO)

            if not rows:
                log.warning(f"[{i}/{total}] {ticker} - sin datos en analisistecnico")
                # Don't mark as downloaded yet — Yahoo might have data
                if use_yahoo and trading_type != "BOND":
                    yahoo_pending.append(ticker)
                else:
                    log_download(conn, ticker, "empty", 0, 0, source=SOURCE_ANALISISTECNICO)
                    mark_downloaded(conn, ticker, 0, SOURCE_ANALISISTECNICO)
                    stats.empty += 1
                    stats.empty_tickers.append(ticker)
                continue

            stored = store_ohlcv(conn, rows)
            mark_downloaded(conn, ticker, stored, SOURCE_ANALISISTECNICO)

            bars_found = len(data.get("t", []))
            log_download(conn, ticker, "ok", bars_found, stored, source=SOURCE_ANALISISTECNICO)
            log.info(f"[{i}/{total}] {ticker} - {stored} barras guardadas (analisistecnico)")
            stats.success += 1

        except KeyboardInterrupt:
            raise
        except Exception as e:
            log.error(f"[{i}/{total}] {ticker} - ERROR: {e}")
            log_download(conn, ticker, "error", 0, 0, str(e), source=SOURCE_ANALISISTECNICO)
            stats.errors += 1
            stats.error_tickers.append((ticker, str(e)))

    # ── Phase 2: Yahoo Finance fallback ──
    if yahoo_client and yahoo_pending:
        log.info(f"\n═══ Fase 2: Yahoo Finance ({len(yahoo_pending)} tickers pendientes) ═══")
        for i, ticker in enumerate(yahoo_pending, 1):
            trading_type = trading_types.get(ticker, "UNKNOWN")
            log.info(f"[{i}/{len(yahoo_pending)}] Yahoo: {ticker} ({trading_type})...")

            try:
                chart_data = yahoo_client.get_history(ticker)
                rows = process_yahoo_history(ticker, chart_data)

                if not rows:
                    log.warning(f"[{i}/{len(yahoo_pending)}] {ticker} - sin datos en Yahoo")
                    log_download(conn, ticker, "empty", 0, 0, source=SOURCE_YAHOO)
                    mark_downloaded(conn, ticker, 0, SOURCE_YAHOO)
                    stats.yahoo_empty += 1
                    stats.empty += 1
                    stats.empty_tickers.append(ticker)
                    continue

                stored = store_ohlcv(conn, rows)
                mark_downloaded(conn, ticker, stored, SOURCE_YAHOO)
                log_download(conn, ticker, "ok", len(rows), stored, source=SOURCE_YAHOO)
                log.info(f"[{i}/{len(yahoo_pending)}] {ticker} - {stored} barras guardadas (Yahoo Finance)")
                stats.yahoo_success += 1
                stats.success += 1

            except KeyboardInterrupt:
                raise
            except Exception as e:
                log.error(f"[{i}/{len(yahoo_pending)}] {ticker} Yahoo ERROR: {e}")
                log_download(conn, ticker, "error", 0, 0, str(e), source=SOURCE_YAHOO)
                stats.yahoo_errors += 1
                stats.errors += 1
                stats.error_tickers.append((ticker, f"Yahoo: {e}"))

    return stats


def print_summary(stats: DownloadStats, total: int):
    print("\n" + "=" * 60)
    print("RESUMEN")
    print("=" * 60)
    print(f"  Total tickers:         {total}")
    print(f"  Descargados OK:        {stats.success}")
    print(f"    - analisistecnico:   {stats.success - stats.yahoo_success}")
    print(f"    - Yahoo Finance:     {stats.yahoo_success}")
    print(f"  Sin datos:             {stats.empty}")
    print(f"  Errores:               {stats.errors}")
    print(f"  Saltados (ya OK):      {stats.skipped}")

    if stats.empty_tickers:
        print(f"\nTickers sin datos ({len(stats.empty_tickers)}):")
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
        description="Descarga datos históricos OHLCV desde analisistecnico.com.ar y Yahoo Finance"
    )
    parser.add_argument("csv_file", help="Ruta al CSV con los tickers")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-descargar tickers aunque ya estén descargados",
    )
    parser.add_argument(
        "--no-yahoo",
        action="store_true",
        help="No usar Yahoo Finance como fallback",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Segundos entre requests (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--resolution",
        default=DEFAULT_RESOLUTION,
        choices=["D", "W", "M"],
        help=f"Resolución temporal (default: {DEFAULT_RESOLUTION})",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help=f"Ruta a la base de datos SQLite (default: {DATA_DIR}/{DB_FILENAME})",
    )
    parser.add_argument(
        "--ticker",
        help="Descargar solo un ticker específico (para testing)",
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

    log.info(f"Cargando tickers desde {args.csv_file}...")
    trading_types = load_tickers(args.csv_file, conn)

    tickers_to_download = list(trading_types.keys())

    if args.ticker:
        if args.ticker not in trading_types:
            log.warning(f"Ticker {args.ticker} no encontrado en CSV, descargando igual")
            trading_types[args.ticker] = "UNKNOWN"
        tickers_to_download = [args.ticker]

    total = len(tickers_to_download)
    estimated_minutes = (total * args.delay) / 60
    use_yahoo = not args.no_yahoo
    log.info(
        f"Descargando {total} tickers | resolución={args.resolution} | "
        f"delay={args.delay}s | Yahoo={'ON' if use_yahoo else 'OFF'} | ~{estimated_minutes:.1f} min estimados"
    )

    client = APIClient(BASE_URL, delay=args.delay)

    interrupted = False

    def handle_sigint(sig, frame):
        nonlocal interrupted
        if interrupted:
            log.warning("Segundo Ctrl+C, saliendo inmediatamente")
            sys.exit(1)
        interrupted = True
        log.warning("Ctrl+C detectado, terminando ticker actual...")

    original_handler = signal.signal(signal.SIGINT, handle_sigint)

    try:
        stats = download_all(
            tickers_to_download,
            conn,
            client,
            trading_types,
            resolution=args.resolution,
            force=args.force,
            use_yahoo=use_yahoo,
        )
    except KeyboardInterrupt:
        log.warning("Interrumpido por el usuario")
        stats = DownloadStats()
        stats.errors = -1  # signal interruption
    finally:
        signal.signal(signal.SIGINT, original_handler)

    conn.close()
    print_summary(stats, total)


if __name__ == "__main__":
    main()
