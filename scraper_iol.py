#!/usr/bin/env python3
"""
Scrapper IOL - Descarga datos históricos OHLCV desde InvertirOnline (IOL)

Usa Playwright para scrapear la tabla de cotizaciones históricas de
https://iol.invertironline.com/Titulo/DatosHistoricos

Diseñado como tercera fuente de datos, después de analisistecnico.com.ar y Yahoo Finance.

Requisitos:
    pip install playwright
    python -m playwright install chromium

Uso:
    python scraper_iol.py tickers.csv                    # Descargar tickers sin datos
    python scraper_iol.py tickers.csv --force            # Re-descargar todo
    python scraper_iol.py tickers.csv --ticker GGAL      # Un solo ticker
    python scraper_iol.py tickers.csv --delay 2.0        # Delay entre tickers
    python scraper_iol.py tickers.csv -v                 # Verbose
"""

import argparse
import csv
import logging
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
except ImportError:
    print("ERROR: Playwright no está instalado.")
    print("  pip install playwright")
    print("  python -m playwright install chromium")
    sys.exit(1)

# ─── Constants ───────────────────────────────────────────────────────────────

IOL_BASE_URL = "https://iol.invertironline.com/Titulo/DatosHistoricos"
DEFAULT_DELAY = 1.0  # seconds between tickers
DB_FILENAME = "historial.db"
DATA_DIR = "data"
SOURCE_IOL = "iol"
DATE_RANGE_FROM = "01/01/2000"

log = logging.getLogger("scraper_iol")

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
        (ticker, now, now, status, bars_found, bars_stored, error_msg, SOURCE_IOL),
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


# ─── Number Parsing ──────────────────────────────────────────────────────────


def parse_number(text: str) -> float:
    """Parse number from IOL format: '7,690.00' or '24,127,424,220.00'

    IOL uses comma as thousands separator and dot as decimal.
    """
    if not text or text == "-" or text == "":
        return 0.0
    # Remove thousands separators (commas)
    cleaned = text.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def parse_date(text: str) -> tuple[str, int]:
    """Parse IOL date format 'MM/DD/YYYY' to ('YYYY-MM-DD', unix_timestamp).

    IOL returns dates in MM/DD/YYYY format.
    """
    try:
        dt = datetime.strptime(text.strip(), "%m/%d/%Y")
        date_str = dt.strftime("%Y-%m-%d")
        timestamp = int(dt.replace(tzinfo=timezone.utc).timestamp())
        return date_str, timestamp
    except ValueError:
        # Try DD/MM/YYYY format as fallback
        try:
            dt = datetime.strptime(text.strip(), "%d/%m/%Y")
            date_str = dt.strftime("%Y-%m-%d")
            timestamp = int(dt.replace(tzinfo=timezone.utc).timestamp())
            return date_str, timestamp
        except ValueError:
            return None, None


# ─── IOL Scraper ─────────────────────────────────────────────────────────────


class IOLScraper:
    """Scrapes historical OHLCV data from InvertirOnline using Playwright."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self._pw = None
        self._browser = None
        self._page = None

    def start(self):
        """Start the browser."""
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=self.headless)
        self._page = self._browser.new_page()
        log.debug("Browser started")

    def stop(self):
        """Stop the browser."""
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()
        log.debug("Browser stopped")

    def scrape_ticker(self, ticker: str) -> list:
        """Scrape all historical OHLCV data for a ticker from IOL.

        Returns list of (ticker, date, timestamp, open, high, low, close, volume, row_index, data_source) tuples.
        """
        page = self._page
        url = f"{IOL_BASE_URL}?simbolo={ticker}&mercado=BCBA"

        try:
            page.goto(url, wait_until="networkidle", timeout=30000)
        except PlaywrightTimeout:
            log.warning(f"  {ticker}: timeout loading page")
            return []

        # Check for "no data" heading
        no_data_heading = page.query_selector("h2")
        if no_data_heading:
            heading_text = no_data_heading.inner_text().strip()
            if "No hay datos" in heading_text:
                log.debug(f"  {ticker}: no data on IOL")
                return []

        # Check if data table exists
        tables = page.query_selector_all("table")
        if len(tables) < 2:
            log.debug(f"  {ticker}: no data table found")
            return []

        # Set date range from far past to today
        today = datetime.now().strftime("%m/%d/%Y")
        page.evaluate(f"""
            document.querySelector('#DesdeHasta').value = '{DATE_RANGE_FROM} - {today}';
        """)

        # Click "Aplicar"
        try:
            page.click("#aplicarbusqueda")
            page.wait_for_timeout(3000)
        except Exception as e:
            log.warning(f"  {ticker}: error clicking Aplicar: {e}")
            return []

        # Set rows per page to 200
        try:
            page.select_option("select[name='tbcotizaciones_length']", "200")
            page.wait_for_timeout(2000)
        except Exception as e:
            log.warning(f"  {ticker}: error setting page size: {e}")

        # Get total entries from info text
        info_el = page.query_selector(".dataTables_info")
        total_entries = 0
        if info_el:
            info_text = info_el.inner_text().strip()
            # Parse "1 - 200 de 9,072" or "1 - 63 de 63"
            match = re.search(r'de\s+([\d,\.]+)', info_text)
            if match:
                total_entries = int(match.group(1).replace(",", "").replace(".", ""))
            log.debug(f"  {ticker}: {total_entries} total entries on IOL")

        if total_entries == 0:
            log.debug(f"  {ticker}: 0 entries on IOL")
            return []

        # Scrape all pages
        all_rows = []
        page_num = 1
        max_pages = (total_entries // 200) + 2  # safety limit

        while page_num <= max_pages:
            # Extract table data from current page
            data_table = page.query_selector_all("table")
            if len(data_table) < 2:
                break

            tbl = data_table[1]
            tr_elements = tbl.query_selector_all("tbody tr")

            if not tr_elements:
                break

            rows_on_page = 0
            for tr in tr_elements:
                cells = tr.query_selector_all("td")
                if len(cells) < 8:
                    continue

                cell_texts = [c.inner_text().strip() for c in cells]
                # Columns: Fecha, Apertura, Máximo, Mínimo, Cierre, Cierre ajustado, Volumen Monto, Volumen Nominal
                date_text = cell_texts[0]
                open_val = parse_number(cell_texts[1])
                high_val = parse_number(cell_texts[2])
                low_val = parse_number(cell_texts[3])
                close_val = parse_number(cell_texts[4])
                # Use Volumen Nominal (column 7) as volume
                volume_val = int(parse_number(cell_texts[7]))

                date_str, timestamp = parse_date(date_text)
                if date_str is None:
                    log.warning(f"  {ticker}: unparseable date '{date_text}'")
                    continue

                # Skip rows with all zeros
                if open_val == 0 and high_val == 0 and low_val == 0 and close_val == 0:
                    continue

                all_rows.append((
                    ticker,
                    date_str,
                    timestamp,
                    open_val,
                    high_val,
                    low_val,
                    close_val,
                    volume_val,
                    0,  # row_index
                    SOURCE_IOL,
                ))
                rows_on_page += 1

            log.debug(f"  {ticker}: page {page_num} → {rows_on_page} rows")

            # Check if there's a next page
            next_btn = page.query_selector(".paginate_button.next:not(.disabled)")
            if not next_btn:
                break

            # Click next page
            next_btn.click()
            page.wait_for_timeout(1500)
            page_num += 1

        return all_rows


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
    scraper: IOLScraper,
    trading_types: dict,
    force: bool = False,
    delay: float = DEFAULT_DELAY,
    only_missing: bool = True,
) -> DownloadStats:
    """Download historical data from IOL for all specified tickers."""
    stats = DownloadStats()
    total = len(tickers)

    for i, ticker in enumerate(tickers, 1):
        # Skip tickers that already have data (unless --force)
        if not force and only_missing and has_ohlcv_data(conn, ticker):
            log.debug(f"[{i}/{total}] {ticker} - ya tiene datos, saltando")
            stats.skipped += 1
            continue

        trading_type = trading_types.get(ticker, "UNKNOWN")
        log.info(f"[{i}/{total}] IOL: {ticker} ({trading_type})...")

        try:
            rows = scraper.scrape_ticker(ticker)

            if not rows:
                log.warning(f"[{i}/{total}] {ticker} - sin datos en IOL")
                log_download(conn, ticker, "empty", 0, 0)
                stats.empty += 1
                stats.empty_tickers.append(ticker)
                # Short delay even for empty ones
                time.sleep(delay * 0.5)
                continue

            stored = store_ohlcv(conn, rows)
            mark_downloaded(conn, ticker, stored, SOURCE_IOL)
            log_download(conn, ticker, "ok", len(rows), stored)

            log.info(f"[{i}/{total}] {ticker} - {stored:,} barras guardadas (IOL)")
            stats.success += 1
            stats.total_bars += stored

        except KeyboardInterrupt:
            log.warning("Interrumpido por el usuario")
            break
        except Exception as e:
            log.error(f"[{i}/{total}] {ticker} IOL ERROR: {e}")
            log_download(conn, ticker, "error", 0, 0, str(e))
            stats.errors += 1
            stats.error_tickers.append((ticker, str(e)))

        # Rate limit between tickers
        time.sleep(delay)

    return stats


def print_summary(stats: DownloadStats, total: int):
    print("\n" + "=" * 60)
    print("RESUMEN IOL")
    print("=" * 60)
    print(f"  Total tickers:         {total}")
    print(f"  Descargados OK:        {stats.success}")
    print(f"  Sin datos:             {stats.empty}")
    print(f"  Errores:               {stats.errors}")
    print(f"  Saltados (ya OK):      {stats.skipped}")
    print(f"  Total barras:          {stats.total_bars:,}")

    if stats.empty_tickers:
        print(f"\nTickers sin datos en IOL ({len(stats.empty_tickers)}):")
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
        description="Descarga datos históricos OHLCV desde InvertirOnline (IOL) usando Playwright"
    )
    parser.add_argument("csv_file", help="Ruta al CSV con los tickers")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-descargar tickers aunque ya tengan datos",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Descargar todos los tickers, no solo los que faltan",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Segundos entre tickers (default: {DEFAULT_DELAY})",
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
        "--headed",
        action="store_true",
        help="Mostrar el navegador (no headless)",
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
    only_missing = not args.all
    log.info(
        f"Descargando {total} tickers desde IOL | "
        f"delay={args.delay}s | solo_faltantes={only_missing}"
    )

    scraper = IOLScraper(headless=not args.headed)
    scraper.start()

    try:
        stats = download_all(
            tickers_to_download,
            conn,
            scraper,
            trading_types,
            force=args.force,
            delay=args.delay,
            only_missing=only_missing,
        )
    except KeyboardInterrupt:
        log.warning("Interrumpido")
        stats = DownloadStats()
    finally:
        scraper.stop()

    conn.close()
    print_summary(stats, total)


if __name__ == "__main__":
    main()
