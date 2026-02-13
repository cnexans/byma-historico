#!/usr/bin/env python3
"""
CLI interactivo para agregar tickers al repositorio.

Agrega un ticker nuevo a tickers.csv y opcionalmente descarga sus datos
históricos usando la cascada de fuentes.

Uso:
    python add_ticker.py                     # Modo interactivo
    python add_ticker.py GGAL                # Pre-completar ticker
    python add_ticker.py GGAL --type STOCK   # Pre-completar ticker y tipo
    python add_ticker.py --batch AAPL,MSFT,GOOG --type CEDEARS  # Agregar varios
"""

import argparse
import csv
import os
import sqlite3
import subprocess
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(SCRIPT_DIR, "tickers.csv")
DB_PATH = os.path.join(SCRIPT_DIR, "data", "historial.db")

TRADING_TYPES = ["STOCK", "CEDEARS", "BOND"]
SETTLEMENT_TYPES = {
    "STOCK": "ONE_WORKING_DAY",
    "CEDEARS": "ONE_WORKING_DAY",
    "BOND": "CONTADO",
}

CSV_COLUMNS = ["id", "name", "ticker", "trading_type", "settlement_type", "enabled", "description"]


# ─── Helpers ─────────────────────────────────────────────────────────────────


def color(text, code):
    """ANSI color wrapper."""
    return f"\033[{code}m{text}\033[0m"


def green(text):
    return color(text, "32")


def yellow(text):
    return color(text, "33")


def red(text):
    return color(text, "31")


def cyan(text):
    return color(text, "36")


def bold(text):
    return color(text, "1")


def read_csv():
    """Read tickers.csv and return list of dicts."""
    if not os.path.exists(CSV_PATH):
        return []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(rows):
    """Write list of dicts to tickers.csv."""
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def get_max_id(rows):
    """Get max ID from CSV rows."""
    max_id = 0
    for r in rows:
        try:
            rid = int(r.get("id", 0) or 0)
            if rid > max_id:
                max_id = rid
        except ValueError:
            pass
    # Also check DB
    if os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        db_max = conn.execute("SELECT COALESCE(MAX(id), 0) FROM tickers").fetchone()[0]
        conn.close()
        max_id = max(max_id, db_max)
    return max_id


def ticker_exists(rows, ticker):
    """Check if ticker already exists in CSV."""
    return any(r["ticker"].strip().upper() == ticker.upper() for r in rows)


def get_db_info(ticker):
    """Check if ticker has data in DB."""
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        "SELECT COUNT(*), MIN(date), MAX(date) FROM ohlcv WHERE ticker = ? AND row_index = 0",
        (ticker,),
    ).fetchone()
    conn.close()
    if row and row[0] > 0:
        return {"bars": row[0], "min_date": row[1], "max_date": row[2]}
    return None


def prompt(msg, default=None, validate=None):
    """Prompt user for input with optional default and validation."""
    while True:
        suffix = f" [{default}]" if default else ""
        try:
            value = input(f"  {msg}{suffix}: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if not value and default:
            value = default
        if not value:
            print(f"    {red('Requerido')}")
            continue
        if validate and not validate(value):
            continue
        return value


def prompt_choice(msg, choices, default=None):
    """Prompt user to choose from a list."""
    print(f"  {msg}")
    for i, c in enumerate(choices, 1):
        marker = " *" if c == default else ""
        print(f"    {cyan(str(i))}. {c}{marker}")
    while True:
        suffix = f" [{choices.index(default) + 1}]" if default else ""
        try:
            value = input(f"  Opcion{suffix}: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            sys.exit(0)
        if not value and default:
            return default
        try:
            idx = int(value) - 1
            if 0 <= idx < len(choices):
                return choices[idx]
        except ValueError:
            # Try matching by name
            upper = value.upper()
            for c in choices:
                if c.upper() == upper or c.upper().startswith(upper):
                    return c
        print(f"    {red('Opcion invalida')}")


def prompt_yn(msg, default=True):
    """Yes/no prompt."""
    suffix = "[Y/n]" if default else "[y/N]"
    try:
        value = input(f"  {msg} {suffix}: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        sys.exit(0)
    if not value:
        return default
    return value in ("y", "yes", "si", "s")


# ─── Add Ticker ──────────────────────────────────────────────────────────────


def add_single_ticker(rows, max_id, preset_ticker=None, preset_type=None):
    """Interactive add of a single ticker. Returns new row dict or None."""
    print()
    print(bold("─── Agregar ticker ───"))
    print()

    # Ticker symbol
    ticker = preset_ticker
    if not ticker:
        ticker = prompt("Ticker (ej: GGAL, AAPL)").upper()
    else:
        ticker = ticker.upper()
        print(f"  Ticker: {green(ticker)}")

    # Check if exists
    if ticker_exists(rows, ticker):
        print(f"\n  {yellow('!')} El ticker {bold(ticker)} ya existe en tickers.csv")
        db_info = get_db_info(ticker)
        if db_info:
            print(f"      {db_info['bars']} barras, {db_info['min_date']} a {db_info['max_date']}")
        return None

    # Check DB for existing data
    db_info = get_db_info(ticker)
    if db_info:
        print(f"  {cyan('i')} Ya tiene datos en DB: {db_info['bars']} barras ({db_info['min_date']} a {db_info['max_date']})")

    # Name
    name = prompt("Nombre (ej: Banco Galicia)")

    # Trading type
    trading_type = preset_type
    if not trading_type:
        trading_type = prompt_choice("Tipo de instrumento:", TRADING_TYPES, default="CEDEARS")
    else:
        trading_type = trading_type.upper()
        if trading_type not in TRADING_TYPES:
            print(f"  {red('Tipo invalido')}: {trading_type}. Opciones: {', '.join(TRADING_TYPES)}")
            return None
        print(f"  Tipo: {green(trading_type)}")

    # Settlement type
    settlement = SETTLEMENT_TYPES.get(trading_type, "ONE_WORKING_DAY")

    # Description (optional)
    desc = ""
    if prompt_yn("Agregar descripcion?", default=False):
        desc = prompt("Descripcion")

    # Assign ID
    new_id = max_id + 1

    new_row = {
        "id": str(new_id),
        "name": name,
        "ticker": ticker,
        "trading_type": trading_type,
        "settlement_type": settlement,
        "enabled": "true",
        "description": desc,
    }

    # Confirm
    print()
    print(f"  {bold('Resumen:')}")
    print(f"    ID:          {new_id}")
    print(f"    Ticker:      {green(ticker)}")
    print(f"    Nombre:      {name}")
    print(f"    Tipo:        {trading_type}")
    print(f"    Settlement:  {settlement}")
    if desc:
        print(f"    Descripcion: {desc[:60]}{'...' if len(desc) > 60 else ''}")
    print()

    if not prompt_yn("Confirmar?", default=True):
        print(f"  {yellow('Cancelado')}")
        return None

    return new_row


def add_batch_tickers(rows, max_id, tickers_str, trading_type):
    """Add multiple tickers at once. Returns list of new row dicts."""
    tickers = [t.strip().upper() for t in tickers_str.split(",") if t.strip()]
    if not tickers:
        print(red("No se proporcionaron tickers"))
        return []

    trading_type = trading_type.upper() if trading_type else "CEDEARS"
    if trading_type not in TRADING_TYPES:
        print(red(f"Tipo invalido: {trading_type}. Opciones: {', '.join(TRADING_TYPES)}"))
        return []

    settlement = SETTLEMENT_TYPES.get(trading_type, "ONE_WORKING_DAY")
    new_rows = []
    current_max = max_id

    print()
    print(bold(f"─── Agregar {len(tickers)} tickers ({trading_type}) ───"))
    print()

    skipped = []
    for ticker in tickers:
        if ticker_exists(rows, ticker):
            skipped.append(ticker)
            continue
        current_max += 1
        new_rows.append({
            "id": str(current_max),
            "name": ticker,  # Use ticker as placeholder name
            "ticker": ticker,
            "trading_type": trading_type,
            "settlement_type": settlement,
            "enabled": "true",
            "description": "",
        })

    if skipped:
        print(f"  {yellow('Saltados')} (ya existen): {', '.join(skipped)}")

    if not new_rows:
        print(f"  {yellow('No hay tickers nuevos para agregar')}")
        return []

    print(f"  {green('Nuevos')}: {', '.join(r['ticker'] for r in new_rows)}")
    print()

    if not prompt_yn(f"Agregar {len(new_rows)} tickers?", default=True):
        print(f"  {yellow('Cancelado')}")
        return []

    return new_rows


def insert_sorted(rows, new_rows):
    """Insert new rows into the list maintaining sort order (type, then ticker)."""
    type_order = {"STOCK": 1, "CEDEARS": 2, "BOND": 3}
    all_rows = rows + new_rows
    all_rows.sort(key=lambda r: (type_order.get(r["trading_type"], 9), r["ticker"]))
    return all_rows


# ─── Main ────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="CLI interactivo para agregar tickers",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python add_ticker.py                           # Modo interactivo
  python add_ticker.py GGAL                      # Pre-completar ticker
  python add_ticker.py GGAL --type STOCK         # Pre-completar ticker y tipo
  python add_ticker.py --batch AAPL,MSFT --type CEDEARS  # Agregar varios de golpe
        """,
    )
    parser.add_argument("ticker", nargs="?", help="Ticker a agregar")
    parser.add_argument("--type", "-t", dest="trading_type", help="Tipo: STOCK, CEDEARS, BOND")
    parser.add_argument("--batch", "-b", help="Lista de tickers separados por coma")
    parser.add_argument("--download", "-d", action="store_true", help="Descargar datos despues de agregar")
    parser.add_argument("--build", action="store_true", help="Rebuild sitio estatico despues de agregar")
    args = parser.parse_args()

    # Check tickers.csv exists
    if not os.path.exists(CSV_PATH):
        print(f"{red('Error')}: No se encontro {CSV_PATH}")
        print(f"  Generalo con: python scraper_cascade.py --export-csv tickers.csv")
        sys.exit(1)

    # Read existing CSV
    rows = read_csv()
    max_id = get_max_id(rows)

    print()
    print(bold("Scrapper Historico - Agregar Tickers"))
    print(f"  CSV: {CSV_PATH}")
    print(f"  Tickers actuales: {len(rows)}")
    print(f"  Habilitados: {sum(1 for r in rows if r.get('enabled', '').lower() in ('true', '1'))}")

    # Batch mode
    if args.batch:
        new_rows = add_batch_tickers(rows, max_id, args.batch, args.trading_type or "CEDEARS")
        if not new_rows:
            return
        rows = insert_sorted(rows, new_rows)
        write_csv(rows)
        added_tickers = [r["ticker"] for r in new_rows]
        print(f"\n  {green('+')} Agregados {len(new_rows)} tickers a tickers.csv")
    else:
        # Interactive single ticker
        new_row = add_single_ticker(rows, max_id, preset_ticker=args.ticker, preset_type=args.trading_type)
        if not new_row:
            return
        rows = insert_sorted(rows, [new_row])
        write_csv(rows)
        added_tickers = [new_row["ticker"]]
        print(f"\n  {green('+')} {bold(new_row['ticker'])} agregado a tickers.csv")

    # Ask to download
    should_download = args.download
    if not should_download and not args.batch:
        should_download = prompt_yn("\nDescargar datos ahora?", default=True)

    if should_download:
        print(f"\n  Descargando datos para: {', '.join(added_tickers)}...")
        for ticker in added_tickers:
            cmd = [
                sys.executable, os.path.join(SCRIPT_DIR, "scraper_cascade.py"),
                "--ticker", ticker, "--skip-iol", "-v",
            ]
            print(f"\n  {cyan('>')} {ticker}")
            subprocess.run(cmd, cwd=SCRIPT_DIR)

    # Ask to build
    should_build = args.build
    if not should_build and should_download:
        should_build = prompt_yn("\nRebuildar sitio estatico?", default=False)

    if should_build:
        print(f"\n  Rebuilding...")
        cmd = [sys.executable, os.path.join(SCRIPT_DIR, "build_static.py")]
        subprocess.run(cmd, cwd=SCRIPT_DIR)

    print(f"\n{green('Listo!')}\n")


if __name__ == "__main__":
    main()
