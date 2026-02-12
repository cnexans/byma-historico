#!/usr/bin/env python3
"""
Build Static Site - Genera un sitio estático a partir de la base de datos SQLite.

Crea una carpeta 'public/' con:
  - index.html (sitio completo standalone)
  - api/tickers.json
  - api/report.json
  - api/ohlcv/{ticker}.json (uno por ticker con datos)

Listo para deploy en Vercel, Netlify, o cualquier hosting estático.

Uso:
    python build_static.py                    # Genera en ./public
    python build_static.py --output dist      # Genera en ./dist
    python build_static.py --db-path data/historial.db
"""

import argparse
import json
import os
import re
import sqlite3
import shutil
from datetime import datetime, timezone

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(SCRIPT_DIR, "data", "historial.db")
DEFAULT_OUTPUT = os.path.join(SCRIPT_DIR, "public")

# ─── Bond expiration date parser ─────────────────────────────────────────────

MONTHS_ES = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
    'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
    'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
}


def parse_bond_expiration(ticker: str, description: str) -> str | None:
    if not description:
        return None
    m = re.search(
        r'vencimiento\s+(?:el\s+)?(\d{1,2})\s+de\s+(\w+)\s+(?:de(?:l)?\s+)?(\d{4})',
        description, re.IGNORECASE
    )
    if m:
        day, month_name, year = m.groups()
        month = MONTHS_ES.get(month_name.lower())
        if month:
            return f"{year}-{month:02d}-{int(day):02d}"
    m = re.search(r'venciendo\s+el\s+(\d{1,2})/(\d{2})', description, re.IGNORECASE)
    if m:
        day, month = m.groups()
        year_digit = re.search(r'(\d)$', ticker)
        if year_digit:
            year = 2020 + int(year_digit.group(1))
            return f"{year}-{int(month):02d}-{int(day):02d}"
    return None


# ─── Database queries ────────────────────────────────────────────────────────


def get_conn(db_path):
    conn = sqlite3.connect(db_path)
    # Run migrations just in case
    for sql in [
        "ALTER TABLE ohlcv ADD COLUMN data_source TEXT DEFAULT 'analisistecnico'",
        "ALTER TABLE tickers ADD COLUMN data_source TEXT",
        "ALTER TABLE download_log ADD COLUMN data_source TEXT",
    ]:
        try:
            conn.execute(sql)
            conn.commit()
        except sqlite3.OperationalError:
            pass
    return conn


def build_tickers_json(conn):
    rows = conn.execute("""
        SELECT
            t.ticker,
            t.name,
            t.trading_type,
            t.enabled,
            t.description,
            t.data_source,
            COUNT(o.date) as bars,
            MIN(o.date) as min_date,
            MAX(o.date) as max_date
        FROM tickers t
        LEFT JOIN ohlcv o ON t.ticker = o.ticker AND o.row_index = 0
        GROUP BY t.ticker
        ORDER BY
            CASE t.trading_type WHEN 'STOCK' THEN 1 WHEN 'CEDEARS' THEN 2 WHEN 'BOND' THEN 3 END,
            t.ticker
    """).fetchall()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    result = []
    for r in rows:
        ticker, name, trading_type, enabled, description, data_source = r[0], r[1], r[2], r[3], r[4], r[5]
        bars, min_date, max_date = r[6], r[7], r[8]

        expiration_date = None
        is_expired = False
        if trading_type == "BOND" and description:
            expiration_date = parse_bond_expiration(ticker, description)
            if expiration_date:
                is_expired = expiration_date < today

        result.append({
            "ticker": ticker,
            "name": name,
            "trading_type": trading_type,
            "enabled": bool(enabled),
            "bars": bars,
            "min_date": min_date,
            "max_date": max_date,
            "expiration_date": expiration_date,
            "is_expired": is_expired,
            "data_source": data_source,
        })

    return result


def build_report_json(conn):
    rows = conn.execute("""
        SELECT
            t.ticker,
            t.trading_type,
            t.description,
            t.data_source,
            COUNT(o.date) as bars
        FROM tickers t
        LEFT JOIN ohlcv o ON t.ticker = o.ticker AND o.row_index = 0
        GROUP BY t.ticker
    """).fetchall()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    stock_total = stock_data = stock_bars = 0
    cedears_total = cedears_data = cedears_bars = 0
    bond_total = bond_data = bond_bars = 0
    bond_active = bond_active_data = 0
    bond_expired = bond_expired_data = 0
    src_analisistecnico = src_yahoo = src_iol = 0

    for ticker, trading_type, description, data_source, bars in rows:
        has_data = bars > 0

        if has_data:
            src = (data_source or "analisistecnico").lower()
            if src == "yahoo":
                src_yahoo += 1
            elif src == "iol":
                src_iol += 1
            else:
                src_analisistecnico += 1

        if trading_type == "STOCK":
            stock_total += 1
            stock_bars += bars
            if has_data:
                stock_data += 1
        elif trading_type == "CEDEARS":
            cedears_total += 1
            cedears_bars += bars
            if has_data:
                cedears_data += 1
        elif trading_type == "BOND":
            bond_total += 1
            bond_bars += bars
            if has_data:
                bond_data += 1
            exp = parse_bond_expiration(ticker, description) if description else None
            if exp and exp < today:
                bond_expired += 1
                if has_data:
                    bond_expired_data += 1
            else:
                bond_active += 1
                if has_data:
                    bond_active_data += 1

    total_with_data = src_analisistecnico + src_yahoo + src_iol

    return {
        "stock_total": stock_total,
        "stock_with_data": stock_data,
        "stock_bars": stock_bars,
        "cedears_total": cedears_total,
        "cedears_with_data": cedears_data,
        "cedears_bars": cedears_bars,
        "bond_total": bond_total,
        "bond_with_data": bond_data,
        "bond_bars": bond_bars,
        "bond_active": bond_active,
        "bond_active_with_data": bond_active_data,
        "bond_expired": bond_expired,
        "bond_expired_with_data": bond_expired_data,
        "src_analisistecnico": src_analisistecnico,
        "src_yahoo": src_yahoo,
        "src_iol": src_iol,
        "total_with_data": total_with_data,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def build_ohlcv_json(conn, ticker):
    rows = conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv "
        "WHERE ticker = ? AND row_index = 0 ORDER BY date",
        (ticker,),
    ).fetchall()

    if not rows:
        return None

    return {
        "dates": [r[0] for r in rows],
        "open": [r[1] for r in rows],
        "high": [r[2] for r in rows],
        "low": [r[3] for r in rows],
        "close": [r[4] for r in rows],
        "volume": [r[5] for r in rows],
    }


# ─── HTML Template ───────────────────────────────────────────────────────────

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scrapper Historico - Dashboard</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #0f1117; color: #e1e4e8; }

  .layout { display: flex; height: 100vh; }

  /* ── Sidebar ── */
  .sidebar { width: 480px; min-width: 480px; border-right: 1px solid #2d333b; display: flex; flex-direction: column; background: #161b22; }
  .sidebar-header { padding: 16px; border-bottom: 1px solid #2d333b; }
  .sidebar-header h1 { font-size: 16px; font-weight: 600; margin-bottom: 10px; }
  .search-box { width: 100%; padding: 8px 12px; background: #0d1117; border: 1px solid #30363d; border-radius: 6px; color: #e1e4e8; font-size: 14px; outline: none; }
  .search-box:focus { border-color: #58a6ff; }
  .filters { display: flex; gap: 6px; margin-top: 8px; flex-wrap: wrap; }
  .filter-btn { padding: 4px 10px; font-size: 12px; border: 1px solid #30363d; border-radius: 12px; background: transparent; color: #8b949e; cursor: pointer; }
  .filter-btn.active { background: #1f6feb; border-color: #1f6feb; color: #fff; }
  .stats-bar { padding: 8px 16px; font-size: 12px; color: #8b949e; border-bottom: 1px solid #2d333b; }

  .ticker-list { flex: 1; overflow-y: auto; }
  .ticker-row { display: grid; grid-template-columns: 80px 1fr 60px 82px 82px; align-items: center; padding: 8px 16px; border-bottom: 1px solid #21262d; cursor: pointer; font-size: 13px; transition: background 0.1s; gap: 4px; }
  .ticker-row:hover { background: #1c2128; }
  .ticker-row.active { background: #1f6feb22; border-left: 3px solid #1f6feb; }
  .ticker-row.expired { opacity: 0.5; }
  .ticker-row .ticker-name { font-weight: 600; color: #58a6ff; display: flex; align-items: center; gap: 4px; }
  .ticker-row .info-col { display: flex; align-items: center; gap: 5px; overflow: hidden; }
  .ticker-row .name-text { white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-size: 12px; }
  .ticker-row .type-badge { font-size: 10px; padding: 2px 6px; border-radius: 4px; font-weight: 500; flex-shrink: 0; }
  .type-badge.BOND { background: #3fb9501a; color: #3fb950; }
  .type-badge.CEDEARS { background: #d2a8ff1a; color: #d2a8ff; }
  .type-badge.STOCK { background: #58a6ff1a; color: #58a6ff; }
  .expired-badge { font-size: 9px; padding: 1px 5px; border-radius: 3px; background: #f851491a; color: #f85149; font-weight: 600; flex-shrink: 0; }
  .active-badge { font-size: 9px; padding: 1px 5px; border-radius: 3px; background: #3fb9501a; color: #3fb950; font-weight: 600; flex-shrink: 0; }
  .ticker-row .bars { text-align: right; color: #8b949e; }
  .ticker-row .date { text-align: right; color: #8b949e; font-size: 11px; }

  .list-header { display: grid; grid-template-columns: 80px 1fr 60px 82px 82px; padding: 8px 16px; font-size: 11px; color: #8b949e; border-bottom: 1px solid #2d333b; text-transform: uppercase; font-weight: 600; letter-spacing: 0.5px; gap: 4px; }
  .list-header span:nth-child(4), .list-header span:nth-child(5) { text-align: right; }
  .list-header span:nth-child(3) { text-align: right; }

  /* ── Main area ── */
  .main { flex: 1; display: flex; flex-direction: column; overflow-y: auto; }

  /* ── Report ── */
  .report { padding: 24px; }
  .report h2 { font-size: 18px; font-weight: 600; margin-bottom: 16px; }
  .report-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px; margin-bottom: 24px; }
  .report-card { background: #161b22; border: 1px solid #2d333b; border-radius: 8px; padding: 20px; }
  .report-card .card-title { font-size: 12px; color: #8b949e; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; display: flex; align-items: center; gap: 6px; }
  .report-card .card-value { font-size: 32px; font-weight: 700; }
  .report-card .card-sub { font-size: 12px; color: #8b949e; margin-top: 4px; }
  .report-card .card-bar { margin-top: 12px; height: 6px; background: #21262d; border-radius: 3px; overflow: hidden; }
  .report-card .card-bar-fill { height: 100%; border-radius: 3px; }
  .report-card.stock .card-value { color: #58a6ff; }
  .report-card.stock .card-bar-fill { background: #58a6ff; }
  .report-card.cedears .card-value { color: #d2a8ff; }
  .report-card.cedears .card-bar-fill { background: #d2a8ff; }
  .report-card.bond .card-value { color: #3fb950; }
  .report-card.bond .card-bar-fill { background: #3fb950; }

  .bond-detail { background: #161b22; border: 1px solid #2d333b; border-radius: 8px; padding: 20px; margin-bottom: 24px; }
  .bond-detail h3 { font-size: 14px; font-weight: 600; margin-bottom: 12px; }
  .bond-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
  .bond-stat { text-align: center; }
  .bond-stat .val { font-size: 24px; font-weight: 700; }
  .bond-stat .lbl { font-size: 11px; color: #8b949e; margin-top: 2px; }
  .bond-stat.green .val { color: #3fb950; }
  .bond-stat.red .val { color: #f85149; }
  .bond-stat.blue .val { color: #58a6ff; }
  .bond-stat.yellow .val { color: #d29922; }

  .coverage-bar { margin-top: 16px; }
  .coverage-bar .bar-label { font-size: 12px; color: #8b949e; margin-bottom: 6px; display: flex; justify-content: space-between; }
  .coverage-bar .bar-track { height: 10px; background: #21262d; border-radius: 5px; overflow: hidden; display: flex; }
  .coverage-bar .bar-seg { height: 100%; }
  .coverage-bar .bar-seg.green { background: #3fb950; }
  .coverage-bar .bar-seg.red { background: #f85149; opacity: 0.5; }
  .coverage-bar .bar-seg.gray { background: #484f58; }

  .coverage-legend { display: flex; gap: 16px; margin-top: 8px; font-size: 11px; color: #8b949e; flex-wrap: wrap; }
  .coverage-legend .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 4px; }

  .source-detail { background: #161b22; border: 1px solid #2d333b; border-radius: 8px; padding: 20px; margin-bottom: 24px; }
  .source-detail h3 { font-size: 14px; font-weight: 600; margin-bottom: 12px; }
  .source-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 12px; }
  .source-stat { text-align: center; padding: 12px; background: #0d1117; border-radius: 6px; }
  .source-stat .val { font-size: 24px; font-weight: 700; }
  .source-stat .lbl { font-size: 11px; color: #8b949e; margin-top: 2px; }
  .source-stat .pct { font-size: 13px; color: #8b949e; }
  .source-stat.src-at .val { color: #f0883e; }
  .source-stat.src-yahoo .val { color: #a371f7; }
  .source-stat.src-iol .val { color: #58a6ff; }
  .source-badge { font-size: 9px; padding: 1px 5px; border-radius: 3px; font-weight: 600; flex-shrink: 0; }
  .source-badge.src-at { background: #f0883e1a; color: #f0883e; }
  .source-badge.src-yahoo { background: #a371f71a; color: #a371f7; }
  .source-badge.src-iol { background: #58a6ff1a; color: #58a6ff; }

  .generated-at { padding: 8px 24px; font-size: 11px; color: #484f58; text-align: right; }

  /* ── Chart ── */
  .chart-header { padding: 16px 24px; border-bottom: 1px solid #2d333b; }
  .chart-header h2 { font-size: 20px; font-weight: 600; display: flex; align-items: center; gap: 8px; }
  .chart-header .subtitle { color: #8b949e; font-size: 13px; margin-top: 2px; }
  .chart-header .price-info { margin-top: 8px; display: flex; gap: 24px; flex-wrap: wrap; }
  .chart-header .price-info .value { font-size: 28px; font-weight: 700; }
  .chart-header .price-info .label { font-size: 11px; color: #8b949e; text-transform: uppercase; }
  .chart-header .price-info .change.positive { color: #3fb950; }
  .chart-header .price-info .change.negative { color: #f85149; }

  .chart-container { flex: 1; padding: 16px 24px; position: relative; min-height: 400px; }
  canvas { width: 100% !important; height: 100% !important; }

  .empty-state { display: none; }

  /* Scrollbar */
  .ticker-list::-webkit-scrollbar { width: 6px; }
  .ticker-list::-webkit-scrollbar-track { background: transparent; }
  .ticker-list::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }
  .main::-webkit-scrollbar { width: 6px; }
  .main::-webkit-scrollbar-track { background: transparent; }
  .main::-webkit-scrollbar-thumb { background: #30363d; border-radius: 3px; }

  /* Loading */
  .loading { display: flex; align-items: center; justify-content: center; height: 100%; color: #8b949e; font-size: 14px; }
</style>
</head>
<body>

<div class="layout">
  <div class="sidebar">
    <div class="sidebar-header">
      <h1>Scrapper Historico</h1>
      <input type="text" class="search-box" id="search" placeholder="Buscar ticker..." autocomplete="off">
      <div class="filters" id="filters">
        <button class="filter-btn active" data-filter="ALL">Todos</button>
        <button class="filter-btn" data-filter="STOCK">Acciones</button>
        <button class="filter-btn" data-filter="BOND">Bonos</button>
        <button class="filter-btn" data-filter="CEDEARS">CEDEARs</button>
        <button class="filter-btn" data-filter="BOND_ACTIVE">Bonos vigentes</button>
        <button class="filter-btn" data-filter="BOND_EXPIRED">Bonos vencidos</button>
      </div>
    </div>
    <div class="stats-bar" id="stats-bar"></div>
    <div class="list-header">
      <span>Ticker</span>
      <span>Info</span>
      <span>Barras</span>
      <span>Desde</span>
      <span>Hasta</span>
    </div>
    <div class="ticker-list" id="ticker-list"></div>
  </div>

  <div class="main" id="main-panel">
    <div class="report" id="report"><div class="loading">Cargando datos...</div></div>
    <div class="chart-header" id="chart-header" style="display:none">
      <h2 id="chart-title"></h2>
      <div class="subtitle" id="chart-subtitle"></div>
      <div class="price-info">
        <div>
          <div class="label">Ultimo cierre</div>
          <div class="value" id="chart-price"></div>
        </div>
        <div>
          <div class="label">Variacion total</div>
          <div class="value change" id="chart-change"></div>
        </div>
        <div>
          <div class="label">Periodo</div>
          <div class="value" id="chart-period" style="font-size:16px;"></div>
        </div>
        <div id="chart-expiry-block" style="display:none">
          <div class="label">Vencimiento</div>
          <div class="value" id="chart-expiry" style="font-size:16px;"></div>
        </div>
      </div>
    </div>
    <div class="chart-container" id="chart-container" style="display:none">
      <canvas id="chart"></canvas>
    </div>
  </div>
</div>

<script>
const BASE_PATH = '';
let allTickers = [];
let report = null;
let currentFilter = 'ALL';
let currentSearch = '';
let selectedTicker = null;

// Cache for OHLCV data
const ohlcvCache = {};

async function init() {
  const [tickersResp, reportResp] = await Promise.all([
    fetch(BASE_PATH + '/api/tickers.json'),
    fetch(BASE_PATH + '/api/report.json')
  ]);
  allTickers = await tickersResp.json();
  report = await reportResp.json();
  renderReport();
  renderList();
}

function renderReport() {
  if (!report) return;
  const r = report;
  const el = document.getElementById('report');

  const pctStock = r.stock_total > 0 ? (r.stock_with_data / r.stock_total * 100) : 0;
  const pctCedears = r.cedears_total > 0 ? (r.cedears_with_data / r.cedears_total * 100) : 0;
  const pctBond = r.bond_active > 0 ? (r.bond_active_with_data / r.bond_active * 100) : 0;

  const bondActivePct = r.bond_active > 0 ? (r.bond_active_with_data / r.bond_active * 100) : 0;
  const bondExpiredPct = r.bond_expired > 0 ? (r.bond_expired_with_data / r.bond_expired * 100) : 0;

  const barGreen = r.bond_total > 0 ? (r.bond_active_with_data / r.bond_total * 100) : 0;
  const barGreenEmpty = r.bond_total > 0 ? ((r.bond_active - r.bond_active_with_data) / r.bond_total * 100) : 0;
  const barRed = r.bond_total > 0 ? (r.bond_expired_with_data / r.bond_total * 100) : 0;
  const barGray = r.bond_total > 0 ? ((r.bond_expired - r.bond_expired_with_data) / r.bond_total * 100) : 0;

  el.innerHTML = `
    <h2>Reporte Ejecutivo</h2>
    <div class="report-grid">
      <div class="report-card stock">
        <div class="card-title">Acciones</div>
        <div class="card-value">${r.stock_with_data}/${r.stock_total}</div>
        <div class="card-sub">${pctStock.toFixed(0)}% con datos historicos | ${r.stock_bars.toLocaleString()} barras</div>
        <div class="card-bar"><div class="card-bar-fill" style="width:${pctStock}%"></div></div>
      </div>
      <div class="report-card cedears">
        <div class="card-title">CEDEARs</div>
        <div class="card-value">${r.cedears_with_data}/${r.cedears_total}</div>
        <div class="card-sub">${pctCedears.toFixed(0)}% con datos historicos | ${r.cedears_bars.toLocaleString()} barras</div>
        <div class="card-bar"><div class="card-bar-fill" style="width:${pctCedears}%"></div></div>
      </div>
      <div class="report-card bond">
        <div class="card-title">Bonos</div>
        <div class="card-value">${r.bond_active_with_data}/${r.bond_active}</div>
        <div class="card-sub">${pctBond.toFixed(0)}% bonos vigentes con datos | ${r.bond_bars.toLocaleString()} barras</div>
        <div class="card-bar"><div class="card-bar-fill" style="width:${pctBond}%"></div></div>
      </div>
    </div>

    <div class="bond-detail">
      <h3>Detalle Bonos - Vigencia</h3>
      <div class="bond-grid">
        <div class="bond-stat green">
          <div class="val">${r.bond_active}</div>
          <div class="lbl">Vigentes</div>
        </div>
        <div class="bond-stat red">
          <div class="val">${r.bond_expired}</div>
          <div class="lbl">Vencidos</div>
        </div>
        <div class="bond-stat green">
          <div class="val">${r.bond_active_with_data}/${r.bond_active}</div>
          <div class="lbl">Vigentes con datos (${bondActivePct.toFixed(0)}%)</div>
        </div>
        <div class="bond-stat yellow">
          <div class="val">${r.bond_expired_with_data}/${r.bond_expired}</div>
          <div class="lbl">Vencidos con datos (${bondExpiredPct.toFixed(0)}%)</div>
        </div>
      </div>
      <div class="coverage-bar">
        <div class="bar-label">
          <span>Cobertura de descarga por estado</span>
          <span>${r.bond_total} bonos totales</span>
        </div>
        <div class="bar-track">
          <div class="bar-seg green" style="width:${barGreen}%"></div>
          <div class="bar-seg" style="width:${barGreenEmpty}%;background:#3fb95040"></div>
          <div class="bar-seg red" style="width:${barRed}%"></div>
          <div class="bar-seg gray" style="width:${barGray}%"></div>
        </div>
        <div class="coverage-legend">
          <span><span class="dot" style="background:#3fb950"></span>Vigente con datos (${r.bond_active_with_data})</span>
          <span><span class="dot" style="background:#3fb95060"></span>Vigente sin datos (${r.bond_active - r.bond_active_with_data})</span>
          <span><span class="dot" style="background:#f8514980"></span>Vencido con datos (${r.bond_expired_with_data})</span>
          <span><span class="dot" style="background:#484f58"></span>Vencido sin datos (${r.bond_expired - r.bond_expired_with_data})</span>
        </div>
      </div>
    </div>

    <div class="source-detail">
      <h3>Fuentes de Datos</h3>
      <div class="source-grid">
        <div class="source-stat src-at">
          <div class="val">${r.src_analisistecnico}</div>
          <div class="pct">${r.total_with_data > 0 ? (r.src_analisistecnico / r.total_with_data * 100).toFixed(0) : 0}%</div>
          <div class="lbl">analisistecnico.com.ar</div>
        </div>
        <div class="source-stat src-yahoo">
          <div class="val">${r.src_yahoo}</div>
          <div class="pct">${r.total_with_data > 0 ? (r.src_yahoo / r.total_with_data * 100).toFixed(0) : 0}%</div>
          <div class="lbl">Yahoo Finance</div>
        </div>
        <div class="source-stat src-iol">
          <div class="val">${r.src_iol}</div>
          <div class="pct">${r.total_with_data > 0 ? (r.src_iol / r.total_with_data * 100).toFixed(0) : 0}%</div>
          <div class="lbl">InvertirOnline</div>
        </div>
      </div>
      <div class="coverage-bar" style="margin-top:12px">
        <div class="bar-label">
          <span>Distribucion por fuente</span>
          <span>${r.total_with_data} tickers con datos</span>
        </div>
        <div class="bar-track">
          <div class="bar-seg" style="width:${r.total_with_data > 0 ? (r.src_analisistecnico / r.total_with_data * 100) : 0}%;background:#f0883e"></div>
          <div class="bar-seg" style="width:${r.total_with_data > 0 ? (r.src_yahoo / r.total_with_data * 100) : 0}%;background:#a371f7"></div>
          <div class="bar-seg" style="width:${r.total_with_data > 0 ? (r.src_iol / r.total_with_data * 100) : 0}%;background:#58a6ff"></div>
        </div>
        <div class="coverage-legend">
          <span><span class="dot" style="background:#f0883e"></span>analisistecnico (${r.src_analisistecnico})</span>
          <span><span class="dot" style="background:#a371f7"></span>Yahoo Finance (${r.src_yahoo})</span>
          <span><span class="dot" style="background:#58a6ff"></span>InvertirOnline (${r.src_iol})</span>
        </div>
      </div>
    </div>

    <div class="generated-at">Generado: ${r.generated_at || ''}</div>
  `;
}

function renderList() {
  const filtered = allTickers.filter(t => {
    if (currentFilter === 'BOND_ACTIVE') return t.trading_type === 'BOND' && !t.is_expired;
    if (currentFilter === 'BOND_EXPIRED') return t.trading_type === 'BOND' && t.is_expired;
    if (currentFilter !== 'ALL' && t.trading_type !== currentFilter) return false;
    if (currentSearch && !t.ticker.toLowerCase().includes(currentSearch) && !t.name.toLowerCase().includes(currentSearch)) return false;
    return true;
  });

  const statsBar = document.getElementById('stats-bar');
  const withData = filtered.filter(t => t.bars > 0).length;
  const expired = filtered.filter(t => t.is_expired).length;
  let statsText = `${filtered.length} tickers | ${withData} con datos`;
  if (expired > 0) statsText += ` | ${expired} vencidos`;
  statsBar.textContent = statsText;

  const list = document.getElementById('ticker-list');
  list.innerHTML = filtered.map(t => {
    const isExpired = t.is_expired;
    const nameShort = t.name.length > 20 ? t.name.slice(0, 20) + '...' : t.name;

    let badges = `<span class="type-badge ${t.trading_type}">${t.trading_type}</span>`;
    if (t.data_source && t.bars > 0) {
      const srcClass = t.data_source === 'yahoo' ? 'src-yahoo' : t.data_source === 'iol' ? 'src-iol' : 'src-at';
      const srcLabel = t.data_source === 'yahoo' ? 'YF' : t.data_source === 'iol' ? 'IOL' : 'AT';
      badges += ` <span class="source-badge ${srcClass}">${srcLabel}</span>`;
    }
    if (t.trading_type === 'BOND') {
      if (isExpired) {
        badges += ` <span class="expired-badge">VENCIDO ${t.expiration_date || ''}</span>`;
      } else if (t.expiration_date) {
        badges += ` <span class="active-badge">Vto ${t.expiration_date}</span>`;
      }
    }

    return `
    <div class="ticker-row ${selectedTicker === t.ticker ? 'active' : ''} ${isExpired ? 'expired' : ''}" data-ticker="${t.ticker}">
      <span class="ticker-name">${t.ticker}</span>
      <span class="info-col">${badges} <span class="name-text">${nameShort}</span></span>
      <span class="bars">${t.bars > 0 ? t.bars.toLocaleString() : '-'}</span>
      <span class="date">${t.min_date || '-'}</span>
      <span class="date">${t.max_date || '-'}</span>
    </div>`;
  }).join('');

  list.querySelectorAll('.ticker-row').forEach(row => {
    row.addEventListener('click', () => loadChart(row.dataset.ticker));
  });
}

async function loadChart(ticker) {
  selectedTicker = ticker;
  renderList();

  // Load OHLCV from static JSON (with cache)
  let data;
  if (ohlcvCache[ticker]) {
    data = ohlcvCache[ticker];
  } else {
    try {
      const resp = await fetch(BASE_PATH + `/api/ohlcv/${ticker}.json`);
      if (!resp.ok) {
        data = { dates: [], close: [], open: [], high: [], low: [], volume: [] };
      } else {
        data = await resp.json();
      }
      ohlcvCache[ticker] = data;
    } catch (e) {
      data = { dates: [], close: [], open: [], high: [], low: [], volume: [] };
    }
  }

  if (!data.dates || data.dates.length === 0) return;

  const meta = allTickers.find(t => t.ticker === ticker);

  document.getElementById('report').style.display = 'none';
  document.getElementById('chart-header').style.display = 'block';
  document.getElementById('chart-container').style.display = 'block';

  let titleHtml = `${ticker} — ${meta ? meta.name : ''}`;
  if (meta && meta.is_expired) titleHtml += ' <span class="expired-badge" style="font-size:12px">VENCIDO</span>';
  document.getElementById('chart-title').innerHTML = titleHtml;

  const srcName = meta?.data_source === 'yahoo' ? 'Yahoo Finance' : meta?.data_source === 'iol' ? 'InvertirOnline' : 'analisistecnico.com.ar';
  let subtitle = meta ? `${meta.trading_type} | ${data.dates.length} barras | Fuente: ${srcName}` : '';
  document.getElementById('chart-subtitle').textContent = subtitle;

  const lastClose = data.close[data.close.length - 1];
  const firstClose = data.close[0];
  const changePct = ((lastClose - firstClose) / firstClose * 100);

  document.getElementById('chart-price').textContent = lastClose.toLocaleString('es-AR', { maximumFractionDigits: 2 });
  const changeEl = document.getElementById('chart-change');
  changeEl.textContent = `${changePct >= 0 ? '+' : ''}${changePct.toFixed(2)}%`;
  changeEl.className = `value change ${changePct >= 0 ? 'positive' : 'negative'}`;
  document.getElementById('chart-period').textContent = `${data.dates[0]} a ${data.dates[data.dates.length - 1]}`;

  const expiryBlock = document.getElementById('chart-expiry-block');
  if (meta && meta.expiration_date) {
    expiryBlock.style.display = 'block';
    const expiryEl = document.getElementById('chart-expiry');
    expiryEl.textContent = meta.expiration_date;
    expiryEl.style.color = meta.is_expired ? '#f85149' : '#3fb950';
  } else {
    expiryBlock.style.display = 'none';
  }

  drawChart(data);
}

function drawChart(data) {
  const canvas = document.getElementById('chart');
  const container = document.getElementById('chart-container');
  const dpr = window.devicePixelRatio || 1;
  const rect = container.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  const W = rect.width;
  const H = rect.height;

  ctx.clearRect(0, 0, W, H);

  const closes = data.close;
  const dates = data.dates;
  const n = closes.length;
  if (n < 2) return;

  const pad = { top: 20, right: 60, bottom: 40, left: 10 };
  const cw = W - pad.left - pad.right;
  const ch = H - pad.top - pad.bottom;

  const minV = Math.min(...closes);
  const maxV = Math.max(...closes);
  const range = maxV - minV || 1;

  function x(i) { return pad.left + (i / (n - 1)) * cw; }
  function y(v) { return pad.top + ch - ((v - minV) / range) * ch; }

  // Grid lines
  ctx.strokeStyle = '#21262d';
  ctx.lineWidth = 1;
  const gridLines = 5;
  for (let i = 0; i <= gridLines; i++) {
    const yy = pad.top + (i / gridLines) * ch;
    ctx.beginPath();
    ctx.moveTo(pad.left, yy);
    ctx.lineTo(W - pad.right, yy);
    ctx.stroke();

    const val = maxV - (i / gridLines) * range;
    ctx.fillStyle = '#484f58';
    ctx.font = '11px -apple-system, sans-serif';
    ctx.textAlign = 'left';
    ctx.fillText(val.toLocaleString('es-AR', { maximumFractionDigits: 0 }), W - pad.right + 8, yy + 4);
  }

  // Date labels
  ctx.fillStyle = '#484f58';
  ctx.font = '11px -apple-system, sans-serif';
  ctx.textAlign = 'center';
  const labelCount = Math.min(8, n);
  for (let i = 0; i < labelCount; i++) {
    const idx = Math.floor(i / (labelCount - 1) * (n - 1));
    ctx.fillText(dates[idx], x(idx), H - pad.bottom + 20);
  }

  // Gradient fill
  const gradient = ctx.createLinearGradient(0, pad.top, 0, pad.top + ch);
  const isPositive = closes[n - 1] >= closes[0];
  if (isPositive) {
    gradient.addColorStop(0, 'rgba(63, 185, 80, 0.15)');
    gradient.addColorStop(1, 'rgba(63, 185, 80, 0)');
  } else {
    gradient.addColorStop(0, 'rgba(248, 81, 73, 0.15)');
    gradient.addColorStop(1, 'rgba(248, 81, 73, 0)');
  }

  ctx.beginPath();
  ctx.moveTo(x(0), y(closes[0]));
  for (let i = 1; i < n; i++) ctx.lineTo(x(i), y(closes[i]));
  ctx.lineTo(x(n - 1), pad.top + ch);
  ctx.lineTo(x(0), pad.top + ch);
  ctx.closePath();
  ctx.fillStyle = gradient;
  ctx.fill();

  // Line
  ctx.beginPath();
  ctx.moveTo(x(0), y(closes[0]));
  for (let i = 1; i < n; i++) ctx.lineTo(x(i), y(closes[i]));
  ctx.strokeStyle = isPositive ? '#3fb950' : '#f85149';
  ctx.lineWidth = 1.5;
  ctx.stroke();
}

// Show report when clicking header
function showReport() {
  selectedTicker = null;
  document.getElementById('report').style.display = 'block';
  document.getElementById('chart-header').style.display = 'none';
  document.getElementById('chart-container').style.display = 'none';
  renderList();
}

// Filters
document.getElementById('filters').addEventListener('click', e => {
  if (!e.target.classList.contains('filter-btn')) return;
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  e.target.classList.add('active');
  currentFilter = e.target.dataset.filter;
  renderList();
});

// Search
document.getElementById('search').addEventListener('input', e => {
  currentSearch = e.target.value.toLowerCase();
  renderList();
});

// Resize
window.addEventListener('resize', () => {
  if (selectedTicker) loadChart(selectedTicker);
});

// Click title to go back to report
document.querySelector('.sidebar-header h1').style.cursor = 'pointer';
document.querySelector('.sidebar-header h1').addEventListener('click', showReport);

init();
</script>
</body>
</html>"""


# ─── Build ───────────────────────────────────────────────────────────────────


def build(db_path: str, output_dir: str):
    print(f"Base de datos: {db_path}")
    print(f"Output: {output_dir}")

    conn = get_conn(db_path)

    # Clean output directory
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "api", "ohlcv"), exist_ok=True)

    # 1. Generate tickers.json
    print("Generando tickers.json...")
    tickers = build_tickers_json(conn)
    with open(os.path.join(output_dir, "api", "tickers.json"), "w") as f:
        json.dump(tickers, f, separators=(",", ":"))
    print(f"  {len(tickers)} tickers")

    # 2. Generate report.json
    print("Generando report.json...")
    report = build_report_json(conn)
    with open(os.path.join(output_dir, "api", "report.json"), "w") as f:
        json.dump(report, f, separators=(",", ":"))
    print(f"  {report['total_with_data']} tickers con datos")

    # 3. Generate per-ticker OHLCV JSON files
    tickers_with_data = [t for t in tickers if t["bars"] > 0]
    print(f"Generando {len(tickers_with_data)} archivos OHLCV...")
    total_bars = 0
    for t in tickers_with_data:
        ohlcv = build_ohlcv_json(conn, t["ticker"])
        if ohlcv:
            filepath = os.path.join(output_dir, "api", "ohlcv", f"{t['ticker']}.json")
            with open(filepath, "w") as f:
                json.dump(ohlcv, f, separators=(",", ":"))
            total_bars += len(ohlcv["dates"])

    print(f"  {total_bars:,} barras totales")

    # 4. Generate index.html
    print("Generando index.html...")
    with open(os.path.join(output_dir, "index.html"), "w") as f:
        f.write(HTML_TEMPLATE)

    conn.close()

    # Calculate total size
    total_size = 0
    for root, dirs, files in os.walk(output_dir):
        for file in files:
            total_size += os.path.getsize(os.path.join(root, file))

    file_count = sum(1 for _ in os.walk(output_dir) for f in _[2])

    print(f"\n{'='*60}")
    print(f"Build completado!")
    print(f"{'='*60}")
    print(f"  Directorio: {output_dir}")
    print(f"  Archivos:   {file_count}")
    print(f"  Tamanio:    {total_size / 1024 / 1024:.1f} MB")
    print(f"\nPara deploy en Vercel:")
    print(f"  cd {output_dir} && npx vercel --prod")
    print(f"\nPara preview local:")
    print(f"  cd {output_dir} && python3 -m http.server 8080")
    print(f"{'='*60}")


# ─── CLI ─────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(
        description="Build static site from SQLite database"
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB,
        help=f"Ruta a la base de datos SQLite (default: {DEFAULT_DB})",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"Directorio de salida (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()
    build(args.db_path, args.output)


if __name__ == "__main__":
    main()
