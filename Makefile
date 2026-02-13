# ─── Scrapper Historico ───────────────────────────────────────────────────────
#
# Makefile para operaciones comunes del proyecto
#
# Uso rapido:
#   make help        - Ver todos los comandos disponibles
#   make download    - Descargar datos con cascada (sin IOL)
#   make build       - Rebuildar sitio estatico
#   make deploy      - Build + deploy a Vercel
#   make add         - Agregar un ticker (interactivo)
#
# ─────────────────────────────────────────────────────────────────────────────

PYTHON   := python3
DB       := data/historial.db
CSV      := tickers.csv
PORT     := 8080

.PHONY: help download download-all download-full download-ticker sync build \
        deploy serve serve-static add add-batch export-csv status clean

# ─── Help ────────────────────────────────────────────────────────────────────

help: ## Mostrar esta ayuda
	@echo ""
	@echo "  \033[1mScrapper Historico\033[0m - Comandos disponibles"
	@echo "  ─────────────────────────────────────────────"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "  \033[1mEjemplos:\033[0m"
	@echo "    make download                  Descargar datos (ByMA + Yahoo + analisistecnico)"
	@echo "    make download-ticker T=GGAL    Descargar un ticker especifico"
	@echo "    make add                       Agregar ticker interactivamente"
	@echo "    make add-batch T=AAPL,MSFT     Agregar varios CEDEARs de golpe"
	@echo "    make sync                      Sync CSV -> DB + descargar nuevos"
	@echo "    make build                     Rebuildar sitio estatico"
	@echo "    make deploy                    Build + deploy a Vercel"
	@echo ""

# ─── Descarga ────────────────────────────────────────────────────────────────

download: ## Descargar datos (ByMA + Yahoo + analisistecnico, sin IOL)
	$(PYTHON) scraper_cascade.py --skip-iol -v

download-all: ## Descargar datos (todas las fuentes, incluye IOL/Playwright)
	$(PYTHON) scraper_cascade.py -v

download-full: ## Descargar forzando re-descarga de todo
	$(PYTHON) scraper_cascade.py --force --skip-iol -v

download-ticker: ## Descargar un ticker especifico (T=GGAL)
ifndef T
	@echo "\033[31mError:\033[0m Especifica un ticker con T=GGAL"
	@echo "  Ejemplo: make download-ticker T=GGAL"
else
	$(PYTHON) scraper_cascade.py --ticker $(T) --skip-iol -v
endif

download-byma: ## Descargar solo desde ByMA
	$(PYTHON) scraper_byma.py -v

fix-cedears: ## Re-descargar CEDEARs con precios incorrectos de analisistecnico
	$(PYTHON) scraper_cascade.py --fix-cedears --skip-iol -v

# ─── Sync (CSV -> DB) ───────────────────────────────────────────────────────

sync: ## Sync tickers.csv -> DB y descargar nuevos/faltantes
	$(PYTHON) scraper_cascade.py --sync --skip-iol -v

sync-full: ## Sync con todas las fuentes (incluye IOL)
	$(PYTHON) scraper_cascade.py --sync -v

# ─── Build & Deploy ─────────────────────────────────────────────────────────

build: ## Rebuildar sitio estatico en public/
	$(PYTHON) build_static.py

deploy: build ## Build + deploy a Vercel
	cd public && npx vercel --prod

deploy-preview: build ## Build + deploy preview a Vercel
	cd public && npx vercel

# ─── Servidor local ─────────────────────────────────────────────────────────

serve: ## Iniciar servidor dinamico (con DB en vivo)
	$(PYTHON) server.py --port $(PORT)

serve-static: build ## Servir sitio estatico localmente
	@echo "Servidor en http://localhost:$(PORT)"
	cd public && $(PYTHON) -m http.server $(PORT)

# ─── Gestion de tickers ─────────────────────────────────────────────────────

add: ## Agregar un ticker (modo interactivo)
	$(PYTHON) add_ticker.py

add-batch: ## Agregar varios tickers (T=AAPL,MSFT,GOOG TYPE=CEDEARS)
ifndef T
	@echo "\033[31mError:\033[0m Especifica tickers con T=AAPL,MSFT,GOOG"
	@echo "  Ejemplo: make add-batch T=AAPL,MSFT TYPE=CEDEARS"
else
	$(PYTHON) add_ticker.py --batch $(T) --type $(or $(TYPE),CEDEARS) --download
endif

export-csv: ## Exportar tickers de DB a tickers.csv
	$(PYTHON) scraper_cascade.py --export-csv $(CSV)

# ─── Info & Status ───────────────────────────────────────────────────────────

status: ## Mostrar estado de la base de datos
	@echo ""
	@echo "  \033[1mEstado de la base de datos\033[0m"
	@echo "  ────────────────────────"
	@$(PYTHON) -c "\
import sqlite3, os; \
conn = sqlite3.connect('$(DB)'); \
total = conn.execute('SELECT COUNT(*) FROM tickers').fetchone()[0]; \
enabled = conn.execute('SELECT COUNT(*) FROM tickers WHERE enabled = 1').fetchone()[0]; \
bars = conn.execute('SELECT COUNT(*) FROM ohlcv WHERE row_index = 0').fetchone()[0]; \
stocks = conn.execute(\"SELECT COUNT(*) FROM tickers WHERE trading_type = 'STOCK' AND enabled = 1\").fetchone()[0]; \
cedears = conn.execute(\"SELECT COUNT(*) FROM tickers WHERE trading_type = 'CEDEARS' AND enabled = 1\").fetchone()[0]; \
bonds = conn.execute(\"SELECT COUNT(*) FROM tickers WHERE trading_type = 'BOND' AND enabled = 1\").fetchone()[0]; \
byma = conn.execute(\"SELECT COUNT(DISTINCT ticker) FROM ohlcv WHERE data_source = 'byma'\").fetchone()[0]; \
iol = conn.execute(\"SELECT COUNT(DISTINCT ticker) FROM ohlcv WHERE data_source = 'iol'\").fetchone()[0]; \
yahoo = conn.execute(\"SELECT COUNT(DISTINCT ticker) FROM ohlcv WHERE data_source = 'yahoo'\").fetchone()[0]; \
at = conn.execute(\"SELECT COUNT(DISTINCT ticker) FROM ohlcv WHERE data_source = 'analisistecnico'\").fetchone()[0]; \
min_d = conn.execute('SELECT MIN(date) FROM ohlcv').fetchone()[0]; \
max_d = conn.execute('SELECT MAX(date) FROM ohlcv').fetchone()[0]; \
no_data = conn.execute('SELECT COUNT(*) FROM tickers WHERE enabled = 1 AND bars_count = 0').fetchone()[0]; \
csv_exists = os.path.exists('$(CSV)'); \
pub_exists = os.path.exists('public/index.html'); \
print(f'  Tickers:     {total} total, {enabled} habilitados ({stocks} STOCK, {cedears} CEDEARS, {bonds} BOND)'); \
print(f'  Barras:      {bars:,}'); \
print(f'  Rango:       {min_d} a {max_d}'); \
print(f'  Sin datos:   {no_data}'); \
print(f'  Fuentes:     ByMA={byma}, IOL={iol}, Yahoo={yahoo}, AT={at}'); \
print(f'  tickers.csv: {\"OK\" if csv_exists else \"NO EXISTE\"}'); \
print(f'  public/:     {\"OK\" if pub_exists else \"NO BUILD\"}'); \
conn.close()"
	@echo ""

# ─── Limpieza ────────────────────────────────────────────────────────────────

clean: ## Limpiar sitio estatico generado
	rm -rf public/
	@echo "  public/ eliminado"
