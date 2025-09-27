"""CLI entrypoint using Typer to orchestrate the pipeline."""
from typing import Optional
import typer
import logging
import json
from pathlib import Path

from .config import load_config
from .data_fetcher import fetch_stock_data
from .processor import process_data
from .signals import detect_golden_crossover, detect_death_cross
from .database import init_db, save_daily_metrics, save_signals

app = typer.Typer()
logger = logging.getLogger("financial_analyzer")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

@app.command()
def main(
    ticker: str = typer.Option(..., "--ticker", "-t", help="Ticker symbol (e.g. NVDA or RELIANCE.NS)"),
    output: Path = typer.Option(..., "--output", "-o", help="Output JSON path"),
    config_path: Path = typer.Option(Path("config.yaml"), "--config", "-c", help="Path to config YAML"),
    db_path: Path = typer.Option(Path("financial_data.db"), "--db", "-d", help="Path to SQLite DB"),
):
    """Orchestrate the full pipeline for a ticker and write JSON results to output."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    try:
        cfg = load_config(str(config_path))
    except Exception:
        logger.warning("Could not load config from %s — using defaults", config_path)
        cfg = {}

    SessionMaker = init_db(str(db_path))
    
    try:
        raw = fetch_stock_data(ticker)
        df = process_data(raw, cfg)

        # detect signals (expect df with date as column or index)
        df_for_signals = df.reset_index() if not "date" in df.columns else df
        gc_dates = detect_golden_crossover(df_for_signals)
        signals = [{"ticker": ticker, "date": d, "signal": "golden_cross"} for d in gc_dates]

        # persist
        save_daily_metrics(SessionMaker, ticker, df_for_signals)
        save_signals(SessionMaker, signals)

        # write output JSON (include provenance)
        out = {
            "ticker": ticker,
            "source_used": raw.get("source_used"),
            "metrics_count": len(df),
            "signals": signals,
            "config": cfg,
        }
        with open(output, "w", encoding="utf-8") as fh:
            json.dump(out, fh, default=str, indent=2)

        logger.info("Pipeline completed and output saved to %s", output)
    except Exception:
        logger.exception("Pipeline failed for %s", ticker)
        raise typer.Exit(code=1)
    
if __name__ == "__main__":
    app()