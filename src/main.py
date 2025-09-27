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
def run(ticker: str, output: Optional[Path] = None, config: Path = Path("config.yaml")):
    """Full pipeline for a single ticker."""
    cfg = load_config(config)
    try:
        raw = fetch_stock_data(ticker, period=cfg["data_settings"]["historical_period"])
        df = process_data(raw, cfg)
    except Exception as e:
        logger.exception("Pipeline failed for %s: %s", ticker, e)
        raise typer.Exit(code=1)

    # detect signals
    goldens = detect_golden_crossover(df)
    deaths = detect_death_cross(df)

    # persist
    engine = init_db(cfg["database"]["path"])
    save_daily_metrics(engine, df)
    save_signals(engine, ticker, goldens, signal_name="golden_cross")
    save_signals(engine, ticker, deaths, signal_name="death_cross")

    # Export JSON
    out = {
        "ticker": ticker,
        "metrics_count": len(df),
        "golden_crosses": goldens,
        "death_crosses": deaths,
        "latest": df.iloc[-1].to_dict() if len(df) else {}
    }
    if output:
        with open(output, "w", encoding="utf-8") as fh:
            json.dump(out, fh, default=str, indent=2)
        logger.info("Saved JSON output to %s", output)
    else:
        print(json.dumps(out, default=str, indent=2))

if __name__ == "__main__":
    app()
