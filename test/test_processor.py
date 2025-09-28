from src.processor import process_data
from src.config import load_config
import pandas as pd

def test_sma_calculation(tmp_path, sample_price_df):
    # build raw_data similar to fetch_stock_data
    raw = {
        "ticker": "TEST",
        "prices": sample_price_df,
        "quarterly_fundamentals": [],
        "info": {}
    }
    cfg = {
        "data_settings": {
            "historical_period": "5y",
            "min_trading_days_for_sma": 200,
            "rolling_days_for_52week": 252
        }
    }
    out = process_data(raw, cfg)
    # check sma_50 and sma_200 exist
    assert "sma_50" in out.columns
    assert "sma_200" in out.columns
    # numerical checks: last sma_50 close to average of last 50 closes
    last50 = out["close"].tail(50).mean()
    assert abs(out["sma_50"].iloc[-1] - last50) < 1e-6
