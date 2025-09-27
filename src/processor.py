"""Process raw data -> daily metrics DataFrame."""

from typing import Dict, Any
import pandas as pd
import numpy as np
from decimal import Decimal, ROUND_HALF_UP
import logging

logger = logging.getLogger(__name__)

def safe_decimal(x) -> Decimal | None:
    try:
        return Decimal(str(x))
    except Exception:
        return None

def process_data(raw_data: Dict[str, Any], cfg: Dict[str, Any]) -> pd.DataFrame:
    """
    Merge daily prices with quarterly fundamentals, compute indicators and ratios.

    Args:
        raw_data: output of fetch_stock_data
        cfg: configuration dict

    Returns:
        pd.DataFrame indexed by date with calculated metrics
    """
    ticker = raw_data["ticker"]
    prices: pd.DataFrame = raw_data["prices"].copy()
    qfunds = raw_data.get("quarterly_fundamentals", [])
    info = raw_data.get("info", {})

    if prices.empty:
        raise ValueError("No price data to process")

    # Ensure date is datetime and sorted
    prices["date"] = pd.to_datetime(prices["date"])
    prices = prices.sort_values("date").set_index("date")

    # 1) Technical indicators
    min_sma_200 = cfg["data_settings"].get("min_trading_days_for_sma", 200)
    prices["sma_50"] = prices["close"].rolling(window=50, min_periods=1).mean()
    prices["sma_200"] = prices["close"].rolling(window=200, min_periods=1).mean()

    # 2) 52-week high (use trading days approx 252)
    rolling_days = cfg["data_settings"].get("rolling_days_for_52week", 252)
    prices["high_52wk"] = prices["high"].rolling(window=rolling_days, min_periods=1).max()
    prices["pct_from_52wk_high"] = (prices["close"] - prices["high_52wk"]) / prices["high_52wk"]

    # 3) Build a fundamentals DataFrame by mapping quarter end to data, then forward-fill daily
    if qfunds:
        qdf = pd.DataFrame(qfunds).set_index(pd.to_datetime([r["end_date"] for r in qfunds]))
        # Keep useful columns if present (raw keys)
        # For safety, pick known names if available
        # We expect keys like 'Total Stockholder Equity', etc; normalize to lowercase keys
        qdf.columns = [str(c) for c in qdf.columns]
        # Put index name and sort
        qdf = qdf.sort_index()
        # Reindex to daily index by forward filling
        qdf_daily = qdf.reindex(prices.index, method="ffill")
    else:
        logger.info("No quarterly fundamentals available for %s; will use 'info' fallbacks", ticker)
        qdf_daily = pd.DataFrame(index=prices.index)

    # 4) Extract BVPS & P/B using info if present (= fallback)
    bvps_series = pd.Series(index=prices.index, dtype="float64")
    pb_series = pd.Series(index=prices.index, dtype="float64")
    ev_series = pd.Series(index=prices.index, dtype="float64")

    # attempt to compute numbers from qdf_daily first
    # Try typical column names
    def get_column_any(df, candidates):
        for c in candidates:
            if c in df.columns:
                return df[c]
        return None

    tse_col = get_column_any(qdf_daily, ["TotalStockholdersEquity", "totalStockholderEquity", "Total Stockholder Equity", "Stockholders' Equity"])
    shares_col = get_column_any(qdf_daily, ["commonStockSharesOutstanding", "Shares Outstanding", "sharesOutstanding", "Common Stock"])
    total_debt_col = get_column_any(qdf_daily, ["Long Term Debt", "totalDebt", "Total Debt"])
    cash_col = get_column_any(qdf_daily, ["Cash", "cash", "totalCash"])

    if tse_col is not None and shares_col is not None:
        bvps_series = tse_col.astype("float") / shares_col.astype("float")
        pb_series = prices["close"] / bvps_series.replace({0: np.nan})
    else:
        # fallback to info
        market_cap = info.get("marketCap")
        shares_out = info.get("sharesOutstanding") or info.get("floatShares")
        book_value = info.get("bookValue")
        if book_value and shares_out:
            # approximate bvps from bookValue (per share)
            bvps_series.fillna(book_value)
            pb_series = prices["close"] / book_value if book_value else pd.Series(np.nan, index=prices.index)

    # EV simplified: marketCap + totalDebt - cash
    total_debt = info.get("totalDebt") or 0
    cash = info.get("totalCash") or 0
    market_cap = info.get("marketCap") or (prices["close"].iloc[-1] * shares_out if shares_out else None)

    try:
        ev_val = float(market_cap or 0) + float(total_debt or 0) - float(cash or 0)
    except Exception:
        ev_val = None
    ev_series[:] = ev_val

    # 5) Compose output DataFrame
    out = prices[["close", "sma_50", "sma_200", "high_52wk", "pct_from_52wk_high"]].copy()
    out["bvps"] = bvps_series
    out["price_to_book"] = pb_series
    out["enterprise_value"] = ev_series
    out["ticker"] = ticker

    # small cleanup - convert infinite -> NaN
    out.replace([np.inf, -np.inf], np.nan, inplace=True)

    return out.reset_index().rename(columns={"index": "date"})
