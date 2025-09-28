# ...existing code...
from __future__ import annotations

import logging
from typing import Dict, Any, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def _find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Return first matching column name in df from candidates, else None."""
    for c in candidates:
        if c in df.columns:
            return c
        # check case-insensitive
        for col in df.columns:
            if col.lower() == c.lower():
                return col
    return None


def process_data(
    raw_data: Dict[str, Any], cfg: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """Process raw stock and fundamental data into a metrics DataFrame.

    The function:
    - normalizes price input (list[dict] or DataFrame),
    - aligns quarterly fundamentals to daily prices using merge_asof (backward),
    - forward-fills fundamentals,
    - computes technical indicators (50/200 SMA, 52-week high, % from high),
    - computes fundamental ratios (BVPS, P/B) and simplified EV,
    - is tolerant to missing config and short histories (recent IPOs).

    Args:
        raw_data: dictionary returned by fetcher with keys like "prices",
            "quarterly_fundamentals", and "info".
        cfg: optional config dict (see config.yaml); sensible defaults used.

    Returns:
        pd.DataFrame with one row per trading date and computed metrics.
    """
    cfg = cfg or {}
    data_settings = cfg.get("data_settings", {})
    window_short: int = data_settings.get("sma_short_window", 50)
    window_long: int = data_settings.get("min_trading_days_for_sma", 200)
    window_52wk: int = data_settings.get("rolling_days_for_52week", 252)

    # Prepare prices DataFrame
    prices_raw = raw_data.get("prices", [])
    if isinstance(prices_raw, pd.DataFrame):
        prices = prices_raw.copy()
    else:
        prices = pd.DataFrame(prices_raw)

    if prices.empty:
        logger.warning("No price data available in raw_data; returning empty DataFrame")
        return pd.DataFrame()

    # Normalize column names
    cols_map = {}
    for c in prices.columns:
        lc = c.lower()
        if lc in {"date", "datetime"}:
            cols_map[c] = "date"
        elif lc in {"open"}:
            cols_map[c] = "open"
        elif lc in {"high"}:
            cols_map[c] = "high"
        elif lc in {"low"}:
            cols_map[c] = "low"
        elif lc in {"close", "adjclose", "adj_close", "close_adj"}:
            cols_map[c] = "close"
        elif lc in {"volume"}:
            cols_map[c] = "volume"
    prices = prices.rename(columns=cols_map)

    # Ensure date column and proper dtypes
    if "date" not in prices.columns:
        logger.error("Price records missing 'date' column")
        return pd.DataFrame()
    prices["date"] = pd.to_datetime(prices["date"]).dt.tz_localize(
        None
    )  # Remove timezone info
    prices = prices.sort_values("date").reset_index(drop=True)

    # Load quarterly fundamentals if present and normalize
    qfunds_raw = raw_data.get("quarterly_fundamentals") or []
    qdf = pd.DataFrame(qfunds_raw)
    if not qdf.empty:
        # Accept either 'as_of' or 'date' as fundamentals date column
        if "as_of" in qdf.columns:
            qdf = qdf.rename(columns={"as_of": "date"})
        elif "date" not in qdf.columns:
            # try to infer column that looks like a date
            for col in qdf.columns:
                if "date" in col.lower() or "period" in col.lower():
                    qdf = qdf.rename(columns={col: "date"})
                    break
        # convert and sort
        if "date" in qdf.columns:
            qdf["date"] = pd.to_datetime(qdf["date"])
            qdf = qdf.sort_values("date").reset_index(drop=True)
            # align fundamentals to prices using merge_asof (use most recent fundamental on or before price date)
            prices = pd.merge_asof(
                prices,
                qdf,
                on="date",
                direction="backward",
                suffixes=("", "_fund"),
            )
        else:
            logger.warning(
                "Quarterly fundamentals present but no recognizable date column; ignoring alignment"
            )
    else:
        # no quarterly df; nothing to merge
        pass

    # Forward-fill fundamentals (reasonable because fundamentals change infrequently)
    # This approach is financially sound because:
    # 1. Financial statements are published quarterly and remain valid until next report
    # 2. Most fundamental metrics don't change significantly between quarters
    # 3. It provides the most recent available data for daily analysis
    # 4. Alternative approaches (interpolation, synthetic values) would be less accurate
    prices = prices.sort_values("date").ffill().reset_index(drop=True)

    # Technical indicators: SMA with min_periods adaptive to available history
    available = len(prices)
    if available <= 0:
        prices["sma_50"] = pd.NA
        prices["sma_200"] = pd.NA
    else:
        minp_short = min(window_short, max(1, available))
        minp_long = min(window_long, max(1, available))
        prices["sma_50"] = (
            prices["close"].rolling(window=window_short, min_periods=minp_short).mean()
        )
        prices["sma_200"] = (
            prices["close"].rolling(window=window_long, min_periods=minp_long).mean()
        )

    # 52-week high and percent from high
    prices["52wk_high"] = (
        prices["close"].rolling(window=window_52wk, min_periods=1).max()
    )
    # avoid divide-by-zero
    prices["pct_from_52wk_high"] = np.where(
        prices["52wk_high"] > 0,
        (prices["close"] - prices["52wk_high"]) / prices["52wk_high"],
        np.nan,
    )

    # Fundamental ratios: BVPS and P/B
    # Accept multiple possible column names for equity and shares
    eq_col = _find_col(
        prices,
        [
            "total_equity",
            "totalShareholdersEquity",
            "totalStockholdersEquity",
            "total_equity_fund",
        ],
    )
    shares_col = _find_col(
        prices, ["shares_outstanding", "shares", "common_shares_outstanding"]
    )
    if eq_col and shares_col:
        # protect against zero division / missing
        prices["bvps"] = np.where(
            (prices[shares_col] > 0) & (~prices[eq_col].isna()),
            prices[eq_col] / prices[shares_col],
            np.nan,
        )
        prices["pb_ratio"] = np.where(
            prices["bvps"].replace({0: np.nan}).notna(),
            prices["close"] / prices["bvps"],
            np.nan,
        )
    else:
        prices["bvps"] = np.nan
        prices["pb_ratio"] = np.nan

    # Simplified Enterprise Value (EV) estimate:
    # EV ≈ marketCap + total_liabilities - cash_and_equivalents
    info = raw_data.get("info") or {}
    market_cap = info.get("marketCap") or info.get("market_cap")
    # find liability and cash columns in merged fundamentals or info
    liab_col = _find_col(
        prices, ["total_liab", "total_liabilities", "totalLiab", "totalLiabilities"]
    )
    cash_col_fund = _find_col(
        prices, ["cash_and_equivalents", "cash", "cash_equivalents"]
    )
    cash_info = (
        info.get("totalCash") or info.get("cash") or info.get("cashAndCashEquivalents")
    )

    # compute EV per-row where possible
    def _compute_ev(row):
        mc = market_cap or np.nan
        tl = row[liab_col] if liab_col and pd.notna(row.get(liab_col)) else np.nan
        cash = (
            row[cash_col_fund]
            if cash_col_fund and pd.notna(row.get(cash_col_fund))
            else (cash_info or np.nan)
        )
        # prefer numeric coercion
        try:
            parts = [float(x) for x in (mc, tl, cash)]
        except Exception:
            return np.nan
        mc_v, tl_v, cash_v = parts
        # if market cap missing, cannot compute reliably
        if np.isnan(mc_v):
            return np.nan
        ev_v = (
            mc_v
            + (tl_v if not np.isnan(tl_v) else 0.0)
            - (cash_v if not np.isnan(cash_v) else 0.0)
        )
        return ev_v

    prices["ev"] = prices.apply(_compute_ev, axis=1)

    # Ensure `date` column is present for downstream use
    if "date" not in prices.columns:
        prices = prices.reset_index().rename(columns={"index": "date"})

    return prices


# ...existing code...
