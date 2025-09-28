"""Fetch price data and fundamentals from yfinance with validation."""

from typing import Dict, Any
import logging
import yfinance as yf
import pandas as pd
from pydantic import ValidationError

from .models import QuarterlyFundamentals, StockDataResponse

logger = logging.getLogger(__name__)


def _frame_to_pricepoints(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize yfinance history DataFrame: ensure columns and types."""
    df = df.reset_index().rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    # Keep only trading days and required cols
    df = df[["date", "open", "high", "low", "close", "volume"]]
    return df


def fetch_stock_data(ticker: str, period: str = "5y") -> Dict[str, Any]:
    """Fetch price history and fundamentals using yfinance.

    Returns dict:
      {
        "ticker": ticker,
        "prices": pd.DataFrame,
        "quarterly_fundamentals": pd.DataFrame or list,
        "annual_fundamentals": pd.DataFrame or list,
        "info": dict
      }
    """
    logger.info("Fetching data for %s (period=%s)", ticker, period)
    tk = yf.Ticker(ticker)

    # 1) Prices with timeout handling
    try:
        hist = tk.history(period=period, auto_adjust=False, actions=False)
        if hist.empty:
            logger.warning("No price history returned for %s", ticker)
            logger.info("Data quality issue: Empty price history for %s", ticker)
        prices = _frame_to_pricepoints(hist)

        # Validate price data quality
        if len(prices) < 50:  # Less than 50 days of data
            logger.warning(
                "Limited price history for %s: only %d days", ticker, len(prices)
            )

    except Exception as e:
        logger.exception("Failed to fetch price history for %s: %s", ticker, e)
        raise

    # 2) Fundamentals: prefer quarterly balance sheets, fallback to annual
    source_used = "quarterly_balance_sheet"
    try:
        qb = tk.quarterly_balance_sheet  # DataFrame
        if qb is None or qb.empty:
            logger.info(
                "quarterly_balance_sheet missing; falling back to annual balance_sheet"
            )
            logger.info(
                "Data quality issue: Missing quarterly data for %s, using annual",
                ticker,
            )
            ab = tk.balance_sheet
            qfund_df = ab.transpose() if ab is not None else pd.DataFrame()
            source_used = "annual_balance_sheet"
        else:
            qfund_df = qb.transpose()

        if qfund_df.empty:
            logger.warning("No fundamental data available for %s", ticker)
            logger.info("Data quality issue: No fundamental data for %s", ticker)

    except Exception as e:
        logger.exception("Error fetching balance sheet: %s", e)
        qfund_df = pd.DataFrame()
        source_used = "none_available"
        logger.info("Data quality issue: Failed to fetch fundamentals for %s", ticker)

    # 3) Info
    try:
        info = tk.info or {}
    except Exception:
        info = {}

    # Convert qfund_df to records
    qfund_df.index.name = "end_date"
    qfund_records = []
    for end_dt, row in qfund_df.iterrows():
        rec = dict(end_date=end_dt, **row.dropna().to_dict())
        qfund_records.append(rec)

    # Validate a sample with Pydantic where useful (lightweight check)
    validated_quarters = []
    for r in qfund_records:
        try:
            q = QuarterlyFundamentals(end_date=r.get("end_date"), raw=r)
            validated_quarters.append(q.dict())
        except ValidationError as exc:
            logger.warning("Quarterly fundamentals validation error: %s", exc)

    # Prepare response data
    response_data = {
        "ticker": ticker,
        "prices": prices.to_dict("records") if not prices.empty else [],
        "quarterly_fundamentals": qfund_records,
        "info": info,
        "source_used": source_used,
    }

    # Validate response with Pydantic model
    try:
        StockDataResponse(**response_data)
        logger.info("Successfully validated API response for %s", ticker)
        return response_data
    except ValidationError as e:
        logger.warning("Response validation failed for %s: %s", ticker, e)
        # Return unvalidated data but log the issue
        return response_data
