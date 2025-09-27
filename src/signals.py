"""Detect golden crossover (50-day SMA crossing above 200-day SMA)."""
from typing import List
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def detect_golden_crossover(df: pd.DataFrame, sma_short: str = "sma_50", sma_long: str = "sma_200") -> List[str]:
    """Detect dates where short SMA crosses above long SMA.

    Args:
        df: DataFrame containing 'date', sma_short, sma_long columns (sma columns numeric).
        sma_short: column name for short SMA
        sma_long: column name for long SMA

    Returns:
        List of ISO date strings when a golden crossover occurred.
    """
    if sma_short not in df.columns or sma_long not in df.columns:
        logger.warning("SMA columns not present; returning empty list")
        return []

    s = df[sma_short]
    l = df[sma_long]

    # Ensure aligned and no NaNs for the comparison days
    mask = (s.notna()) & (l.notna())

    # previous-day relation
    prev_short = s.shift(1)
    prev_long = l.shift(1)

    cross_up = (s > l) & (prev_short <= prev_long) & mask
    dates = df.loc[cross_up, "date"].dt.strftime("%Y-%m-%d").tolist()
    return dates

def detect_death_cross(df: pd.DataFrame) -> List[str]:
    """Bonus: detect death cross (50-day SMA crossing below 200-day SMA)."""
    s = df["sma_50"]
    l = df["sma_200"]
    prev_s = s.shift(1)
    prev_l = l.shift(1)
    cross_down = (s < l) & (prev_s >= prev_l) & s.notna() & l.notna()
    return df.loc[cross_down, "date"].dt.strftime("%Y-%m-%d").tolist()
