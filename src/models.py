"""Pydantic models for validation of API responses and processed data."""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, validator, condecimal

Money = condecimal(max_digits=30, decimal_places=6)


class StockDataResponse(BaseModel):
    """Validate raw API responses from yfinance."""

    ticker: str
    prices: List[Dict[str, Any]]
    quarterly_fundamentals: List[Dict[str, Any]]
    info: Dict[str, Any]
    source_used: str


class PricePoint(BaseModel):
    date: datetime
    open: Money
    high: Money
    low: Money
    close: Money
    volume: int

    @validator("high")
    def high_ge_low(cls, v, values):
        if "low" in values and v < values["low"]:
            raise ValueError("high must be >= low")
        return v

    @validator("close")
    def close_ge_low(cls, v, values):
        if "low" in values and v < values["low"]:
            raise ValueError("close must be >= low")
        return v

    @validator("close")
    def close_le_high(cls, v, values):
        if "high" in values and v > values["high"]:
            raise ValueError("close must be <= high")
        return v

    @validator("open")
    def open_ge_low(cls, v, values):
        if "low" in values and v < values["low"]:
            raise ValueError("open must be >= low")
        return v

    @validator("open")
    def open_le_high(cls, v, values):
        if "high" in values and v > values["high"]:
            raise ValueError("open must be <= high")
        return v


class QuarterlyFundamentals(BaseModel):
    end_date: datetime
    total_assets: Optional[Money] = None
    total_liabilities: Optional[Money] = None
    total_stockholder_equity: Optional[Money] = None
    total_debt: Optional[Money] = None
    cash: Optional[Money] = None
    shares_outstanding: Optional[Decimal] = None
    # Raw dict to keep any extra keys
    raw: Dict[str, Any] = {}


class ProcessedDailyMetrics(BaseModel):
    date: datetime
    ticker: str
    close: Money
    sma_50: Optional[Money] = None
    sma_200: Optional[Money] = None
    high_52wk: Optional[Money] = None
    pct_from_52wk_high: Optional[Decimal] = None
    bvps: Optional[Decimal] = None
    price_to_book: Optional[Decimal] = None
    enterprise_value: Optional[Money] = None
    # allow extension
    extras: Dict[str, Any] = {}


class SignalEvent(BaseModel):
    ticker: str
    date: str  # ISO date string
    signal: str
    meta: Optional[Dict[str, Any]] = None


class AnalysisOutput(BaseModel):
    ticker: str
    source_used: Optional[str] = None
    metrics_count: int
    signals: List[SignalEvent]
    config: Dict[str, Any]
