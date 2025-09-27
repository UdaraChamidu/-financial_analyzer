"""Pydantic models for validation of API responses and processed data."""
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, validator, condecimal

Money = condecimal(max_digits=30, decimal_places=6)

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
