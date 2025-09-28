"""Test Pydantic model validation."""
import pytest
from datetime import datetime
from decimal import Decimal
from pydantic import ValidationError

from src.models import (
    PricePoint,
    QuarterlyFundamentals,
    ProcessedDailyMetrics,
    SignalEvent,
    AnalysisOutput,
    StockDataResponse,
)


def test_price_point_validation():
    """Test PricePoint model validation."""
    # Valid price point
    valid_price = PricePoint(
        date=datetime(2020, 1, 1),
        open=Decimal("100.50"),
        high=Decimal("105.00"),
        low=Decimal("99.50"),
        close=Decimal("102.00"),
        volume=1000000,
    )
    assert valid_price.close == Decimal("102.00")

    # Test price relationship validation
    with pytest.raises(ValidationError):
        PricePoint(
            date=datetime(2020, 1, 1),
            open=Decimal("100.00"),
            high=Decimal("95.00"),  # high < low should fail
            low=Decimal("99.00"),
            close=Decimal("101.00"),
            volume=1000000,
        )

    with pytest.raises(ValidationError):
        PricePoint(
            date=datetime(2020, 1, 1),
            open=Decimal("100.00"),
            high=Decimal("105.00"),
            low=Decimal("99.00"),
            close=Decimal("110.00"),  # close > high should fail
            volume=1000000,
        )


def test_quarterly_fundamentals_validation():
    """Test QuarterlyFundamentals model validation."""
    valid_fundamentals = QuarterlyFundamentals(
        end_date=datetime(2020, 12, 31),
        total_assets=Decimal("1000000000.00"),
        total_liabilities=Decimal("500000000.00"),
        raw={"extra_field": "value"},
    )
    assert valid_fundamentals.total_assets == Decimal("1000000000.00")
    assert valid_fundamentals.raw == {"extra_field": "value"}

    # Test with minimal data
    minimal_fundamentals = QuarterlyFundamentals(end_date=datetime(2020, 12, 31))
    assert minimal_fundamentals.end_date == datetime(2020, 12, 31)


def test_signal_event_validation():
    """Test SignalEvent model validation."""
    valid_signal = SignalEvent(
        ticker="AAPL",
        date="2020-01-15",
        signal="golden_cross",
        meta={"strength": "strong"},
    )
    assert valid_signal.ticker == "AAPL"
    assert valid_signal.signal == "golden_cross"


def test_analysis_output_validation():
    """Test AnalysisOutput model validation."""
    signals = [
        SignalEvent(ticker="AAPL", date="2020-01-15", signal="golden_cross")
    ]
    
    valid_output = AnalysisOutput(
        ticker="AAPL",
        source_used="quarterly_balance_sheet",
        metrics_count=252,
        signals=signals,
        config={"sma_window": 50},
    )
    assert valid_output.ticker == "AAPL"
    assert valid_output.metrics_count == 252


def test_stock_data_response_validation():
    """Test StockDataResponse model validation."""
    valid_response = StockDataResponse(
        ticker="AAPL",
        prices=[{"date": "2020-01-01", "open": 100.0, "high": 105.0, "low": 99.0, "close": 102.0, "volume": 1000000}],
        quarterly_fundamentals=[{"end_date": "2020-12-31", "total_assets": 1000000000}],
        info={"longName": "Apple Inc."},
        source_used="quarterly_balance_sheet",
    )
    assert valid_response.ticker == "AAPL"
    assert valid_response.source_used == "quarterly_balance_sheet"
