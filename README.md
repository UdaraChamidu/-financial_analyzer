# Financial Analyzer - Production-Grade Financial Analysis Pipeline

A command-line financial analysis factory that ingests daily price data and fundamental metrics from yfinance API, processes and merges different data frequencies, calculates technical indicators and fundamental ratios, detects trading signals, and persists results to SQLite with proper schema design.

## Project Overview

This financial analysis pipeline demonstrates production-grade data engineering practices including:

- **Data Engineering**: ETL pipeline design, missing data strategies
- **Software Architecture**: Modular design, error handling
- **Database Design**: Schema design, idempotent operations
- **Production Practices**: Logging, testing, documentation

## Features

- ✅ Fetches 5 years of daily OHLCV data (or as available for recent IPOs)
- ✅ Validates raw data using Pydantic schemas
- ✅ Processes and merges different data frequencies (daily vs quarterly)
- ✅ Calculates technical indicators (50/200-day SMA, 52-week high, % from high)
- ✅ Calculates fundamental ratios (BVPS, P/B, Enterprise Value)
- ✅ Detects Golden Crossover trading signals
- ✅ Persists results to SQLite with proper schema design
- ✅ Delivers analysis via CLI with JSON export
- ✅ Handles multi-market stocks (US & Indian markets)
- ✅ Gracefully handles recent IPOs with limited data

## Setup Instructions

### Prerequisites

- Python 3.9+
- uv (recommended) or poetry

### Installation

```bash
# Using uv (recommended)
uv init financial_analyzer
cd financial_analyzer
uv add pandas yfinance pydantic "typer[all]" sqlalchemy pyyaml
uv add --dev ruff pytest

# Using poetry
poetry new financial_analyzer
cd financial_analyzer
poetry add pandas yfinance pydantic "typer[all]" sqlalchemy pyyaml
poetry add --group dev ruff pytest
```

### Quick Start

1. Clone the repository
2. Install dependencies: `uv sync`
3. Run the pipeline:

```bash
# US stock
uv run python -m src.main --ticker NVDA --output nvda_analysis.json

# Indian stock
uv run python -m src.main --ticker RELIANCE.NS --output reliance_analysis.json

# Recent IPO
uv run python -m src.main --ticker SWIGGY.NS --output swiggy_analysis.json
```

## Usage Examples

### Basic Usage

```bash
# US Stocks
uv run python -m src.main --ticker NVDA --output nvda_analysis.json
uv run python -m src.main --ticker AAPL --output aapl_analysis.json

# Indian Stocks
uv run python -m src.main --ticker RELIANCE.NS --output reliance_analysis.json
uv run python -m src.main --ticker TCS.NS --output tcs_analysis.json

# Recent IPOs/Listings (<10 months)
uv run python -m src.main --ticker SWIGGY.NS --output swiggy_analysis.json
uv run python -m src.main --ticker HYUNDAI.NS --output hyundai_analysis.json
```

### Advanced Usage

```bash
# With custom config
uv run python -m src.main --ticker NVDA --output nvda_analysis.json --config config.yaml

# With custom database
uv run python -m src.main --ticker NVDA --output nvda_analysis.json --db custom.db
```

## Project Structure

```
financial_analyzer/
├── src/
│   ├── __init__.py
│   ├── data_fetcher.py      # API calls & validation
│   ├── processor.py         # Data merging & metrics
│   ├── signals.py           # Signal detection
│   ├── database.py          # SQLite operations
│   ├── models.py            # Pydantic schemas
│   ├── main.py              # CLI entry point
│   └── config.py            # Configuration
├── tests/
│   ├── test_processor.py    # Test calculations
│   ├── test_signals.py      # Test signal detection
│   └── conftest.py
├── config.yaml.example
├── pyproject.toml
└── README.md
```

## Database Schema

The SQLite database uses SQLAlchemy ORM with proper schema design for financial data persistence.

### Tables

#### `tickers`
- `id`: Primary key (auto-increment)
- `ticker`: Unique ticker symbol (indexed, not null)
- `name`: Company name (optional)

#### `daily_metrics`
- `id`: Primary key (auto-increment)
- `ticker`: Ticker symbol (indexed, not null)
- `date`: Trading date (not null)
- `close`: Closing price (float)
- `sma_50`: 50-day Simple Moving Average (float)
- `sma_200`: 200-day Simple Moving Average (float)
- `pct_from_52wk_high`: Percentage from 52-week high (float)
- `bvps`: Book Value per Share (float, nullable)
- `pb_ratio`: Price-to-Book Ratio (float, nullable)
- `ev`: Enterprise Value (float, nullable)
- **Unique constraint**: `(ticker, date)` - Prevents duplicate daily records

#### `signal_events`
- `id`: Primary key (auto-increment)
- `ticker`: Ticker symbol (indexed, not null)
- `date`: Signal date (not null)
- `signal`: Signal type (not null, e.g., "golden_cross", "death_cross")
- `meta`: Additional metadata (text, nullable, for JSON/text data)
- **Unique constraint**: `(ticker, date, signal)` - Prevents duplicate signal events

### Database Design Features

- **Idempotent Operations**: Uses `session.merge()` for INSERT OR REPLACE behavior
- **UNIQUE Constraints**: Prevents duplicate data on re-runs
- **Proper Indexing**: Ticker columns are indexed for fast queries
- **Nullable Fields**: Fundamental metrics are nullable for missing data scenarios
- **Data Types**: Appropriate SQLite types (Float for prices, Date for dates, Text for strings)

## Design Decisions

### 1. Frequency Mismatch Problem

**Problem**: Stock prices update daily, but financial statements only update quarterly.

**Solution**: 
- Use `pd.merge_asof()` with backward direction to align quarterly fundamentals to daily prices
- Forward-fill fundamental data between quarterly reports
- This approach makes financial sense because fundamentals change infrequently and the most recent available data is most relevant for current analysis

**Trade-offs**:
- ✅ Provides reasonable fundamental data for daily analysis
- ✅ Handles missing fundamental data gracefully
- ⚠️ May not reflect sudden fundamental changes until next quarterly report

### 2. Missing Fundamental Data Strategy

**Problem**: yfinance fundamental data is often missing or incomplete.

**Solution**:
- Primary: Try `ticker.quarterly_balance_sheet`
- Fallback: Use `ticker.balance_sheet` (annual data)
- Fallback: Use `ticker.info` for basic metrics
- Final fallback: Continue processing with synthetic/NaN values

**Implementation**:
```python
source_used = "quarterly_balance_sheet"
try:
    qb = tk.quarterly_balance_sheet
    if qb is None or qb.empty:
        ab = tk.balance_sheet
        source_used = "annual_balance_sheet"
except Exception:
    source_used = "none_available"
```

### 3. Golden Crossover Detection

**Logic**: Detect when the 50-day SMA crosses above the 200-day SMA.

**Implementation**:
- Use vectorized pandas operations for performance
- Handle edge cases (insufficient data, NaN values)
- Return list of crossover dates as ISO strings

**Edge Cases Handled**:
- Insufficient data for SMA calculations
- NaN values in SMA columns
- Missing date columns

### 4. Multi-Market & Recent Stock Handling

**Challenge**: Handle all types of stocks across different markets and ages.

**Solutions**:
- **Ticker Format**: Automatically handle `.NS` suffix for Indian stocks
- **Data Availability**: Gracefully handle limited price history for recent IPOs
- **Market Differences**: Unified processing regardless of market origin
- **Recent IPOs**: Adaptive SMA calculations with minimum periods

**Testing Coverage**:
- ✅ Old/regular US stocks (NVDA, AAPL)
- ✅ Old/regular Indian stocks (RELIANCE.NS, TCS.NS)
- ✅ Recent IPOs (SWIGGY.NS)

### 5. Idempotent Database Operations

**Problem**: Prevent duplicate data on re-runs.

**Solution**:
- Use `session.merge()` instead of `session.add()`
- Implement UNIQUE constraints on `(ticker, date)` combinations
- Handle IntegrityError gracefully

### 6. Error Handling Strategy

**Approach**:
- Use logging instead of print statements
- Handle API failures gracefully
- Continue processing with partial data when possible
- Document data quality issues in output
- Ensure pipeline works even for short histories (recent IPOs)

## Data Quality Notes

### Known Limitations

1. **Fundamental Data**: 
   - May be incomplete for some tickers
   - Quarterly data forward-filled may not reflect recent changes
   - Some ratios may be NaN for companies with missing data

2. **Recent IPOs**:
   - Limited price history affects SMA calculations
   - May have fewer or no golden crossover signals
   - Fundamental data often completely missing

3. **Market Differences**:
   - Indian stocks use `.NS` suffix
   - Different trading calendars may affect calculations
   - Currency differences not handled (all prices in local currency)

### Data Validation

- All price data validated with Pydantic models
- Price relationships enforced (high >= low, etc.)
- Technical indicators calculated with appropriate minimum periods
- Fundamental ratios calculated only when sufficient data available

## Testing Instructions

### Running Tests

```bash
# Run all tests
uv run python -m pytest test/ -v

# Run specific test file
uv run python -m pytest test/test_signals.py -v
```

### Test Coverage

- ✅ Metric calculations (SMA math verification)
- ✅ Signal detection logic (crossover detection)
- ✅ Data validation (Pydantic model tests)

### Manual Testing

Test the pipeline on the following ticker types:

**US Stocks (Old/Regular)**:
```bash
uv run python -m src.main --ticker NVDA --output nvda_analysis.json
uv run python -m src.main --ticker AAPL --output aapl_analysis.json
```

**Indian Stocks (Old/Regular)**:
```bash
uv run python -m src.main --ticker RELIANCE.NS --output reliance_analysis.json
uv run python -m src.main --ticker TCS.NS --output tcs_analysis.json
```

**Recent IPOs (<10 months)**:
```bash
uv run python -m src.main --ticker SWIGGY.NS --output swiggy_analysis.json
uv run python -m src.main --ticker HYUNDAI.NS --output hyundai_analysis.json
```

## Configuration

The `config.yaml.example` file contains default settings:

```yaml
database:
  path: "financial_data.db"

logging:
  level: "INFO"

data_settings:
  historical_period: "5y"
  min_trading_days_for_sma: 200
  rolling_days_for_52week: 252  # trading days approx.
```

## Output Format

The pipeline generates JSON output with the following structure:

```json
{
  "ticker": "NVDA",
  "source_used": "quarterly_balance_sheet",
  "metrics_count": 1256,
  "signals": [
    {
      "ticker": "NVDA",
      "date": "2022-09-28",
      "signal": "golden_cross"
    }
  ],
  "config": {
    "data_settings": {
      "historical_period": "5y",
      "min_trading_days_for_sma": 200
    }
  }
}
```

## Development

### Code Quality

- Type hints required for all functions
- Google-style docstrings for all public functions
- Linting with ruff: `ruff check . --fix && ruff format .`

### Adding New Features

1. Add Pydantic models in `src/models.py`
2. Update database schema in `src/database.py`
3. Add processing logic in `src/processor.py`
4. Add tests in `tests/`
5. Update documentation

## License

This project is part of the Fund-Screener Intern Screening Project.

## Contributing

This is a screening project. For questions or issues, please refer to the project requirements document.
