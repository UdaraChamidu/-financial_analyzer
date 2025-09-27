import pytest
import pandas as pd
from datetime import datetime, timedelta

@pytest.fixture
def sample_price_df():
    base = datetime(2020,1,1)
    dates = [base + timedelta(days=i) for i in range(400)]
    df = pd.DataFrame({
        "date": dates,
        "open": 100 + (pd.Series(range(400))*0.1),
        "high": 101 + (pd.Series(range(400))*0.1),
        "low": 99 + (pd.Series(range(400))*0.1),
        "close": 100 + (pd.Series(range(400))*0.1),
        "volume": 1000
    })
    return df
