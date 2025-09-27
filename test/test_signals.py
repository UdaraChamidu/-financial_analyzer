import pandas as pd
from src.signals import detect_golden_crossover

def test_detect_golden_crossover():
    # Create synthetic SMA series where cross happens at index 3
    df = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-01","2020-01-02","2020-01-03","2020-01-04","2020-01-05"]),
        "sma_50": [5, 5, 5, 6, 7],
        "sma_200": [5, 5, 6, 6, 6]
    })
    res = detect_golden_crossover(df)
    assert res == ["2020-01-04"]  # where 50 crosses above 200
