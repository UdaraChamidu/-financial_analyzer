import pandas as pd
from src.signals import detect_golden_crossover, detect_death_cross


def test_detect_golden_crossover():
    # Create synthetic SMA series where cross happens at index 4 (Day 5)
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"]
            ),
            "sma_50": [5, 5, 5, 6, 7],
            "sma_200": [5, 5, 6, 6, 6],
        }
    )
    res = detect_golden_crossover(df)
    assert res == ["2020-01-05"]  # where 50 crosses above 200


def test_detect_death_cross():
    # Create synthetic SMA series where death cross happens
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(
                ["2020-01-01", "2020-01-02", "2020-01-03", "2020-01-04", "2020-01-05"]
            ),
            "sma_50": [7, 6, 6, 5, 5],
            "sma_200": [6, 6, 6, 6, 6],
        }
    )
    res = detect_death_cross(df)
    assert res == ["2020-01-04"]  # where 50 crosses below 200


def test_signal_detection_edge_cases():
    # Test with missing SMA columns
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-01", "2020-01-02"]),
            "close": [100, 101],
        }
    )
    res_gc = detect_golden_crossover(df)
    res_dc = detect_death_cross(df)
    assert res_gc == []
    assert res_dc == []

    # Test with NaN values
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-03"]),
            "sma_50": [5, pd.NA, 7],
            "sma_200": [6, 6, 6],
        }
    )
    res_gc = detect_golden_crossover(df)
    res_dc = detect_death_cross(df)
    assert res_gc == []
    assert res_dc == []
