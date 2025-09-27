"""SQLAlchemy ORM + helper functions for idempotent SQLite persistence."""
from typing import Optional
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError
import pandas as pd
import logging

logger = logging.getLogger(__name__)
Base = declarative_base()

class Ticker(Base):
    __tablename__ = "tickers"
    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, nullable=False)
    name = Column(String)

class DailyMetric(Base):
    __tablename__ = "daily_metrics"
    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    close = Column(Float)
    sma_50 = Column(Float)
    sma_200 = Column(Float)
    high_52wk = Column(Float)
    pct_from_52wk_high = Column(Float)
    bvps = Column(Float)
    price_to_book = Column(Float)
    enterprise_value = Column(Float)
    __table_args__ = (UniqueConstraint("ticker", "date", name="uix_ticker_date"),)

class SignalEvent(Base):
    __tablename__ = "signal_events"
    id = Column(Integer, primary_key=True)
    ticker = Column(String, nullable=False)
    date = Column(Date, nullable=False)
    signal = Column(String, nullable=False)
    metadata = Column(String)  # JSON string
    __table_args__ = (UniqueConstraint("ticker", "date", "signal", name="uix_signal"),)

def init_db(path: str):
    engine = create_engine(f"sqlite:///{path}", future=True)
    Base.metadata.create_all(engine)
    return engine

def save_daily_metrics(engine, df: pd.DataFrame):
    """Idempotent save: use CREATE+INSERT OR REPLACE via pandas to_sql fallback.

    Simpler approach: write to a temp table and upsert rows.
    """
    conn = engine.connect()
    df_to_save = df.copy()
    df_to_save["date"] = pd.to_datetime(df_to_save["date"]).dt.date
    # Use pandas to_sql to append, then de-duplicate by unique constraint by executing SQL
    df_to_save.to_sql("daily_metrics", conn, if_exists="append", index=False)
    # To ensure idempotency (no duplicates), run SQL to keep newest rows; simpler approach:
    # Because SQLite lacks easy MERGE, for this small project it's acceptable to deduplicate:
    dedupe_sql = """
    DELETE FROM daily_metrics
    WHERE rowid NOT IN (
      SELECT MIN(rowid) FROM daily_metrics GROUP BY ticker, date
    );
    """
    conn.execute(dedupe_sql)
    conn.commit()
    conn.close()
    logger.info("Saved %d daily metric rows", len(df_to_save))

def save_signals(engine, ticker: str, dates: list[str], signal_name: str = "golden_cross"):
    conn = engine.connect()
    for d in dates:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO signal_events (ticker, date, signal, metadata) VALUES (?, ?, ?, ?)",
                (ticker, d, signal_name, "{}")
            )
        except Exception as e:
            logger.exception("Failed to insert signal row: %s", e)
    conn.commit()
    conn.close()
