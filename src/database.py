"""SQLAlchemy ORM + helper functions for idempotent SQLite persistence."""

from typing import Optional, Iterable
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Date,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
from datetime import date


# logger = logging.getLogger(__name__)
Base = declarative_base()


class Ticker(Base):
    __tablename__ = "tickers"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=True)


class DailyMetric(Base):
    __tablename__ = "daily_metrics"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False)
    close = Column(Float)
    sma_50 = Column(Float)
    sma_200 = Column(Float)
    pct_from_52wk_high = Column(Float)
    bvps = Column(Float)
    pb_ratio = Column(Float)
    ev = Column(Float)
    __table_args__ = (UniqueConstraint("ticker", "date", name="uix_ticker_date"),)


class SignalEvent(Base):
    __tablename__ = "signal_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False)
    signal = Column(String, nullable=False)
    meta = Column(String, nullable=True)  # JSON/text metadata
    __table_args__ = (UniqueConstraint("ticker", "date", "signal", name="uix_signal"),)


def init_db(db_path: str = "financial_data.db") -> sessionmaker:
    """Initialize SQLite database and return session maker.

    Args:
        db_path: Path to SQLite database file.

    Returns:
        SQLAlchemy sessionmaker instance.
    """
    engine = create_engine(
        f"sqlite:///{db_path}", connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


def save_daily_metrics(
    session_maker: sessionmaker, ticker: str, df: pd.DataFrame
) -> None:
    """Save daily metrics to database with idempotent operations.

    Args:
        session_maker: SQLAlchemy sessionmaker instance.
        ticker: Stock ticker symbol.
        df: DataFrame containing daily metrics data.
    """
    session = session_maker()
    try:
        for _, row in df.iterrows():
            obj = DailyMetric(
                ticker=ticker,
                date=row["date"]
                if isinstance(row["date"], date)
                else row["date"].date(),
                close=float(row.get("close") or 0.0),
                sma_50=float(row.get("sma_50") or 0.0),
                sma_200=float(row.get("sma_200") or 0.0),
                pct_from_52wk_high=float(row.get("pct_from_52wk_high") or 0.0),
                bvps=float(row.get("bvps") or 0.0)
                if row.get("bvps") is not None
                else None,
                pb_ratio=float(row.get("pb_ratio") or 0.0)
                if row.get("pb_ratio") is not None
                else None,
                ev=float(row.get("ev") or 0.0) if row.get("ev") is not None else None,
            )
            session.merge(obj)
        session.commit()
    finally:
        session.close()


def save_signals(session_maker: sessionmaker, signals: Iterable[dict]) -> None:
    """Save trading signals to database with idempotent operations.

    Args:
        session_maker: SQLAlchemy sessionmaker instance.
        signals: Iterable of signal dictionaries containing ticker, date, signal, and meta.
    """
    session = session_maker()
    try:
        for s in signals:
            # Handle both string and datetime dates
            signal_date = s["date"]
            if isinstance(signal_date, str):
                signal_date = pd.to_datetime(signal_date).date()
            elif hasattr(signal_date, "date"):
                signal_date = signal_date.date()

            obj = SignalEvent(
                ticker=s["ticker"],
                date=signal_date,
                signal=s["signal"],
                meta=s.get("meta"),
            )
            session.merge(obj)
        session.commit()
    finally:
        session.close()


# def save_ticker_info(
#     session_maker: sessionmaker, ticker: str, name: Optional[str] = None
# ) -> None:
#     """Save or update ticker basic information."""
#     session = session_maker()
#     try:
#         ticker_obj = Ticker(ticker=ticker, name=name)
#         session.merge(ticker_obj)
#         session.commit()
#     finally:
#         session.close()

def save_ticker_info(SessionMaker, ticker: str, company_name: str):
    """Save ticker info idempotently (no duplicates)."""
    session = SessionMaker()
    try:
        # Check if ticker already exists
        obj = session.query(Ticker).filter_by(ticker=ticker).first()
        if obj:
            # Optionally update the name if it changed
            obj.name = company_name
        else:
            obj = Ticker(ticker=ticker, name=company_name)
            session.add(obj)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
