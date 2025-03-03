"""
SQLAlchemy models for the stocks picking competition.
"""
from datetime import datetime
from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship

from src.db.database import Base

class Position(Base):
    """
    Model for storing the initial positions data.
    """
    __tablename__ = "positions"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    ticker = Column(String, index=True)
    exchange = Column(String)
    weight = Column(Float)
    formatted_ticker = Column(String, index=True)
    shares = Column(Float, nullable=True)
    price_at_start = Column(Float, nullable=True)
    allocation_usd = Column(Float, nullable=True)
    
    # Ensure each person-ticker combination is unique
    __table_args__ = (
        UniqueConstraint('name', 'ticker', name='uix_position_name_ticker'),
    )

class DailyPrice(Base):
    """
    Model for storing daily price data for each ticker.
    """
    __tablename__ = "daily_prices"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, index=True)
    ticker = Column(String, index=True)
    price = Column(Float)
    
    # Ensure each date-ticker combination is unique
    __table_args__ = (
        UniqueConstraint('date', 'ticker', name='uix_daily_price_date_ticker'),
    )

class ExchangeRate(Base):
    """
    Model for storing currency exchange rates.
    """
    __tablename__ = "exchange_rates"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, index=True)
    from_currency = Column(String, index=True)
    to_currency = Column(String, index=True)
    rate = Column(Float)
    
    # Ensure each date-currency pair combination is unique
    __table_args__ = (
        UniqueConstraint('date', 'from_currency', 'to_currency', name='uix_exchange_rate_date_currencies'),
    )

class PortfolioValue(Base):
    """
    Model for storing calculated portfolio values.
    """
    __tablename__ = "portfolio_values"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, index=True)
    name = Column(String, index=True)
    value = Column(Float)
    pct_change = Column(Float, nullable=True)
    
    # Ensure each date-name combination is unique
    __table_args__ = (
        UniqueConstraint('date', 'name', name='uix_portfolio_value_date_name'),
    )

class PerformanceMetric(Base):
    """
    Model for storing performance metrics.
    """
    __tablename__ = "performance_metrics"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, unique=True)
    initial_value = Column(Float)
    final_value = Column(Float)
    total_return_pct = Column(Float)
    annualized_return_pct = Column(Float)
    last_updated = Column(Date, default=datetime.now().date)
