"""
Database connection and operations for the stocks picking competition.
"""
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session

# Create the data directory if it doesn't exist
os.makedirs('data', exist_ok=True)

# Database URL
DATABASE_URL = "sqlite:///data/stocks.db"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL, echo=False)

# Create a session factory
session_factory = sessionmaker(bind=engine)
SessionLocal = scoped_session(session_factory)

# Create a base class for declarative models
Base = declarative_base()

def get_db():
    """
    Get a database session.
    
    Returns:
        SQLAlchemy session
    """
    db = SessionLocal()
    try:
        return db
    finally:
        db.close()

def init_db():
    """
    Initialize the database by creating all tables.
    """
    Base.metadata.create_all(bind=engine)
