#!/usr/bin/env python3
"""
Script to update stock data and statistics to the latest data using the remote database.
"""
from src.data_processing import process_all_data
from src.db.database_remote import get_db
from datetime import datetime

def main():
    """
    Main function to update stock data and statistics.
    """
    print(f"Starting data update at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Get database connection
    db = get_db()
    
    # Process all data with today's date
    process_all_data(db)
    
    print(f"Data update completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
