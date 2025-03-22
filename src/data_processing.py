"""
Data processing functions for the stocks picking competition.
Uses SQLite Cloud for remote database storage.
"""
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from tqdm import tqdm

# Import remote database functions
from src.db.database_remote import get_db, execute_query, write_data

# Constants
INITIAL_VALUE_USD = 1e5
START_DATE = '2000-01-01'
FIXED_DATE = '2025-03-04'

def get_formatted_ticker(ticker, exchange):
    """
    Format ticker based on exchange.
    
    Args:
        ticker (str): The ticker symbol
        exchange (str): The exchange
        
    Returns:
        str: The formatted ticker
    """
    if exchange == 'NYSE' or exchange == 'NASDAQ':
        return ticker
    elif exchange == 'HKG':
        return f"{ticker}.HK"  # Hong Kong stocks use .HK suffix
    elif exchange == 'EPA':
        return f"{ticker}.PA"  # Paris stocks use .PA suffix
    elif exchange == 'LON':
        return f"{ticker}.L"  # London stocks use .L suffix
    elif exchange == 'TSE':
        return f"{ticker}-A.TO"  # Toronto stocks use .TO suffix (CTC-A is a special case)
    elif exchange == 'CVE':
        return f"{ticker}.V"  # .V according to the Yahoo Finance
    elif exchange == 'XETRA':
        return f"{ticker}.DE"  # .DE according to the Yahoo Finance (ETR/XETRA stock exchange)
    else:
        return ticker

def load_positions():
    """
    Load the positions data.
    
    Returns:
        list: List of position dictionaries
    """
    return [
        {'name': 'Apoorva', 'ticker': 'TSM', 'exchange':'NYSE', 'weight': 0.4},
        {'name': 'Apoorva', 'ticker': 'GOOGL', 'exchange':'NASDAQ', 'weight': 0.1},
        {'name': 'Apoorva', 'ticker': 'TAK', 'exchange':'NYSE', 'weight': 0.25},
        {'name': 'Apoorva', 'ticker': '0700', 'exchange':'HKG', 'weight': 0.25},

        {'name': 'Alessandro', 'ticker': 'DUOL', 'exchange':'NASDAQ', 'weight': 0.4},
        {'name': 'Alessandro', 'ticker': 'GOOG', 'exchange':'NASDAQ', 'weight': 0.25},
        {'name': 'Alessandro', 'ticker': 'ZG', 'exchange':'NASDAQ', 'weight': 0.35},

        {'name': 'Ivan', 'ticker': 'VST', 'exchange':'NYSE', 'weight': 0.4},
        {'name': 'Ivan', 'ticker': 'EXPE', 'exchange':'NASDAQ', 'weight': 0.3},
        {'name': 'Ivan', 'ticker': 'DGX', 'exchange':'NYSE', 'weight': 0.3},

        {'name': 'Abhi', 'ticker': 'ASR', 'exchange':'NYSE', 'weight': 0.25},
        {'name': 'Abhi', 'ticker': '0700', 'exchange':'HKG', 'weight': 0.25},
        {'name': 'Abhi', 'ticker': 'SMCI', 'exchange':'NASDAQ', 'weight': 0.25},
        {'name': 'Abhi', 'ticker': 'BRO', 'exchange':'NASDAQ', 'weight': 0.25},
        
        {'name': 'Conor', 'ticker': 'PFE', 'exchange':'NYSE', 'weight': 0.2},
        {'name': 'Conor', 'ticker': 'RR', 'exchange':'LON', 'weight': 0.2},
        {'name': 'Conor', 'ticker': 'MCD', 'exchange':'NYSE', 'weight': 0.2},
        {'name': 'Conor', 'ticker': 'SHEL', 'exchange':'LON', 'weight': 0.2},
        {'name': 'Conor', 'ticker': 'ALV', 'exchange':'XETRA', 'weight': 0.2},
        
        {'name': 'Diarbhail', 'ticker': 'FLUT', 'exchange':'NYSE', 'weight': 0.34},
        {'name': 'Diarbhail', 'ticker': 'NVDA', 'exchange':'NASDAQ', 'weight': 0.33},
        {'name': 'Diarbhail', 'ticker': 'SCAN', 'exchange':'CVE', 'weight': 0.33},

        {'name': 'Radu', 'ticker': 'MP', 'exchange':'NYSE', 'weight': 0.1},
        {'name': 'Radu', 'ticker': 'DBK', 'exchange':'XETRA', 'weight': 0.5},
        {'name': 'Radu', 'ticker': 'RTX', 'exchange':'NYSE', 'weight': 0.2},
        {'name': 'Radu', 'ticker': 'RHM', 'exchange':'XETRA', 'weight': 0.2},

        {'name': 'Silvia', 'ticker': 'NVO', 'exchange':'NYSE', 'weight': 0.33},
        {'name': 'Silvia', 'ticker': 'NVDA', 'exchange':'NASDAQ', 'weight': 0.33},
        {'name': 'Silvia', 'ticker': 'OPRA', 'exchange':'NASDAQ', 'weight': 0.34},
    ]

def save_positions_to_db(db):
    """
    Save positions to the database.
    
    Args:
        db: Database connection
    """
    positions = load_positions()
    
    # Calculate person weights
    person_weights = {}
    for position in positions:
        name = position['name']
        weight = position['weight']
        
        if name not in person_weights:
            person_weights[name] = 0
        
        person_weights[name] += weight
    
    # Prepare data for bulk insert
    positions_data = []
    
    for position in positions:
        name = position['name']
        ticker = position['ticker']
        exchange = position['exchange']
        weight = position['weight']
        formatted_ticker = get_formatted_ticker(ticker, exchange)
        
        positions_data.append({
            'name': name,
            'ticker': ticker,
            'exchange': exchange,
            'weight': weight,
            'formatted_ticker': formatted_ticker
        })
    
    # Convert to DataFrame for bulk operations
    if positions_data:
        positions_df = pd.DataFrame(positions_data)
        
        # Use bulk write operation with smaller batch size to avoid timeouts
        print(f"Saving {len(positions_df)} positions to database...")
        write_data(positions_df, 'positions', if_exists='replace', batch_size=1000)
    
    return positions

def download_and_save_stock_data(db, end_date=None):
    """
    Download stock data from Yahoo Finance and save to database.
    
    Args:
        db: Database connection
        end_date (str, optional): End date for data download. Defaults to today.
    
    Returns:
        pandas.DataFrame: DataFrame with price data
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    # Get positions
    positions_query = "SELECT * FROM positions"
    positions_df = execute_query(positions_query)
    
    if positions_df is None or positions_df.empty:
        print("No positions found in the database")
        return pd.DataFrame()
    
    # Get unique tickers and track which ones are from different exchanges
    unique_tickers = []
    hk_tickers = []  # Hong Kong stocks (HKD)
    eur_tickers = []  # European stocks (EUR)
    gbp_tickers = []  # UK stocks (GBP)
    cad_tickers = []  # Canadian stocks (CAD)
    
    for _, position in positions_df.iterrows():
        formatted_ticker = position['formatted_ticker']
        
        if formatted_ticker.endswith('.HK'):
            hk_tickers.append(formatted_ticker)
        elif formatted_ticker.endswith('.PA'):
            eur_tickers.append(formatted_ticker)
        elif formatted_ticker.endswith('.L'):
            gbp_tickers.append(formatted_ticker)
        elif formatted_ticker.endswith('.TO') or formatted_ticker.endswith('.V'):
            cad_tickers.append(formatted_ticker)
        elif formatted_ticker.endswith('.DE'):
            eur_tickers.append(formatted_ticker)
        
        unique_tickers.append(formatted_ticker)
    
    print(f"Downloading data for {len(unique_tickers)} unique tickers from {START_DATE} to {end_date}")
    
    # Download historical data for all tickers
    stock_data = yf.download(unique_tickers, start=START_DATE, end=end_date)
    
    # We'll use the 'Close' prices for our calculations
    prices_df = stock_data['Close'].copy()
    
    # Download and process exchange rates
    if hk_tickers:
        print("\nDownloading HKD/USD exchange rate data for currency conversion...")
        exchange_rate_data = yf.download('HKDUSD=X', start=START_DATE, end=end_date)
        exchange_rate = exchange_rate_data['Close']
        
        # Save exchange rates to database
        print(f"Saving HKD/USD exchange rates to database...")
        
        # Prepare data for bulk insert
        hkd_rates_data = []
        for date_idx, rate_value in zip(exchange_rate.index, exchange_rate.values):
            if pd.notna(rate_value):
                date_str = date_idx.date().isoformat()
                hkd_rates_data.append({
                    'date': date_str,
                    'from_currency': 'HKD',
                    'to_currency': 'USD',
                    'rate': float(rate_value)
                })
        
        # Convert to DataFrame for bulk operations
        if hkd_rates_data:
            hkd_rates_df = pd.DataFrame(hkd_rates_data)
            
            # Check for existing exchange rate data to implement incremental writes
            print("Checking existing HKD/USD exchange rate data for incremental update...")
            latest_date_query = "SELECT MAX(date) as max_date FROM exchange_rates WHERE from_currency = 'HKD' AND to_currency = 'USD'"
            latest_date_result = execute_query(latest_date_query)
            
            if latest_date_result is not None and not latest_date_result.empty and latest_date_result['max_date'].iloc[0] is not None:
                latest_date = latest_date_result['max_date'].iloc[0]
                print(f"Latest HKD/USD exchange rate date in database: {latest_date}")
                
                # Filter to only include new data
                new_rates_df = hkd_rates_df[hkd_rates_df['date'] > latest_date]
                
                if not new_rates_df.empty:
                    print(f"Saving {len(new_rates_df)} new HKD/USD exchange rates to database...")
                    write_data(new_rates_df, 'exchange_rates', if_exists='append', batch_size=1000)
                else:
                    print("No new HKD/USD exchange rate data to save.")
            else:
                # If no data exists yet, save all data
                print(f"No existing HKD/USD exchange rate data found. Saving {len(hkd_rates_df)} rates to database...")
                write_data(hkd_rates_df, 'exchange_rates', if_exists='replace', batch_size=1000)
        
        # Convert Hong Kong stock prices from HKD to USD
        for ticker in hk_tickers:
            if ticker in prices_df.columns:
                hkd_prices = prices_df[ticker]
                
                # Create a temporary DataFrame with both series properly indexed
                temp_df = pd.DataFrame({'HKD_Price': hkd_prices})
                temp_df['USD_Rate'] = exchange_rate  # This will align indices automatically
                
                # Calculate USD prices only where both values exist
                temp_df['USD_Price'] = temp_df['HKD_Price'] * temp_df['USD_Rate']
                
                # Add the converted prices to prices_df
                prices_df.loc[:, ticker] = temp_df['USD_Price']
    
    # Download and process EUR to USD exchange rates
    if eur_tickers:
        print("\nDownloading EUR/USD exchange rate data for currency conversion...")
        eur_usd_data = yf.download('EURUSD=X', start=START_DATE, end=end_date)
        eur_usd_rate = eur_usd_data['Close']
        
        # Save exchange rates to database
        print(f"Saving EUR/USD exchange rates to database...")
        
        # Prepare data for bulk insert
        eur_rates_data = []
        for date_idx, rate_value in zip(eur_usd_rate.index, eur_usd_rate.values):
            if pd.notna(rate_value):
                date_str = date_idx.date().isoformat()
                eur_rates_data.append({
                    'date': date_str,
                    'from_currency': 'EUR',
                    'to_currency': 'USD',
                    'rate': float(rate_value)
                })
        
        # Convert to DataFrame for bulk operations
        if eur_rates_data:
            eur_rates_df = pd.DataFrame(eur_rates_data)
            
            # Check for existing exchange rate data to implement incremental writes
            print("Checking existing EUR/USD exchange rate data for incremental update...")
            latest_date_query = "SELECT MAX(date) as max_date FROM exchange_rates WHERE from_currency = 'EUR' AND to_currency = 'USD'"
            latest_date_result = execute_query(latest_date_query)
            
            if latest_date_result is not None and not latest_date_result.empty and latest_date_result['max_date'].iloc[0] is not None:
                latest_date = latest_date_result['max_date'].iloc[0]
                print(f"Latest EUR/USD exchange rate date in database: {latest_date}")
                
                # Filter to only include new data
                new_rates_df = eur_rates_df[eur_rates_df['date'] > latest_date]
                
                if not new_rates_df.empty:
                    print(f"Saving {len(new_rates_df)} new EUR/USD exchange rates to database...")
                    write_data(new_rates_df, 'exchange_rates', if_exists='append', batch_size=1000)
                else:
                    print("No new EUR/USD exchange rate data to save.")
            else:
                # If no data exists yet, save all data
                print(f"No existing EUR/USD exchange rate data found. Saving {len(eur_rates_df)} rates to database...")
                write_data(eur_rates_df, 'exchange_rates', if_exists='append', batch_size=1000)
        
        # Convert EUR stocks to USD
        for ticker in eur_tickers:
            if ticker in prices_df.columns:
                eur_prices = prices_df[ticker]
                
                # Create a temporary DataFrame with both series properly indexed
                temp_df = pd.DataFrame({'EUR_Price': eur_prices})
                temp_df['USD_Rate'] = eur_usd_rate  # This will align indices automatically
                
                # Calculate USD prices only where both values exist
                temp_df['USD_Price'] = temp_df['EUR_Price'] * temp_df['USD_Rate']
                
                # Add the converted prices to prices_df
                prices_df.loc[:, ticker] = temp_df['USD_Price']
    
    # Download and process GBP to USD exchange rates
    if gbp_tickers:
        print("\nDownloading GBP/USD exchange rate data for currency conversion...")
        gbp_usd_data = yf.download('GBPUSD=X', start=START_DATE, end=end_date)
        gbp_usd_rate = gbp_usd_data['Close']
        
        # Save exchange rates to database
        print(f"Saving GBP/USD exchange rates to database...")
        
        # Prepare data for bulk insert
        gbp_rates_data = []
        for date_idx, rate_value in zip(gbp_usd_rate.index, gbp_usd_rate.values):
            if pd.notna(rate_value):
                date_str = date_idx.date().isoformat()
                gbp_rates_data.append({
                    'date': date_str,
                    'from_currency': 'GBP',
                    'to_currency': 'USD',
                    'rate': float(rate_value)
                })
        
        # Convert to DataFrame for bulk operations
        if gbp_rates_data:
            gbp_rates_df = pd.DataFrame(gbp_rates_data)
            
            # Check for existing exchange rate data to implement incremental writes
            print("Checking existing GBP/USD exchange rate data for incremental update...")
            latest_date_query = "SELECT MAX(date) as max_date FROM exchange_rates WHERE from_currency = 'GBP' AND to_currency = 'USD'"
            latest_date_result = execute_query(latest_date_query)
            
            if latest_date_result is not None and not latest_date_result.empty and latest_date_result['max_date'].iloc[0] is not None:
                latest_date = latest_date_result['max_date'].iloc[0]
                print(f"Latest GBP/USD exchange rate date in database: {latest_date}")
                
                # Filter to only include new data
                new_rates_df = gbp_rates_df[gbp_rates_df['date'] > latest_date]
                
                if not new_rates_df.empty:
                    print(f"Saving {len(new_rates_df)} new GBP/USD exchange rates to database...")
                    write_data(new_rates_df, 'exchange_rates', if_exists='append', batch_size=1000)
                else:
                    print("No new GBP/USD exchange rate data to save.")
            else:
                # If no data exists yet, save all data
                print(f"No existing GBP/USD exchange rate data found. Saving {len(gbp_rates_df)} rates to database...")
                write_data(gbp_rates_df, 'exchange_rates', if_exists='append', batch_size=1000)
        
        # Convert GBP stocks to USD
        for ticker in gbp_tickers:
            if ticker in prices_df.columns:
                gbp_prices = prices_df[ticker]
                
                # Create a temporary DataFrame with both series properly indexed
                temp_df = pd.DataFrame({'GBP_Price': gbp_prices})
                temp_df['USD_Rate'] = gbp_usd_rate  # This will align indices automatically
                
                # Calculate USD prices only where both values exist
                temp_df['USD_Price'] = temp_df['GBP_Price'] * temp_df['USD_Rate']
                
                # Add the converted prices to prices_df
                prices_df.loc[:, ticker] = temp_df['USD_Price']
    
    # Download and process CAD to USD exchange rates
    if cad_tickers:
        print("\nDownloading CAD/USD exchange rate data for currency conversion...")
        cad_usd_data = yf.download('CADUSD=X', start=START_DATE, end=end_date)
        cad_usd_rate = cad_usd_data['Close']
        
        # Save exchange rates to database
        print(f"Saving CAD/USD exchange rates to database...")
        
        # Prepare data for bulk insert
        cad_rates_data = []
        for date_idx, rate_value in zip(cad_usd_rate.index, cad_usd_rate.values):
            if pd.notna(rate_value):
                date_str = date_idx.date().isoformat()
                cad_rates_data.append({
                    'date': date_str,
                    'from_currency': 'CAD',
                    'to_currency': 'USD',
                    'rate': float(rate_value)
                })
        
        # Convert to DataFrame for bulk operations
        if cad_rates_data:
            cad_rates_df = pd.DataFrame(cad_rates_data)
            
            # Check for existing exchange rate data to implement incremental writes
            print("Checking existing CAD/USD exchange rate data for incremental update...")
            latest_date_query = "SELECT MAX(date) as max_date FROM exchange_rates WHERE from_currency = 'CAD' AND to_currency = 'USD'"
            latest_date_result = execute_query(latest_date_query)
            
            if latest_date_result is not None and not latest_date_result.empty and latest_date_result['max_date'].iloc[0] is not None:
                latest_date = latest_date_result['max_date'].iloc[0]
                print(f"Latest CAD/USD exchange rate date in database: {latest_date}")
                
                # Filter to only include new data
                new_rates_df = cad_rates_df[cad_rates_df['date'] > latest_date]
                
                if not new_rates_df.empty:
                    print(f"Saving {len(new_rates_df)} new CAD/USD exchange rates to database...")
                    write_data(new_rates_df, 'exchange_rates', if_exists='append', batch_size=1000)
                else:
                    print("No new CAD/USD exchange rate data to save.")
            else:
                # If no data exists yet, save all data
                print(f"No existing CAD/USD exchange rate data found. Saving {len(cad_rates_df)} rates to database...")
                write_data(cad_rates_df, 'exchange_rates', if_exists='append', batch_size=1000)
        
        # Convert CAD stocks to USD
        for ticker in cad_tickers:
            if ticker in prices_df.columns:
                cad_prices = prices_df[ticker]
                
                # Create a temporary DataFrame with both series properly indexed
                temp_df = pd.DataFrame({'CAD_Price': cad_prices})
                temp_df['USD_Rate'] = cad_usd_rate  # This will align indices automatically
                
                # Calculate USD prices only where both values exist
                temp_df['USD_Price'] = temp_df['CAD_Price'] * temp_df['USD_Rate']
                
                # Add the converted prices to prices_df
                prices_df.loc[:, ticker] = temp_df['USD_Price']
    
    # Prepare data for bulk insert
    all_prices_data = []
    
    print("Processing price data for database storage...")
    for ticker in tqdm(unique_tickers, desc="Processing tickers"):
        if ticker in prices_df.columns:
            ticker_prices = prices_df[ticker]
            
            for date, price in ticker_prices.items():
                if pd.notna(price):
                    date_str = date.date().isoformat()
                    all_prices_data.append({
                        'date': date_str,
                        'ticker': ticker,
                        'price': float(price)
                    })
    
    # Convert to DataFrame for bulk operations
    if all_prices_data:
        prices_df_for_db = pd.DataFrame(all_prices_data)
        
        # Check what data already exists in the database to implement incremental writes
        print("Checking existing price data for incremental update...")
        
        # Get the latest date in the database
        latest_date_query = "SELECT MAX(date) as max_date FROM daily_prices"
        latest_date_result = execute_query(latest_date_query)
        
        if latest_date_result is not None and not latest_date_result.empty and latest_date_result['max_date'].iloc[0] is not None:
            latest_date = latest_date_result['max_date'].iloc[0]
            print(f"Latest date in database: {latest_date}")
            
            # Filter to only include new data (dates after the latest date in the database)
            new_prices_df = prices_df_for_db[prices_df_for_db['date'] > latest_date]
            
            if not new_prices_df.empty:
                print(f"Saving {len(new_prices_df)} new price records to database...")
                write_data(new_prices_df, 'daily_prices', if_exists='append', batch_size=1000)
            else:
                print("No new price data to save.")
        else:
            # If no data exists yet, save all data
            print(f"No existing price data found. Saving {len(prices_df_for_db)} price records to database...")
            write_data(prices_df_for_db, 'daily_prices', if_exists='replace', batch_size=1000)
    return prices_df

def calculate_and_save_portfolio_allocations(db):
    """
    Calculate portfolio allocations and save to database.
    
    Args:
        db: Database connection
    
    Returns:
        pandas.DataFrame: DataFrame with portfolio allocations
    """
    # Check if the necessary columns exist in the positions table, add them if they don't
    check_columns_query = "PRAGMA table_info(positions)"
    columns_info = execute_query(check_columns_query)
    
    # Get existing column names
    existing_columns = columns_info['name'].tolist() if columns_info is not None else []
    
    # Check and add missing columns
    missing_columns = []
    if 'shares' not in existing_columns:
        missing_columns.append("ADD COLUMN shares REAL")
    if 'price_at_start' not in existing_columns:
        missing_columns.append("ADD COLUMN price_at_start REAL")
    if 'allocation_usd' not in existing_columns:
        missing_columns.append("ADD COLUMN allocation_usd REAL")
    
    # Alter table to add missing columns if any
    if missing_columns:
        for column_def in missing_columns:
            alter_table_query = f"ALTER TABLE positions {column_def}"
            print(f"Adding missing column: {alter_table_query}")
            execute_query(alter_table_query)
    # Get positions
    positions_query = "SELECT * FROM positions"
    positions_df = execute_query(positions_query)
    
    if positions_df is None or positions_df.empty:
        print("No positions found in the database")
        return pd.DataFrame()
    
    # Calculate person weights
    person_weights = {}
    for _, position in positions_df.iterrows():
        name = position['name']
        weight = position['weight']
        
        if name not in person_weights:
            person_weights[name] = 0
        
        person_weights[name] += weight
    
    # Find the closest date in our data to FIXED_DATE
    fixed_date_obj = datetime.strptime(FIXED_DATE, '%Y-%m-%d').date()
    closest_date_query = f"""
    SELECT date FROM daily_prices 
    WHERE date <= '{fixed_date_obj.isoformat()}' 
    ORDER BY date DESC LIMIT 1
    """
    closest_date_result = execute_query(closest_date_query)
    
    if closest_date_result is None or closest_date_result.empty:
        raise ValueError(f"No price data available before {FIXED_DATE}")
    
    closest_date = closest_date_result['date'].iloc[0]
    print(f"Using prices from {closest_date} for allocation calculations")
    
    # Prepare data for bulk insert
    portfolio_allocations = []
    position_updates = []
    
    for _, position in positions_df.iterrows():
        name = position['name']
        ticker = position['formatted_ticker']
        weight = position['weight']
        
        # Calculate allocation amount
        # Normalize by the person's total weight to ensure 100% allocation
        allocation_usd = INITIAL_VALUE_USD * (weight / person_weights[name])
        
        # Get the price on the fixed date
        price_query = f"""
        SELECT price FROM daily_prices 
        WHERE date = '{closest_date}' AND ticker = '{ticker}'
        """
        price_result = execute_query(price_query)
        
        if price_result is not None and not price_result.empty:
            price = price_result['price'].iloc[0]
            shares = allocation_usd / price
            actual_allocation = shares * price
            
            # Add to position updates
            position_updates.append({
                'name': name,
                'ticker': position['ticker'],  # Use original ticker for update
                'shares': shares,
                'price_at_start': price,
                'allocation_usd': actual_allocation
            })
            
            # Add to portfolio allocations for return value
            portfolio_allocations.append({
                'name': name,
                'ticker': ticker,
                'shares': shares,
                'price_at_start': price,
                'allocation_usd': actual_allocation
            })
        else:
            print(f"Warning: No price data available for {ticker} on {closest_date}")
    
    # Update positions with allocation data
    if position_updates:
        # For each position, update the database
        print("Updating positions with allocation data...")
        for update in tqdm(position_updates, desc="Updating positions"):
            update_query = f"""
            UPDATE positions 
            SET shares = {update['shares']}, 
                price_at_start = {update['price_at_start']}, 
                allocation_usd = {update['allocation_usd']} 
            WHERE name = '{update['name']}' AND ticker = '{update['ticker']}'
            """
            execute_query(update_query)
    
    # Create a DataFrame for the portfolio
    portfolio_df = pd.DataFrame(portfolio_allocations)
    return portfolio_df

def calculate_and_save_portfolio_values(db):
    """
    Calculate daily portfolio values and save to database.
    
    Args:
        db: Database connection
    
    Returns:
        pandas.DataFrame: DataFrame with portfolio values
    """
    # Get positions with share allocations
    positions_query = "SELECT * FROM positions WHERE shares IS NOT NULL"
    positions_df = execute_query(positions_query)
    
    if positions_df is None or positions_df.empty:
        print("No positions with share allocations found in the database")
        return pd.DataFrame()
    
    # Find the closest date to FIXED_DATE
    fixed_date_obj = datetime.strptime(FIXED_DATE, '%Y-%m-%d').date()
    closest_date_query = f"""
    SELECT date FROM daily_prices 
    WHERE date <= '{fixed_date_obj.isoformat()}' 
    ORDER BY date DESC LIMIT 1
    """
    closest_date_result = execute_query(closest_date_query)
    
    if closest_date_result is None or closest_date_result.empty:
        raise ValueError(f"No price data available before {FIXED_DATE}")
    
    closest_date = closest_date_result['date'].iloc[0]
    
    # Get all dates since the closest date
    dates_query = f"""
    SELECT DISTINCT date FROM daily_prices 
    WHERE date >= '{closest_date}' 
    ORDER BY date
    """
    dates_result = execute_query(dates_query)
    
    if dates_result is None or dates_result.empty:
        print("No dates found after the closest date")
        return pd.DataFrame()
    
    dates = dates_result['date'].tolist()
    
    # Initialize a DataFrame to store portfolio values
    portfolio_values = pd.DataFrame(index=dates)
    
    # Get unique names
    names = positions_df['name'].unique()
    
    # Calculate daily value for each person
    for name in names:
        person_portfolio = positions_df[positions_df['name'] == name]
        daily_values = pd.Series(0, index=dates)
        
        for _, position in person_portfolio.iterrows():
            ticker = position['formatted_ticker']
            shares = position['shares']
            
            # Get prices for this ticker
            prices_query = f"""
            SELECT date, price FROM daily_prices 
            WHERE ticker = '{ticker}' 
            AND date IN ('{"', '".join(dates)}')
            """
            prices_result = execute_query(prices_query)
            
            if prices_result is not None and not prices_result.empty:
                # Create a dictionary of date -> price
                price_dict = dict(zip(prices_result['date'], prices_result['price']))
                
                # Calculate daily value of this position
                for date in dates:
                    if date in price_dict:
                        daily_values[date] += price_dict[date] * shares
        
        portfolio_values[name] = daily_values
        
        # Calculate percentage change
        initial_value = daily_values.iloc[0]
        pct_change = ((daily_values / initial_value) - 1) * 100
        
        # Prepare data for bulk insert
        portfolio_values_data = []
        for date, value in daily_values.items():
            portfolio_values_data.append({
                'date': date,
                'name': name,
                'value': float(value),
                'pct_change': float(pct_change[date])
            })
        
        # Convert to DataFrame for bulk operations
        if portfolio_values_data:
            portfolio_values_df = pd.DataFrame(portfolio_values_data)
            
            # Use bulk write operation with smaller batch size to avoid timeouts
            print(f"Saving {len(portfolio_values_df)} portfolio values for {name} to database...")
            write_data(portfolio_values_df, 'portfolio_values', if_exists='replace' if name == names[0] else 'append', batch_size=1000)
    
    return portfolio_values

def calculate_and_save_performance_metrics(db):
    """
    Calculate performance metrics and save to database.
    
    Args:
        db: Database connection
    
    Returns:
        pandas.DataFrame: DataFrame with performance metrics
    """
    # Get unique names
    names_query = "SELECT DISTINCT name FROM positions"
    names_result = execute_query(names_query)
    
    if names_result is None or names_result.empty:
        print("No names found in the database")
        return pd.DataFrame()
    
    names = names_result['name'].tolist()
    
    # Prepare data for bulk insert
    performance_data = []
    metrics_data = []
    current_date = datetime.now().date().isoformat()
    
    for name in names:
        # Get portfolio values for this person
        values_query = f"""
        SELECT date, value FROM portfolio_values 
        WHERE name = '{name}' 
        ORDER BY date
        """
        values_result = execute_query(values_query)
        
        if values_result is None or values_result.empty:
            print(f"Warning: No portfolio values found for {name}")
            continue
        
        initial_value = values_result['value'].iloc[0]
        final_value = values_result['value'].iloc[-1]
        
        # Calculate returns
        total_return = (final_value / initial_value - 1) * 100
        
        # Calculate annualized returns
        first_date = datetime.fromisoformat(values_result['date'].iloc[0])
        last_date = datetime.fromisoformat(values_result['date'].iloc[-1])
        days = (last_date - first_date).days
        years = days / 365.25
        annualized_return = ((final_value / initial_value) ** (1 / years) - 1) * 100
        
        # Add to metrics data for database
        metrics_data.append({
            'name': name,
            'initial_value': float(initial_value),
            'final_value': float(final_value),
            'total_return_pct': float(total_return),
            'annualized_return_pct': float(annualized_return),
            'last_updated': current_date
        })
        
        # Add to performance data for return value
        performance_data.append({
            'Portfolio': name,
            'Initial Value': initial_value,
            'Final Value': final_value,
            'Total Return (%)': total_return,
            'Annualized Return (%)': annualized_return
        })
    
    # Convert to DataFrame for bulk operations
    if metrics_data:
        metrics_df = pd.DataFrame(metrics_data)
        
        # Use bulk write operation with smaller batch size to avoid timeouts
        print(f"Saving {len(metrics_df)} performance metrics to database...")
        write_data(metrics_df, 'performance_metrics', if_exists='replace', batch_size=1000)
    
    # Create a DataFrame for the performance metrics
    performance_df = pd.DataFrame(performance_data)
    return performance_df

def process_all_data(db=None, end_date=None):
    """
    Process all data and save to database.
    
    Args:
        db: Database connection (optional)
        end_date (str, optional): End date for data download. Defaults to today.
    """
    # Get database connection if not provided
    if db is None:
        db = get_db()
    
    print("Saving positions to database...")
    save_positions_to_db(db)
    
    print("Downloading and saving stock data...")
    download_and_save_stock_data(db, end_date)
    
    print("Calculating and saving portfolio allocations...")
    calculate_and_save_portfolio_allocations(db)
    
    print("Calculating and saving portfolio values...")
    calculate_and_save_portfolio_values(db)
    
    print("Calculating and saving performance metrics...")
    calculate_and_save_performance_metrics(db)
    
    print("Data processing complete!")
