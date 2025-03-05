"""
Data processing functions for the stocks picking competition.
"""
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from src.db.models import Position, DailyPrice, ExchangeRate, PortfolioValue, PerformanceMetric

# Constants
INITIAL_VALUE_USD = 1e5
START_DATE = '2000-01-01'
FIXED_DATE = '2025-03-01'

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

def save_positions_to_db(db: Session):
    """
    Save positions to the database.
    
    Args:
        db (Session): SQLAlchemy session
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
    
    # Save positions to database
    for position in positions:
        name = position['name']
        ticker = position['ticker']
        exchange = position['exchange']
        weight = position['weight']
        formatted_ticker = get_formatted_ticker(ticker, exchange)
        
        # Check if position already exists
        db_position = db.query(Position).filter(
            Position.name == name,
            Position.ticker == ticker
        ).first()
        
        if db_position:
            # Update existing position
            db_position.exchange = exchange
            db_position.weight = weight
            db_position.formatted_ticker = formatted_ticker
        else:
            # Create new position
            db_position = Position(
                name=name,
                ticker=ticker,
                exchange=exchange,
                weight=weight,
                formatted_ticker=formatted_ticker
            )
            db.add(db_position)
    
    db.commit()
    return positions

def download_and_save_stock_data(db: Session, end_date=None):
    """
    Download stock data from Yahoo Finance and save to database.
    
    Args:
        db (Session): SQLAlchemy session
        end_date (str, optional): End date for data download. Defaults to today.
    
    Returns:
        pandas.DataFrame: DataFrame with price data
    """
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    
    # Get positions
    positions = db.query(Position).all()
    
    # Get unique tickers and track which ones are from different exchanges
    unique_tickers = []
    hk_tickers = []  # Hong Kong stocks (HKD)
    eur_tickers = []  # European stocks (EUR)
    gbp_tickers = []  # UK stocks (GBP)
    cad_tickers = []  # Canadian stocks (CAD)
    
    for position in positions:
        formatted_ticker = position.formatted_ticker
        
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
        count = 0
        
        # Process each date and rate
        for date_idx, rate_value in zip(exchange_rate.index, exchange_rate.values):
            if pd.notna(rate_value):
                count += 1
                try:
                    # Check if exchange rate already exists
                    db_rate = db.query(ExchangeRate).filter(
                        ExchangeRate.date == date_idx.date(),
                        ExchangeRate.from_currency == 'HKD',
                        ExchangeRate.to_currency == 'USD'
                    ).first()
                    
                    if db_rate:
                        # Update existing rate
                        db_rate.rate = float(rate_value)
                    else:
                        # Create new rate
                        db_rate = ExchangeRate(
                            date=date_idx.date(),
                            from_currency='HKD',
                            to_currency='USD',
                            rate=float(rate_value)
                        )
                        db.add(db_rate)
                except Exception as e:
                    if count <= 5:
                        print(f"Error saving HKD/USD rate for {date_idx.date()}: {e}")
        
        print(f"Processed {count} HKD/USD exchange rates")
        
        # Commit the changes
        db.commit()
        
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
        count = 0
        
        # Process each date and rate
        for date_idx, rate_value in zip(eur_usd_rate.index, eur_usd_rate.values):
            if pd.notna(rate_value):
                count += 1
                try:
                    # Check if exchange rate already exists
                    db_rate = db.query(ExchangeRate).filter(
                        ExchangeRate.date == date_idx.date(),
                        ExchangeRate.from_currency == 'EUR',
                        ExchangeRate.to_currency == 'USD'
                    ).first()
                    
                    if db_rate:
                        # Update existing rate
                        db_rate.rate = float(rate_value)
                    else:
                        # Create new rate
                        db_rate = ExchangeRate(
                            date=date_idx.date(),
                            from_currency='EUR',
                            to_currency='USD',
                            rate=float(rate_value)
                        )
                        db.add(db_rate)
                except Exception as e:
                    if count <= 5:
                        print(f"Error saving EUR/USD rate for {date_idx.date()}: {e}")
        
        print(f"Processed {count} EUR/USD exchange rates")
        
        # Commit the changes
        db.commit()
        
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
        count = 0
        
        # Process each date and rate
        for date_idx, rate_value in zip(gbp_usd_rate.index, gbp_usd_rate.values):
            if pd.notna(rate_value):
                count += 1
                try:
                    # Check if exchange rate already exists
                    db_rate = db.query(ExchangeRate).filter(
                        ExchangeRate.date == date_idx.date(),
                        ExchangeRate.from_currency == 'GBP',
                        ExchangeRate.to_currency == 'USD'
                    ).first()
                    
                    if db_rate:
                        # Update existing rate
                        db_rate.rate = float(rate_value)
                    else:
                        # Create new rate
                        db_rate = ExchangeRate(
                            date=date_idx.date(),
                            from_currency='GBP',
                            to_currency='USD',
                            rate=float(rate_value)
                        )
                        db.add(db_rate)
                except Exception as e:
                    if count <= 5:
                        print(f"Error saving GBP/USD rate for {date_idx.date()}: {e}")
        
        print(f"Processed {count} GBP/USD exchange rates")
        
        # Commit the changes
        db.commit()
        
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
        count = 0
        
        # Process each date and rate
        for date_idx, rate_value in zip(cad_usd_rate.index, cad_usd_rate.values):
            if pd.notna(rate_value):
                count += 1
                try:
                    # Check if exchange rate already exists
                    db_rate = db.query(ExchangeRate).filter(
                        ExchangeRate.date == date_idx.date(),
                        ExchangeRate.from_currency == 'CAD',
                        ExchangeRate.to_currency == 'USD'
                    ).first()
                    
                    if db_rate:
                        # Update existing rate
                        db_rate.rate = float(rate_value)
                    else:
                        # Create new rate
                        db_rate = ExchangeRate(
                            date=date_idx.date(),
                            from_currency='CAD',
                            to_currency='USD',
                            rate=float(rate_value)
                        )
                        db.add(db_rate)
                except Exception as e:
                    if count <= 5:
                        print(f"Error saving CAD/USD rate for {date_idx.date()}: {e}")
        
        print(f"Processed {count} CAD/USD exchange rates")
        
        # Commit the changes
        db.commit()
        
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
    
    # Save price data to database
    for ticker in unique_tickers:
        if ticker in prices_df.columns:
            ticker_prices = prices_df[ticker]
            
            for date, price in ticker_prices.items():
                if pd.notna(price):
                    # Check if price already exists
                    db_price = db.query(DailyPrice).filter(
                        DailyPrice.date == date.date(),
                        DailyPrice.ticker == ticker
                    ).first()
                    
                    if db_price:
                        # Update existing price
                        db_price.price = price
                    else:
                        # Create new price
                        db_price = DailyPrice(
                            date=date.date(),
                            ticker=ticker,
                            price=price
                        )
                        db.add(db_price)
    
    db.commit()
    return prices_df

def calculate_and_save_portfolio_allocations(db: Session):
    """
    Calculate portfolio allocations and save to database.
    
    Args:
        db (Session): SQLAlchemy session
    
    Returns:
        pandas.DataFrame: DataFrame with portfolio allocations
    """
    # Get positions
    positions = db.query(Position).all()
    
    # Calculate person weights
    person_weights = {}
    for position in positions:
        name = position.name
        weight = position.weight
        
        if name not in person_weights:
            person_weights[name] = 0
        
        person_weights[name] += weight
    
    # Find the closest date in our data to FIXED_DATE
    closest_date_record = db.query(DailyPrice.date).filter(
        DailyPrice.date <= datetime.strptime(FIXED_DATE, '%Y-%m-%d').date()
    ).order_by(DailyPrice.date.desc()).first()
    
    if not closest_date_record:
        raise ValueError(f"No price data available before {FIXED_DATE}")
    
    closest_date = closest_date_record[0]
    print(f"Using prices from {closest_date} for allocation calculations")
    
    # Calculate allocations
    portfolio_allocations = []
    
    for position in positions:
        name = position.name
        ticker = position.formatted_ticker
        weight = position.weight
        
        # Calculate allocation amount
        # Normalize by the person's total weight to ensure 100% allocation
        allocation_usd = INITIAL_VALUE_USD * (weight / person_weights[name])
        
        # Get the price on the fixed date
        price_record = db.query(DailyPrice).filter(
            DailyPrice.date == closest_date,
            DailyPrice.ticker == ticker
        ).first()
        
        if price_record:
            price = price_record.price
            shares = allocation_usd / price
            actual_allocation = shares * price
            
            # Update position in database
            position.shares = shares
            position.price_at_start = price
            position.allocation_usd = actual_allocation
            
            portfolio_allocations.append({
                'name': name,
                'ticker': ticker,
                'shares': shares,
                'price_at_start': price,
                'allocation_usd': actual_allocation
            })
        else:
            print(f"Warning: No price data available for {ticker} on {closest_date}")
    
    db.commit()
    
    # Create a DataFrame for the portfolio
    portfolio_df = pd.DataFrame(portfolio_allocations)
    return portfolio_df

def calculate_and_save_portfolio_values(db: Session):
    """
    Calculate daily portfolio values and save to database.
    
    Args:
        db (Session): SQLAlchemy session
    
    Returns:
        pandas.DataFrame: DataFrame with portfolio values
    """
    # Get positions with share allocations
    positions = db.query(Position).filter(Position.shares.isnot(None)).all()
    
    # Find the closest date to FIXED_DATE
    closest_date_record = db.query(DailyPrice.date).filter(
        DailyPrice.date <= datetime.strptime(FIXED_DATE, '%Y-%m-%d').date()
    ).order_by(DailyPrice.date.desc()).first()
    
    if not closest_date_record:
        raise ValueError(f"No price data available before {FIXED_DATE}")
    
    closest_date = closest_date_record[0]
    
    # Get all dates since the closest date
    dates = db.query(DailyPrice.date).filter(
        DailyPrice.date >= closest_date
    ).distinct().order_by(DailyPrice.date).all()
    dates = [date[0] for date in dates]
    
    # Initialize a DataFrame to store portfolio values
    portfolio_values = pd.DataFrame(index=dates)
    
    # Calculate daily value for each person
    for name in set(position.name for position in positions):
        person_portfolio = [p for p in positions if p.name == name]
        daily_values = pd.Series(0, index=dates)
        
        for position in person_portfolio:
            ticker = position.formatted_ticker
            shares = position.shares
            
            # Get prices for this ticker
            prices = db.query(DailyPrice).filter(
                DailyPrice.ticker == ticker,
                DailyPrice.date.in_(dates)
            ).all()
            
            # Create a dictionary of date -> price
            price_dict = {price.date: price.price for price in prices}
            
            # Calculate daily value of this position
            for date in dates:
                if date in price_dict:
                    daily_values[date] += price_dict[date] * shares
        
        portfolio_values[name] = daily_values
        
        # Calculate percentage change
        initial_value = daily_values.iloc[0]
        pct_change = ((daily_values / initial_value) - 1) * 100
        
        # Save portfolio values to database
        for date, value in daily_values.items():
            # Check if portfolio value already exists
            db_value = db.query(PortfolioValue).filter(
                PortfolioValue.date == date,
                PortfolioValue.name == name
            ).first()
            
            if db_value:
                # Update existing value
                db_value.value = value
                db_value.pct_change = pct_change[date]
            else:
                # Create new value
                db_value = PortfolioValue(
                    date=date,
                    name=name,
                    value=value,
                    pct_change=pct_change[date]
                )
                db.add(db_value)
    
    db.commit()
    return portfolio_values

def calculate_and_save_performance_metrics(db: Session):
    """
    Calculate performance metrics and save to database.
    
    Args:
        db (Session): SQLAlchemy session
    
    Returns:
        pandas.DataFrame: DataFrame with performance metrics
    """
    # Get unique names
    names = db.query(Position.name).distinct().all()
    names = [name[0] for name in names]
    
    performance_data = []
    
    for name in names:
        # Get portfolio values for this person
        portfolio_values = db.query(PortfolioValue).filter(
            PortfolioValue.name == name
        ).order_by(PortfolioValue.date).all()
        
        if not portfolio_values:
            print(f"Warning: No portfolio values found for {name}")
            continue
        
        initial_value = portfolio_values[0].value
        final_value = portfolio_values[-1].value
        
        # Calculate returns
        total_return = (final_value / initial_value - 1) * 100
        
        # Calculate annualized returns
        days = (portfolio_values[-1].date - portfolio_values[0].date).days
        years = days / 365.25
        annualized_return = ((final_value / initial_value) ** (1 / years) - 1) * 100
        
        # Check if performance metric already exists
        db_metric = db.query(PerformanceMetric).filter(
            PerformanceMetric.name == name
        ).first()
        
        if db_metric:
            # Update existing metric
            db_metric.initial_value = initial_value
            db_metric.final_value = final_value
            db_metric.total_return_pct = total_return
            db_metric.annualized_return_pct = annualized_return
            db_metric.last_updated = datetime.now().date()
        else:
            # Create new metric
            db_metric = PerformanceMetric(
                name=name,
                initial_value=initial_value,
                final_value=final_value,
                total_return_pct=total_return,
                annualized_return_pct=annualized_return
            )
            db.add(db_metric)
        
        performance_data.append({
            'Portfolio': name,
            'Initial Value': initial_value,
            'Final Value': final_value,
            'Total Return (%)': total_return,
            'Annualized Return (%)': annualized_return
        })
    
    db.commit()
    
    # Create a DataFrame for the performance metrics
    performance_df = pd.DataFrame(performance_data)
    return performance_df

def process_all_data(db: Session, end_date=None):
    """
    Process all data and save to database.
    
    Args:
        db (Session): SQLAlchemy session
        end_date (str, optional): End date for data download. Defaults to today.
    """
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
