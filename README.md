# Stocks Picking Competition
## H1 2025 Analyst Paper Trading Challenge

This repository hosts a friendly paper trading competition among a group of financial analysts. Each participant has selected a portfolio of stocks from various global exchanges and currencies, and we're tracking their performance over time.

### Dashboard

The project includes a Streamlit dashboard that visualizes the competition data. The dashboard shows:

- Portfolio weight distribution by person
- Portfolio allocations by person and stock
- Portfolio performance over time
- Percentage change since start date
- Performance summary with total and annualized returns

The dashboard uses a local SQLite database to store the data, which can be easily migrated to SQLite Cloud later.

### Competition Overview
- **Time Period**: First half of 2025
- **Starting Capital**: $100,000 (paper money)
- **Participants**: Multiple analysts with diverse investment strategies
- **Global Coverage**: Stocks from various exchanges (NYSE, NASDAQ, Hong Kong, Paris, London, Toronto)
- **Multi-Currency**: Handles USD, HKD, EUR, GBP, CAD with automatic currency conversion

## Environment Setup

This project uses a Python virtual environment to manage dependencies. To set up the environment:

```bash
# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
# .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

## Running the Dashboard

To run the dashboard:

```bash
# Activate your virtual environment
source .venv/bin/activate  # On macOS/Linux
# .venv\Scripts\activate   # On Windows

# Run the dashboard
python main.py
```

The dashboard will:
1. Initialize the SQLite database if it doesn't exist
2. Prompt you to load initial data if the database is empty
3. Display visualizations of the competition data
4. Allow you to refresh the data with the latest stock prices

## Project Structure

```
stocks-picking-competition/
├── data/                  # Directory for SQLite database
│   └── stocks.db          # Local SQLite database
├── src/                   # Source code
│   ├── __init__.py
│   ├── db/                # Database related code
│   │   ├── __init__.py
│   │   ├── models.py      # SQLAlchemy models
│   │   └── database.py    # Database connection and operations
│   ├── data_processing.py # Data processing functions
│   └── dashboard.py       # Streamlit dashboard
├── main.py                # Main entry point
├── manual_start.ipynb     # Original notebook with data processing
├── requirements.txt
└── README.md
```

## Dependencies

The project uses the following packages:

### Core data science packages
- numpy>=1.24.0
- pandas>=2.0.0
- scikit-learn>=1.3.0

### Visualization
- matplotlib>=3.7.0
- plotly>=5.18.0
- streamlit>=1.31.0  # For web dashboard

### Data sources
- yfinance>=0.2.35

### Database
- sqlite3-api>=2.0.1
- sqlitecloud>=0.0.83  # SQLite Cloud SDK
- sqlalchemy>=2.0.0    # For SQLAlchemy ORM and database connection
- python-dotenv>=1.0.0  # For .env file support

### Development tools
- jupyter>=1.0.0
- ipykernel>=6.0.0
- notebook>=7.0.0
