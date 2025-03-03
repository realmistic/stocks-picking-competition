# Stocks Picking Competition
## H1 2025 Analyst Paper Trading Challenge

This repository hosts a friendly paper trading competition among a group of financial analysts. Each participant has selected a portfolio of stocks from various global exchanges and currencies, and we're tracking their performance over time.

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
