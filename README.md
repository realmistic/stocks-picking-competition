# Stocks Picking Competition Dashboard

A Streamlit dashboard for tracking and visualizing the performance of stock portfolios in a competition.

## Features

- Portfolio weight distribution visualization
- Portfolio allocations treemap
- Performance tracking over time
- Performance metrics comparison

## Local Development

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/stocks-picking-competition.git
   cd stocks-picking-competition
   ```

2. Create a virtual environment and install dependencies:
   ```
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Run the Streamlit app:
   ```
   streamlit run src/dashboard.py
   ```

## Deployment to Streamlit Cloud

1. Push your code to a GitHub repository.

2. Go to [Streamlit Cloud](https://streamlit.io/cloud) and sign in.

3. Click "New app" and select your repository.

4. Set the main file path to `src/dashboard.py`.

5. Deploy the app.

## Data Storage

The application uses SQLite for local data storage. The database file is stored in the `data/` directory.

## License

This project is licensed under the terms of the license included in the repository.
