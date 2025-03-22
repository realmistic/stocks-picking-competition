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

## Automated Updates with GitHub Actions

This project includes a GitHub Actions workflow that automatically updates the remote database with the latest stock data on a daily basis. The workflow:

1. Runs every day at midnight UTC
2. Fetches the latest stock data
3. Updates the remote SQLite Cloud database incrementally
4. Does not make any local commits to the repository

### Environment Variables

This project uses the following environment variable:

- `SQLITECLOUD_URL`: Your complete SQLite Cloud connection string

#### Local Development

For local development, you can set this environment variable in one of two ways:

1. Create a `.env` file in the project root:
   - Copy the provided `.env.sample` file to `.env`
   - Replace the placeholder values with your actual connection string
   ```
   SQLITECLOUD_URL=sqlitecloud://hostname:port/database?apikey=your_api_key_here
   ```

2. Set the environment variable directly in your shell:
   ```bash
   # For Linux/macOS
   export SQLITECLOUD_URL="sqlitecloud://hostname:port/database?apikey=your_api_key_here"
   
   # For Windows Command Prompt
   set SQLITECLOUD_URL=sqlitecloud://hostname:port/database?apikey=your_api_key_here
   
   # For Windows PowerShell
   $env:SQLITECLOUD_URL="sqlitecloud://hostname:port/database?apikey=your_api_key_here"
   ```

#### GitHub Actions Setup

To set up the GitHub Actions workflow:

1. Go to your GitHub repository settings
2. Navigate to "Secrets and variables" > "Actions"
3. Add a new repository secret:
   - Name: `SQLITECLOUD_URL`
   - Value: Your complete SQLite Cloud connection string

The workflow can also be manually triggered from the "Actions" tab in your GitHub repository.

## License

This project is licensed under the terms of the license included in the repository.
