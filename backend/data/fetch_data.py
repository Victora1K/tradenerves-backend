import os
import requests
import sqlite3
from datetime import datetime, timezone

#API_KEY = '4ku7YB5AIpIL_IRvfIiI4xZV09EoLGD6'        #Prod API Key
API_KEY = 'gNUdx8Rrob9OtDQSGK9EBX7K179qpNjQ'         #Test API Key

# Set the database path using absolute path resolution
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../db/stocks.db'))

def fetch_and_store_stock_data(symbol):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    start_date = '1994-01-01'
    end_date = datetime.now().strftime('%Y-%m-%d')

    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={API_KEY}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json().get('results', [])
        for entry in data:
            open_price = float(entry['o']) if 'o' in entry and entry['o'] is not None else 0.0
            high_price = float(entry['h']) if 'h' in entry and entry['h'] is not None else 0.0
            low_price = float(entry['l']) if 'l' in entry and entry['l'] is not None else 0.0
            close_price = float(entry['c']) if 'c' in entry and entry['c'] is not None else 0.0
            volume = int(entry['v']) if 'v' in entry and entry['v'] is not None else 0

            cur.execute(
                '''
                INSERT INTO stock_prices (symbol, date, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    symbol, 
                    datetime.fromtimestamp(entry['t'] / 1000, timezone.utc).strftime('%Y-%m-%d'),
                    open_price, high_price, low_price, close_price, volume
                )
            )
    
    conn.commit()
    conn.close()

# List of top 7 highest market cap stocks
symbols = [
        'SPY',
        'AAPL', 'NVDA', 'TSLA', 'MSFT', 'GOOGL', 'AMZN',  # Technology
        'JPM', 'BAC', 'WFC', 'GS', 'AXP',  # Banking
        'JNJ', 'UNH', 'PFE',  # Healthcare
        'XOM', 'CVX', 'COP',  # Energy
        'WMT', 'PG', 'KO',  # Consumer
        'META', 'NFLX', 'ADBE',  # Internet/Software
        'AAL', 'PLTR', 'FTNT', 'PANW', 'ZS' #Security & Interests
    ]

# Fetch stock data for the listed symbols
for symbol in symbols:
    fetch_and_store_stock_data(symbol)

print("Stock data fetched and stored.")
