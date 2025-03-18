from flask import Flask, jsonify
from flask_cors import CORS
import sqlite3
import os
from data.intra_day import get_intra_day
from data.detect_patterns import detect_double_bottoms, detect_hammer, calculate_volatility, detect_and_store_patterns
from data.detect_green_five import detect_and_store_patterns as detect_patterns_five
from data.fetch_data import fetch_and_store_stock_data as fetch_daily
from data.fetch_data_five import fetch_and_store_stock_data as fetch_five_min
import requests
from datetime import datetime, timedelta, timezone
import random
import threading

# API Key for Polygon.io
API_KEY = 'gNUdx8Rrob9OtDQSGK9EBX7K179qpNjQ'  # Test API Key

app = Flask(__name__)
CORS(app)  # Enable CORS for the entire Flask app

# Path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'db/stocks.db'))
five_db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'db/stocks_five.db'))

#init_db_day()

def get_db_connection():
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def get_db():
    conn = sqlite3.connect('trading.db')
    conn.row_factory = sqlite3.Row
    return conn

def get_five_db():
    conn = sqlite3.connect(five_db_path)
    conn.row_factory = sqlite3.Row
    return conn

# Initialize database and fetch data
def init_db():
    db = get_db()
    db.execute("""
        CREATE TABLE IF NOT EXISTS historical_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            query_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)
    db.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_queries_symbol 
        ON historical_queries(symbol)
    """)
    db.commit()

        


def fetch_all_data():
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

    print("Starting data fetch and pattern detection...")
    
    # Fetch daily data and detect patterns
    for symbol in symbols:
        try:
            print(f"Fetching daily data for {symbol}...")
            fetch_daily(symbol)
            print(f"Detecting patterns for {symbol}...")
            detect_and_store_patterns(symbol)
        except Exception as e:
            print(f"Error processing daily data for {symbol}: {str(e)}")

    # Fetch 5-minute data
    # for symbol in symbols:
    #     try:
    #         print(f"Fetching 5-minute data for {symbol}...")
    #         fetch_five_min(symbol)
    #         print(f"Detecting 5-minute patterns for {symbol}...")
    #         detect_patterns_five(symbol)
    #     except Exception as e:
    #         print(f"Error processing 5-minute data for {symbol}: {str(e)}")

    print("Data fetch and pattern detection completed!")

# Initialize database and start data fetch in background
with app.app_context():
    init_db()
    # Start data fetch in a background thread
    threading.Thread(target=fetch_all_data, daemon=True).start()

# Register database close function
def close_db(e=None):
    db = get_db()
    db.close()

app.teardown_appcontext(close_db)

@app.route('/api/historical/<symbol>/<start_date>/<end_date>')
def get_historical_data(symbol, start_date, end_date):
    try:
        # Basic validation
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d')
            end = datetime.strptime(end_date, '%Y-%m-%d')
            if start > end:
                return jsonify({'error': 'Start date must be before end date'}), 400
            if (end - start).days > 365:
                return jsonify({'error': 'Date range cannot exceed 1 year'}), 400
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use YYYY-MM-DD'}), 400

        # Fetch data from Polygon.io
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?apiKey={API_KEY}"
        response = requests.get(url)
        
        if response.status_code != 200:
            return jsonify({'error': f'API request failed with status {response.status_code}'}), 500

        data = response.json().get('results', [])
        if not data:
            return jsonify({'error': 'No data found for the specified date range'}), 404

        # Format the response
        formatted_data = []
        for entry in data:
            timestamp = datetime.fromtimestamp(entry['t'] / 1000).strftime('%Y-%m-%d')
            formatted_data.append({
                'date': timestamp,
                'open': float(entry['o']) if 'o' in entry and entry['o'] is not None else 0.0,
                'high': float(entry['h']) if 'h' in entry and entry['h'] is not None else 0.0,
                'low': float(entry['l']) if 'l' in entry and entry['l'] is not None else 0.0,
                'close': float(entry['c']) if 'c' in entry and entry['c'] is not None else 0.0,
                'volume': int(entry['v']) if 'v' in entry and entry['v'] is not None else 0
            })

        # Sort by date
        formatted_data.sort(key=lambda x: x['date'])

        # Log the query
        db = get_db()
        db.execute(
            'INSERT INTO historical_queries (symbol, start_date, end_date) VALUES (?, ?, ?)',
            (symbol, start_date, end_date)
        )
        db.commit()

        return jsonify(formatted_data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stock_prices/<symbol>/<timestamp>', methods=['GET'])
def get_stock_data_from_timestamp(symbol, timestamp):
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Fetch stock data from the given timestamp onwards
        cur.execute('''
            SELECT date, open, high, low, close, volume
            FROM stock_prices
            WHERE symbol = ? AND date >= ?
            ORDER BY date ASC
        ''', (symbol, timestamp))
        
        rows = cur.fetchall()
        conn.close()

        if rows:
            results = [
                {
                    'date': row['date'],
                    'open': row['open'],
                    'high': row['high'],
                    'low': row['low'],
                    'close': row['close'],
                    'volume': row['volume']
                }
                for row in rows
            ]
            return jsonify(results)
        else:
            return jsonify({'error': 'No stock data found from this timestamp'}), 404
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500

@app.route('/api/stock_prices_intra/<symbol>/<timestamp>', methods=['GET'])
def get_stock_intra_from_timestamp(symbol, timestamp):
    try:
        rows = get_intra_day(symbol, timestamp)
        
        if rows:
            results = [
                {
                    'date': datetime.fromtimestamp(row['t'] / 1000, timezone.utc).strftime('%y/%m/%d-%H:%M'),
                    'open': row['o'],
                    'high': row['h'],
                    'low': row['l'],
                    'close': row['c'],
                    'volume': row['v']
                }
                for row in rows
            ]
            return jsonify(results)
        else:
            return jsonify({'error': 'No stock data found from this timestamp'}), 404
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500

# Random Stock and Timestamp API
@app.route('/api/random_stock', methods=['GET'])
def random_stock():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Select a random stock symbol
        cur.execute('SELECT symbol FROM stock_prices GROUP BY symbol ORDER BY RANDOM() LIMIT 1')
        stock_row = cur.fetchone()
        
        if stock_row:
            symbol = stock_row['symbol']
            
            # Select a random timestamp for that stock
            cur.execute('SELECT date FROM stock_prices WHERE symbol = ? ORDER BY RANDOM() LIMIT 1', (symbol,))
            timestamp_row = cur.fetchone()

            conn.close()
            
            if timestamp_row:
                return jsonify({
                    'symbol': symbol,
                    'timestamp': timestamp_row['date']
                })
            else:
                return jsonify({'error': 'No timestamp found for this stock'}), 404
        else:
            return jsonify({'error': 'No stock found'}), 404
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500


# High Volatility Stocks API
@app.route('/api/stocks/high_volatility', methods=['GET'])
def high_volatility_stocks():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Select a random high volatility period
        cur.execute('SELECT symbol, start_date FROM high_volatility ORDER BY RANDOM() LIMIT 1')
        volatility_row = cur.fetchone()
        
        if volatility_row:
            symbol = volatility_row['symbol']
            start_date = volatility_row['start_date']
            
            # Send the start date instead of row index
            return jsonify({'symbol': symbol, 'timestamp': start_date})
        else:
            return jsonify({'error': 'No high volatility periods found'}), 404
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500


# Double Bottom Stocks API
@app.route('/api/stocks/double_bottoms', methods=['GET'])
def double_bottom_stocks():
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Select a random double bottom pattern
        cur.execute('SELECT symbol, first_bottom_date FROM double_bottoms ORDER BY RANDOM() LIMIT 1')
        double_bottom_row = cur.fetchone()
        
        if double_bottom_row:
            symbol = double_bottom_row['symbol']
            first_bottom_date = double_bottom_row['first_bottom_date']
            
            # Send the date instead of row index
            return jsonify({'symbol': symbol, 'timestamp': first_bottom_date})
        else:
            return jsonify({'error': 'No double bottom patterns found'}), 404
    except Exception as e:
        print(f"Error occurred: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500


#Hammer API 
@app.route('/api/stocks/hammer', methods=['GET'])
def hammer_stocks():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        #Select Hammer pattern
        cur.execute('SELECT symbol, start_date FROM hammer ORDER BY RANDOM() LIMIT 1')
        hammer_row = cur.fetchone()
        
        if hammer_row:
            symbol = hammer_row['symbol']
            hammer_date = hammer_row['start_date']
            
            return jsonify({'symbol': symbol, 'timestamp': hammer_date})
        else:
            return jsonify({'error': 'No hammer patterns found'}), 404
    except Exception as e:
        print(f"Error occured: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500
        
        
#Green day API 
@app.route('/api/stocks/green', methods=['GET'])
def green_stocks():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
            
            #Select green pattern
        cur.execute('SELECT symbol, start_date FROM green ORDER BY RANDOM() LIMIT 1')
        green_row = cur.fetchone()
            
        if green_row:
            symbol = green_row['symbol']
            green_date = green_row['start_date']
                
            return jsonify({'symbol': symbol, 'timestamp': green_date})
        else:
            return jsonify({'error': 'No green patterns found'}), 404
    except Exception as e:
        print(f"Error occured: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500
    
    
#Green_five day API 
@app.route('/api/stocks/green_five', methods=['GET'])
def green_five_stocks():
    try:
        conn = get_five_db()
        cur = conn.cursor()
            
            #Select green pattern
        cur.execute('SELECT symbol, start_date FROM green ORDER BY RANDOM() LIMIT 1')
        green_row = cur.fetchone()
            
        if green_row:
            symbol = green_row['symbol']
            green_date = datetime.strptime(green_row['start_date'], '%Y-%m-%d-%H:%M').strftime('%Y-%m-%d')
                
            return jsonify({'symbol': symbol, 'timestamp': green_date})
        else:
            return jsonify({'error': 'No green patterns found'}), 404
    except Exception as e:
        print(f"Error occured: {e}")
        return jsonify({'error': 'Internal Server Error'}), 500

if __name__ == '__main__':
    app.run(debug=True)
