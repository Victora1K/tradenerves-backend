from flask import Flask, jsonify
from flask_cors import CORS  # Import CORS
import sqlite3
import os
from data.intra_day import get_intra_day
from datetime import datetime, timezone

app = Flask(__name__)
CORS(app)  # Enable CORS for the entire Flask app

# Path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'db/stocks.db'))

def get_db_connection():
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

# Fetch stock prices from a specific symbol and timestamp onwards
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

if __name__ == '__main__':
    app.run(debug=True)
