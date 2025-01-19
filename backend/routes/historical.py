from flask import Blueprint, jsonify, request
from datetime import datetime
from functools import wraps
import yfinance as yf
from ..database import get_db
from ..middleware.validation import validate_date_range, validate_symbol
from ..middleware.error_handler import handle_errors

historical_bp = Blueprint('historical', __name__)

def fetch_stock_data(symbol, start_date, end_date):
    """Fetch stock data from yfinance"""
    stock = yf.Ticker(symbol)
    df = stock.history(start=start_date, end=end_date)
    return df

def format_stock_data(df):
    """Format stock data for API response"""
    return [{
        'date': index.strftime('%Y-%m-%d'),
        'open': float(row['Open']),
        'high': float(row['High']),
        'low': float(row['Low']),
        'close': float(row['Close']),
        'volume': int(row['Volume'])
    } for index, row in df.iterrows()]

@historical_bp.route('/api/historical/<symbol>/<start_date>/<end_date>')
@validate_symbol
@validate_date_range
@handle_errors
def get_historical_data(symbol, start_date, end_date):
    """Get historical stock data"""
    try:
        df = fetch_stock_data(symbol, start_date, end_date)
        if df.empty:
            return jsonify({'error': 'No data found for the specified date range'}), 404

        formatted_data = format_stock_data(df)

        # Log the query in SQLite
        db = get_db()
        db.execute(
            'INSERT INTO historical_queries (symbol, start_date, end_date) VALUES (?, ?, ?)',
            (symbol, start_date, end_date)
        )
        db.commit()

        return jsonify(formatted_data)

    except Exception as e:
        return jsonify({'error': str(e)}), 500
