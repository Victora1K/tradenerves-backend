from functools import wraps
from flask import jsonify
from datetime import datetime
import re

def validate_symbol(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        symbol = kwargs.get('symbol', '').upper()
        if not re.match(r'^[A-Z]{1,5}$', symbol):
            return jsonify({
                'error': 'Invalid symbol format. Symbol should be 1-5 uppercase letters.'
            }), 400
        kwargs['symbol'] = symbol
        return f(*args, **kwargs)
    return decorated_function

def validate_date_range(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            start_date = datetime.strptime(kwargs.get('start_date', ''), '%Y-%m-%d')
            end_date = datetime.strptime(kwargs.get('end_date', ''), '%Y-%m-%d')
            
            if start_date > end_date:
                return jsonify({
                    'error': 'Start date must be before end date.'
                }), 400
                
            if (end_date - start_date).days > 365:
                return jsonify({
                    'error': 'Date range cannot exceed 1 year.'
                }), 400
                
            return f(*args, **kwargs)
        except ValueError:
            return jsonify({
                'error': 'Invalid date format. Use YYYY-MM-DD.'
            }), 400
    return decorated_function
