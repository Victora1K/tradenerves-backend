from functools import wraps
from flask import jsonify
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def handle_errors(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            # Log the error with stack trace
            logger.exception("An error occurred while processing the request")
            
            # Return a user-friendly error message
            return jsonify({
                'error': 'An unexpected error occurred',
                'message': str(e) if not isinstance(e, Exception) else 'Internal server error'
            }), 500
    return decorated_function
