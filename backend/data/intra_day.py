import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
#from dotenv import load_dotenv

# Load environment variables
#load_dotenv()
API_KEY = os.getenv('POLYGON_API_KEY', '4ku7YB5AIpIL_IRvfIiI4xZV09EoLGD6')

class PolygonAPIError(Exception):
    """Custom exception for Polygon API errors"""
    pass

def get_next_day(day: str) -> str:
    """
    Calculate the next day using datetime.
    
    Args:
        day (str): Date string in YYYY-MM-DD format
        
    Returns:
        str: Next day in YYYY-MM-DD format
    """
    try:
        current_date = datetime.strptime(day, "%Y-%m-%d")
        next_date = current_date + timedelta(days=1)
        return next_date.strftime("%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Please use YYYY-MM-DD format: {e}")

def get_intra_day(symbol: str, day: str) -> List[Dict]:
    """
    Get 5-minute intraday data for a given symbol and day.
    
    Args:
        symbol (str): Stock ticker symbol
        day (str): Date in YYYY-MM-DD format
        
    Returns:
        List[Dict]: List of intraday data points
        
    Raises:
        PolygonAPIError: If API request fails
        ValueError: If input parameters are invalid
    """
    # Input validation
    if not symbol or not isinstance(symbol, str):
        raise ValueError("Symbol must be a non-empty string")
    
    try:
        next_day = get_next_day(day)
    except ValueError as e:
        raise ValueError(f"Invalid date: {e}")

    # Construct API URL
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/5/minute/"
        f"{day}/{next_day}?adjusted=true&sort=asc&apiKey={API_KEY}"
    )
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception for bad status codes
        
        data = response.json()
        if 'error' in data:
            raise PolygonAPIError(f"API Error: {data['error']}")
            
        return data.get('results', [])
        
    except requests.RequestException as e:
        raise PolygonAPIError(f"Failed to fetch data from Polygon API: {e}")

if __name__ == "__main__":
    # Example usage
    try:
        symbol = input("Enter stock symbol: ").upper()
        day = input("Enter date (YYYY-MM-DD): ")
        data = get_intra_day(symbol, day)
        print(f"Retrieved {len(data)} data points for {symbol} on {day}")
    except (ValueError, PolygonAPIError) as e:
        print(f"Error: {e}")