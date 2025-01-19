import os
import sqlite3
import numpy as np

# Path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../db/stocks_five.db'))

#Function to detect green candles that were above one point on spy
def detect_green(prices, prices_close, prices_high, prices_open, dates, symbol):
    valid_greens = []
    print(f"All prices {len(prices)}")
    for candle in range(len(prices)-1):
        #print(f"{prices[candle]} occured at date: {dates[candle]}")
        if prices_close[candle] - prices_open[candle] > 0.8: #and prices_high[candle] - prices_close[candle] < 0.28:
            valid_greens.append((symbol , dates[candle]))
            #print(f"Appending {dates[candle]} and {symbol} ticker to valid greens. ")
            #print(f"Found a green day at date: {dates[candle]}")
    if len(valid_greens) > 0:
        print(f"Valid green days found: {valid_greens}")
        print(len(valid_greens))
    return valid_greens


def detect_and_store_patterns(symbol):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Fetch low prices and dates for the symbol
    cur.execute(f"SELECT low, date, close, high, open FROM stock_prices WHERE symbol = ? ORDER BY date ASC", (symbol,))
    rows = cur.fetchall()

    prices = [row[0] for row in rows]  # Using low prices    
    dates = [row[1] for row in rows]
    prices_close = [row[2] for row in rows]
    prices_high = [row[3] for row in rows]
    prices_open = [row[4] for row in rows]

    green_days = detect_green(prices, prices_close, prices_high, prices_open, dates, symbol)
    if green_days:
        for (symbol, start_date) in green_days:
            cur.execute('''
                        INSERT INTO green (symbol, start_date)
                        VALUES (?,?)
                        ''',(symbol, start_date))
            

    conn.commit()
    conn.close()

# Example usage for multiple symbols
symbols = [
    'SPY' # Technology
]

for symbol in symbols:
    detect_and_store_patterns(symbol)

print("Patterns detected and stored.")