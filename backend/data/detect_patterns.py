import os
import sqlite3
import numpy as np

# Path to the database
db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../db/stocks.db'))

       

#collects prices list from database
def calculate_volatility(prices, period=14):
    #np.diff subtracts index value i from i+1 and returns a list i.e. np.diff[ 2 3 5] returns [1 2] & prices[:-1] returns all
    #except the last value of prices list or array
    daily_returns = np.diff(prices) / prices[:-1]
    volatility = np.std(daily_returns[-period:])
    return volatility

def is_valid_low(prices):
    low_price = []
    low_index = []
    valid_lows = []
    for i in range(2,len(prices) - 2):
                # Detect a potential bottom: price is lower than the previous and next day
        if prices[i-1] < prices[i-2] and prices[i] <= prices[i-1] and prices[i] < prices[i+1] and prices[i+1] <= prices[i+2]:
            low_price.append(prices[i])
            low_index.append(i)
            
    valid_lows.append((low_index))  
    #print(f"{valid_lows} are lows fed to valid lows")      
    #print(f"{low_index} are lows fed to low index")
    return low_index
    
    
#hammer detection function uses valid lows to see if they also meet a wick structure pattern
def detect_hammer(prices, prices_close, prices_high, prices_open, dates, symbol):
    valid_hammers = []
    check_for_lows = is_valid_low(prices)
    #print(f"{check_for_lows} are lows fed to hammer for symbol {symbol}")
    #low_prices = check_for_lows[0]
    #low_indices = check_for_lows[1]
    if check_for_lows:
        low_check_prices = check_for_lows[0]
        for i in range(1,len(check_for_lows)):
            open = prices_open[check_for_lows[i]]
            close = prices_close[check_for_lows[i]]
            high = prices_high[check_for_lows[i]]
            low = prices[check_for_lows[i]]
            date = dates[check_for_lows[i]]
            body = abs(close - open)
            wick = abs(high - low)
            if (wick > 1.25 * body and (open == high or close == high)):
                valid_hammers.append((symbol, date))
            else:
                print(f"{prices[i]} is a low but not a hammer. ")
    else: 
        valid_hammers = []  #Because there's no lows.
        # Return the list of double bottom pairs (if any)
    if len(valid_hammers) > 0:
        #print(f"Valid hammers found: {valid_hammers}")
        return valid_hammers
    else:
        print(f"No Valid hammers found.")
        return []    
    
    
    

# Updated double bottom detection to handle multiple bottoms and return the symbol
def detect_double_bottoms(prices, dates, symbol, tolerance=0.0000125):
    double_bottoms = []  # To store multiple pairs of double bottoms (tuples of symbol, start and end dates)
    current_bottom = None  # Track the current lowest bottom
    bottom_index = []
    #bottom_price = []
    
    check_for_lows = is_valid_low(prices)
    #print(f"{check_for_lows} are lows fed to double_bottom for symbol {symbol}")
    
    

        # Detect a potential bottom: price is lower than the previous and next day
    if check_for_lows:
            low_check_prices = check_for_lows
            print(f"{low_check_prices} are lows zero index fed to double_bottom for symbol {symbol}")
            for bottom in range(2,len(low_check_prices)-2):
                for second_bottom in range(bottom + 1, len(low_check_prices) -2):
                    #price_difference = abs(low_check_prices[bottom] - low_check_prices[second_bottom]) / low_check_prices[bottom]
                    price_difference = abs(prices[low_check_prices[bottom]] - prices[low_check_prices[second_bottom]]) / prices[low_check_prices[bottom]]
                    
                    if price_difference <= tolerance and second_bottom - bottom >= 5:
                        #print(f"{low_check_prices[bottom]} and {low_check_prices[second_bottom]} are lows. ")
                        if low_check_prices[bottom] not in double_bottoms :
                            #print(f"Price difference: {price_difference}, Tolerance: {tolerance}")                  
                            #print(f"Double bottom confirmed between indices {bottom} and {second_bottom}")
                            #print(f"Double bottom match between prices {bottom_price[bottom]} and {bottom_price[second_bottom]}")
                            #print(f"Double bottom confirmed between indices {bottom} and {second_bottom} already in list")
                            double_bottoms.append((symbol, dates[bottom], dates[second_bottom]))
                        else:
                            print(f"Bottom at index {bottom} does not meet criteria (tolerance or distance).")
                            # Re-assign this as the new potential first bottom
    else:
        low_check_prices = None
                                                
    
    # Return the list of double bottom pairs (if any)
    if len(double_bottoms) > 0:
        #print(f"Double bottoms found: {double_bottoms}")
        return double_bottoms
    else:
        #print(f"No double bottoms found.")
        return []
    
def detect_green(prices, prices_close, prices_high, prices_open, dates, symbol):
    valid_greens = []
    print(f"All prices {len(prices)}")
    for candle in range(len(prices)-1):
        #print(f"{prices[candle]} occured at date: {dates[candle]}")
        if prices_close[candle] - prices_open[candle] > 4 and prices_high[candle] - prices_close[candle] < 0.28:
            valid_greens.append((symbol , dates[candle]))
            #print(f"Appending {dates[candle]} and {symbol} ticker to valid greens. ")
            #print(f"Found a green day at date: {dates[candle]}")
    if len(valid_greens) > 0:
        #print(f"Valid green days found: {valid_greens}")
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


    # Detect high volatility periods
    for i in range(14, len(prices), 3):
        lows = prices[i-14:i]  # Using low prices for volatility calculation
        volatility = calculate_volatility(lows)
        if volatility > 0.02:  # Threshold for high volatility
            cur.execute('''
                INSERT INTO high_volatility (symbol, start_date, end_date, volatility)
                VALUES (?, ?, ?, ?)
            ''', (symbol, dates[i-14], dates[i], volatility))

    # Detect double bottoms
    double_bottoms = detect_double_bottoms(prices, dates, symbol, tolerance=0.0028)
    if double_bottoms:
        for (symbol, first_bottom_date, second_bottom_date) in double_bottoms:
            cur.execute('''
                INSERT INTO double_bottoms (symbol, first_bottom_date, second_bottom_date)
                VALUES (?, ?, ?)
            ''', (symbol, first_bottom_date, second_bottom_date))
            
    #Detect hammer pattern and occurences
    hammer_results = detect_hammer(prices, prices_close, prices_high, prices_open, dates, symbol)
    if hammer_results:
        for (symbol, start_date) in hammer_results:
            cur.execute('''
                    INSERT INTO hammer (symbol, start_date)
                    VALUES (?,?)
                    ''',(symbol, start_date))
    green_days = detect_green(prices, prices_close, prices_high, prices_open, dates, symbol)
    if green_days:
        for (symbol, start_date) in green_days:
            cur.execute('''
                        INSERT INTO green (symbol, start_date)
                        VALUES (?,?)
                        ''',(symbol, start_date))
            

    conn.commit()
    conn.close()

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

for symbol in symbols:
    detect_and_store_patterns(symbol)

print("Patterns detected and stored.")
