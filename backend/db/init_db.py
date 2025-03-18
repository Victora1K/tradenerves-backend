# File: /backend/db/init_db.py

import os
import sqlite3

# Ensure that the database path points to the right directory
db_path = os.path.abspath(r'C:\Users\Victo\OneDrive\Desktop\Codebase\Tradenerves\tradenerves-backend\backend\db\stocks.db')

# Connect to the database using the absolute path
conn = sqlite3.connect(db_path)

# Create cursor
cur = conn.cursor()

# Create tables
cur.execute('''
CREATE TABLE IF NOT EXISTS stock_prices (
    id INTEGER PRIMARY KEY,
    symbol TEXT,
    date TEXT,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume INTEGER
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS hammer (
    id INTEGER PRIMARY KEY,
    symbol TEXT,
    start_date
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS green (
    id INTEGER PRIMARY KEY,
    symbol TEXT,
    start_date
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS high_volatility (
    id INTEGER PRIMARY KEY,
    symbol TEXT,
    start_date TEXT,
    end_date TEXT,
    volatility REAL
)
''')

cur.execute('''
CREATE TABLE IF NOT EXISTS double_bottoms (
    id INTEGER PRIMARY KEY,
    symbol TEXT,
    first_bottom_date TEXT,  -- Fixed column names
    second_bottom_date TEXT
)
''')

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Database and tables initialized.")
