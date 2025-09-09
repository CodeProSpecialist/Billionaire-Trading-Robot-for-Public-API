#!/usr/bin/env python3
import os
import time
import csv
import logging
import threading
from uuid import uuid4
from datetime import datetime, timedelta
import pytz
import requests
import yfinance as yf
import talib
import pandas_market_calendars as mcal

from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# --- Environment & API ---
secret_key = os.getenv('PUBLIC_API_ACCESS_TOKEN')
if not secret_key:
    raise ValueError("PUBLIC_API_ACCESS_TOKEN environment variable is not set.")

HEADERS = {"Authorization": f"Bearer {secret_key}", "Content-Type": "application/json"}
BASE_URL = "https://api.public.com/userapigateway"

# --- Flags ---
FRACTIONAL_BUY_ORDERS = True
ALL_BUY_ORDERS_ARE_1_DOLLAR = False
PRINT_DB_TRADES = True

# --- Timezone ---
eastern = pytz.timezone('US/Eastern')

# --- CSV ---
CSV_FILENAME = 'log-file-of-buy-and-sell-signals.csv'
CSV_FIELDS = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']
with open(CSV_FILENAME, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
    writer.writeheader()

# --- Logging ---
logging.basicConfig(filename='trading-bot.log', level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(message)s')

# --- DB ---
Base = declarative_base()

class TradeHistory(Base):
    __tablename__ = 'trade_history'
    id = Column(Integer, primary_key=True)
    symbols = Column(String)
    action = Column(String)  # buy or sell
    quantity = Column(Float)
    price = Column(Float)
    date = Column(String)

class Position(Base):
    __tablename__ = 'positions'
    symbols = Column(String, primary_key=True)
    quantity = Column(Float)
    avg_price = Column(Float)
    purchase_date = Column(String)

engine = create_engine('sqlite:///trading_bot.db', connect_args={"check_same_thread": False})
with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
SessionLocal = scoped_session(sessionmaker(bind=engine))
Base.metadata.create_all(engine)

SYMBOLS_FILE = "electricity-or-utility-stocks-to-buy-list.txt"

def load_symbols_from_file(file_path=SYMBOLS_FILE):
    symbols = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                s = line.strip().upper()
                if s:
                    symbols.append(s)
        logging.info(f"Loaded {len(symbols)} symbols from {file_path}")
    except Exception as e:
        logging.error(f"Error loading symbols file: {e}")
    return symbols

nyse_cal = mcal.get_calendar('NYSE')

def robot_can_run():
    """
    Determines if the trading robot can run now:
    - Fractional buys only during regular market hours
    - Whole shares allowed in pre-market or post-market
    Returns: (can_run: bool, message: str)
    """
    now = datetime.now(eastern)
    schedule = nyse_cal.schedule(start_date=now.date(), end_date=now.date())
    if schedule.empty:
        return False, "Market closed today"

    market_open = schedule.iloc[0]['market_open'].tz_convert(eastern)
    market_close = schedule.iloc[0]['market_close'].tz_convert(eastern)
    pre_market_open = market_open - timedelta(hours=2)  # 7:00 AM ET approx
    post_market_close = market_close + timedelta(hours=4) # 8:00 PM ET approx

    if market_open <= now <= market_close:
        return True, "Market open - trading allowed for all shares"

    if pre_market_open <= now <= post_market_close:
        return True, "Extended hours - trading allowed for whole shares"
    else:
        return False, f"Outside extended hours ({pre_market_open.strftime('%I:%M %p')} - {post_market_close.strftime('%I:%M %p')})"

# Simplified wrappers; replace with Public API or REST
def client_get_account():
    try:
        resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        a = resp.json()[0]
        return {'equity': float(a.get('equity', 0)), 'cash': float(a.get('cash', 0)), 'raw': a}
    except Exception as e:
        logging.error(f"Account fetch error: {e}")
        return {'equity':0.0, 'cash':0.0, 'raw':{}}

def client_list_positions():
    try:
        resp = requests.get(f"{BASE_URL}/trading/accounts/positions", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        pos_list = resp.json().get('positions', [])
        out = []
        for p in pos_list:
            sym = p.get('symbol')
            qty = float(p.get('quantity', 0))
            avg = float(p.get('avg_price', 0))
            date_str = p.get('purchase_date', datetime.now().strftime("%Y-%m-%d"))
            out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'purchase_date': date_str})
        return out
    except Exception as e:
        logging.error(f"Positions fetch error: {e}")
        return []

def client_get_quote(symbol):
    try:
        df = yf.Ticker(symbol.replace('.', '-')).history(period="1d", interval="1m")
        return float(df['Close'].iloc[-1])
    except Exception as e:
        logging.error(f"Quote fetch error for {symbol}: {e}")
        return None

def buy_stocks(symbols):
    account = client_get_account()
    cash = account['cash']
    positions = client_list_positions()
    today_str = datetime.now(eastern).strftime("%Y-%m-%d")

    for sym in symbols:
        current_price = client_get_quote(sym)
        if current_price is None or cash < 3.0:  # leave $2 buffer
            continue

        # --- Score calculation ---
        score = 0
        df = yf.Ticker(sym.replace('.', '-')).history(period="20d")
        close = df['Close'].values
        open_ = df['Open'].values
        high = df['High'].values
        low = df['Low'].values

        # Candlestick bullish reversal patterns
        bullish_patterns = [
            talib.CDLHAMMER, talib.CDLHANGINGMAN, talib.CDLENGULFING,
            talib.CDLPIERCING, talib.CDLMORNINGSTAR, talib.CDLINVERTEDHAMMER,
            talib.CDLDRAGONFLYDOJI, talib.CDLBULLISHENGULFING
        ]
        for f in bullish_patterns:
            res = f(open_, high, low, close)
            if res[-1] > 0:
                score += 1
                break

        # RSI decrease
        rsi = talib.RSI(close)
        if rsi[-1] < 50:
            score += 1

        # Price decrease 0.3%
        if close[-1] <= close[-2] * 0.997:
            score += 1

        if score < 4:
            continue

        # Determine buy quantity
        qty = round((cash - 2.0)/current_price, 2)
        if qty <= 0:
            continue

        # Place order (placeholder)
        logging.info(f"Buying {qty} of {sym} at {current_price}")
        cash -= qty*current_price

        # Record CSV/DB
        with open(CSV_FILENAME, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writerow({'Date': today_str, 'Buy': 'Buy', 'Sell':'', 'Quantity': qty, 'Symbol': sym, 'Price Per Share': round(current_price,2)})

def sell_stocks():
    account = client_get_account()
    positions = client_list_positions()
    today_str = datetime.now(eastern).strftime("%Y-%m-%d")

    for p in positions:
        # Only sell if bought before today
        if p['purchase_date'] == today_str:
            continue
        cur_price = client_get_quote(p['symbol'])
        if cur_price is None:
            continue

        # Only sell if profit >=0.5%
        if cur_price >= 1.005*p['avg_price']:
            logging.info(f"Selling {p['qty']} of {p['symbol']} at {cur_price}")
            with open(CSV_FILENAME, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writerow({'Date': today_str, 'Buy':'', 'Sell':'Sell', 'Quantity': p['qty'], 'Symbol': p['symbol'], 'Price Per Share': round(cur_price,2)})

def trading_robot(interval=120):
    symbols = load_symbols_from_file()
    if not symbols:
        print("No symbols loaded.")
        return

    while True:
        try:
            can_run, msg = robot_can_run()
            print(msg)
            if not can_run:
                time.sleep(300)
                continue

            # Print account summary
            acc = client_get_account()
            print(f"Equity: ${acc['equity']:.2f} | Cash: ${acc['cash']:.2f}")
            positions = client_list_positions()
            print("Positions:")
            for p in positions:
                cur = client_get_quote(p['symbol']) or 0
                pct = (cur - p['avg_price'])/p['avg_price']*100 if p['avg_price'] else 0
                color = "\033[92m" if pct>0 else "\033[91m"
                reset = "\033[0m"
                print(f"{p['symbol']} | Qty: {p['qty']} | Avg: {p['avg_price']:.2f} | Cur: {cur:.2f} | Change: {color}{pct:.2f}%{reset}")

            # Sell first
            sell_stocks()
            # Then buy
            buy_stocks(symbols)

            time.sleep(interval)
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(120)

if __name__ == "__main__":
    trading_robot(interval=120)
