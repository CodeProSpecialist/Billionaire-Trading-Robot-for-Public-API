#!/usr/bin/env python3
"""
Electricity/Utility Stocks Day Trading Bot
- Public API for trading
- yfinance for historical candles & indicators
- Talib for technical analysis & candlestick patterns
- SQLAlchemy for positions & trade history
- pandas-market-calendars for market hours
- Fractional buy support, scoring system, minimum 0.5% profit sell
- Rate-limited yfinance API calls
"""

import os
import time
import csv
import logging
import threading
from uuid import uuid4
from datetime import datetime, timedelta
from functools import wraps

import pytz
import requests
import numpy as np
import yfinance as yf
import talib
import pandas_market_calendars as mcal

from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# -------------------------
# Configuration & Environment
# -------------------------
secret_key = os.getenv('PUBLIC_API_ACCESS_TOKEN')
if not secret_key:
    raise ValueError("PUBLIC_API_ACCESS_TOKEN environment variable is not set.")

# REST headers fallback
HEADERS = {"Authorization": f"Bearer {secret_key}", "Content-Type": "application/json"}
BASE_URL = "https://api.public.com/userapigateway"

# Flags
FRACTIONAL_BUY_ORDERS = True
PRINT_ROBOT_DB = True
MIN_CASH_BUFFER = 2.0  # leave $2.00 in account after buy
MIN_PROFIT_SELL = 0.005  # +0.5% profit
BUY_SCORE_THRESHOLD = 4  # min points to buy

# Timezone
eastern = pytz.timezone('US/Eastern')

# CSV trade log
CSV_FILENAME = 'log-file-of-buy-and-sell-signals.csv'
CSV_FIELDS = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']

# Initialize CSV file
with open(CSV_FILENAME, mode='w', newline='') as csv_f:
    writer = csv.DictWriter(csv_f, fieldnames=CSV_FIELDS)
    writer.writeheader()

# Logging
LOGFILE = 'trading-bot-program-logging-messages.txt'
logging.basicConfig(filename=LOGFILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

# -------------------------
# SQLAlchemy setup (WAL + scoped session)
# -------------------------
Base = declarative_base()

class TradeHistory(Base):
    __tablename__ = 'trade_history'
    id = Column(Integer, primary_key=True)
    symbols = Column(String, index=True)
    action = Column(String)  # 'buy' or 'sell'
    quantity = Column(Float)
    price = Column(Float)
    date = Column(String)

class Position(Base):
    __tablename__ = 'positions'
    symbols = Column(String, primary_key=True)
    quantity = Column(Float)
    avg_price = Column(Float)
    purchase_date = Column(String)

engine = create_engine('sqlite:///trading_bot.db',
                       connect_args={"check_same_thread": False},
                       pool_pre_ping=True,
                       future=True)
with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
    conn.execute(text("PRAGMA synchronous=NORMAL;"))

Base.metadata.create_all(engine)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False))

def get_db_session():
    return SessionLocal()

# -------------------------
# Rate limiting decorator for yfinance
# -------------------------
def rate_limit(seconds):
    def decorator(func):
        last_called = {}

        @wraps(func)
        def wrapper(symbol, *args, **kwargs):
            now = time.time()
            last = last_called.get(symbol, 0)
            if now - last < seconds:
                time.sleep(seconds - (now - last))
            result = func(symbol, *args, **kwargs)
            last_called[symbol] = time.time()
            return result
        return wrapper
    return decorator

# -------------------------
# yfinance helpers
# -------------------------
@rate_limit(2)  # 2 seconds minimum between calls per symbol
def get_yf_history(symbol, period='5d', interval='5m'):
    try:
        symbol_fixed = symbol.replace('.', '-')
        df = yf.Ticker(symbol_fixed).history(period=period, interval=interval, prepost=True)
        if df is None or df.empty:
            return None
        return df
    except Exception as e:
        logging.error(f"yfinance error for {symbol}: {e}")
        return None

@rate_limit(2)
def get_previous_close(symbol):
    df = get_yf_history(symbol, period='3d', interval='1d')
    if df is None or len(df) < 2:
        return None
    return float(df['Close'].iloc[-2])

# -------------------------
# Market hours (NYSE) using pandas-market-calendars
# -------------------------
nyse_cal = mcal.get_calendar('NYSE')

def market_is_open():
    now = datetime.now(eastern)
    schedule = nyse_cal.schedule(start_date=now.date(), end_date=now.date())
    if schedule.empty:
        print("Market closed today (weekend/holiday).")
        return False
    market_open = schedule.iloc[0]['market_open'].tz_convert(eastern)
    market_close = schedule.iloc[0]['market_close'].tz_convert(eastern)
    if market_open <= now <= market_close:
        return True
    print(f"Market closed — regular hours: {market_open.strftime('%I:%M %p')} to {market_close.strftime('%I:%M %p')} ET.")
    return False

# -------------------------
# Public API wrappers
# -------------------------
def get_account_info():
    try:
        resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        accounts = resp.json()
        a = accounts[0] if isinstance(accounts, list) and accounts else accounts
        return {
            'equity': float(a.get('equity', 0)),
            'cash': float(a.get('cash', 0)),
            'day_trade_count': a.get('day_trade_count', 'N/A'),
            'raw': a
        }
    except Exception as e:
        logging.error(f"Error fetching account info: {e}")
        return {'equity': 0.0, 'cash': 0.0, 'day_trade_count': 'N/A', 'raw': {}}

def get_positions():
    out = []
    try:
        resp = requests.get(f"{BASE_URL}/trading/accounts/positions", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        positions = resp.json()
        for p in positions:
            sym = p.get('symbol')
            qty = float(p.get('quantity', 0))
            avg = float(p.get('avg_price', 0))
            created_at = p.get('purchase_date') or p.get('created_at')
            buy_date = datetime.now().date()
            if created_at:
                try:
                    buy_date = datetime.fromisoformat(created_at).date()
                except Exception:
                    pass
            out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'buy_date': buy_date})
    except Exception as e:
        logging.error(f"Error fetching positions: {e}")
    return out

def get_quote(symbol):
    try:
        resp = requests.post(f"{BASE_URL}/marketdata/quotes", headers=HEADERS,
                             json={"instruments":[{"symbol":symbol,"type":"EQUITY"}]}, timeout=10)
        resp.raise_for_status()
        quotes = resp.json().get('quotes', [])
        if quotes:
            return float(quotes[0].get('last') or quotes[0].get('close') or 0)
    except Exception as e:
        logging.error(f"Error fetching quote for {symbol}: {e}")
    return None

def place_market_order(symbol, side, qty):
    try:
        qty_str = f"{qty:.2f}"  # limit to 2 decimal places
        body = {
            "orderId": str(uuid4()),
            "instrument": {"symbol": symbol, "type": "EQUITY"},
            "orderSide": side.upper(),
            "orderType": "MARKET",
            "expiration": {"timeInForce": "DAY"},
            "quantity": qty_str,
            "amount": "0",
            "limitPrice": "0",
            "stopPrice": "0",
            "openCloseIndicator": "OPEN"
        }
        resp = requests.post(f"{BASE_URL}/trading/order", headers=HEADERS, json=body, timeout=10)
        if resp.status_code in (200,201):
            return resp.json()
        logging.error(f"Order error ({side} {symbol}): {resp.status_code} {resp.text}")
    except Exception as e:
        logging.error(f"Exception placing order: {e}")
    return None

# -------------------------
# CSV & DB logging
# -------------------------
def log_trade(date_str, buy, sell, qty, symbol, price):
    with open(CSV_FILENAME, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow({'Date': date_str, 'Buy': buy, 'Sell': sell, 'Quantity': qty, 'Symbol': symbol, 'Price Per Share': price})

def record_trade_db(action, symbol, qty, price, date_str):
    session = get_db_session()
    try:
        th = TradeHistory(symbols=symbol, action=action, quantity=qty, price=price, date=date_str)
        if action == 'buy':
            pos = Position(symbols=symbol, quantity=qty, avg_price=price, purchase_date=date_str)
            session.add(pos)
        elif action == 'sell':
            session.query(Position).filter_by(symbols=symbol).delete()
        session.add(th)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"DB error in record_trade_db: {e}")
        session.rollback()
    finally:
        session.close()

# -------------------------
# Price tracking dictionaries
# -------------------------
stock_data = {}
previous_prices = {}
price_changes = {}
price_history = {}  # symbol -> interval -> list of prices
last_stored = {}    # symbol -> interval -> last_timestamp
interval_map = {
    '1min': 60,
    '5min': 300,
    '10min': 600,
    '15min': 900,
    '30min': 1800,
    '45min': 2700,
    '60min': 3600
}

# -------------------------
# Load symbols from text file
# -------------------------
SYMBOLS_FILE = "electricity-or-utility-stocks-to-buy-list.txt"

def load_symbols():
    symbols = []
    try:
        with open(SYMBOLS_FILE, 'r') as f:
            for line in f:
                s = line.strip().upper()
                if s:
                    symbols.append(s)
        logging.info(f"Loaded {len(symbols)} symbols from {SYMBOLS_FILE}")
    except FileNotFoundError:
        logging.error(f"Symbols file not found: {SYMBOLS_FILE}")
    return symbols

