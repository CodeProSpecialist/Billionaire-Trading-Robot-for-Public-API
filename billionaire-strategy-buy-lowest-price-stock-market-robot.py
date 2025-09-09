#!/usr/bin/env python3
"""
Professional Trading Bot:
- Fractional buys, scoring system, price history
- Buys based on bullish reversal, RSI decrease, volume decrease, price decline
- Sells previous stocks with >=0.5% profit
- Rate-limited yfinance calls
- Tracks prices, previous prices, and price history
- Color-coded P/L, account summary
"""

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
import numpy as np
import pandas_market_calendars as mcal

from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from ratelimit import limits, sleep_and_retry

# -------------------------
# Config & Environment
# -------------------------
PUBLIC_API_TOKEN = os.getenv('PUBLIC_API_ACCESS_TOKEN')
if not PUBLIC_API_TOKEN:
    raise ValueError("PUBLIC_API_ACCESS_TOKEN environment variable not set")

HEADERS = {"Authorization": f"Bearer {PUBLIC_API_TOKEN}", "Content-Type": "application/json"}
BASE_URL = "https://api.public.com/userapigateway"
SYMBOLS_FILE = "electricity-or-utility-stocks-to-buy-list.txt"

FRACTIONAL_BUY_ORDERS = True
MIN_BUY_SCORE = 4
MIN_SELL_PROFIT = 0.005
MIN_CASH_RESERVE = 2.0

CSV_FILENAME = 'log-file-of-buy-and-sell-signals.csv'
CSV_FIELDS = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']

LOGFILE = 'trading-bot-program-logging-messages.txt'
logging.basicConfig(filename=LOGFILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

eastern = pytz.timezone('US/Eastern')

# Initialize CSV
if not os.path.exists(CSV_FILENAME):
    with open(CSV_FILENAME, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()

# -------------------------
# DB Setup: SQLAlchemy
# -------------------------
Base = declarative_base()

class TradeHistory(Base):
    __tablename__ = 'trade_history'
    id = Column(Integer, primary_key=True)
    symbols = Column(String, index=True)
    action = Column(String)
    quantity = Column(Float)
    price = Column(Float)
    date = Column(String)

class Position(Base):
    __tablename__ = 'positions'
    symbols = Column(String, primary_key=True)
    quantity = Column(Float)
    avg_price = Column(Float)
    purchase_date = Column(String)

engine = create_engine('sqlite:///trading_bot.db', connect_args={"check_same_thread": False}, future=True)
with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
    conn.execute(text("PRAGMA synchronous=NORMAL;"))

Base.metadata.create_all(engine)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False))

def get_db_session():
    return SessionLocal()

# -------------------------
# Public API client wrappers
# -------------------------
_account_id_cache = None

def get_account_id():
    """Return account ID from cache, client, or REST."""
    global _account_id_cache
    if _account_id_cache:
        return _account_id_cache
    try:
        resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        arr = resp.json()
        if isinstance(arr, list) and arr:
            _account_id_cache = arr[0].get('id')
            return _account_id_cache
    except Exception as e:
        logging.error(f"Error getting account id: {e}")
    return None

def client_get_account():
    """Return dict: {'equity': float, 'cash': float, 'day_trade_count': int (optional), 'raw': dict}"""
    try:
        resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        arr = resp.json()
        a = arr[0] if isinstance(arr, list) and len(arr) > 0 else arr
        return {
            'equity': float(a.get('equity', 0)),
            'cash': float(a.get('cash', 0)),
            'day_trade_count': a.get('day_trade_count', 'N/A'),
            'raw': a
        }
    except Exception as e:
        logging.error(f"Error fetching account: {e}")
        return {'equity': 0.0, 'cash': 0.0, 'day_trade_count': 'N/A', 'raw': {}}

def client_list_positions():
    """Return list of positions with symbol, qty, avg_price, purchase_date"""
    out = []
    try:
        resp = requests.get(f"{BASE_URL}/trading/accounts/positions", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        positions = resp.json() or []
        for p in positions:
            sym = p.get('symbol')
            qty = float(p.get('quantity', 0))
            avg = float(p.get('avg_price', 0))
            purchase_date = p.get('purchase_date', datetime.now().strftime("%Y-%m-%d"))
            out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'purchase_date': purchase_date})
    except Exception as e:
        logging.error(f"Error fetching positions: {e}")
    return out

def client_get_quote(symbol):
    """Return last price float or None"""
    try:
        url = f"{BASE_URL}/marketdata/quotes"
        body = {"instruments":[{"symbol":symbol,"type":"EQUITY"}]}
        resp = requests.post(url, headers=HEADERS, json=body, timeout=10)
        if resp.status_code == 200:
            quotes = resp.json().get('quotes', [])
            if quotes:
                p = quotes[0].get('last') or quotes[0].get('close')
                return float(p) if p is not None else None
    except Exception as e:
        logging.error(f"Error fetching quote for {symbol}: {e}")
    return None

def client_place_market_order(symbol, side, quantity):
    """
    Place market order (supports fractional quantity)
    side: 'BUY' or 'SELL'
    quantity: float with 2 decimals
    """
    try:
        acct = get_account_id()
        if not acct:
            logging.error("No account ID for order placement")
            return None
        body = {
            "orderId": str(uuid4()),
            "instrument": {"symbol": symbol, "type": "EQUITY"},
            "orderSide": side.upper(),
            "orderType": "MARKET",
            "expiration": {"timeInForce": "DAY"},
            "quantity": f"{quantity:.2f}",
            "amount": "0",
            "limitPrice": "0",
            "stopPrice": "0",
            "openCloseIndicator": "OPEN"
        }
        resp = requests.post(f"{BASE_URL}/trading/{acct}/order", headers=HEADERS, json=body, timeout=15)
        if resp.status_code in (200, 201):
            return resp.json()
        else:
            logging.error(f"Order error ({side} {symbol}) status={resp.status_code} body={resp.text}")
            return None
    except Exception as e:
        logging.error(f"Exception placing order: {e}")
        return None

# -------------------------
# Rate limiting for yfinance
# -------------------------
from ratelimit import limits, sleep_and_retry

# Limit: max 60 calls per minute
CALLS = 60
SECONDS = 60

@sleep_and_retry
@limits(calls=CALLS, period=SECONDS)
def yf_download(*args, **kwargs):
    return yf.download(*args, **kwargs, progress=False)

def yf_symbol(sym):
    """Convert dot to dash for yfinance"""
    return sym.replace('.', '-')

def calculate_technical_indicators(symbol, lookback_days=5, interval='5m'):
    """Return OHLCV DataFrame for indicators"""
    symbol_conv = yf_symbol(symbol)
    try:
        df = yf.Ticker(symbol_conv).history(period=f'{lookback_days}d', interval=interval, prepost=True)
        if df is None or df.empty:
            return None
        return df
    except Exception as e:
        logging.error(f"yfinance error for {symbol}: {e}")
        return None

def get_daily_rsi(symbol):
    """Return latest daily RSI"""
    symbol_conv = yf_symbol(symbol)
    try:
        df = yf.Ticker(symbol_conv).history(period='60d', interval='1d')
        if df is None or len(df) < 14:
            return None
        rsi = talib.RSI(df['Close'].values, timeperiod=14)
        return float(rsi[-1])
    except Exception as e:
        logging.error(f"RSI error for {symbol}: {e}")
        return None

def get_previous_close(symbol):
    """Return previous close price"""
    symbol_conv = yf_symbol(symbol)
    try:
        df = yf.Ticker(symbol_conv).history(period='3d', interval='1d')
        if df is None or len(df) < 2:
            return None
        return float(df['Close'].iloc[-2])
    except Exception as e:
        logging.error(f"Previous close error for {symbol}: {e}")
        return None

def detect_bullish_reversal_candles(hist):
    """
    Returns True if any bullish reversal pattern detected.
    Recognizes: Hammer, Inverted Hammer, Bullish Engulfing, Piercing Line, Morning Star, Tweezer Bottom
    """
    patterns = {
        'CDLHAMMER': talib.CDLHAMMER(hist['Open'], hist['High'], hist['Low'], hist['Close']),
        'CDLINVERTEDHAMMER': talib.CDLINVERTEDHAMMER(hist['Open'], hist['High'], hist['Low'], hist['Close']),
        'CDLENGULFING': talib.CDLENGULFING(hist['Open'], hist['High'], hist['Low'], hist['Close']),
        'CDLMORNINGSTAR': talib.CDLMORNINGSTAR(hist['Open'], hist['High'], hist['Low'], hist['Close'], penetration=0.3),
        'CDLTWEEZERBOTTOM': talib.CDLTWEZERBOTTOM(hist['Open'], hist['High'], hist['Low'], hist['Close'])
    }
    for name, vals in patterns.items():
        if vals[-1] > 0:
            return True, name
    return False, None

def calculate_buy_score(symbol, hist, current_price):
    """
    Buy score rules:
    - Bullish reversal candlestick = 1
    - Decrease in RSI = 1
    - Decrease in volume = 1
    - Price decline >= 0.3% = 1
    Only buy if score >= 4
    """
    score = 0
    # Candlestick pattern
    bullish, pattern_name = detect_bullish_reversal_candles(hist)
    if bullish:
        score += 1

    # RSI decrease
    daily_rsi = get_daily_rsi(symbol)
    if daily_rsi is not None and daily_rsi < 50:
        score += 1

    # Volume decrease over last 5 vs previous 5 candles
    if len(hist) >= 10:
        recent_vol = hist['Volume'].iloc[-5:].mean()
        prior_vol = hist['Volume'].iloc[-10:-5].mean()
        if recent_vol < prior_vol:
            score += 1

    # Price decline >= 0.3%
    prev_close = get_previous_close(symbol)
    if prev_close and (current_price <= prev_close * 0.997):
        score += 1

    return score

# -------------------------
# Trading helpers
# -------------------------
FRACTIONAL_BUY_ORDERS = True
MIN_CASH_RESERVE = 2.0  # Leave $2.00 in account
SELL_PROFIT_THRESHOLD = 0.005  # +0.5%
PRICE_DECIMALS = 2
QTY_DECIMALS = 2

def can_buy(symbol, current_price, account_cash):
    """Check if enough cash to buy at least $1 and leave reserve"""
    if account_cash - MIN_CASH_RESERVE < 1.0:
        return False
    if current_price < 0.01:  # prevent tiny-priced symbols
        return False
    return True

def place_buy_order(symbol, qty, current_price):
    """Place fractional market buy order"""
    qty = round(qty, QTY_DECIMALS)
    price = round(current_price, PRICE_DECIMALS)
    resp = client_place_market_order(symbol, 'BUY', qty)
    if resp:
        now_str = datetime.now(eastern).strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
        log_trade_csv(now_str, 'Buy', '', qty, symbol, price)
        record_db_trade('buy', symbol, qty, price, datetime.now(eastern).date().strftime("%Y-%m-%d"))
        print(f"\033[92mBought {qty} {symbol} at ${price}\033[0m")  # green text
    else:
        print(f"Buy order failed for {symbol}")
    return resp

def place_sell_order(symbol, qty, current_price):
    """Place market sell order"""
    qty = round(qty, QTY_DECIMALS)
    price = round(current_price, PRICE_DECIMALS)
    resp = client_place_market_order(symbol, 'SELL', qty)
    if resp:
        now_str = datetime.now(eastern).strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
        log_trade_csv(now_str, '', 'Sell', qty, symbol, price)
        record_db_trade('sell', symbol, qty, price, datetime.now(eastern).date().strftime("%Y-%m-%d"))
        print(f"\033[91mSold {qty} {symbol} at ${price}\033[0m")  # red text
    else:
        print(f"Sell order failed for {symbol}")
    return resp

# -------------------------
# Main trading robot
# -------------------------
def trading_robot(interval=60):
    symbols_to_buy = load_symbols_from_file()
    if not symbols_to_buy:
        print(f"No symbols loaded from {SYMBOLS_FILE}. Exiting.")
        return

    account_id = get_account_id()
    if not account_id:
        logging.warning("No account id detected; some REST calls may fail.")

    while True:
        try:
            now = datetime.now(eastern)
            if market_is_open():
                # Refresh account and positions
                acc = client_get_account()
                cash_avail = float(acc.get('cash', 0))
                positions = client_list_positions()

                # Print summary
                print(f"\n=== Account @ {now.strftime('%Y-%m-%d %I:%M:%S %p %Z')} ===")
                print(f"Equity: ${acc.get('equity', 0):.2f} | Cash: ${cash_avail:.2f}")
                print("Held positions:")
                for p in positions:
                    cur = client_get_quote(p['symbol']) or 0.0
                    pct = ((cur - p['avg_price']) / p['avg_price']) * 100 if p['avg_price'] else 0
                    color = "\033[92m" if pct > 0 else "\033[91m"
                    print(f"{p['symbol']} | Qty: {p['qty']:.2f} | Avg: {p['avg_price']:.2f} | "
                          f"Current: {cur:.2f} | Change: {color}{pct:+.2f}%\033[0m")

                # Sell logic: only yesterday or earlier, +0.5% profit
                for p in positions:
                    if p['buy_date'] < now.date():
                        cur_price = client_get_quote(p['symbol'])
                        if cur_price and cur_price >= p['avg_price'] * (1 + SELL_PROFIT_THRESHOLD):
                            place_sell_order(p['symbol'], p['qty'], cur_price)

                # Buy logic
                for sym in symbols_to_buy:
                    cur_price = client_get_quote(sym)
                    if cur_price is None or not can_buy(sym, cur_price, cash_avail):
                        continue

                    hist = calculate_technical_indicators(sym, lookback_days=5, interval='5m')
                    if hist is None or hist.empty:
                        continue

                    score = calculate_buy_score(sym, hist, cur_price)
                    if score < 4:
                        continue

                    # Determine buy qty
                    if FRACTIONAL_BUY_ORDERS:
                        max_affordable = cash_avail - MIN_CASH_RESERVE
                        qty = round(max_affordable / cur_price, QTY_DECIMALS)
                    else:
                        qty = 1
                    place_buy_order(sym, qty, cur_price)
                    cash_avail -= qty * cur_price

            else:
                # Market closed
                schedule = nyse_cal.schedule(start_date=now.date(), end_date=now.date())
                if not schedule.empty:
                    open_time = schedule.iloc[0]['market_open'].tz_convert(eastern)
                    close_time = schedule.iloc[0]['market_close'].tz_convert(eastern)
                    print(f"Market closed. Regular hours: {open_time.strftime('%I:%M %p')} - {close_time.strftime('%I:%M %p')} ET")
                time.sleep(interval)

            time.sleep(interval)

        except KeyboardInterrupt:
            print("Trading robot stopped by user.")
            logging.info("Stopped by user.")
            break
        except Exception as e:
            logging.error(f"Error in main loop: {e}", exc_info=True)
            print(f"Unexpected error: {e}. Restarting in 2 minutes...")
            time.sleep(120)  # wait 2 minutes before restarting

# -------------------------
# Initialization
# -------------------------
def main():
    print("Starting Professional Trading Bot for Electricity/Utility Stocks")
    print(f"Fractional buys: {'Enabled' if FRACTIONAL_BUY_ORDERS else 'Disabled'}")
    print(f"Leaving ${MIN_CASH_RESERVE} as cash reserve for fees.")
    print(f"Buy score threshold: 4 points")
    print(f"Sell profit threshold: {SELL_PROFIT_THRESHOLD*100:.2f}%")
    print(f"Checking market hours in Eastern Time (US/Eastern)")
    print("Loading symbols from:", SYMBOLS_FILE)
    
    symbols = load_symbols_from_file()
    print(f"Symbols loaded: {symbols}")

    print("Trading robot is starting... Press Ctrl+C to stop.")

    # Start the main loop with 60-second interval
    trading_robot(interval=60)

# -------------------------
# Run if executed as script
# -------------------------
if __name__ == "__main__":
    main()
    
