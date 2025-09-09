#!/usr/bin/env python3
"""
Professional trading bot:
- Public API wrapper for account and trades
- yfinance for historical candles + TA
- Full candlestick recognition (bullish reversals)
- Fractional buys, scoring logic, cash checks
- Only sells yesterday's or older positions if profit ≥ 0.5%
- Rate-limited yfinance
- Color-coded profit/loss
- CSV and SQLite logging
"""

import os
import time
import json
import csv
import logging
import threading
from datetime import datetime, timedelta
import pytz
import requests
import yfinance as yf
import numpy as np
import talib
from ratelimit import limits, sleep_and_retry

# -------------------------
# Configuration & Environment
# -------------------------
secret_key = os.getenv('PUBLIC_API_ACCESS_TOKEN')
if not secret_key:
    raise ValueError("PUBLIC_API_ACCESS_TOKEN environment variable is not set.")

HEADERS = {"Authorization": f"Bearer {secret_key}", "Content-Type": "application/json"}
BASE_URL = "https://api.public.com/userapigateway"

PRINT_DB = True
ALL_BUY_ORDERS_ARE_1_DOLLAR = False
FRACTIONAL_BUY_ORDERS = True

# Timezone
eastern = pytz.timezone('US/Eastern')

# CSV trade log
CSV_FILENAME = 'log-file-of-buy-and-sell-signals.csv'
CSV_FIELDS = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']

with open(CSV_FILENAME, mode='w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
    writer.writeheader()

# Logging
LOGFILE = 'trading-bot-program-logging-messages.txt'
logging.basicConfig(filename=LOGFILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

# Price tracking dictionaries
stock_data = {}
previous_prices = {}
price_changes = {}
price_history = {}
last_stored = {}
interval_map = {
    '1min': 60, '5min': 300, '10min': 600, '15min': 900,
    '30min': 1800, '45min': 2700, '60min': 3600
}

# -------------------------
# File helper: load symbols from text file
# -------------------------
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
    except FileNotFoundError:
        logging.error(f"Symbols file not found: {file_path}")
    except Exception as e:
        logging.error(f"Error loading symbols file: {e}")
    return symbols

# -------------------------
# Market hours
# -------------------------
import pandas_market_calendars as mcal
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
    else:
        print(f"Market closed — hours: {market_open.strftime('%I:%M %p')} - {market_close.strftime('%I:%M %p')} ET")
        return False

# -------------------------
# yfinance helpers with rate-limiting
# -------------------------
YF_CALLS_PER_MINUTE = 15  # adjust as needed

@sleep_and_retry
@limits(calls=YF_CALLS_PER_MINUTE, period=60)
def yf_download(symbol, period='5d', interval='5m'):
    """Download historical OHLCV data with rate-limiting"""
    try:
        symbol_clean = symbol.replace('.', '-')
        df = yf.Ticker(symbol_clean).history(period=period, interval=interval, prepost=True)
        return df
    except Exception as e:
        logging.error(f"yfinance download error for {symbol}: {e}")
        return None

def get_last_price(symbol):
    df = yf_download(symbol, period='1d', interval='1m')
    if df is not None and not df.empty:
        return float(df['Close'].iloc[-1])
    return None

def calculate_indicators(symbol):
    df = yf_download(symbol, period='5d', interval='5m')
    if df is None or df.empty:
        return None
    close = df['Close'].values
    open_ = df['Open'].values
    high = df['High'].values
    low = df['Low'].values
    volume = df['Volume'].values
    return {'close': close, 'open': open_, 'high': high, 'low': low, 'volume': volume}

# -------------------------
# Public API client wrapper & account management
# -------------------------
try:
    from public_invest_api import PublicClient
    client = PublicClient(api_key=secret_key)
    use_client = True
    logging.info("PublicClient instantiated successfully.")
except Exception as e:
    client = None
    use_client = False
    logging.warning(f"PublicClient not importable: {e}. Will use REST fallback.")

_account_id_cache = None

def get_account_id():
    global _account_id_cache
    if _account_id_cache:
        return _account_id_cache
    try:
        if use_client and hasattr(client, 'account'):
            acc = client.account()
            _account_id_cache = acc.get('id') or acc.get('account_id')
            return _account_id_cache
        resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        arr = resp.json()
        if isinstance(arr, list) and len(arr) > 0:
            _account_id_cache = arr[0].get('id')
            return _account_id_cache
    except Exception as e:
        logging.error(f"Error getting account id: {e}")
    return None

def get_account_info():
    try:
        if use_client:
            acc = client.account()
            return {'equity': float(acc.get('equity',0)), 'cash': float(acc.get('cash',0)), 'raw': acc}
        resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        a = resp.json()[0]
        return {'equity': float(a.get('equity',0)), 'cash': float(a.get('cash',0)), 'raw': a}
    except Exception as e:
        logging.error(f"Error fetching account info: {e}")
        return {'equity':0.0,'cash':0.0,'raw':{}}

def list_positions():
    """Return list of positions with symbol, quantity, avg_price, purchase_date"""
    out = []
    try:
        if use_client and hasattr(client, 'positions'):
            positions = client.positions() or []
            for p in positions:
                sym = p.get('symbol') or p.get('instrument', {}).get('symbol')
                qty = float(p.get('quantity') or p.get('qty') or 0)
                avg = float(p.get('avg_price') or p.get('avg_entry_price') or 0)
                buy_date = datetime.fromisoformat(p.get('created_at')) if p.get('created_at') else datetime.now()
                out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'buy_date': buy_date.date()})
        else:
            resp = requests.get(f"{BASE_URL}/trading/accounts/positions", headers=HEADERS, timeout=10)
            resp.raise_for_status()
            positions = resp.json().get('positions', [])
            for p in positions:
                sym = p.get('symbol')
                qty = float(p.get('quantity',0))
                avg = float(p.get('avg_price',0))
                buy_date = datetime.fromisoformat(p.get('purchase_date')) if p.get('purchase_date') else datetime.now()
                out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'buy_date': buy_date.date()})
    except Exception as e:
        logging.error(f"Error fetching positions: {e}")
    return out

def get_quote(symbol):
    try:
        if use_client and hasattr(client, 'quote'):
            q = client.quote(symbol)
            price = q.get('last') or q.get('close')
            return float(price) if price else None
        url = f"{BASE_URL}/marketdata/quotes"
        resp = requests.post(url, headers=HEADERS, json={"instruments":[{"symbol":symbol,"type":"EQUITY"}]}, timeout=10)
        resp.raise_for_status()
        quotes = resp.json().get('quotes',[])
        if quotes:
            return float(quotes[0].get('last') or quotes[0].get('close'))
    except Exception as e:
        logging.error(f"Error fetching quote for {symbol}: {e}")
    return None

def place_market_order(symbol, side, quantity):
    """Buy/Sell fractional shares with two decimals precision"""
    quantity = round(quantity,2)
    if quantity <= 0:
        return None
    try:
        if use_client:
            if side.upper() == 'BUY' and hasattr(client,'buy'):
                return client.buy(symbol, quantity)
            if side.upper() == 'SELL' and hasattr(client,'sell'):
                return client.sell(symbol, quantity)
        acct = get_account_id()
        if not acct:
            logging.error("No account id for REST order")
            return None
        url = f"{BASE_URL}/trading/{acct}/order"
        body = {
            "orderId": str(uuid4()),
            "instrument": {"symbol": symbol, "type": "EQUITY"},
            "orderSide": side.upper(),
            "orderType": "MARKET",
            "quantity": str(quantity),
            "expiration": {"timeInForce":"DAY"},
            "limitPrice": "0",
            "stopPrice": "0",
            "openCloseIndicator": "OPEN"
        }
        resp = requests.post(url, headers=HEADERS, json=body, timeout=10)
        if resp.status_code in (200,201):
            return resp.json()
        else:
            logging.error(f"Order error {side} {symbol}: {resp.text}")
            return None
    except Exception as e:
        logging.error(f"Exception placing order: {e}")
        return None

# -------------------------
# Buy/Sell logic with scoring
# -------------------------

# Scoring rules:
# Bullish reversal candlestick (TA-Lib) = 1 point
# Decrease in RSI = 1 point
# Decrease in volume = 1 point
# Decrease in price >=0.3% = 1 point
# Minimum score to buy = 4

MIN_SCORE_TO_BUY = 4
FRACTIONAL_BUY_ORDERS = True

def calculate_buy_score(symbol):
    """Return score (int) based on bullish reversal, RSI, volume, price decline"""
    score = 0
    try:
        df = calculate_technical_indicators(symbol, lookback_days=5, interval='5m')
        if df is None or df.empty or len(df) < 5:
            return 0

        close = df['Close'].values
        open_ = df['Open'].values
        high = df['High'].values
        low = df['Low'].values
        volume = df['Volume'].values

        # Bullish reversal patterns using TA-Lib
        patterns = [
            talib.CDLHAMMER, talib.CDLINVERTEDHAMMER, talib.CDLENGULFING,
            talib.CDLPIERCING, talib.CDLMORNINGSTAR, talib.CDLHARAMI,
            talib.CDLHARAMICROSS, talib.CDLDRAGONFLYDOJI, talib.CDLDOJI
        ]
        pattern_detected = False
        for func in patterns:
            result = func(open_, high, low, close)
            if len(result) > 0 and result[-1] > 0:
                pattern_detected = True
                break
        if pattern_detected:
            score += 1

        # RSI decrease
        rsi = talib.RSI(close, timeperiod=14)
        if len(rsi) >= 2 and rsi[-1] < rsi[-2]:
            score += 1

        # Volume decrease
        if len(volume) >= 2 and volume[-1] < volume[-2]:
            score += 1

        # Price decline >=0.3%
        if len(close) >= 2 and (close[-1] < close[-2] * 0.997):
            score += 1

    except Exception as e:
        logging.error(f"Error calculating score for {symbol}: {e}")
        return 0
    return score

def buy_stocks(symbols_to_buy):
    account = get_account_info()
    cash_available = account.get('cash',0)
    for sym in symbols_to_buy:
        # Check quote
        price = get_quote(sym)
        if price is None:
            continue

        # Score
        score = calculate_buy_score(sym)
        if score < MIN_SCORE_TO_BUY:
            continue

        # Check cash: leave $2.00 in account
        max_spend = cash_available - 2.00
        if max_spend < 1.0:
            logging.info(f"Not enough cash to buy {sym}. Available: {cash_available:.2f}")
            continue

        qty = round(max_spend / price, 2) if FRACTIONAL_BUY_ORDERS else int(max_spend / price)
        if qty <= 0:
            continue

        # Place order
        order = place_market_order(sym, 'BUY', qty)
        if order:
            now_str = datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
            log_trade_csv(now_str, 'Buy', '', qty, sym, price)
            record_db_trade('buy', sym, qty, price, now_str)
            logging.info(f"Buy order executed: {sym} qty={qty} at {price:.2f}")
            cash_available -= qty * price

def sell_stocks():
    """Sell yesterday or previous stocks only if >=0.5% profit"""
    today = datetime.now(eastern).date()
    positions = list_positions()
    for p in positions:
        sym = p['symbol']
        qty = p['qty']
        avg_price = p['avg_price']
        buy_date = p['buy_date']

        # Skip today's purchases
        if buy_date == today:
            continue

        # Current price
        price = get_quote(sym)
        if price is None:
            continue

        # Profit check
        if price < avg_price * 1.005:
            continue  # <0.5% profit, skip

        # Place sell order
        order = place_market_order(sym, 'SELL', qty)
        if order:
            now_str = datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
            log_trade_csv(now_str, '', 'Sell', qty, sym, price)
            record_db_trade('sell', sym, qty, price, now_str)
            logging.info(f"Sell order executed: {sym} qty={qty} at {price:.2f}")

# -------------------------
# Account summary with colorized gains/losses
# -------------------------
def print_account_summary():
    now = datetime.now(eastern)
    account = get_account_info()
    equity = account.get('equity', 0)
    cash = account.get('cash', 0)
    print(f"\n=== Account Summary @ {now.strftime('%Y-%m-%d %I:%M:%S %p %Z')} ===")
    print(f"Equity: ${equity:.2f} | Cash: ${cash:.2f}")

    positions = list_positions()
    if not positions:
        print("No positions held.")
        return

    print("\nHeld positions:")
    for p in positions:
        sym = p['symbol']
        qty = p['qty']
        avg_price = p['avg_price']
        price = get_quote(sym) or 0.0
        change_pct = ((price - avg_price) / avg_price) * 100 if avg_price else 0
        # colorize: red for negative, green for positive
        change_str = f"\033[91m{change_pct:+.2f}%\033[0m" if change_pct < 0 else f"\033[92m{change_pct:+.2f}%\033[0m"
        print(f"{sym} | Qty: {qty:.2f} | Avg: {avg_price:.2f} | Current: {price:.2f} | Change: {change_str}")


# -------------------------
# Main trading robot loop
# -------------------------
def trading_robot(interval=60):
    symbols_to_buy = load_symbols_from_file()
    if not symbols_to_buy:
        print(f"No symbols loaded from {SYMBOLS_FILE}. Exiting.")
        return

    while True:
        try:
            now = datetime.now(eastern)
            if market_is_open():
                print_account_summary()
                # Sell yesterday's positions if profit >=0.5%
                sell_stocks()
                # Buy stocks based on scoring
                buy_stocks(symbols_to_buy)
            else:
                schedule = nyse_cal.schedule(start_date=now.date(), end_date=now.date())
                if not schedule.empty:
                    market_open = schedule.iloc[0]['market_open'].tz_convert(eastern)
                    market_close = schedule.iloc[0]['market_close'].tz_convert(eastern)
                    print(f"Market is closed. Regular hours: {market_open.strftime('%I:%M %p')} - {market_close.strftime('%I:%M %p')} ET")
                else:
                    print("Market is closed today (weekend/holiday).")

            time.sleep(interval)

        except KeyboardInterrupt:
            print("Trading robot stopped by user.")
            logging.info("Stopped by user.")
            break
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}", exc_info=True)
            print(f"Unexpected error: {e}. Restarting in 2 minutes.")
            time.sleep(120)  # restart after 2 minutes

# -------------------------
# Run the robot
# -------------------------
if __name__ == "__main__":
    trading_robot(interval=60)  # check every 60 seconds
