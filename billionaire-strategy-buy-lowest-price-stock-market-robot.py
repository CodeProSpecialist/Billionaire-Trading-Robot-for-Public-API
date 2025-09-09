#!/usr/bin/env python3
"""
Professional day trading bot for electricity/utility stocks:
- Buys based on scoring (bullish reversal candlestick patterns, RSI/volume drop, price decline)
- Buys only during market hours, leaves $2 cash buffer
- Fractional buy support
- Sells outside market hours for non-fractional shares using limit orders
- Tracks all positions, prices, and percent changes
- Color-coded terminal display for profit/loss
- Loads symbols from 'electricity-or-utility-stocks-to-buy-list.txt'
- Uses PublicClient API for trading
- yfinance for historical candles and TA indicators
- Rate limits yfinance calls
"""

import os, time, csv, logging, threading
from datetime import datetime, timedelta
import pytz
import requests
import yfinance as yf
import talib
import numpy as np
from ratelimit import limits, sleep_and_retry
from colorama import init, Fore, Style

# Initialize colorama
init(autoreset=True)

# --- Environment & Config ---
PUBLIC_API_ACCESS_TOKEN = os.getenv('PUBLIC_API_ACCESS_TOKEN')
if not PUBLIC_API_ACCESS_TOKEN:
    raise ValueError("PUBLIC_API_ACCESS_TOKEN env var not set.")

HEADERS = {"Authorization": f"Bearer {PUBLIC_API_ACCESS_TOKEN}", "Content-Type": "application/json"}
BASE_URL = "https://api.public.com/userapigateway"

CSV_FILENAME = "log-file-of-buy-and-sell-signals.csv"
CSV_FIELDS = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']

FRACTIONAL_BUY_ORDERS = True
MIN_SCORE_TO_BUY = 4
CASH_BUFFER = 2.0

eastern = pytz.timezone('US/Eastern')

# Logging
logging.basicConfig(filename='trading-bot.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

# --- Initialize CSV ---
with open(CSV_FILENAME, mode='w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
    writer.writeheader()

# --- Symbol file ---
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
    except Exception as e:
        logging.error(f"Error loading symbols: {e}")
    return symbols

# --- Market hours ---
import pandas_market_calendars as mcal
nyse = mcal.get_calendar('NYSE')

def market_hours_today():
    today = datetime.now(eastern).date()
    schedule = nyse.schedule(start_date=today, end_date=today)
    if schedule.empty:
        return None, None
    return schedule.iloc[0]['market_open'].tz_convert(eastern), schedule.iloc[0]['market_close'].tz_convert(eastern)

def market_is_open():
    now = datetime.now(eastern)
    open_time, close_time = market_hours_today()
    if open_time and close_time:
        if open_time <= now <= close_time:
            return True
        else:
            print(f"Market closed. Hours: {open_time.strftime('%I:%M %p')} - {close_time.strftime('%I:%M %p')}")
            return False
    else:
        print("Market closed today (holiday/weekend).")
        return False

# --- PublicClient API wrapper ---
try:
    from public_invest_api import PublicClient
    client = PublicClient(api_key=PUBLIC_API_ACCESS_TOKEN)
    use_client = True
    logging.info("PublicClient initialized.")
except Exception as e:
    client = None
    use_client = False
    logging.info(f"PublicClient not importable: {e}")

def get_account_info():
    try:
        if use_client:
            acc = client.account()
            return {'cash': float(acc.get('cash',0)), 'equity': float(acc.get('equity',0)), 'raw': acc}
        else:
            resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
            resp.raise_for_status()
            a = resp.json()[0]
            return {'cash': float(a.get('cash',0)), 'equity': float(a.get('equity',0)), 'raw': a}
    except Exception as e:
        logging.error(f"Error fetching account info: {e}")
        return {'cash':0, 'equity':0, 'raw':{}}

def list_positions():
    out = []
    try:
        if use_client:
            pos = client.positions()
            for p in pos:
                sym = p.get('symbol')
                qty = float(p.get('quantity',0))
                avg = float(p.get('avg_price',0))
                date_str = p.get('created_at') or datetime.now().isoformat()
                out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'buy_date': date_str[:10]})
        else:
            resp = requests.get(f"{BASE_URL}/trading/accounts/positions", headers=HEADERS, timeout=10)
            resp.raise_for_status()
            for p in resp.json():
                sym = p.get('symbol')
                qty = float(p.get('quantity',0))
                avg = float(p.get('avg_price',0))
                date_str = p.get('purchase_date') or datetime.now().isoformat()
                out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'buy_date': date_str[:10]})
    except Exception as e:
        logging.error(f"Error listing positions: {e}")
    return out

def get_price(symbol):
    try:
        if use_client:
            q = client.quote(symbol)
            return float(q.get('last',0))
        else:
            resp = requests.post(f"{BASE_URL}/marketdata/quotes", headers=HEADERS, json={"instruments":[{"symbol":symbol,"type":"EQUITY"}]}, timeout=10)
            resp.raise_for_status()
            quotes = resp.json().get('quotes',[])
            if quotes: return float(quotes[0].get('last',0))
    except Exception as e:
        logging.error(f"Error fetching price {symbol}: {e}")
    return None

def place_order(symbol, side, qty):
    qty = round(qty,2)
    try:
        if use_client:
            if side=='BUY': return client.buy(symbol, qty)
            if side=='SELL': return client.sell(symbol, qty)
        else:
            url = f"{BASE_URL}/trading/orders"
            body = {"symbol":symbol,"side":side,"type":"market","quantity":str(qty)}
            resp = requests.post(url, headers=HEADERS, json=body, timeout=10)
            if resp.status_code in [200,201]: return resp.json()
    except Exception as e:
        logging.error(f"Error placing {side} order for {symbol}: {e}")
    return None

# --- Price & History Tracking ---
stock_data = {}
previous_prices = {}
price_changes = {}
price_history = {}
last_stored = {}
interval_map = {'1min':60,'5min':300,'10min':600,'15min':900,'30min':1800,'45min':2700,'60min':3600}

@sleep_and_retry
@limits(calls=50, period=60)  # limit yfinance to 50 calls/min
def get_yf_history(symbol, period='5d', interval='5m'):
    try:
        s = symbol.replace('.','-')
        df = yf.Ticker(s).history(period=period, interval=interval)
        return df
    except Exception as e:
        logging.error(f"yfinance error for {symbol}: {e}")
        return None

# --- Buy Logic ---
def buy_stocks(symbols):
    account = get_account_info()
    cash = account['cash']
    positions = list_positions()
    today = datetime.now(eastern).date().isoformat()
    for sym in symbols:
        price = get_price(sym)
        if price is None or cash < 1+CASH_BUFFER:
            continue

        df = get_yf_history(sym)
        if df is None or len(df)<5:
            continue

        # Indicators
        close = df['Close'].values
        open_p = df['Open'].values
        high = df['High'].values
        low = df['Low'].values
        rsi = talib.RSI(close)[-1]
        latest_price = close[-1]

        # Scoring
        score = 0
        patterns = [
            talib.CDLHAMMER, talib.CDLENGULFING, talib.CDLMORNINGSTAR, talib.CDLPIERCING,
            talib.CDLINVERTEDHAMMER, talib.CDLMORNINGDOJISTAR, talib.CDLHANGINGMAN
        ]
        for f in patterns:
            res = f(open_p, high, low, close)
            if res[-1]>0:
                score+=1
        # RSI decrease
        if rsi<50: score+=1
        # Volume decrease
        if len(df)>=10 and df['Volume'].iloc[-1]<df['Volume'].iloc[-5:-1].mean():
            score+=1
        # Price decline 0.3%
        prev_close = close[-2]
        if latest_price <= prev_close*0.997: score+=1

        if score<MIN_SCORE_TO_BUY:
            continue

        # Determine qty
        risk_cash = min(cash-CASH_BUFFER,1000)
        qty = round(risk_cash/latest_price,2) if FRACTIONAL_BUY_ORDERS else int(risk_cash/latest_price)
        if qty<=0: continue

        resp = place_order(sym,'BUY',qty)
        if resp:
            cash -= qty*latest_price
            ts = datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
            print(f"{ts} BUY {sym} qty:{qty} price:{latest_price:.2f}")
            with open(CSV_FILENAME,'a',newline='') as f:
                writer = csv.DictWriter(f,fieldnames=CSV_FIELDS)
                writer.writerow({'Date':ts,'Buy':'BUY','Sell':'','Quantity':qty,'Symbol':sym,'Price Per Share':latest_price})

# --- Sell Logic ---
def sell_stocks():
    positions = list_positions()
    today = datetime.now(eastern).date().isoformat()
    for p in positions:
        buy_date = p['buy_date']
        if buy_date==today:
            continue  # skip today's buys
        price = get_price(p['symbol'])
        if price is None:
            continue
        # Sell rule: price >= avg_price*1.005
        if price >= p['avg_price']*1.005:
            resp = place_order(p['symbol'],'SELL',p['qty'])
            if resp:
                ts = datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
                change = price - p['avg_price']
                color = Fore.GREEN if change>0 else Fore.RED
                print(f"{ts} SELL {p['symbol']} qty:{p['qty']} price:{price:.2f} {color}{change:.2f}{Style.RESET_ALL}")
                with open(CSV_FILENAME,'a',newline='') as f:
                    writer = csv.DictWriter(f,fieldnames=CSV_FIELDS)
                    writer.writerow({'Date':ts,'Buy':'','Sell':'SELL','Quantity':p['qty'],'Symbol':p['symbol'],'Price Per Share':price})

# --- Main Loop ---
def print_positions():
    positions = list_positions()
    print("\n--- Positions ---")
    for p in positions:
        price = get_price(p['symbol'])
        change = price - p['avg_price']
        color = Fore.GREEN if change>=0 else Fore.RED
        print(f"{p['symbol']} | Qty:{p['qty']:.2f} Avg:{p['avg_price']:.2f} Current:{price:.2f} Change:{color}{change:.2f}{Style.RESET_ALL}")

def trading_bot():
    symbols = load_symbols()
    while True:
        try:
            if market_is_open():
                print_positions()
                sell_stocks()
                buy_stocks(symbols)
            else:
                print("Waiting for market to open...")
            time.sleep(60)
        except KeyboardInterrupt:
            print("Stopped by user.")
            break
        except Exception as e:
            logging.error(f"Main loop error: {e}", exc_info=True)
            print(f"Error: {e}. Restarting in 120s...")
            time.sleep(120)

if __name__=="__main__":
    trading_bot()
