#!/usr/bin/env python3
"""
Complete trading bot:
- PublicClient (public_invest_api) for all trading operations
- yfinance for historical candles and TA indicators
- SQLAlchemy (SQLite WAL + scoped_session) for trade history & positions
- pandas-market-calendars for market hours
- Detailed logging + CSV of trades
- Reads symbols from electricity-or-utility-stocks-to-buy-list.txt
"""

import os
import time
import json
import csv
import logging
import threading
from uuid import uuid4
from datetime import datetime, timedelta

import pytz
import requests
import numpy as np
import talib
import yfinance as yf
import pandas_market_calendars as mcal

from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# --- Configuration & Environment ---
secret_key = os.getenv('PUBLIC_API_ACCESS_TOKEN')
if not secret_key:
    raise ValueError("PUBLIC_API_ACCESS_TOKEN environment variable is not set.")

# REST headers fallback (if direct client does not support methods)
HEADERS = {"Authorization": f"Bearer {secret_key}", "Content-Type": "application/json"}
BASE_URL = "https://api.public.com/userapigateway"

# Flags
PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE = True
ALL_BUY_ORDERS_ARE_1_DOLLAR = False

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

# --- DB: SQLAlchemy setup (scoped session for threads + WAL) ---
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
# Enable WAL for better concurrency
with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
    conn.execute(text("PRAGMA synchronous=NORMAL;"))

Base.metadata.create_all(engine)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False))

def get_db_session():
    return SessionLocal()

# --- Public API client wrapper ---
try:
    # Try to use the official client
    from public_invest_api import PublicClient
    try:
        client = PublicClient(api_key=secret_key)  # many clients allow token param
    except TypeError:
        # fallback to a no-arg constructor if API differs
        client = PublicClient()
    use_client = True
    logging.info("PublicClient instantiated.")
except Exception as e:
    client = None
    use_client = False
    logging.info(f"PublicClient not available/importable: {e}. Falling back to REST calls.")

def client_get_account():
    """Return dict: {'equity': float, 'cash': float, 'day_trade_count': int (if available)}"""
    try:
        if use_client and hasattr(client, 'account'):
            acc = client.account()
            # adapt fields if structure differs
            return {
                'equity': float(acc.get('equity', 0)),
                'cash': float(acc.get('cash', 0)),
                'raw': acc
            }
        else:
            resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
            resp.raise_for_status()
            arr = resp.json()
            if isinstance(arr, list) and len(arr) > 0:
                a = arr[0]
            else:
                a = arr
            return {'equity': float(a.get('equity', 0)), 'cash': float(a.get('cash', 0)), 'raw': a}
    except Exception as e:
        logging.error(f"Error fetching account: {e}")
        return {'equity': 0.0, 'cash': 0.0, 'raw': {}}

def client_list_positions():
    """
    Return list of positions with keys:
      'symbol' (str), 'qty' (float), 'avg_price' (float), 'buy_date' (datetime.date)
    """
    out = []
    try:
        if use_client and hasattr(client, 'positions'):
            positions = client.positions() or []
            # adapt to likely structure of PublicClient
            for p in positions:
                # fields may differ, attempt to map
                sym = p.get('symbol') or p.get('instrument', {}).get('symbol') if isinstance(p, dict) else None
                qty = float(p.get('quantity') or p.get('qty') or p.get('shares') or 0)
                avg = float(p.get('avg_price') or p.get('avg_entry_price') or p.get('avgPrice') or 0)
                # creation/buy date may be missing; use today's date if not provided
                created_at = p.get('created_at') or p.get('createdAt') or p.get('purchase_date') or None
                buy_date = None
                if created_at:
                    try:
                        buy_date = datetime.fromisoformat(created_at).date()
                    except Exception:
                        try:
                            buy_date = datetime.strptime(created_at[:10], "%Y-%m-%d").date()
                        except Exception:
                            buy_date = None
                out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'buy_date': buy_date or datetime.now().date()})
        else:
            resp = requests.get(f"{BASE_URL}/trading/accounts/positions", headers=HEADERS, timeout=10)
            # REST path may differ; try the portfolio endpoint as fallback
            if resp.status_code != 200:
                resp = requests.get(f"{BASE_URL}/trading/{get_account_id()}/portfolio/v2", headers=HEADERS, timeout=10)
            resp.raise_for_status()
            payload = resp.json()
            # Attempt to parse positions from common shapes
            positions = payload.get('positions') if isinstance(payload, dict) else payload
            if not positions:
                positions = []
            for p in positions:
                sym = p.get('symbol') or (p.get('instrument') or {}).get('symbol')
                qty = float(p.get('quantity') or p.get('qty') or 0)
                avg = float(p.get('avg_price') or p.get('avg_entry_price') or p.get('avgPrice') or 0)
                buy_date = p.get('purchase_date') or p.get('created_at') or None
                if buy_date:
                    try:
                        buy_date = datetime.fromisoformat(buy_date).date()
                    except Exception:
                        try:
                            buy_date = datetime.strptime(buy_date[:10], "%Y-%m-%d").date()
                        except Exception:
                            buy_date = datetime.now().date()
                else:
                    buy_date = datetime.now().date()
                out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'buy_date': buy_date})
    except Exception as e:
        logging.error(f"Error fetching positions: {e}")
    return out

def client_get_quote(symbol):
    """Return last price as float or None. Uses client.quote or REST quotes endpoint."""
    try:
        if use_client and hasattr(client, 'quote'):
            q = client.quote(symbol)
            # adapt common names
            # try last, last_price, close
            price = q.get('last') or q.get('last_price') or q.get('close') or q.get('mid')
            if price is None:
                return None
            return float(price)
        else:
            # Use REST quotes endpoint; Public uses /marketdata/{accountId}/quotes or similar
            # Using account-less quotes endpoint if available
            acct = get_account_id()
            if acct:
                url = f"{BASE_URL}/marketdata/{acct}/quotes"
                body = {"instruments": [{"symbol": symbol, "type": "EQUITY"}]}
                resp = requests.post(url, headers=HEADERS, json=body, timeout=10)
                resp.raise_for_status()
                payload = resp.json() or {}
                quotes = payload.get('quotes', [])
                if quotes:
                    q = quotes[0]
                    p = q.get('last') or q.get('close')
                    return float(p) if p is not None else None
            # If no account id or fallback:
            url2 = f"{BASE_URL}/marketdata/quotes"
            resp = requests.post(url2, headers=HEADERS, json={"instruments":[{"symbol":symbol,"type":"EQUITY"}]}, timeout=10)
            if resp.status_code == 200:
                payload = resp.json() or {}
                quotes = payload.get('quotes', [])
                if quotes:
                    p = quotes[0].get('last') or quotes[0].get('close')
                    return float(p) if p is not None else None
    except Exception as e:
        logging.error(f"Error fetching quote for {symbol}: {e}")
    return None

def client_place_market_order(symbol, side, quantity):
    """
    Place a market order through client or REST fallback.
    side: 'BUY' or 'SELL', quantity: fractional decimal number
    Returns response dict or None.
    """
    try:
        # Try client method names
        if use_client:
            # Common method names: buy, sell, create_order - try options
            if hasattr(client, 'buy') and side.upper() == 'BUY':
                return client.buy(symbol, quantity)
            if hasattr(client, 'sell') and side.upper() == 'SELL':
                return client.sell(symbol, quantity)
            if hasattr(client, 'order'):
                return client.order(symbol=symbol, side=side.upper(), type='market', quantity=str(quantity))
            if hasattr(client, 'create_order'):
                return client.create_order(symbol=symbol, side=side.upper(), order_type='market', quantity=str(quantity))
        # REST fallback:
        acct = get_account_id()
        if not acct:
            logging.error("No account id available for REST order")
            return None
        url = f"{BASE_URL}/trading/{acct}/order"
        body = {
            "orderId": str(uuid4()),
            "instrument": {"symbol": symbol, "type": "EQUITY"},
            "orderSide": side.upper(),
            "orderType": "MARKET",
            "expiration": {"timeInForce": "DAY"},
            "quantity": str(quantity),
            "amount": "0",
            "limitPrice": "0",
            "stopPrice": "0",
            "openCloseIndicator": "OPEN"
        }
        resp = requests.post(url, headers=HEADERS, json=body, timeout=15)
        if resp.status_code in (200,201):
            return resp.json()
        else:
            logging.error(f"Order error ({side} {symbol}) status={resp.status_code} body={resp.text}")
            return None
    except Exception as e:
        logging.error(f"Exception placing order: {e}")
        return None

# Utility to obtain an account id (tries client or REST)
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
        # REST: try /accounts
        resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        arr = resp.json()
        if isinstance(arr, list) and len(arr) > 0:
            _account_id_cache = arr[0].get('id')
            return _account_id_cache
    except Exception as e:
        logging.error(f"Error getting account id: {e}")
    return None

# Lightweight wrappers for naming parity with earlier helper names
def get_account_info():
    return client_get_account()

def list_positions_wrapper():
    return client_list_positions()

def get_price(account_id_unused, symbol):
    # keep same signature as older code: account_id param unused for client-backed quotes
    return client_get_quote(symbol)

def place_market_order(account_id_unused, symbol, side, quantity):
    return client_place_market_order(symbol, side, quantity)

# --- yfinance & TA helpers (use '.'->'-' conversion) ---
def yf_symbol(sym):
    return sym.replace('.', '-')

def calculate_technical_indicators(symbols, lookback_days=5, interval='5m'):
    """Return DataFrame with OHLCV; raises or returns empty df when no data."""
    symbol = yf_symbol(symbols)
    try:
        df = yf.Ticker(symbol).history(period=f'{lookback_days}d', interval=interval, prepost=True)
        if df is None:
            return None
        return df
    except Exception as e:
        logging.error(f"yfinance error for {symbol}: {e}")
        return None

def get_average_true_range(symbols):
    symbol = yf_symbol(symbols)
    try:
        df = yf.Ticker(symbol).history(period='30d', interval='1d')
        if df is None or df.empty:
            return None
        atr = talib.ATR(df['High'].values, df['Low'].values, df['Close'].values, timeperiod=14)
        return float(atr[-1]) if len(atr) > 0 else None
    except Exception as e:
        logging.error(f"ATR error for {symbols}: {e}")
        return None

def is_in_uptrend(symbols_to_buy):
    symbol = yf_symbol(symbols_to_buy)
    try:
        df = yf.Ticker(symbol).history(period='250d', interval='1d')
        if df is None or len(df) < 200:
            return False
        sma200 = talib.SMA(df['Close'].values, timeperiod=200)
        return float(df['Close'].iloc[-1]) > float(sma200[-1])
    except Exception as e:
        logging.error(f"is_in_uptrend error for {symbols_to_buy}: {e}")
        return False

def get_daily_rsi(symbols_to_buy):
    symbol = yf_symbol(symbols_to_buy)
    try:
        df = yf.Ticker(symbol).history(period='60d', interval='1d')
        if df is None or len(df) < 14:
            return None
        rsi = talib.RSI(df['Close'].values, timeperiod=14)
        return float(rsi[-1])
    except Exception as e:
        logging.error(f"get_daily_rsi error for {symbols_to_buy}: {e}")
        return None

def get_previous_close(symbols_to_buy):
    symbol = yf_symbol(symbols_to_buy)
    try:
        df = yf.Ticker(symbol).history(period='3d', interval='1d')
        if df is None or len(df) < 2:
            return None
        return float(df['Close'].iloc[-2])
    except Exception as e:
        logging.error(f"get_previous_close error for {symbols_to_buy}: {e}")
        return None

def get_last_price_within_past_5_minutes(symbols_to_buy_list):
    """Return dict symbol->last price if within 5 minutes (using yfinance 1m)."""
    results = {}
    end_time = datetime.now(eastern)
    start_time = end_time - timedelta(minutes=6)
    for s in symbols_to_buy_list:
        symbol = yf_symbol(s)
        try:
            df = yf.download(symbol, start=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                             end=end_time.strftime('%Y-%m-%d %H:%M:%S'),
                             interval='1m', progress=False)
            if df is None or df.empty:
                results[s] = None
            else:
                results[s] = float(df['Close'].iloc[-1])
        except Exception as e:
            logging.error(f"Error fetching 1m data for {s}: {e}")
            results[s] = None
    return results

# -------------------------
# Price history storage (multi-interval)
# -------------------------
price_history = {}   # symbol -> {interval_name: [prices]}
last_stored = {}     # symbol -> {interval_name: last_ts}
interval_map = {'5min': 5 * 60, '1min': 60}

# -------------------------
# File helper: load symbols from given file
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
# Market hours using pandas-market-calendars (NYSE regular hours)
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
    else:
        print(f"Market closed — regular hours are {market_open.strftime('%I:%M %p')} to {market_close.strftime('%I:%M %p')} ET.")
        return False

# -------------------------
# Buy / Sell functions using client wrappers + DB logging
# -------------------------
def log_trade_csv(date_str, buy, sell, qty, symbol, price):
    with open(CSV_FILENAME, mode='a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writerow({'Date': date_str, 'Buy': buy, 'Sell': sell, 'Quantity': qty, 'Symbol': symbol, 'Price Per Share': price})

def record_db_trade(action, symbol, qty, price, date_str):
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
        logging.error(f"DB error in record_db_trade: {e}")
        session.rollback()
    finally:
        session.close()

# -------------------------
# Sell logic: sell yesterday's buys if price >= bought*1.005
# -------------------------
def sell_yesterdays_positions_if_up(account_id):
    now = datetime.now(eastern)
    yesterday = (now.date() - timedelta(days=1)).strftime("%Y-%m-%d")
    db = get_db_session()
    try:
        positions = db.query(Position).all()
        for p in positions:
            sym = p.symbols
            if p.purchase_date == yesterday:
                current_price = client_get_quote(sym)
                if current_price is None:
                    logging.info(f"No quote for {sym} while checking yesterday sell rule.")
                    continue
                if current_price >= float(p.avg_price) * 1.005:
                    logging.info(f"Selling {sym} bought {p.purchase_date} (avg {p.avg_price}) at current {current_price}")
                    order = client_place_market_order(sym, 'SELL', p.quantity)
                    now_str = datetime.now(eastern).strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
                    if order is not None:
                        # log & DB cleanup
                        log_trade_csv(now_str, '', 'Sell', p.quantity, sym, current_price)
                        record_db_trade('sell', sym, p.quantity, current_price, now.date().strftime("%Y-%m-%d"))
    except Exception as e:
        logging.error(f"Error in sell_yesterdays_positions_if_up: {e}")
    finally:
        db.close()

# -------------------------
# Buy logic (detailed scoring; uses yfinance for indicators)
# -------------------------
def buy_stocks(account_id, symbols_to_buy_list):
    if not symbols_to_buy_list:
        logging.info("No symbols to buy.")
        return

    # Refresh account for cash/exposure
    account = client_get_account()
    cash_available = float(account.get('cash', 0))
    # compute current exposure via client positions
    portfolio_positions = client_list_positions()
    current_exposure = sum((p.get('qty', 0) * (client_get_quote(p.get('symbol')) or 0)) for p in portfolio_positions)
    total_equity = cash_available + current_exposure
    max_new_exposure = total_equity * 0.98 - current_exposure
    logging.info(f"Equity={total_equity:.2f}, Cash={cash_available:.2f}, Current exposure={current_exposure:.2f}, Max new exposure={max_new_exposure:.2f}")
    if max_new_exposure <= 0:
        logging.info("Exposure cap reached; skipping buys.")
        return

    # Filter symbols that have live quotes and historical candles
    valid_symbols = []
    for sym in symbols_to_buy_list:
        q = client_get_quote(sym)
        if q is None:
            logging.info(f"No quote for {sym}; skipping.")
            continue
        hist = calculate_technical_indicators(sym, lookback_days=5, interval='5m')
        if hist is None or hist.empty or len(hist) < 5:
            logging.info(f"Insufficient history for {sym}; skipping.")
            continue
        valid_symbols.append(sym)
    logging.info(f"Valid symbols for buy pass: {valid_symbols}")
    if not valid_symbols:
        return

    for sym in valid_symbols:
        try:
            now = datetime.now(eastern)
            now_str = now.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
            today_date_str = now.date().strftime("%Y-%m-%d")
            logging.info(f"Processing {sym} for buy checks")

            current_price = client_get_quote(sym)
            if current_price is None:
                continue

            # price history store
            ts = time.time()
            if sym not in price_history:
                price_history[sym] = {k: [] for k in interval_map}
                last_stored[sym] = {k: 0 for k in interval_map}
            for k, delta in interval_map.items():
                if ts - last_stored[sym][k] >= delta:
                    price_history[sym][k].append(current_price)
                    last_stored[sym][k] = ts
                    logging.info(f"Stored price {current_price:.4f} for {sym} at {k}")

            # historical candles
            hist = calculate_technical_indicators(sym, lookback_days=5, interval='5m')
            close_prices = hist['Close'].values
            open_prices = hist['Open'].values
            high_prices = hist['High'].values
            low_prices = hist['Low'].values
            # indicators
            macd, macd_signal, _ = talib.MACD(close_prices, fastperiod=12, slowperiod=26, signalperiod=9)
            rsi_series = talib.RSI(close_prices, timeperiod=14)
            latest_macd = float(macd[-1]) if len(macd) > 0 else None
            latest_macd_signal = float(macd_signal[-1]) if len(macd_signal) > 0 else None
            latest_rsi = float(rsi_series[-1]) if len(rsi_series) > 0 else None
            logging.info(f"{sym}: current={current_price:.4f}, MACD={latest_macd}, MACD_sig={latest_macd_signal}, RSI={latest_rsi}")

            # volume & previous
            recent_avg_volume = hist['Volume'].iloc[-5:].mean() if len(hist) >= 5 else 0
            prior_avg_volume = hist['Volume'].iloc[-10:-5].mean() if len(hist) >= 10 else recent_avg_volume
            volume_decrease = recent_avg_volume < prior_avg_volume if len(hist) >= 10 else False
            logging.info(f"{sym}: recent_avg_vol={recent_avg_volume:.0f}, prior_avg_vol={prior_avg_volume:.0f}, vol_decrease={volume_decrease}")

            # last price within 5m using yfinance 1m fallback
            last5 = get_last_price_within_past_5_minutes([sym]).get(sym)
            if last5 is None:
                try:
                    last5 = float(yf.Ticker(yf_symbol(sym)).history(period='1d')['Close'].iloc[-1])
                except Exception:
                    last5 = current_price

            price_decline_threshold = last5 * (1 - 0.002)
            price_decline = current_price <= price_decline_threshold

            # candlestick detection (lookback last 20 candles)
            bullish_reversal_detected = False
            detected_patterns = []
            for i in range(-1, -21, -1):
                if len(hist) < abs(i):
                    continue
                try:
                    patterns = {
                        'Hammer': talib.CDLHAMMER(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1], close_prices[:i + 1])[i] != 0,
                        'Bullish Engulfing': talib.CDLENGULFING(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1], close_prices[:i + 1])[i] > 0
                    }
                    current_detected = [name for name, det in patterns.items() if det]
                    if current_detected:
                        bullish_reversal_detected = True
                        detected_patterns = current_detected
                        logging.info(f"{sym}: detected patterns {detected_patterns} at offset {i}")
                        break
                except Exception as e:
                    continue

            # trend & daily RSI
            if not is_in_uptrend(sym):
                logging.info(f"{sym} not in uptrend; skip")
                continue
            daily_rsi = get_daily_rsi(sym)
            if daily_rsi is None or daily_rsi > 50:
                logging.info(f"{sym} daily RSI {daily_rsi} too high; skip")
                continue

            # scoring
            score = 0
            if bullish_reversal_detected:
                score += 2
                price_stable = True
                if sym in price_history and '5min' in price_history[sym] and len(price_history[sym]['5min']) >= 2:
                    recent_prices = price_history[sym]['5min'][-2:]
                    if recent_prices[1] and recent_prices[0]:
                        price_stable = abs(recent_prices[-1] - recent_prices[-2]) / recent_prices[-2] < 0.005
                if price_stable:
                    score += 1
                if latest_macd is not None and latest_macd_signal is not None and latest_macd > latest_macd_signal:
                    score += 1
                if not volume_decrease:
                    score += 1
                if latest_rsi is not None and latest_rsi < 40:
                    score += 1
                # pattern-specific adjustments
                for pat in detected_patterns:
                    if pat == 'Bullish Engulfing' and recent_avg_volume > 1.5 * prior_avg_volume:
                        score += 1
                    if pat == 'Hammer' and latest_rsi is not None and latest_rsi < 35:
                        score += 1

            logging.info(f"{sym}: final buy score={score}")
            if score < 3:
                continue

            # sizing
            if ALL_BUY_ORDERS_ARE_1_DOLLAR:
                total_cost = 1.00
                qty = round(total_cost / current_price, 4)
            else:
                atr = get_average_true_range(sym)
                if atr is None or atr <= 0:
                    continue
                stop_loss_distance = 2 * atr
                risk_amount = 0.01 * max(1.0, total_equity)
                qty = risk_amount / stop_loss_distance if stop_loss_distance > 0 else 0
                total_cost = qty * current_price
                # cap by cash and exposure
                with threading.Lock():
                    account_now = client_get_account()
                    cash_now = float(account_now.get('cash', 0))
                total_cost = min(total_cost, cash_now - 1.00 if cash_now > 1.00 else 0, max_new_exposure)
                if total_cost < 1.00:
                    logging.info(f"{sym}: not enough allocation (notional {total_cost:.2f})")
                    continue
                qty = round(total_cost / current_price, 4)
                # slippage estimate
                total_cost *= 0.999
                qty = round(total_cost / current_price, 4)

            if qty <= 0:
                continue

            # final cash check
            with threading.Lock():
                account_now = client_get_account()
                cash_now = float(account_now.get('cash', 0))
            if cash_now < total_cost + 1.00:
                logging.info(f"{sym}: insufficient cash {cash_now:.2f} for order notional {total_cost:.2f}")
                continue

            # place order
            logging.info(f"Placing BUY for {sym}: qty={qty} notional={total_cost:.2f}")
            order_resp = client_place_market_order(sym, 'BUY', qty)
            nowstamp = datetime.now(eastern).strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
            if order_resp is not None:
                # log csv + DB
                log_trade_csv(nowstamp, 'Buy', '', qty, sym, current_price)
                record_db_trade('buy', sym, qty, current_price, today_date_str)
                logging.info(f"Buy order placed for {sym}, resp={order_resp}")
            else:
                logging.error(f"Buy order failed for {sym}")

        # end for valid symbols
    # end buy_stocks

# -------------------------
# Account summary print for main loop
# -------------------------
def print_account_summary():
    now = datetime.now(eastern)
    acc = client_get_account()
    print(f"\n=== Account Summary @ {now.strftime('%Y-%m-%d %I:%M:%S %p %Z')} ===")
    print(f"Equity: ${acc.get('equity', 0):.2f} | Cash: ${acc.get('cash', 0):.2f}")
    # day trades: fallback to DB or raw account data
    positions = client_list_positions()
    # gather day trade count if available in raw acc
    try:
        raw = acc.get('raw', {}) if isinstance(acc, dict) else {}
        dt = raw.get('day_trade_count') or raw.get('dayTradeCount') or raw.get('pattern_day_trader_count') or 'N/A'
    except Exception:
        dt = 'N/A'
    print(f"Day trades (recent): {dt}")
    print("\nHeld positions:")
    for p in positions:
        cur = client_get_quote(p['symbol']) or 0.0
        pct = ((cur - p['avg_price']) / p['avg_price']) * 100 if p['avg_price'] else 0
        print(f"{p['symbol']} | Qty: {p['qty']:.4f} | Avg: {p['avg_price']:.4f} | Current: {cur:.4f} | Change: {pct:+.2f}%")

# -------------------------
# Main trading loop (orchestrates)
# -------------------------
def trading_robot(interval=60):
    symbols_to_buy_list = load_symbols_from_file()
    if not symbols_to_buy_list:
        print(f"No symbols loaded from {SYMBOLS_FILE}. Exiting.")
        return

    account_id = get_account_id()
    if not account_id:
        logging.warning("No account id detected; continuing but some REST calls may fail.")

    while True:
        try:
            if market_is_open():
                print_account_summary()
                # sell yesterday's profitable positions first
                sell_yesterdays_positions_if_up(account_id)
                # run buy pass
                buy_stocks(account_id, symbols_to_buy_list)

                # optional: print DB contents
                if PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE:
                    session = get_db_session()
                    try:
                        print("\nTrade History (DB):")
                        for r in session.query(TradeHistory).order_by(TradeHistory.id).all():
                            print(f"{r.id} | {r.symbols} | {r.action} | {r.quantity:.4f} | {r.price:.4f} | {r.date}")
                        print("\nPositions (DB):")
                        for r in session.query(Position).all():
                            cur = client_get_quote(r.symbols) or 0.0
                            pct = ((cur - r.avg_price) / r.avg_price) * 100 if r.avg_price else 0
                            print(f"{r.symbols} | {r.quantity:.4f} | {r.avg_price:.4f} | {r.purchase_date} | Change: {pct:+.2f}%")
                    finally:
                        session.close()
            else:
                # market_is_open prints a message about hours
                pass

            time.sleep(interval)

        except KeyboardInterrupt:
            print("Trading robot stopped by user.")
            logging.info("Stopped by user.")
            break
        except Exception as e:
            logging.error(f"Unexpected error in main loop: {e}", exc_info=True)
            print(f"Unexpected error: {e}")
            time.sleep(interval)

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    trading_robot(interval=120)
