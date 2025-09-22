import os
import time
import csv
import logging
import threading
import schedule
from uuid import uuid4
from datetime import datetime, timedelta, date, timezone
from datetime import time as time2
import pytz
import requests
import urllib.parse
import json
import sys
import yfinance as yf
import talib
import pandas_market_calendars as mcal
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from requests.exceptions import HTTPError, ConnectionError, Timeout
from ratelimit import limits, sleep_and_retry
import numpy as np
import pandas as pd
import traceback
from filelock import FileLock

# ANSI color codes
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
RESET = "\033[0m"

# Configuration flags
PRINT_SYMBOLS_TO_BUY = False
PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE = True
PRINT_DATABASE = True
DEBUG = False
ALL_BUY_ORDERS_ARE_5_DOLLARS = False
FRACTIONAL_BUY_ORDERS = True

# Global variables
YOUR_SECRET_KEY = os.getenv("YOUR_SECRET_KEY")
CALLMEBOT_API_KEY = os.getenv("CALLMEBOT_API_KEY")
CALLMEBOT_PHONE = os.getenv("CALLMEBOT_PHONE")
CSV_PATH = os.getenv("TRADING_BOT_CSV_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'log-file-of-buy-and-sell-signals.csv'))
CSV_LOCK_PATH = CSV_PATH + ".lock"
ALERTS_PATH = os.getenv("TRADING_BOT_ALERTS_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), 'alerts.log'))
ALERTS_LOCK_PATH = ALERTS_PATH + ".lock"
secret = None
access_token = None
account_id = None
last_token_fetch_time = None
BASE_URL = "https://api.public.com/userapigateway"
HEADERS = None
symbols_to_buy = []
symbols_to_sell_dict = {}
today_date = datetime.today().date()
today_datetime = datetime.now(pytz.timezone('US/Eastern'))
csv_filename = CSV_PATH
fieldnames = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']
price_changes = {}
current_price = 0
today_date_str = today_date.strftime("%Y-%m-%d")
qty = 0
price_history = {}
last_stored = {}
interval_map = {
    '1min': 60,
    '5min': 300,
    '10min': 600,
    '15min': 900,
    '30min': 1800,
    '45min': 2700,
    '60min': 3600
}
stock_data = {}
previous_prices = {}
buy_sell_lock = threading.Lock()
yf_lock = threading.Lock()
data_cache = {}
csv_lock = FileLock(CSV_LOCK_PATH, timeout=10)
alerts_lock = FileLock(ALERTS_LOCK_PATH, timeout=10)
CACHE_EXPIRY = 120
CALLS = 10
PERIOD = 1
RETRY_COUNT = 5
db_lock = threading.Lock()
price_history_lock = threading.Lock()
task_running = {
    'buy_stocks': False,
    'sell_stocks': False,
    'check_price_moves': False,
    'check_stop_order_status': False,
    'monitor_stop_losses': False,
    'sync_db_with_api': False,
    'refresh_token_if_needed': False
}

# Timezone
eastern = pytz.timezone('US/Eastern')

# Logging configuration
logging.basicConfig(filename='trading-bot-program-logging-messages.txt', level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(message)s')

# Initialize CSV file
def initialize_csv():
    try:
        with csv_lock:
            os.makedirs(os.path.dirname(csv_filename), exist_ok=True)
            if not os.path.exists(csv_filename):
                with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
                    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                    csv_writer.writeheader()
                print(f"Initialized CSV file at {csv_filename}")
                logging.info(f"Initialized CSV file at {csv_filename}")
            else:
                print(f"CSV file already exists at {csv_filename}")
                logging.info(f"CSV file already exists at {csv_filename}")
    except Exception as e:
        logging.error(f"Failed to initialize CSV file at {csv_filename}: {e}")
        print(f"Failed to initialize CSV file at {csv_filename}: {e}")

# Initialize alerts log
def initialize_alerts_log():
    try:
        with alerts_lock:
            os.makedirs(os.path.dirname(ALERTS_PATH), exist_ok=True)
            if not os.path.exists(ALERTS_PATH):
                with open(ALERTS_PATH, mode='w', encoding='utf-8') as f:
                    f.write("Timestamp,Subject,Message\n")
                print(f"Initialized alerts log at {ALERTS_PATH}")
                logging.info(f"Initialized alerts log at {ALERTS_PATH}")
            else:
                print(f"Alerts log already exists at {ALERTS_PATH}")
                logging.info(f"Alerts log already exists at {ALERTS_PATH}")
    except Exception as e:
        logging.error(f"Failed to initialize alerts log at {ALERTS_PATH}: {e}")
        print(f"Failed to initialize alerts log at {ALERTS_PATH}: {e}")

# Send alert with thread-safe file logging
def send_alert(message, subject="Trading Bot Alert"):
    timestamp = datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with alerts_lock:
            with open(ALERTS_PATH, mode='a', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([timestamp, subject, message])
        print(f"Alert logged: {subject} - {message}")
        logging.info(f"Alert logged: {subject} - {message}")
    except Exception as e:
        logging.error(f"Failed to log alert to {ALERTS_PATH}: {e}")
        print(f"Failed to log alert to {ALERTS_PATH}: {e}")
    try:
        if CALLMEBOT_API_KEY and CALLMEBOT_PHONE:
            encoded_message = urllib.parse.quote(f"{subject}: {message}")
            url = f"https://api.callmebot.com/whatsapp.php?phone={CALLMEBOT_PHONE}&text={encoded_message}&apikey={CALLMEBOT_API_KEY}"
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            print(f"Alert sent via WhatsApp: {subject} - {message}")
            logging.info(f"Alert sent via WhatsApp: {subject} - {message}")
    except Exception as e:
        logging.error(f"Failed to send WhatsApp alert: {e}")
        print(f"Failed to send WhatsApp alert: {e}")

# Database Models
Base = declarative_base()

class TradeHistory(Base):
    __tablename__ = 'trade_history'
    id = Column(Integer, primary_key=True)
    symbols = Column(String, nullable=False)
    action = Column(String, nullable=False)
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    date = Column(String, nullable=False)

class Position(Base):
    __tablename__ = 'positions'
    symbols = Column(String, primary_key=True, nullable=False)
    quantity = Column(Float, nullable=False)
    avg_price = Column(Float, nullable=False)
    purchase_date = Column(String, nullable=False)
    stop_order_id = Column(String, nullable=True)
    stop_price = Column(Float, nullable=True)

# Initialize SQLAlchemy
def initialize_database():
    engine = create_engine('sqlite:///trading_bot.db', connect_args={"check_same_thread": False})
    with engine.connect() as conn:
        conn.execute(text("PRAGMA journal_mode=WAL;"))
    SessionLocal = scoped_session(sessionmaker(bind=engine))
    Base.metadata.create_all(engine)
    return SessionLocal

SessionLocal = initialize_database()

# NYSE Calendar
nyse_cal = mcal.get_calendar('NYSE')

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_cached_data(symbols, data_type, fetch_func, *args, **kwargs):
    key = (symbols, data_type)
    current_time = time.time()
    if key in data_cache and current_time - data_cache[key]['timestamp'] < CACHE_EXPIRY:
        data = data_cache[key]['data']
        if data is None or isinstance(data, (list, dict)) and not data:
            print(f"Invalid cached data for {symbols} {data_type}. Fetching new data...")
        else:
            print(f"Using cached {data_type} for {symbols}.")
            return data
    print(f"Fetching new {data_type} for {symbols}...")
    data = fetch_func(*args, **kwargs)
    data_cache[key] = {'timestamp': current_time, 'data': data}
    print(f"Cached {data_type} for {symbols}.")
    return data

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_get_quote(symbol, retries=3):
    for attempt in range(retries):
        try:
            return get_cached_data(symbol, 'current_price_public', _fetch_current_price_public, symbol)
        except Exception as e:
            if attempt == retries - 1:
                logging.error(f"All retries failed for {symbol}: {e}")
                return None
            time.sleep(2 ** attempt)
    return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def _fetch_current_price_public(symbol):
    if not account_id:
        raise ValueError("No account_id available")
    url = f"{BASE_URL}/marketdata/{account_id}/quotes"
    request_body = {
        "instruments": [
            {"symbol": symbol, "type": "EQUITY"}
        ]
    }
    response = requests.post(url, headers=HEADERS, json=request_body, timeout=5)
    response.raise_for_status()
    data = response.json()
    quotes = data.get("quotes", [])
    if quotes and quotes[0].get("outcome") == "SUCCESS":
        last = float(quotes[0].get("last", 0))
        price_color = GREEN if last >= 0 else RED
        print(f"Public.com last price for {symbol}: {price_color}${last:.4f}{RESET}")
        return round(last, 4)
    else:
        raise ValueError("No successful quote from Public.com")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def place_market_order(symbol, side, fractional=False, amount=None, quantity=None):
    url = f"{BASE_URL}/trading/{account_id}/order"
    order_id = str(uuid4())
    is_fractional = fractional or (amount is not None) or (quantity is not None and quantity % 1 != 0)
    expiration = {"timeInForce": "DAY"}
    payload = {
        "orderId": order_id,
        "instrument": {"symbol": symbol, "type": "EQUITY"},
        "orderSide": side.upper(),
        "orderType": "MARKET",
        "expiration": expiration
    }
    if amount is not None:
        payload["amount"] = f"{amount:.2f}"
    elif quantity is not None:
        if not is_fractional:
            quantity = int(quantity)
            payload["quantity"] = str(quantity)
        else:
            payload["quantity"] = f"{quantity:.4f}"
    else:
        raise ValueError("Must provide 'amount' for fractional orders or 'quantity' for full-share orders")
    try:
        response = requests.post(url, headers=HEADERS, json=payload, timeout=10)
        if response.status_code >= 400:
            print(f"HTTP Error Response for {symbol}: {response.status_code} {response.text}")
            logging.error(f"HTTP Error Response for {symbol}: {response.status_code} {response.text}")
            return {"error": f"HTTP {response.status_code}: {response.text}"}
        response.raise_for_status()
        logging.info(f"Order placed successfully for {symbol}: {response.json()}")
        return response.json()
    except Exception as e:
        print(f"ERROR placing order for {symbol}:")
        logging.error(f"Error placing order for {symbol}: {e}")
        traceback.print_exc()
        return {"error": str(e)}

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_place_order(symbol, side, amount=None, quantity=None, order_type="MARKET", stop_price=None):
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId")
            return None
        logging.info(f"Placing {side} order for {symbol}: amount={amount}, quantity={quantity}, order_type={order_type}, stop_price={stop_price}")
        if order_type == "MARKET":
            order_response = place_market_order(
                symbol=symbol,
                side=side,
                fractional=FRACTIONAL_BUY_ORDERS if amount is not None else False,
                amount=amount,
                quantity=quantity
            )
        else:  # STOP
            url = f"{BASE_URL}/trading/{account_id}/order"
            order_id = str(uuid4())
            payload = {
                "orderId": order_id,
                "instrument": {"symbol": symbol, "type": "EQUITY"},
                "orderSide": side.upper(),
                "orderType": "STOP",
                "stopPrice": stop_price,
                "quantity": f"{quantity:.4f}" if quantity % 1 != 0 else str(int(quantity)),
                "expiration": {"timeInForce": "GTD", "expirationTime": get_expiration()}
            }
            response = requests.post(url, headers=HEADERS, json=payload, timeout=10)
            if response.status_code >= 400:
                print(f"HTTP Error Response for {symbol}: {response.status_code} {response.text}")
                logging.error(f"HTTP Error Response for {symbol}: {response.status_code} {response.text}")
                return {"error": f"HTTP {response.status_code}: {response.text}"}
            response.raise_for_status()
            order_response = response.json()
            logging.info(f"Stop sell order placed successfully for {symbol}: {order_response}")
        if order_response.get('error'):
            logging.error(f"Order placement error for {symbol}: {order_response['error']}")
            return None
        order_id = order_response.get('orderId')
        if amount is not None:
            logging.info(f"Order placed: {side} ${amount:.2f} of {symbol}, Order ID: {order_id}")
        else:
            logging.info(f"Order placed: {side} {quantity} shares of {symbol}, Order ID: {order_id}")
        return order_id
    except Exception as e:
        logging.error(f"Order placement error for {symbol}: {e}")
        return None

def get_expiration():
    exp = datetime.now(timezone.utc) + timedelta(days=30)
    if exp.weekday() == 5:
        exp += timedelta(days=2)
    elif exp.weekday() == 6:
        exp += timedelta(days=1)
    return exp.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_get_order_status(order_id):
    try:
        if not account_id or not order_id:
            logging.error("No account_id or order_id")
            return None
        url = f"{BASE_URL}/trading/{account_id}/order/{order_id}"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        order_data = resp.json()
        status = order_data.get("status")
        filled_qty = float(order_data.get("filledQuantity", 0))
        avg_price = float(order_data.get("averagePrice", 0)) if order_data.get("averagePrice") else None
        price_color = GREEN if avg_price and avg_price >= 0 else RED
        print(f"Order {order_id} status: {status}, filled: {filled_qty}, avg price: {price_color}${avg_price:.2f}{RESET}")
        logging.info(f"Order {order_id} status: {status}, filled: {filled_qty}, avg price: {avg_price}")
        return {
            "status": status,
            "filled_qty": filled_qty,
            "avg_price": avg_price
        }
    except Exception as e:
        logging.error(f"Order status fetch error for {order_id}: {e}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_list_all_orders():
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId")
            return []
        url = f"{BASE_URL}/trading/{account_id}/portfolio/v2"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        all_orders = data.get('orders', [])
        print(f"Retrieved {len(all_orders)} total orders.")
        return all_orders
    except Exception as e:
        logging.error(f"Error listing orders: {e}")
        return []

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_cancel_order(order):
    order_id = order.get('orderId') or order.get('id')
    symbol = order.get('instrument', {}).get('symbol')
    cancel_url = f"{BASE_URL}/trading/{account_id}/order/{order_id}"
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.delete(cancel_url, headers=HEADERS, timeout=10)
            resp.raise_for_status()
            print(f"Cancelled order {order_id} ({symbol})")
            logging.info(f"Cancelled order {order_id} ({symbol})")
            return True
        except Exception as e:
            print(f"Attempt {attempt} failed to cancel order {order_id} ({symbol}): {e}")
            logging.error(f"Attempt {attempt} failed to cancel order {order_id} ({symbol}): {e}")
            if attempt < RETRY_COUNT:
                time.sleep(2 ** attempt)
            else:
                print(f"Giving up on order {order_id} after {RETRY_COUNT} attempts.")
                logging.error(f"Giving up on order {order_id} after {RETRY_COUNT} attempts.")
                return False

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_list_positions():
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId")
            return []
        url = f"{BASE_URL}/trading/{account_id}/portfolio/v2"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        positions = data.get('positions', [])
        print(f"Retrieved {len(positions)} positions.")
        return [
            {
                'symbol': p['instrument']['symbol'],
                'qty': float(p['quantity']),
                'avg_entry_price': float(p['averagePrice'])
            }
            for p in positions
        ]
    except Exception as e:
        logging.error(f"Error listing positions: {e}")
        return []

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_get_account():
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId")
            return {}
        url = f"{BASE_URL}/trading/{account_id}/portfolio/v2"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        equity = float(data.get('equity', {}).get('total', 0))
        buying_power_cash = float(data.get('cash', {}).get('amount', 0))
        return {
            'equity': equity,
            'buying_power_cash': buying_power_cash
        }
    except Exception as e:
        logging.error(f"Error fetching account info: {e}")
        return {}

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def fetch_token_and_account():
    """Fetch new access token and brokerage account ID using YOUR_SECRET_KEY."""
    global access_token, account_id, HEADERS, last_token_fetch_time
    try:
        resp = requests.post(
            "https://api.public.com/userapiauthservice/personal/access-tokens",
            headers={"Content-Type": "application/json"},
            json={"secret": YOUR_SECRET_KEY, "validityInMinutes": 1440},
            timeout=10
        )
        print("Token endpoint response:", resp.status_code, resp.text)
        resp.raise_for_status()
        access_token = resp.json().get("accessToken")
        if not access_token:
            raise ValueError("No access token returned")
        resp = requests.get(
            f"{BASE_URL}/trading/account",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            timeout=10
        )
        print("Account endpoint response:", resp.status_code, resp.text)
        resp.raise_for_status()
        accounts = resp.json().get("accounts", [])
        brokerage = next((a for a in accounts if a.get("accountType") == "BROKERAGE"), None)
        if not brokerage:
            raise ValueError("No BROKERAGE account found")
        account_id = brokerage["accountId"]
        HEADERS = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        last_token_fetch_time = datetime.now()
        print(f"Access token and brokerage account fetched: {account_id}")
        logging.info(f"Access token and brokerage account fetched: {account_id}")
        return True
    except Exception as e:
        print("Error fetching token/account:", e)
        logging.error("Error fetching token/account:")
        traceback.print_exc()
        return False

def refresh_token_if_needed():
    if task_running['refresh_token_if_needed']:
        print("refresh_token_if_needed already running. Skipping.")
        logging.info("refresh_token_if_needed already running. Skipping")
        return
    task_running['refresh_token_if_needed'] = True
    try:
        current_time = time.time()
        if last_token_fetch_time and current_time - last_token_fetch_time > 3600:
            print("Token expired. Refreshing token...")
            logging.info("Token expired. Refreshing token...")
            if fetch_token_and_account():
                print("Token refreshed successfully.")
                logging.info("Token refreshed successfully.")
            else:
                print("Failed to refresh token.")
                logging.error("Failed to refresh token.")
    except Exception as e:
        logging.error(f"Error in refresh_token_if_needed: {e}")
        print(f"Error in refresh_token_if_needed: {e}")
    finally:
        task_running['refresh_token_if_needed'] = False

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def rate_limited_yf_history(symbol):
    with yf_lock:
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period="1mo", interval="1d")
            if df.empty:
                print(f"No historical data for {symbol}")
                logging.info(f"No historical data for {symbol}")
                return pd.DataFrame()
            return df
        except Exception as e:
            logging.error(f"Error fetching Yahoo Finance history for {symbol}: {e}")
            return pd.DataFrame()

def get_last_price_within_past_5_minutes(symbols):
    return {sym: client_get_quote(sym) for sym in symbols}

def get_last_price_within_past_5_days(symbols):
    prices = {}
    for sym in symbols:
        df = rate_limited_yf_history(sym)
        if not df.empty and 'Close' in df.columns:
            prices[sym] = df['Close'].iloc[-1]
        else:
            prices[sym] = None
    return prices

def is_in_uptrend(symbol):
    try:
        df = rate_limited_yf_history(symbol)
        if df.empty or len(df) < 20:
            return False
        close = df['Close'].values
        sma_short = talib.SMA(close, timeperiod=10)
        sma_long = talib.SMA(close, timeperiod=20)
        if len(sma_short) < 1 or len(sma_long) < 1:
            return False
        return sma_short[-1] > sma_long[-1]
    except Exception as e:
        logging.error(f"Error checking uptrend for {symbol}: {e}")
        return False

def get_previous_price(symbol):
    with price_history_lock:
        if symbol in price_history and '1min' in price_history[symbol] and len(price_history[symbol]['1min']) > 1:
            return price_history[symbol]['1min'][-2]
    return rate_limited_get_quote(symbol) or 0

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def rate_limited_get_quote(symbol):
    return client_get_quote(symbol)

def load_positions_from_database():
    positions_dict = {}
    with db_lock:
        session = SessionLocal()
        try:
            positions = session.query(Position).all()
            for pos in positions:
                positions_dict[pos.symbols] = (pos.avg_price, pos.purchase_date)
            if PRINT_DATABASE:
                print(f"\nPositions in database: {len(positions)}")
                for pos in positions:
                    print(f"Symbol: {pos.symbols}, Qty: {pos.quantity:.4f}, Avg Price: ${pos.avg_price:.2f}, Date: {pos.purchase_date}, Stop Order ID: {pos.stop_order_id}, Stop Price: {pos.stop_price}")
                logging.info(f"Positions in database: {len(positions)}")
        except Exception as e:
            logging.error(f"Error loading positions from database: {e}")
        finally:
            session.close()
    return positions_dict

def sync_db_with_api():
    if task_running['sync_db_with_api']:
        print("sync_db_with_api already running. Skipping.")
        logging.info("sync_db_with_api already running. Skipping")
        return
    task_running['sync_db_with_api'] = True
    try:
        api_positions = client_list_positions()
        with db_lock:
            session = SessionLocal()
            try:
                db_positions = session.query(Position).all()
                db_symbols = {pos.symbols for pos in db_positions}
                api_symbols = {pos['symbol'] for pos in api_positions}
                for pos in api_positions:
                    symbol = pos['symbol']
                    qty = pos['qty']
                    avg_price = pos['avg_entry_price']
                    if symbol not in db_symbols:
                        position = Position(
                            symbols=symbol,
                            quantity=qty,
                            avg_price=avg_price,
                            purchase_date=today_date_str
                        )
                        session.add(position)
                    else:
                        db_pos = session.query(Position).filter_by(symbols=symbol).first()
                        db_pos.quantity = qty
                        db_pos.avg_price = avg_price
                for db_pos in db_positions:
                    if db_pos.symbols not in api_symbols:
                        session.delete(db_pos)
                session.commit()
                print("Database synchronized with API positions.")
                logging.info("Database synchronized with API positions.")
            except Exception as e:
                session.rollback()
                logging.error(f"Error syncing database with API: {e}")
            finally:
                session.close()
    except Exception as e:
        logging.error(f"Error in sync_db_with_api: {e}")
    finally:
        task_running['sync_db_with_api'] = False

def get_symbols_to_buy():
    return ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'BRK.B', 'JPM', 'V']

def ensure_no_open_orders(symbol):
    all_orders = client_list_all_orders()
    for order in all_orders:
        if order.get('instrument', {}).get('symbol') == symbol and order.get('status') in ['NEW', 'OPEN', 'PARTIALLY_FILLED']:
            print(f"Open order found for {symbol}. Cancelling...")
            logging.info(f"Open order found for {symbol}. Cancelling...")
            client_cancel_order(order)
            time.sleep(1)
            return False
    return True

def place_stop_loss_order(symbol, quantity, filled_price):
    try:
        stop_price = filled_price * 0.95
        order_id = client_place_order(
            symbol=symbol,
            side="SELL",
            quantity=quantity,
            order_type="STOP",
            stop_price=stop_price
        )
        if order_id:
            print(f"Stop loss order placed for {quantity:.4f} shares of {symbol} at ${stop_price:.2f}")
            logging.info(f"Stop loss order placed for {quantity:.4f} shares of {symbol} at ${stop_price:.2f}")
            return order_id, stop_price
        else:
            print(f"Failed to place stop loss order for {symbol}.")
            logging.info(f"Failed to place stop loss order for {symbol}.")
            return None, None
    except Exception as e:
        logging.error(f"Error placing stop loss order for {symbol}: {e}")
        return None, None

def check_stop_order_status():
    if task_running['check_stop_order_status']:
        print("check_stop_order_status already running. Skipping.")
        logging.info("check_stop_order_status already running. Skipping")
        return
    task_running['check_stop_order_status'] = True
    try:
        with db_lock:
            session = SessionLocal()
            try:
                positions = session.query(Position).filter(Position.stop_order_id.isnot(None)).all()
                for pos in positions:
                    status_info = client_get_order_status(pos.stop_order_id)
                    if status_info and status_info["status"] == "FILLED":
                        filled_qty = status_info["filled_qty"]
                        filled_price = status_info["avg_price"] or rate_limited_get_quote(pos.symbols)
                        trade = TradeHistory(
                            symbols=pos.symbols,
                            action='sell',
                            quantity=filled_qty,
                            price=filled_price,
                            date=today_date_str
                        )
                        session.add(trade)
                        pos.quantity -= filled_qty
                        if pos.quantity <= 0:
                            session.delete(pos)
                        else:
                            pos.stop_order_id = None
                            pos.stop_price = None
                        session.commit()
                        # CSV and alert removed from scheduled task
                        print(f"Stop loss triggered for {pos.symbols}: Sold {filled_qty:.4f} shares at ${filled_price:.2f}")
                        logging.info(f"Stop loss triggered for {pos.symbols}: Sold {filled_qty:.4f} shares at ${filled_price:.2f}")
            except Exception as e:
                session.rollback()
                logging.error(f"Error checking stop order status: {e}")
            finally:
                session.close()
    except Exception as e:
        logging.error(f"Error in check_stop_order_status: {e}")
    finally:
        task_running['check_stop_order_status'] = False

def monitor_stop_losses():
    if task_running['monitor_stop_losses']:
        print("monitor_stop_losses already running. Skipping.")
        logging.info("monitor_stop_losses already running. Skipping")
        return
    task_running['monitor_stop_losses'] = True
    try:
        with db_lock:
            session = SessionLocal()
            try:
                positions = session.query(Position).filter(Position.stop_price.isnot(None)).all()
                for pos in positions:
                    current_price = rate_limited_get_quote(pos.symbols)
                    if current_price and current_price <= pos.stop_price:
                        order_id = client_place_order(
                            symbol=pos.symbols,
                            side="SELL",
                            quantity=pos.quantity
                        )
                        if order_id:
                            status_info = client_get_order_status(order_id)
                            if status_info and status_info["status"] == "FILLED":
                                filled_qty = status_info["filled_qty"]
                                filled_price = status_info["avg_price"] or current_price
                                trade = TradeHistory(
                                    symbols=pos.symbols,
                                    action='sell',
                                    quantity=filled_qty,
                                    price=filled_price,
                                    date=today_date_str
                                )
                                session.add(trade)
                                pos.quantity -= filled_qty
                                if pos.quantity <= 0:
                                    session.delete(pos)
                                else:
                                    pos.stop_order_id = None
                                    pos.stop_price = None
                                session.commit()
                                # CSV and alert removed from scheduled task
                                print(f"Stop loss executed for {pos.symbols}: Sold {filled_qty:.4f} shares at ${filled_price:.2f}")
                                logging.info(f"Stop loss executed for {pos.symbols}: Sold {filled_qty:.4f} shares at ${filled_price:.2f}")
            except Exception as e:
                session.rollback()
                logging.error(f"Error monitoring stop losses: {e}")
            finally:
                session.close()
    except Exception as e:
        logging.error(f"Error in monitor_stop_losses: {e}")
    finally:
        task_running['monitor_stop_losses'] = False

def check_price_moves():
    if task_running['check_price_moves']:
        print("check_price_moves already running. Skipping.")
        logging.info("check_price_moves already running. Skipping")
        return
    task_running['check_price_moves'] = True
    try:
        symbols = get_symbols_to_buy()
        for symbol in symbols:
            current_price = rate_limited_get_quote(symbol)
            if current_price is None:
                continue
            previous_price = get_previous_price(symbol)
            if previous_price == 0:
                continue
            change_percent = ((current_price - previous_price) / previous_price) * 100
            if abs(change_percent) >= 5:
                change_color = GREEN if change_percent >= 0 else RED
                price_changes[symbol] = change_percent
                print(f"{symbol}: Price changed by {change_color}{change_percent:.2f}%{RESET}")
                logging.info(f"{symbol}: Price changed by {change_percent:.2f}%")
                # Alert removed from scheduled task
    except Exception as e:
        logging.error(f"Error in check_price_moves: {e}")
    finally:
        task_running['check_price_moves'] = False

def print_database_tables():
    if not PRINT_DATABASE:
        return
    with db_lock:
        session = SessionLocal()
        try:
            trades = session.query(TradeHistory).all()
            print(f"\nTrade History: {len(trades)} records")
            for trade in trades:
                print(f"ID: {trade.id}, Symbol: {trade.symbols}, Action: {trade.action}, Qty: {trade.quantity:.4f}, Price: ${trade.price:.2f}, Date: {trade.date}")
            logging.info(f"Trade History: {len(trades)} records")
        except Exception as e:
            logging.error(f"Error printing database tables: {e}")
        finally:
            session.close()

def stop_if_stock_market_is_closed():
    nyse = mcal.get_calendar('NYSE')
    while True:
        eastern = pytz.timezone('US/Eastern')
        current_datetime = datetime.now(eastern)
        current_time_str = current_datetime.strftime("%A, %B %d, %Y, %I:%M:%S %p")
        schedule = nyse.schedule(start_date=current_datetime.date(), end_date=current_datetime.date())
        if not schedule.empty:
            market_open = schedule.iloc[0]['market_open'].astimezone(eastern)
            market_close = schedule.iloc[0]['market_close'].astimezone(eastern)
            if market_open <= current_datetime <= market_close:
                print("Market is open. Proceeding with trading operations.")
                logging.info(f"{current_time_str}: Market is open. Proceeding with trading operations.")
                break
            else:
                print("\n")
                print('''
                *********************************************************************************
                ************ Billionaire Buying Strategy Version ********************************
                *********************************************************************************
                    2025 Edition of the Advanced Stock Market Trading Robot, Version 8 
                                https://github.com/CodeProSpecialist
                       Featuring an Accelerated Database Engine with Python 3 SQLAlchemy  
                ''')
                print(f'Current date & time (Eastern Time): {current_time_str}')
                print(f"Market is closed. Open hours: {market_open.strftime('%I:%M %p')} - {market_close.strftime('%I:%M %p')}")
                print("Waiting until Stock Market Hours to begin the Stockbot Trading Program.")
                print("\n")
                logging.info(f"{current_time_str}: Market is closed. Waiting for market open.")
                time.sleep(60)
        else:
            print("\n")
            print('''
            *********************************************************************************
            ************ Billionaire Buying Strategy Version ********************************
            *********************************************************************************
                2025 Edition of the Advanced Stock Market Trading Robot, Version 8 
                            https://github.com/CodeProSpecialist
                   Featuring an Accelerated Database Engine with Python 3 SQLAlchemy  
            ''')
            print(f'Current date & time (Eastern Time): {current_time_str}')
            print("Market is closed today (holiday or weekend).")
            print("Waiting until Stock Market Hours to begin the Stockbot Trading Program.")
            print("\n")
            logging.info(f"{current_time_str}: Market is closed today (holiday or weekend).")
            time.sleep(60)

def remove_symbols_from_trade_list(symbol):
    if symbol in symbols_to_buy:
        symbols_to_buy.remove(symbol)
        print(f"Removed {symbol} from symbols_to_buy")
        logging.info(f"Removed {symbol} from symbols_to_buy")

def buy_stocks(symbols_to_sell_dict, symbols_to_buy_list, buy_sell_lock):
    if task_running['buy_stocks']:
        print("buy_stocks already running. Skipping.")
        logging.info("buy_stocks already running. Skipping")
        return
    task_running['buy_stocks'] = True
    try:
        print("Starting buy_stocks function...")
        logging.info("Starting buy_stocks function")
        global price_history, last_stored
        if not symbols_to_buy_list:
            print("No symbols to buy.")
            logging.info("No symbols to buy.")
            return
        symbols_to_remove = []
        buy_signal = 0
        acc = client_get_account()
        total_equity = acc['equity']
        buying_power = float(acc['buying_power_cash'])
        reserved_buying_power = 5.00
        available_buying_power = buying_power - reserved_buying_power
        print(f"Total account equity: ${total_equity:.2f}, Buying power: ${buying_power:.2f}, Available after reserving ${reserved_buying_power:.2f}: ${available_buying_power:.2f}")
        logging.info(f"Total account equity: ${total_equity:.2f}, Buying power: ${buying_power:.2f}, Available after reserving ${reserved_buying_power:.2f}: ${available_buying_power:.2f}")
        if available_buying_power <= 0:
            print(f"Available buying power after reserving ${reserved_buying_power:.2f} is ${available_buying_power:.2f}. Skipping all buys.")
            logging.info(f"Available buying power after reserving ${reserved_buying_power:.2f} is ${available_buying_power:.2f}. Skipping all buys.")
            return
        positions = client_list_positions()
        current_exposure = sum(float(p['qty'] * (rate_limited_get_quote(p['symbol']) or p['avg_entry_price'])) for p in positions)
        max_new_exposure = total_equity * 0.98 - current_exposure
        exposure_color = GREEN if max_new_exposure >= 0 else RED
        print(f"Current exposure: ${current_exposure:.2f}, Max new exposure: {exposure_color}${max_new_exposure:.2f}{RESET}")
        logging.info(f"Current exposure: ${current_exposure:.2f}, Max new exposure: ${max_new_exposure:.2f}")
        if max_new_exposure <= 0:
            print("Portfolio exposure limit reached. No new buys.")
            logging.info("Portfolio exposure limit reached.")
            return
        valid_symbols = []
        for sym in symbols_to_buy_list:
            current_price = rate_limited_get_quote(sym)
            if current_price is None or current_price <= 0:
                print(f"No valid price data for {sym}. Skipping.")
                logging.info(f"No valid price data for {sym}. Skipping")
                continue
            df = rate_limited_yf_history(sym)
            if df.empty or len(df) < 14:
                print(f"Insufficient historical data for {sym} (daily rows: {len(df)}). Skipping.")
                logging.info(f"Insufficient historical data for {sym} (daily rows: {len(df)}). Skipping")
                continue
            valid_symbols.append(sym)
        print(f"Valid symbols to process: {valid_symbols}")
        logging.info(f"Valid symbols to process: {valid_symbols}")
        if not valid_symbols:
            print("No valid symbols to buy after filtering.")
            logging.info("No valid symbols to buy after filtering.")
            return
        min_5_prices = get_last_price_within_past_5_minutes(valid_symbols)
        day_5_prices = get_last_price_within_past_5_days(valid_symbols)
        if ALL_BUY_ORDERS_ARE_5_DOLLARS:
            dollar_amount = 5.0
        else:
            dollar_amount = max_new_exposure / len(valid_symbols) if len(valid_symbols) > 0 else 0
            total_planned_spend = dollar_amount * len(valid_symbols)
            if total_planned_spend > available_buying_power:
                dollar_amount = available_buying_power / len(valid_symbols) if len(valid_symbols) > 0 else 0
                print(f"Adjusted dollar_amount to ${dollar_amount:.2f} to maintain ${reserved_buying_power:.2f} buying power")
                logging.info(f"Adjusted dollar_amount to ${dollar_amount:.2f} to maintain ${reserved_buying_power:.2f} buying power")
        if dollar_amount <= 0:
            print(f"Calculated dollar amount is <= 0 (${dollar_amount:.2f}). Skipping buys.")
            logging.info(f"Calculated dollar amount is <= 0 (${dollar_amount:.2f}). Skipping buys.")
            return
        if FRACTIONAL_BUY_ORDERS and dollar_amount < 1.0:
            print(f"Dollar amount (${dollar_amount:.2f}) too low for fractional orders. Skipping buys.")
            logging.info(f"Dollar amount (${dollar_amount:.2f}) too low for fractional orders. Skipping buys.")
            return
        for sym in valid_symbols:
            print(f"\n{'='*60}")
            print(f"Processing {sym}...")
            logging.info(f"Processing {sym}")
            current_datetime = datetime.now(eastern)
            current_time_str = current_datetime.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
            print(f"Analysis time: {current_time_str}")
            logging.info(f"Analysis time: {current_time_str}")
            current_price = rate_limited_get_quote(sym)
            if current_price is None or current_price <= 0:
                print(f"No valid price data for {sym}.")
                logging.info(f"No valid price data for {sym}")
                continue
            current_color = GREEN if current_price >= 0 else RED
            print(f"Current price for {sym}: {current_color}${current_price:.4f}{RESET}")
            logging.info(f"Current price for {sym}: ${current_price:.4f}")
            min_5_price = min_5_prices.get(sym)
            day_5_price = day_5_prices.get(sym)
            if min_5_price and day_5_price:
                min_vs_day_change = ((min_5_price - day_5_price) / day_5_price * 100) if day_5_price else 0
                change_color = GREEN if min_vs_day_change >= 0 else RED
                print(f"5-min price: ${min_5_price:.4f} vs 5-day close: ${day_5_price:.2f} | Change: {change_color}{min_vs_day_change:.2f}%{RESET}")
                logging.info(f"5-min price: ${min_5_price:.4f} vs 5-day close: ${day_5_price:.2f} | Change: {min_vs_day_change:.2f}%")
            current_timestamp = time.time()
            with price_history_lock:
                if sym not in price_history:
                    price_history[sym] = {interval: [] for interval in interval_map}
                    last_stored[sym] = {interval: 0 for interval in interval_map}
                if current_timestamp - last_stored[sym]['1min'] >= interval_map['1min']:
                    price_history[sym]['1min'].append(current_price)
                    last_stored[sym]['1min'] = current_timestamp
                    print(f"Stored price {current_price} for {sym} at 1min interval.")
                    logging.info(f"Stored price {current_price} for {sym} at 1min interval")
            yf_symbol = sym.replace('.', '-')
            df = rate_limited_yf_history(yf_symbol)
            if df.empty or len(df) < 14:
                print(f"Insufficient historical data for {sym} (rows: {len(df)}). Skipping.")
                logging.info(f"Insufficient historical data for {sym} (rows: {len(df)}). Skipping")
                continue
            df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])
            if len(df) < 14:
                print(f"After cleaning, insufficient data for {yf_symbol} (rows: {len(df)}). Skipping.")
                logging.info(f"After cleaning, insufficient data for {yf_symbol} (rows: {len(df)}). Skipping")
                continue
            score = 0
            close = df['Close'].values
            open_ = df['Open'].values
            high = df['High'].values
            low = df['Low'].values
            try:
                rsi = talib.RSI(close, timeperiod=14)
                latest_rsi = rsi[-1] if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50.00
                latest_rsi = round(latest_rsi, 2)
                if latest_rsi < 50:
                    score += 1
                    print(f"{yf_symbol}: RSI < 50 ({latest_rsi:.2f}): +1 score")
                    logging.info(f"{yf_symbol}: RSI < 50 ({latest_rsi:.2f}): +1 score")
                print(f"Latest RSI: {latest_rsi:.2f}")
                logging.info(f"Latest RSI: {latest_rsi:.2f}")
            except Exception as e:
                print(f"Error calculating RSI for {yf_symbol}: {e}")
                logging.error(f"Error calculating RSI for {yf_symbol}: {e}")
                latest_rsi = 50.00
            if close[-1] <= close[-2] * 0.997:
                score += 1
                print(f"{yf_symbol}: Price decrease >= 0.3% from previous close: +1 score")
                logging.info(f"{yf_symbol}: Price decrease >= 0.3% from previous close: +1 score")
            print(f"Checking for bullish reversal patterns in {sym}...")
            logging.info(f"Checking for bullish reversal patterns in {sym}")
            bullish_reversal_detected = False
            detected_patterns = []
            patterns = {
                'Hammer': talib.CDLHAMMER,
                'Bullish Engulfing': talib.CDLENGULFING,
                'Morning Star': talib.CDLMORNINGSTAR,
                'Piercing Line': talib.CDLPIERCING,
                'Three White Soldiers': talib.CDL3WHITESOLDIERS,
                'Dragonfly Doji': talib.CDLDRAGONFLYDOJI,
                'Inverted Hammer': talib.CDLINVERTEDHAMMER,
                'Tweezer Bottom': talib.CDLMATCHINGLOW
            }
            valid_mask = ~np.isnan(open_) & ~np.isnan(high) & ~np.isnan(low) & ~np.isnan(close)
            if valid_mask.sum() < 2:
                print(f"Insufficient valid data for {sym} after removing NaN values. Skipping candlestick analysis.")
                logging.info(f"Insufficient valid data for {sym} after removing NaN values.")
                continue
            open_valid = np.array(open_[valid_mask], dtype=np.float64)
            high_valid = np.array(high[valid_mask], dtype=np.float64)
            low_valid = np.array(low[valid_mask], dtype=np.float64)
            close_valid = np.array(close[valid_mask], dtype=np.float64)
            for name, func in patterns.items():
                res = func(open_valid, high_valid, low_valid, close_valid)
                if res[-1] > 0:
                    detected_patterns.append(name)
                    bullish_reversal_detected = True
            if bullish_reversal_detected:
                score += 1
                print(f"{yf_symbol}: Detected bullish reversal patterns: {', '.join(detected_patterns)} (+1 score)")
                logging.info(f"{yf_symbol}: Detected bullish reversal patterns: {', '.join(detected_patterns)}")
            if score < 3:
                print(f"{yf_symbol}: Score too low ({score} < 3). Skipping.")
                logging.info(f"{yf_symbol}: Score too low ({score} < 3). Skipping")
                continue
            recent_avg_volume = df['Volume'].iloc[-5:].mean() if len(df) >= 5 else 0
            prior_avg_volume = df['Volume'].iloc[-10:-5].mean() if len(df) >= 10 else recent_avg_volume
            volume_decrease = recent_avg_volume < prior_avg_volume if len(df) >= 10 else False
            print(f"{yf_symbol}: Recent avg volume = {recent_avg_volume:.0f}, Prior avg volume = {prior_avg_volume:.0f}, Volume decrease = {volume_decrease}")
            logging.info(f"{yf_symbol}: Recent avg volume = {recent_avg_volume:.0f}, Prior avg volume = {prior_avg_volume:.0f}, Volume decrease = {volume_decrease}")
            try:
                rsi_series = talib.RSI(close, timeperiod=14)
                recent_avg_rsi = 50.00
                prior_avg_rsi = 50.00
                if len(rsi_series) >= 10:
                    recent_rsi_values = rsi_series[-5:][~np.isnan(rsi_series[-5:])]
                    prior_rsi_values = rsi_series[-10:-5][~np.isnan(rsi_series[-10:-5])]
                    if len(recent_rsi_values) > 0 and len(prior_rsi_values) > 0:
                        recent_avg_rsi = round(np.mean(recent_rsi_values), 2)
                        prior_avg_rsi = round(np.mean(prior_rsi_values), 2)
                print(f"{yf_symbol}: Recent avg RSI = {recent_avg_rsi:.2f}, Prior avg RSI = {prior_avg_rsi:.2f}")
                logging.info(f"{yf_symbol}: Recent avg RSI = {recent_avg_rsi:.2f}, Prior avg RSI = {prior_avg_rsi:.2f}")
            except Exception as e:
                print(f"Error calculating RSI metrics for {yf_symbol}: {e}")
                logging.error(f"Error calculating RSI metrics for {yf_symbol}: {e}")
                recent_avg_rsi = 50.00
                prior_avg_rsi = 50.00
            previous_price = get_previous_price(sym)
            price_increase = current_price > previous_price * 1.005
            print(f"{yf_symbol}: Price increase check: Current = {GREEN if current_price > previous_price else RED}${current_price:.2f}{RESET}, Previous = ${previous_price:.2f}, Increase = {price_increase}")
            logging.info(f"{yf_symbol}: Price increase check: Current = ${current_price:.2f}, Previous = ${previous_price:.2f}, Increase = {price_increase}")
            last_price = min_5_prices.get(sym)
            if last_price is None:
                try:
                    last_price = round(float(df['Close'].iloc[-1].item()), 4)
                    print(f"No 5-min price found for {yf_symbol}. Using last closing price: {last_price}")
                    logging.info(f"No 5-min price found for {yf_symbol}. Using last closing price: {last_price}")
                except Exception as e:
                    print(f"Error fetching last closing price for {yf_symbol}: {e}")
                    logging.error(f"Error fetching last closing price for {yf_symbol}: {e}")
                    continue
            price_decline_threshold = last_price * (1 - 0.002)
            price_decline = current_price <= price_decline_threshold
            print(f"{yf_symbol}: Price decline check: Current = {GREEN if current_price > previous_price else RED}${current_price:.2f}{RESET}, Threshold = ${price_decline_threshold:.2f}, Decline = {price_decline}")
            logging.info(f"{yf_symbol}: Price decline check: Current = ${current_price:.2f}, Threshold = ${price_decline_threshold:.2f}, Decline = {price_decline}")
            if not is_in_uptrend(sym):
                print(f"{sym} is not in an uptrend. Skipping.")
                logging.info(f"{sym} is not in an uptrend. Skipping")
                continue
            if recent_avg_rsi > prior_avg_rsi:
                print(f"{sym}: RSI increasing ({recent_avg_rsi:.2f} > {prior_avg_rsi:.2f}). Skipping.")
                logging.info(f"{sym}: RSI increasing ({recent_avg_rsi:.2f} > {prior_avg_rsi:.2f}). Skipping")
                continue
            if price_increase:
                print(f"{sym}: Price increasing ({current_price:.2f} > {previous_price * 1.005:.2f}). Skipping.")
                logging.info(f"{sym}: Price increasing ({current_price:.2f} > {previous_price * 1.005:.2f}). Skipping")
                continue
            if not price_decline:
                print(f"{sym}: Price did not decline enough ({current_price:.2f} > {price_decline_threshold:.2f}). Skipping.")
                logging.info(f"{sym}: Price did not decline enough ({current_price:.2f} > {price_decline_threshold:.2f}). Skipping")
                continue
            buy_qty = dollar_amount / current_price if FRACTIONAL_BUY_ORDERS else int(dollar_amount / current_price)
            if buy_qty <= 0:
                print(f"Calculated quantity for {sym} is <= 0 ({buy_qty:.4f}). Skipping buy.")
                logging.info(f"Calculated quantity for {sym} is <= 0 ({buy_qty:.4f}). Skipping buy.")
                continue
            if FRACTIONAL_BUY_ORDERS and dollar_amount < 1.0:
                print(f"Dollar amount for {sym} is too low (${dollar_amount:.2f}). Skipping buy.")
                logging.info(f"Dollar amount for {sym} is too low (${dollar_amount:.2f}). Skipping buy.")
                continue
            buy_signal += 1
            print(f"Buy signal detected for {sym} (Signal count: {buy_signal})")
            logging.info(f"Buy signal detected for {sym} (Signal count: {buy_signal})")
            with buy_sell_lock:
                if not ensure_no_open_orders(sym):
                    print(f"Cannot buy {sym}: Open orders exist.")
                    logging.info(f"Cannot buy {sym}: Open orders exist.")
                    continue
                order_id = client_place_order(
                    symbol=sym,
                    side="BUY",
                    amount=dollar_amount if FRACTIONAL_BUY_ORDERS else None,
                    quantity=buy_qty if not FRACTIONAL_BUY_ORDERS else None
                )
                if order_id:
                    status_info = client_get_order_status(order_id)
                    if status_info and status_info["status"] == "FILLED":
                        filled_qty = status_info["filled_qty"]
                        filled_price = status_info["avg_price"] or current_price
                        with db_lock:
                            session = SessionLocal()
                            try:
                                trade = TradeHistory(
                                    symbols=sym,
                                    action='buy',
                                    quantity=filled_qty,
                                    price=filled_price,
                                    date=today_date_str
                                )
                                session.add(trade)
                                position = session.query(Position).filter_by(symbols=sym).first()
                                if position:
                                    position.quantity += filled_qty
                                    position.avg_price = ((position.avg_price * position.quantity) + (filled_price * filled_qty)) / (position.quantity + filled_qty)
                                else:
                                    position = Position(
                                        symbols=sym,
                                        quantity=filled_qty,
                                        avg_price=filled_price,
                                        purchase_date=today_date_str
                                    )
                                    session.add(position)
                                session.commit()
                                with csv_lock:
                                    try:
                                        with open(csv_filename, mode='a', newline='', encoding='utf-8') as csv_file:
                                            csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                                            csv_writer.writerow({
                                                'Date': today_date_str,
                                                'Buy': filled_qty,
                                                'Sell': 0,
                                                'Quantity': filled_qty,
                                                'Symbol': sym,
                                                'Price Per Share': filled_price
                                            })
                                        print(f"Buy order filled for {filled_qty:.4f} shares of {sym} at ${filled_price:.2f}")
                                        logging.info(f"Buy order filled for {filled_qty:.4f} shares of {sym} at ${filled_price:.2f}")
                                        send_alert(
                                            f"Bought {filled_qty:.4f} shares of {sym} at ${filled_price:.2f}",
                                            subject=f"Buy Order Filled: {sym}"
                                        )
                                    except Exception as e:
                                        logging.error(f"Failed to write buy to CSV for {sym}: {e}")
                                        print(f"Failed to write buy to CSV for {sym}: {e}")
                                        send_alert(
                                            f"Failed to write buy to CSV for {sym}: {e}",
                                            subject=f"CSV Write Error: {sym}"
                                        )
                                order_id, stop_price = place_stop_loss_order(sym, filled_qty, filled_price)
                                if order_id:
                                    position.stop_order_id = order_id
                                    position.stop_price = stop_price
                                    session.commit()
                                symbols_to_remove.append(sym)
                            except Exception as e:
                                session.rollback()
                                logging.error(f"Error updating database for {sym} buy: {e}")
                                print(f"Error updating database for {sym} buy: {e}")
                            finally:
                                session.close()
                    else:
                        print(f"Buy order not filled for {sym}.")
                        logging.info(f"Buy order not filled for {sym}")
                        if status_info and status_info["status"] == "CANCELLED":
                            print(f"Buy order for {sym} was cancelled.")
                            logging.info(f"Buy order for {sym} was cancelled")
                        elif status_info and status_info["status"] == "REJECTED":
                            print(f"Buy order for {sym} was rejected.")
                            logging.info(f"Buy order for {sym} was rejected")
                        else:
                            print(f"Buy order for {sym} timed out or failed.")
                            logging.info(f"Buy order for {sym} timed out or failed")
                else:
                    print(f"Failed to place buy order for {sym}.")
                    logging.info(f"Failed to place buy order for {sym}")
        for sym in symbols_to_remove:
            remove_symbols_from_trade_list(sym)
    except Exception as e:
        logging.error(f"Error in buy_stocks: {e}")
        print(f"Error in buy_stocks: {e}")
        traceback.print_exc()
    finally:
        task_running['buy_stocks'] = False

def sell_stocks(symbols_to_sell_dict, buy_sell_lock):
    if task_running['sell_stocks']:
        print("sell_stocks already running. Skipping.")
        logging.info("sell_stocks already running. Skipping")
        return
    task_running['sell_stocks'] = True
    try:
        print("\nStarting sell_stocks function...")
        logging.info("Starting sell_stocks function")
        positions = client_list_positions()
        if not positions:
            print("No positions to sell.")
            logging.info("No positions to sell")
            return
        for pos in positions:
            sym = pos['symbol']
            qty = pos['qty']
            avg_price = pos['avg_entry_price']
            api_position = next((p for p in positions if p['symbol'] == sym), None)
            if not api_position or api_position['qty'] < qty:
                print(f"Cannot sell {qty:.4f} shares of {sym}: Available qty is {api_position['qty'] if api_position else 0:.4f}")
                logging.info(f"Cannot sell {qty:.4f} shares of {sym}: Available qty is {api_position['qty'] if api_position else 0:.4f}")
                continue
            current_price = rate_limited_get_quote(sym)
            if current_price is None or current_price <= 0:
                print(f"No valid price for {sym}. Skipping.")
                logging.info(f"No valid price for {sym}. Skipping")
                continue
            sell_qty = round(qty, 4) if FRACTIONAL_BUY_ORDERS else int(qty)
            if sell_qty <= 0:
                print(f"Calculated sell quantity for {sym} is <= 0 ({sell_qty:.4f}). Skipping.")
                logging.info(f"Calculated sell quantity for {sym} is <= 0 ({sell_qty:.4f}). Skipping")
                continue
            if sym not in symbols_to_sell_dict:
                print(f"{sym} not in sell list. Skipping.")
                logging.info(f"{sym} not in sell list. Skipping")
                continue
            avg_price, purchase_date = symbols_to_sell_dict[sym]
            df = rate_limited_yf_history(sym)
            if df.empty or len(df) < 14:
                print(f"Insufficient historical data for {sym} (rows: {len(df)}). Skipping.")
                logging.info(f"Insufficient historical data for {sym} (rows: {len(df)}). Skipping")
                continue
            df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])
            if len(df) < 14:
                print(f"After cleaning, insufficient data for {sym} (rows: {len(df)}). Skipping.")
                logging.info(f"After cleaning, insufficient data for {sym} (rows: {len(df)}). Skipping")
                continue
            close = df['Close'].values
            try:
                rsi = talib.RSI(close, timeperiod=14)
                latest_rsi = rsi[-1] if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50.00
                latest_rsi = round(latest_rsi, 2)
                print(f"Latest RSI for {sym}: {latest_rsi}")
                logging.info(f"Latest RSI for {sym}: {latest_rsi}")
            except Exception as e:
                print(f"Error calculating RSI for {sym}: {e}")
                logging.error(f"Error calculating RSI for {sym}: {e}")
                latest_rsi = 50.00
            purchase_date_dt = datetime.strptime(purchase_date, "%Y-%m-%d").date()
            days_held = (today_date - purchase_date_dt).days
            price_change = ((current_price - avg_price) / avg_price * 100) if avg_price else 0
            change_color = GREEN if price_change >= 0 else RED
            print(f"{sym}: Held {days_held} days, Price change: {change_color}{price_change:.2f}%{RESET}")
            logging.info(f"{sym}: Held {days_held} days, Price change: {price_change:.2f}%")
            sell_signal = False
            if days_held >= 30:
                print(f"{sym}: Held >= 30 days. Sell signal.")
                logging.info(f"{sym}: Held >= 30 days. Sell signal")
                sell_signal = True
            elif price_change >= 10:
                print(f"{sym}: Price up >= 10% ({price_change:.2f}%). Sell signal.")
                logging.info(f"{sym}: Price up >= 10% ({price_change:.2f}%). Sell signal")
                sell_signal = True
            elif price_change <= -5:
                print(f"{sym}: Price down >= 5% ({price_change:.2f}%). Sell signal.")
                logging.info(f"{sym}: Price down >= 5% ({price_change:.2f}%). Sell signal")
                sell_signal = True
            elif latest_rsi >= 70:
                print(f"{sym}: RSI >= 70 ({latest_rsi}). Sell signal.")
                logging.info(f"{sym}: RSI >= 70 ({latest_rsi}). Sell signal")
                sell_signal = True
            if not sell_signal:
                print(f"No sell signal for {sym}. Skipping.")
                logging.info(f"No sell signal for {sym}. Skipping")
                continue
            with buy_sell_lock:
                if not ensure_no_open_orders(sym):
                    print(f"Cannot sell {sym}: Open orders exist.")
                    logging.info(f"Cannot sell {sym}: Open orders exist.")
                    continue
                order_id = client_place_order(
                    symbol=sym,
                    side="SELL",
                    amount=None,
                    quantity=sell_qty
                )
                if order_id:
                    status_info = client_get_order_status(order_id)
                    if status_info and status_info["status"] == "FILLED":
                        filled_qty = status_info["filled_qty"]
                        filled_price = status_info["avg_price"] or current_price
                        with db_lock:
                            session = SessionLocal()
                            try:
                                trade = TradeHistory(
                                    symbols=sym,
                                    action='sell',
                                    quantity=filled_qty,
                                    price=filled_price,
                                    date=today_date_str
                                )
                                session.add(trade)
                                position = session.query(Position).filter_by(symbols=sym).first()
                                if position:
                                    position.quantity -= filled_qty
                                    if position.quantity <= 0:
                                        if position.stop_order_id:
                                            client_cancel_order({'orderId': position.stop_order_id, 'instrument': {'symbol': sym}})
                                        session.delete(position)
                                    session.commit()
                                with csv_lock:
                                    try:
                                        with open(csv_filename, mode='a', newline='', encoding='utf-8') as csv_file:
                                            csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                                            csv_writer.writerow({
                                                'Date': today_date_str,
                                                'Buy': 0,
                                                'Sell': filled_qty,
                                                'Quantity': filled_qty,
                                                'Symbol': sym,
                                                'Price Per Share': filled_price
                                            })
                                        print(f"Sell order filled for {filled_qty:.4f} shares of {sym} at ${filled_price:.2f}")
                                        logging.info(f"Sell order filled for {filled_qty:.4f} shares of {sym} at ${filled_price:.2f}")
                                        send_alert(
                                            f"Sold {filled_qty:.4f} shares of {sym} at ${filled_price:.2f}",
                                            subject=f"Sell Order Filled: {sym}"
                                        )
                                    except Exception as e:
                                        logging.error(f"Failed to write sell to CSV for {sym}: {e}")
                                        print(f"Failed to write sell to CSV for {sym}: {e}")
                                        send_alert(
                                            f"Failed to write sell to CSV for {sym}: {e}",
                                            subject=f"CSV Write Error: {sym}"
                                        )
                            except Exception as e:
                                session.rollback()
                                logging.error(f"Error updating database for {sym} sell: {e}")
                                print(f"Error updating database for {sym} sell: {e}")
                            finally:
                                session.close()
                    else:
                        print(f"Sell order not filled for {sym}.")
                        logging.info(f"Sell order not filled for {sym}")
                        if status_info and status_info["status"] == "CANCELLED":
                            print(f"Sell order for {sym} was cancelled.")
                            logging.info(f"Sell order for {sym} was cancelled")
                        elif status_info and status_info["status"] == "REJECTED":
                            print(f"Sell order for {sym} was rejected.")
                            logging.info(f"Sell order for {sym} was rejected")
                        else:
                            print(f"Sell order for {sym} timed out or failed.")
                            logging.info(f"Sell order for {sym} timed out or failed")
                else:
                    print(f"Failed to place sell order for {sym}.")
                    logging.info(f"Failed to place sell order for {sym}")
    except Exception as e:
        logging.error(f"Error in sell_stocks: {e}")
        print(f"Error in sell_stocks: {e}")
        traceback.print_exc()
    finally:
        task_running['sell_stocks'] = False

def run_scheduled_tasks():
    print("Running scheduled tasks...")
    logging.info("Running scheduled tasks")
    try:
        stop_if_stock_market_is_closed()
        sync_db_with_api()
        symbols_to_buy_list = get_symbols_to_buy()
        symbols_to_sell_dict = load_positions_from_database()
        if PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE:
            print("\nSymbols to buy:", symbols_to_buy_list)
            print("Symbols to sell:", list(symbols_to_sell_dict.keys()))
            logging.info(f"Symbols to buy: {symbols_to_buy_list}")
            logging.info(f"Symbols to sell: {list(symbols_to_sell_dict.keys())}")
        buy_stocks(symbols_to_sell_dict, symbols_to_buy_list, buy_sell_lock)
        sell_stocks(symbols_to_sell_dict, buy_sell_lock)
        check_price_moves()
        monitor_stop_losses()
        check_stop_order_status()
        print_database_tables()
    except Exception as e:
        logging.error(f"Error in scheduled tasks: {e}")
        print(f"Error in scheduled tasks: {e}")
        traceback.print_exc()

def main():
    print("Starting main function...")
    logging.info("Starting main function")
    refresh_token_if_needed()
    fetch_token_and_account()
    initialize_csv()
    initialize_alerts_log()
    if not YOUR_SECRET_KEY:
        logging.error("Missing required environment variable: YOUR_SECRET_KEY")
        print("Missing required environment variable: YOUR_SECRET_KEY")
        return
    if not fetch_token_and_account():
        logging.error("Failed to initialize token and account. Exiting.")
        print("Failed to initialize token and account. Exiting.")
        return
    stop_if_stock_market_is_closed()
    sync_db_with_api()
    schedule.every(5).minutes.do(run_scheduled_tasks)
    schedule.every(30).minutes.do(refresh_token_if_needed)
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("Program interrupted by user. Exiting...")
        logging.info("Program interrupted by user. Exiting")
    except Exception as e:
        logging.error(f"Unexpected error in main loop: {e}")
        print(f"Unexpected error in main loop: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()

