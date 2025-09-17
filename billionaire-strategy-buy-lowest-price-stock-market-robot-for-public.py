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
import json
import yfinance as yf
import talib
import pandas_market_calendars as mcal
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm.exc import NoResultFound
from requests.exceptions import HTTPError, ConnectionError, Timeout
from ratelimit import limits, sleep_and_retry
import numpy as np
import pandas as pd
import traceback

# ANSI color codes for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
RESET = "\033[0m"

# Configuration flags
PRINT_SYMBOLS_TO_BUY = False  # Set to False for faster execution
PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE = True  # Set to True to view database
PRINT_DATABASE = True  # Set to True to view stocks to sell
DEBUG = False  # Set to False for faster execution
ALL_BUY_ORDERS_ARE_1_DOLLAR = False  # When True, every buy order is a $1.00 fractional share market day order
FRACTIONAL_BUY_ORDERS = True  # Enable fractional share orders

# Global variables
YOUR_SECRET_KEY = os.getenv("YOUR_SECRET_KEY")
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
csv_filename = 'log-file-of-buy-and-sell-signals.csv'
fieldnames = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']
price_changes = {}
current_price = 0
today_date_str = today_date.strftime("%Y-%m-%d")
qty = 0
price_history = {}  # symbols -> interval -> list of prices
last_stored = {}  # symbols -> interval -> last_timestamp
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
CACHE_EXPIRY = 120  # 2 minutes
CALLS = 60
PERIOD = 60
db_lock = threading.Lock()  # Database lock for thread safety
price_history_lock = threading.Lock()  # Lock for price_history and last_stored
task_running = {
    'buy_stocks': False,
    'sell_stocks': False,
    'check_price_moves': False,
    'check_stop_order_status': False,
    'monitor_stop_losses': False,
    'sync_db_with_api': False,
    'refresh_token_if_needed': False
}  # Task running flags

# Timezone
eastern = pytz.timezone('US/Eastern')

# Logging configuration
logging.basicConfig(filename='trading-bot-program-logging-messages.txt', level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(message)s')

# Initialize CSV file
with open(csv_filename, mode='w', newline='') as csv_file:
    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    csv_writer.writeheader()

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
engine = create_engine('sqlite:///trading_bot.db', connect_args={"check_same_thread": False})
with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
SessionLocal = scoped_session(sessionmaker(bind=engine))
Base.metadata.create_all(engine)

# NYSE Calendar
nyse_cal = mcal.get_calendar('NYSE')

# Rate limiting decorator
@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_cached_data(symbols, data_type, fetch_func, *args, **kwargs):
    print(f"Checking cache for {symbols} {data_type}...")
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

# Public.com API Functions
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
            {
                "symbol": symbol,
                "type": "EQUITY"
            }
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
def place_market_order(symbol, side, amt=None, quantity=None):
    """Place MARKET order (fractional or full-share)"""
    url = f"{BASE_URL}/trading/{account_id}/order"
    order_id = str(uuid4())
    expiration = {"timeInForce": "DAY"} if amt else {"timeInForce": "GTD", "expirationTime": get_expiration()}
    payload = {
        "orderId": order_id,
        "instrument": {"symbol": symbol, "type": "EQUITY"},
        "orderSide": side.upper(),
        "orderType": "MARKET",
        "expiration": expiration,
        "openCloseIndicator": "OPEN"
    }
    if amt is not None:
        payload["amount"] = f"{amt:.2f}"
    elif quantity is not None:
        payload["quantity"] = str(quantity)
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
def client_place_order(symbol, side, amt=None, quantity=None, order_type="MARKET", limit_price=None, stop_price=None):
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId")
            return None
        order_response = place_market_order(symbol, side, amt=amt, quantity=quantity)
        if order_response.get('error'):
            logging.error(f"Order placement error for {symbol}: {order_response['error']}")
            return None
        order_id = order_response.get('orderId')
        if amt is not None:
            logging.info(f"Order placed: {side} ${amt:.2f} of {symbol}, Order ID: {order_id}")
        else:
            logging.info(f"Order placed: {side} {quantity} shares of {symbol}, Order ID: {order_id}")
        return order_id
    except Exception as e:
        logging.error(f"Order placement error for {symbol}: {e}")
        return None

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
def client_cancel_order(order_id):
    try:
        if not account_id or not order_id:
            logging.error("No account_id or order_id")
            return False
        url = f"{BASE_URL}/trading/{account_id}/order/{order_id}"
        resp = requests.delete(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        logging.info(f"Order {order_id} cancelled successfully")
        return True
    except Exception as e:
        logging.error(f"Order cancellation error for {order_id}: {e}")
        return False

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def fetch_token_and_account():
    global access_token, account_id, HEADERS, last_token_fetch_time
    try:
        if not YOUR_SECRET_KEY:
            raise ValueError("YOUR_SECRET_KEY not set")
        resp = requests.post(
            "https://api.public.com/userapiauthservice/personal/access-tokens",
            headers={"Content-Type": "application/json"},
            json={"secret": YOUR_SECRET_KEY, "validityInMinutes": 1440},
            timeout=10
        )
        resp.raise_for_status()
        access_token = resp.json().get("accessToken")
        if not access_token:
            raise ValueError("No access token returned")
        resp = requests.get(
            f"{BASE_URL}/trading/account",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            timeout=10
        )
        resp.raise_for_status()
        accounts = resp.json().get("accounts", [])
        brokerage = next((a for a in accounts if a.get("accountType") == "BROKERAGE"), None)
        if not brokerage:
            raise ValueError("No BROKERAGE account found")
        account_id = brokerage["accountId"]
        HEADERS = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        last_token_fetch_time = datetime.now()
        logging.info(f"Access token and brokerage account fetched: {account_id}")
        return True
    except Exception:
        print("\nERROR fetching token/account:")
        logging.error("Error fetching token/account:")
        traceback.print_exc()
        return False

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def refresh_token_if_needed():
    if task_running['refresh_token_if_needed']:
        print("refresh_token_if_needed already running. Skipping.")
        return
    task_running['refresh_token_if_needed'] = True
    try:
        global last_token_fetch_time
        if last_token_fetch_time and (datetime.now() - last_token_fetch_time) > timedelta(hours=23):
            print("Refreshing access token...")
            return fetch_token_and_account()
        return True
    finally:
        task_running['refresh_token_if_needed'] = False

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_get_account():
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId")
            return {'equity': 0.0, 'buying_power_cash': 0.0, 'cash_only_buying_power': 0.0, 'cash_on_hand': 0.0, 'accountId': None}
        resp = requests.get(f"{BASE_URL}/trading/{account_id}/portfolio/v2", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        account = resp.json()
        equity_list = account.get('equity', [])
        total_equity = round(sum(float(e.get('value', 0)) for e in equity_list), 2)
        cash_on_hand = round(sum(float(e.get('value', 0)) for e in equity_list if e.get('type') == 'CASH'), 2)
        buying_power_dict = account.get('buyingPower', {})
        buying_power_cash = round(float(buying_power_dict.get('buyingPower', 0)), 2)
        cash_only_buying_power = round(float(buying_power_dict.get('cashOnlyBuyingPower', 0)), 2)
        print(f"Account equity: ${total_equity:.2f}, Buying power cash: ${buying_power_cash:.2f}")
        return {
            'equity': total_equity,
            'buying_power_cash': buying_power_cash,
            'cash_only_buying_power': cash_only_buying_power,
            'cash_on_hand': cash_on_hand,
            'accountId': account_id
        }
    except Exception as e:
        logging.error(f"Account fetch error: {e}")
        return {'equity': 0.0, 'buying_power_cash': 0.0, 'cash_only_buying_power': 0.0, 'cash_on_hand': 0.0, 'accountId': account_id}

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_list_positions():
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId")
            return []
        resp = requests.get(f"{BASE_URL}/trading/{account_id}/portfolio/v2", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        pos_list = data.get('positions', [])
        out = []
        for p in pos_list:
            sym = p.get('instrument', {}).get('symbol')
            qty = float(p.get('quantity', 0))
            avg = round(float(p.get('costBasis', {}).get('unitCost', 0)), 2)
            opened_at = p.get('openedAt', datetime.now(eastern).strftime("%Y-%m-%d"))
            try:
                date_str = datetime.fromisoformat(opened_at.replace('Z', '+00:00')).astimezone(eastern).strftime("%Y-%m-%d")
            except ValueError:
                date_str = datetime.now(eastern).strftime("%Y-%m-%d")
            if sym and qty > 0:
                current_price = client_get_quote(sym)
                price_color = GREEN if current_price >= 0 else RED
                print(f"Position: {sym} | Qty: {qty:.4f} | Avg Price: ${avg:.2f} | Current Price: {price_color}${current_price:.2f}{RESET}")
                out.append({'symbol': sym, 'qty': qty, 'avg_entry_price': avg, 'purchase_date': date_str})
        return out
    except Exception as e:
        logging.error(f"Positions fetch error: {e}")
        return []

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_list_open_orders():
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId")
            return []
        url = f"{BASE_URL}/trading/{account_id}/orders"
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        orders = resp.json().get('orders', [])
        open_orders = [o for o in orders if o.get('status') == 'OPEN']
        print(f"Retrieved {len(open_orders)} open orders.")
        return open_orders
    except Exception as e:
        logging.error(f"Error listing open orders: {e}")
        return []

def get_expiration():
    """Return expirationTime string for GTD orders (full-share), skip weekends"""
    exp = datetime.now(timezone.utc) + timedelta(days=30)
    if exp.weekday() == 5:
        exp += timedelta(days=2)
    elif exp.weekday() == 6:
        exp += timedelta(days=1)
    return exp.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def sync_db_with_api():
    if task_running['sync_db_with_api']:
        print("sync_db_with_api already running. Skipping.")
        return
    task_running['sync_db_with_api'] = True
    try:
        with db_lock:
            session = SessionLocal()
            try:
                for attempt in range(3):
                    try:
                        api_positions = client_list_positions()
                        break
                    except Exception as e:
                        logging.error(f"Retry {attempt + 1}/3: Error syncing DB with API: {e}")
                        time.sleep(2 ** attempt)
                        if attempt == 2:
                            logging.error("All retries failed for syncing DB with API.")
                            return
                api_symbols = {pos['symbol'] for pos in api_positions}
                positions_to_delete = []
                for pos in api_positions:
                    symbol = pos['symbol']
                    qty = pos['qty']
                    avg_price = pos['avg_entry_price']
                    purchase_date = pos['purchase_date']
                    db_pos = session.query(Position).filter_by(symbols=symbol).first()
                    if db_pos:
                        db_pos.quantity = qty
                        db_pos.avg_price = avg_price
                    else:
                        db_pos = Position(
                            symbols=symbol,
                            quantity=qty,
                            avg_price=avg_price,
                            purchase_date=purchase_date
                        )
                        session.add(db_pos)
                for db_pos in session.query(Position).all():
                    if db_pos.symbols not in api_symbols and db_pos.quantity <= 0:
                        positions_to_delete.append(db_pos)
                time.sleep(5)
                for db_pos in positions_to_delete:
                    if db_pos.stop_order_id:
                        client_cancel_order(db_pos.stop_order_id)
                    session.delete(db_pos)
                session.commit()
                print("Database synced with API.")
            except Exception as e:
                session.rollback()
                logging.error(f"Error syncing DB with API: {e}")
            finally:
                session.close()
    finally:
        task_running['sync_db_with_api'] = False

def load_positions_from_database():
    print("Loading positions from database...")
    with db_lock:
        session = SessionLocal()
        try:
            positions = session.query(Position).all()
            symbols_to_sell_dict = {}
            for position in positions:
                symbols_to_sell = position.symbols
                avg_price = position.avg_price
                purchase_date = position.purchase_date
                symbols_to_sell_dict[symbols_to_sell] = (avg_price, purchase_date)
            print(f"Loaded {len(symbols_to_sell_dict)} positions from database.")
            return symbols_to_sell_dict
        finally:
            session.close()

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_technical_indicators(symbols, lookback_days=200):
    print(f"Calculating technical indicators for {symbols} using yfinance...")
    logging.info(f"Calculating technical indicators for {symbols}")
    yf_symbol = symbols.replace('.', '-')
    historical_data = yf.Ticker(yf_symbol).history(period='200d', interval='1d')
    if historical_data.empty or len(historical_data) < lookback_days:
        logging.error(f"Insufficient data for {symbols} (rows: {len(historical_data)})")
        print(f"Insufficient data for {symbols} (rows: {len(historical_data)}).")
        return None
    historical_data = historical_data.dropna(subset=['Open', 'High', 'Low', 'Close'])
    if len(historical_data) < 35:
        logging.error(f"After cleaning, insufficient data for {symbols} (rows: {len(historical_data)})")
        print(f"After cleaning, insufficient data for {symbols} (rows: {len(historical_data)}).")
        return None
    short_window = 12
    long_window = 26
    signal_window = 9
    try:
        macd, signal, _ = talib.MACD(historical_data['Close'].values,
                                     fastperiod=short_window,
                                     slowperiod=long_window,
                                     signalperiod=signal_window)
        historical_data['macd'] = macd
        historical_data['signal'] = signal
    except Exception as e:
        print(f"Error calculating MACD for {yf_symbol}: {e}")
        logging.error(f"Error calculating MACD for {yf_symbol}: {e}")
        historical_data['macd'] = np.nan
        historical_data['signal'] = np.nan
    try:
        rsi = talib.RSI(historical_data['Close'].values, timeperiod=14)
        historical_data['rsi'] = rsi
    except Exception as e:
        print(f"Error calculating RSI for {yf_symbol}: {e}")
        logging.error(f"Error calculating RSI for {yf_symbol}: {e}")
        historical_data['rsi'] = np.nan
    historical_data['volume'] = historical_data['Volume']
    print(f"Technical indicators calculated for {yf_symbol}.")
    logging.info(f"Technical indicators calculated for {yf_symbol}")
    print_technical_indicators(symbols, historical_data)
    return historical_data

def print_technical_indicators(symbols, historical_data):
    print(f"\nTechnical Indicators for {symbols}:\n")
    tail_data = historical_data[['Close', 'macd', 'signal', 'rsi', 'volume']].tail()
    for idx, row in tail_data.iterrows():
        close_color = GREEN if row['Close'] >= 0 else RED
        macd_value = row['macd']
        signal_value = row['signal']
        if np.isnan(macd_value) or np.isnan(signal_value):
            macd_display = "N/A"
            signal_display = "N/A"
            macd_color = YELLOW
        else:
            macd_display = f"{macd_value:.4f}"
            signal_display = f"{signal_value:.4f}"
            macd_color = GREEN if macd_value >= signal_value else RED
        rsi_value = row['rsi']
        rsi_display = f"{rsi_value:.2f}" if not np.isnan(rsi_value) else "50.00"
        print(f"Time: {idx} | Close: {close_color}${row['Close']:.2f}{RESET} | "
              f"MACD: {macd_color}{macd_display}{RESET} (Signal: {signal_display}) | "
              f"RSI: {rsi_display} | Volume: {row['volume']:.0f}")
    print("")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_daily_rsi(symbol):
    print(f"Calculating daily RSI for {symbol} using yfinance...")
    logging.info(f"Calculating daily RSI for {symbol}")
    yf_symbol = symbol.replace('.', '-')
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period='30d', interval='1d')
    if historical_data.empty or len(historical_data['Close']) < 14:
        print(f"Insufficient daily data for {yf_symbol} (rows: {len(historical_data)}).")
        logging.error(f"Insufficient daily data for {yf_symbol} (rows: {len(historical_data)}).")
        return 50.00
    try:
        rsi = talib.RSI(historical_data['Close'], timeperiod=14)[-1]
        rsi_value = round(rsi, 2) if not np.isnan(rsi) else 50.00
        print(f"Daily RSI for {yf_symbol}: {rsi_value}")
        logging.info(f"Daily RSI for {yf_symbol}: {rsi_value}")
        return rsi_value
    except Exception as e:
        print(f"Error calculating daily RSI for {yf_symbol}: {e}")
        logging.error(f"Error calculating daily RSI for {yf_symbol}: {e}")
        return 50.00

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_average_true_range(symbol):
    print(f"Calculating ATR for {symbol} using yfinance...")
    logging.info(f"Calculating ATR for {symbol}")
    def _fetch_atr(symbol):
        yf_symbol = symbol.replace('.', '-')
        ticker = yf.Ticker(yf_symbol)
        data = ticker.history(period='30d')
        try:
            atr = talib.ATR(data['High'].values, data['Low'].values, data['Close'].values, timeperiod=22)
            atr_value = atr[-1]
            print(f"ATR value for {yf_symbol}: {atr_value:.4f}")
            logging.info(f"ATR value for {yf_symbol}: {atr_value:.4f}")
            return atr_value
        except Exception as e:
            logging.error(f"Error calculating ATR for {yf_symbol}: {e}")
            print(f"Error calculating ATR for {yf_symbol}: {e}")
            return None
    return get_cached_data(symbol, 'atr', _fetch_atr, symbol)

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def is_in_uptrend(symbol):
    print(f"Checking if {symbol} is in uptrend using yfinance...")
    logging.info(f"Checking if {symbol} is in uptrend")
    yf_symbol = symbol.replace('.', '-')
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period='200d')
    if historical_data.empty or len(historical_data) < 200:
        print(f"Insufficient data for {yf_symbol}.")
        logging.error(f"Insufficient data for {yf_symbol}.")
        return False
    sma_200 = talib.SMA(historical_data['Close'].values, timeperiod=200)[-1]
    current_price = client_get_quote(symbol)
    in_uptrend = current_price > sma_200 if current_price else False
    sma_color = GREEN if sma_200 >= 0 else RED
    price_color = GREEN if current_price >= 0 else RED
    print(f"{yf_symbol} {'is' if in_uptrend else 'is not'} in uptrend (Current: {price_color}${current_price:.2f}{RESET}, SMA200: {sma_color}${sma_200:.2f}{RESET}")
    logging.info(f"{yf_symbol} {'is' if in_uptrend else 'is not'} in uptrend (Current: ${current_price:.2f}, SMA200: ${sma_200:.2f})")
    return in_uptrend

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_last_price_within_past_5_minutes(symbols_to_buy_list):
    print("Fetching last prices within past 5 minutes using yfinance...")
    logging.info("Fetching last prices within past 5 minutes")
    results = {}
    for symbol in symbols_to_buy_list:
        print(f"Fetching 5-minute price data for {symbol}...")
        try:
            yf_symbol = symbol.replace('.', '-')
            data = yf.Ticker(yf_symbol).history(period="1d", interval='5m')
            if not data.empty:
                last_price = round(float(data['Close'].iloc[-1]), 4)
                price_color = GREEN if last_price >= 0 else RED
                print(f"Last price for {yf_symbol} within 5 minutes: {price_color}${last_price:.4f}{RESET}")
                logging.info(f"Last price for {yf_symbol} within 5 minutes: ${last_price:.4f}")
                results[symbol] = last_price
            else:
                results[symbol] = None
        except Exception as e:
            logging.error(f"Error fetching 5-min data for {yf_symbol}: {e}")
            print(f"Error fetching 5-min data for {yf_symbol}: {e}")
            results[symbol] = None
    return results

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_last_price_within_past_5_days(symbols_to_buy_list):
    print("Fetching last prices within past 5 days using yfinance...")
    logging.info("Fetching last prices within past 5 days")
    results = {}
    for symbol in symbols_to_buy_list:
        print(f"Fetching 5-day price data for {symbol}...")
        try:
            yf_symbol = symbol.replace('.', '-')
            data = yf.Ticker(yf_symbol).history(period='5d', interval='1d')
            if not data.empty:
                last_price = round(float(data['Close'].iloc[-1]), 2)
                price_color = GREEN if last_price >= 0 else RED
                print(f"Last price for {yf_symbol} within 5 days: {price_color}${last_price:.2f}{RESET}")
                logging.info(f"Last price for {yf_symbol} within 5 days: ${last_price:.2f}")
                results[symbol] = last_price
            else:
                results[symbol] = None
        except Exception as e:
            logging.error(f"Error fetching data for {yf_symbol}: {e}")
            print(f"Error fetching data for {yf_symbol}: {e}")
            results[symbol] = None
    return results

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_opening_price(symbol):
    print(f"Fetching opening price for {symbol}...")
    logging.info(f"Fetching opening price for {symbol}")
    yf_symbol = symbol.replace('.', '-')
    stock_data = yf.Ticker(yf_symbol)
    try:
        opening_price = round(float(stock_data.history(period="1d")["Open"].iloc[0]), 4)
        price_color = GREEN if opening_price >= 0 else RED
        print(f"Opening price for {yf_symbol}: {price_color}${opening_price:.4f}{RESET}")
        logging.info(f"Opening price for {yf_symbol}: ${opening_price:.4f}")
        return opening_price
    except IndexError:
        logging.error(f"Opening price not found for {yf_symbol}.")
        print(f"Opening price not found for {yf_symbol}.")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_atr_high_price(symbol):
    print(f"Calculating ATR high price for {symbol}...")
    logging.info(f"Calculating ATR high price for {symbol}")
    atr_value = get_average_true_range(symbol)
    current_price = client_get_quote(symbol)
    atr_high = round(current_price + 0.40 * atr_value, 4) if current_price and atr_value else None
    price_color = GREEN if atr_high and atr_high >= 0 else RED
    print(f"ATR high price for {symbol}: {price_color}${atr_high:.4f}{RESET}" if atr_high else f"Failed to calculate ATR high price for {symbol}.")
    logging.info(f"ATR high price for {symbol}: ${atr_high:.4f}" if atr_high else f"Failed to calculate ATR high price for {symbol}.")
    return atr_high

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_atr_low_price(symbol):
    print(f"Calculating ATR low price for {symbol}...")
    logging.info(f"Calculating ATR low price for {symbol}")
    atr_value = get_average_true_range(symbol)
    current_price = client_get_quote(symbol)
    atr_low = round(current_price - 0.10 * atr_value, 4) if current_price and atr_value else None
    price_color = GREEN if atr_low and atr_low >= 0 else RED
    print(f"ATR low price for {symbol}: {price_color}${atr_low:.4f}{RESET}" if atr_low else f"Failed to calculate ATR low price for {symbol}.")
    logging.info(f"ATR low price for {symbol}: ${atr_low:.4f}" if atr_low else f"Failed to calculate ATR low price for {symbol}.")
    return atr_low

def get_previous_price(symbol):
    return previous_prices.get(symbol, client_get_quote(symbol) or 0.0)

def update_previous_price(symbol, current_price):
    previous_prices[symbol] = current_price

def track_price_changes(symbol):
    current_price = client_get_quote(symbol)
    previous_price = get_previous_price(symbol)
    price_change = current_price - previous_price if current_price and previous_price else 0
    change_color = GREEN if price_change >= 0 else RED
    current_color = GREEN if current_price >= 0 else RED
    previous_color = GREEN if previous_price >= 0 else RED
    price_changes[symbol] = price_changes.get(symbol, {'increased': 0, 'decreased': 0})
    if current_price > previous_price:
        price_changes[symbol]['increased'] += 1
        print(f"{symbol} price just increased | current price: {current_color}${current_price:.2f}{RESET} (change: {change_color}${price_change:.2f}{RESET})")
        logging.info(f"{symbol} price just increased | current price: ${current_price:.2f} (change: ${price_change:.2f})")
    elif current_price < previous_price:
        price_changes[symbol]['decreased'] += 1
        print(f"{symbol} price just decreased | current price: {current_color}${current_price:.2f}{RESET} (change: {change_color}${price_change:.2f}{RESET})")
        logging.info(f"{symbol} price just decreased | current price: ${current_price:.2f} (change: ${price_change:.2f})")
    else:
        print(f"{symbol} price has not changed | current price: {current_color}${current_price:.2f}{RESET}")
        logging.info(f"{symbol} price has not changed | current price: ${current_price:.2f}")
    update_previous_price(symbol, current_price)

def print_database_tables():
    if PRINT_DATABASE:
        with db_lock:
            session = SessionLocal()
            try:
                print("\nTrade History In This Robot's Database:")
                print("\nStock | Buy or Sell | Quantity | Avg. Price | Date")
                for record in session.query(TradeHistory).all():
                    print(f"{record.symbols} | {record.action} | {record.quantity:.4f} | ${record.price:.2f} | {record.date}")
                print("\nPositions in the Database To Sell:")
                print("\nStock | Quantity | Avg. Price | Date | Current Price | % Change")
                for record in session.query(Position).all():
                    current_price = client_get_quote(record.symbols)
                    percentage_change = ((current_price - record.avg_price) / record.avg_price * 100) if current_price and record.avg_price else 0
                    color = GREEN if percentage_change >= 0 else RED
                    price_color = GREEN if current_price >= 0 else RED
                    print(f"{record.symbols} | {record.quantity:.4f} | ${record.avg_price:.2f} | {record.purchase_date} | {price_color}${current_price:.2f}{RESET} | {color}{percentage_change:.2f}%{RESET}")
            except Exception as e:
                logging.error(f"Error printing database: {e}")
                print(f"Error printing database: {e}")
            finally:
                session.close()

def get_symbols_to_buy():
    print("Loading symbols to buy...")
    logging.info("Loading symbols to buy")
    try:
        with open('electricity-or-utility-stocks-to-buy-list.txt', 'r') as file:
            symbols = [line.strip().upper() for line in file if line.strip()]
        print(f"Loaded {len(symbols)} symbols.")
        logging.info(f"Loaded {len(symbols)} symbols")
        return symbols
    except FileNotFoundError:
        print("Error: Symbols file not found.")
        logging.error("Symbols file not found.")
        return []

def remove_symbols_from_trade_list(symbol):
    print(f"Removing {symbol} from trade list...")
    logging.info(f"Removing {symbol} from trade list")
    try:
        with open('electricity-or-utility-stocks-to-buy-list.txt', 'r') as file:
            lines = file.readlines()
        with open('electricity-or-utility-stocks-to-buy-list.txt', 'w') as file:
            for line in lines:
                if line.strip() != symbol:
                    file.write(line)
        print(f"Removed {symbol} from trade list.")
        logging.info(f"Removed {symbol} from trade list")
    except Exception as e:
        logging.error(f"Error removing {symbol} from trade list: {e}")
        print(f"Error removing {symbol} from trade list: {e}")

def get_open_orders_for_symbol(symbol):
    open_orders = client_list_open_orders()
    return [o for o in open_orders if o.get('instrument', {}).get('symbol') == symbol]

def ensure_no_open_orders(symbol):
    print(f"Checking for open orders for {symbol} before placing new order...")
    logging.info(f"Checking for open orders for {symbol}")
    open_orders = get_open_orders_for_symbol(symbol)
    if not open_orders:
        print(f"No open orders found for {symbol}.")
        logging.info(f"No open orders found for {symbol}")
        return True
    print(f"Found {len(open_orders)} open orders for {symbol}. Initiating cancellation process...")
    logging.info(f"Found {len(open_orders)} open orders for {symbol}")
    while open_orders:
        print(f"Cancelling {len(open_orders)} open orders for {symbol}...")
        for order in open_orders:
            order_id = order.get('orderId')
            if client_cancel_order(order_id):
                print(f"Cancelled order {order_id} for {symbol}.")
                logging.info(f"Cancelled order {order_id} for {symbol}")
            else:
                print(f"Failed to cancel order {order_id} for {symbol}.")
                logging.error(f"Failed to cancel order {order_id} for {symbol}")
        print("Waiting 60 seconds for cancellations to process...")
        time.sleep(60)
        print("Checking status every 30 seconds until all cancelled...")
        while True:
            time.sleep(30)
            open_orders = get_open_orders_for_symbol(symbol)
            if not open_orders:
                print(f"All open orders for {symbol} have been cancelled.")
                logging.info(f"All open orders for {symbol} have been cancelled")
                break
            print(f"Still {len(open_orders)} open orders for {symbol}. Cancelling again...")
            logging.info(f"Still {len(open_orders)} open orders for {symbol}")
            for order in open_orders:
                order_id = order.get('orderId')
                client_cancel_order(order_id)
    print("Waiting 30 seconds for final confirmation...")
    time.sleep(30)
    open_orders = get_open_orders_for_symbol(symbol)
    if open_orders:
        print(f"Warning: Still {len(open_orders)} open orders for {symbol} after final check. Cancelling one more time...")
        logging.warning(f"Still {len(open_orders)} open orders for {symbol} after final check")
        for order in open_orders:
            order_id = order.get('orderId')
            client_cancel_order(order_id)
        time.sleep(30)
    else:
        print(f"Confirmed: No open orders for {symbol}.")
        logging.info(f"Confirmed: No open orders for {symbol}")
    return True

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def place_stop_loss_order(symbol, qty, avg_price, atr_multiplier=2.0):
    try:
        atr = get_average_true_range(symbol)
        if atr is None:
            logging.error(f"No ATR for {symbol}. Skipping stop-loss.")
            print(f"No ATR for {symbol}. Skipping stop-loss.")
            return None, None
        stop_price = round(avg_price * (1 - atr_multiplier * atr / avg_price), 2)
        stop_color = GREEN if stop_price >= 0 else RED
        sell_qty = qty if FRACTIONAL_BUY_ORDERS else int(qty)
        if sell_qty == 0:
            logging.error(f"Skipped stop-loss for {symbol}: Quantity {sell_qty} is zero.")
            print(f"Skipped stop-loss for {symbol}: Quantity {sell_qty} is zero.")
            return None, None
        order_id = client_place_order(symbol, "SELL", quantity=sell_qty, order_type="STOP_MARKET", stop_price=stop_price)
        if order_id:
            print(f"Placed stop-loss order for {sell_qty:.4f} shares of {symbol} at {stop_color}${stop_price:.2f}{RESET}, Order ID: {order_id}")
            logging.info(f"Placed stop-loss for {sell_qty:.4f} shares of {symbol} at ${stop_price:.2f}, Order ID: {order_id}")
            return order_id, stop_price
        return None, None
    except Exception as e:
        logging.error(f"Error placing stop-loss for {symbol}: {e}")
        print(f"Error placing stop-loss for {symbol}: {e}")
        return None, None

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
                positions = session.query(Position).filter(Position.stop_order_id != None).all()
                for pos in positions:
                    status = client_get_order_status(pos.stop_order_id)
                    if status and status["status"] in ["FILLED", "CANCELLED"]:
                        pos.stop_order_id = None
                        pos.stop_price = None
                        session.commit()
                    symbol = pos.symbols
                    current_price = client_get_quote(symbol)
                    if current_price is None:
                        continue
                    if current_price >= pos.avg_price * 1.01:
                        new_stop_price = round(current_price * 0.99, 2)
                        if new_stop_price > pos.stop_price:
                            print(f"Tightening stop for {symbol}: Old={pos.stop_price:.2f}, New={new_stop_price:.2f}")
                            logging.info(f"Tightening stop for {symbol}: Old=${pos.stop_price:.2f}, New=${new_stop_price:.2f}")
                            if pos.stop_order_id and client_cancel_order(pos.stop_order_id):
                                print(f"Cancelled old stop order {pos.stop_order_id} for {symbol}")
                                logging.info(f"Cancelled old stop order {pos.stop_order_id} for {symbol}")
                            new_order_id, new_stop_price = place_stop_loss_order(symbol, pos.quantity, current_price, atr_multiplier=1.0)
                            if new_order_id:
                                pos.stop_order_id = new_order_id
                                pos.stop_price = new_stop_price
                                session.commit()
            except Exception as e:
                session.rollback()
                logging.error(f"Error monitoring stop-losses: {e}")
                print(f"Error monitoring stop-losses: {e}")
            finally:
                session.close()
    finally:
        task_running['monitor_stop_losses'] = False

