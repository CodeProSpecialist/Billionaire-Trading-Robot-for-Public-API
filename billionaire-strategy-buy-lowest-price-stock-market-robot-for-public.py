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
ALL_BUY_ORDERS_ARE_5_DOLLARS = False  # When True, every buy order is a $1.00 fractional share market day order
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
def initialize_csv():
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
def place_market_order(symbol, side, fractional=False, amount=None, quantity=None):
    """Place MARKET order (fractional or full-share)"""
    url = f"{BASE_URL}/trading/{account_id}/order"
    order_id = str(uuid4())
    expiration = {"timeInForce": "DAY"} if fractional else {"timeInForce": "GTD", "expirationTime": get_expiration()}
    payload = {
        "orderId": order_id,
        "instrument": {"symbol": symbol, "type": "EQUITY"},
        "orderSide": side.upper(),
        "orderType": "MARKET",
        "expiration": expiration,
        "openCloseIndicator": "OPEN"
    }
    if fractional and amount is not None:
        payload["amount"] = f"{amount:.2f}"
    elif quantity is not None:
        payload["quantity"] = str(quantity)
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
def client_place_order(symbol, side, amount=None, quantity=None, order_type="MARKET", limit_price=None, stop_price=None):
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId")
            return None
        if order_type == "MARKET":
            order_response = place_market_order(
                symbol=symbol,
                side=side,
                fractional=FRACTIONAL_BUY_ORDERS if amount is not None else False,
                amount=amount,
                quantity=quantity
            )
        else:  # STOP_MARKET
            url = f"{BASE_URL}/trading/{account_id}/order"
            order_id = str(uuid4())
            payload = {
                "orderId": order_id,
                "instrument": {"symbol": symbol, "type": "EQUITY"},
                "orderSide": side.upper(),
                "orderType": "STOP_MARKET",
                "stopPrice": stop_price,
                "quantity": str(quantity),
                "expiration": {"timeInForce": "GTD", "expirationTime": get_expiration()},
                "openCloseIndicator": "OPEN"
            }
            response = requests.post(url, headers=HEADERS, json=payload, timeout=10)
            if response.status_code >= 400:
                print(f"HTTP Error Response for {symbol}: {response.status_code} {response.text}")
                logging.error(f"HTTP Error Response for {symbol}: {response.status_code} {response.text}")
                return {"error": f"HTTP {response.status_code}: {response.text}"}
            response.raise_for_status()
            order_response = response.json()
            logging.info(f"Stop-market order placed successfully for {symbol}: {order_response}")
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
                positions = session.query(Position).filter(Position.stop_order_id != None).all()
                today_date_str = datetime.today().strftime("%Y-%m-%d")
                for pos in positions:
                    status_info = client_get_order_status(pos.stop_order_id)
                    if status_info and status_info["status"] == "FILLED":
                        filled_qty = status_info["filled_qty"]
                        filled_price = status_info["avg_price"] or client_get_quote(pos.symbols)
                        send_alert(
                            f"Stop-loss triggered for {pos.symbols}: {filled_qty:.4f} shares sold at ${filled_price:.2f}",
                            subject=f"Stop-Loss Triggered: {pos.symbols}",
                            use_whatsapp=True
                        )
                        trade = TradeHistory(
                            symbols=pos.symbols,
                            action='sell',
                            quantity=filled_qty,
                            price=filled_price,
                            date=today_date_str
                        )
                        session.add(trade)
                        pos.quantity = 0
                        pos.stop_order_id = None
                        pos.stop_price = None
                        session.delete(pos)
                        session.commit()
                        with open(csv_filename, mode='a', newline='') as csv_file:
                            csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                            csv_writer.writerow({
                                'Date': today_date_str,
                                'Buy': 0,
                                'Sell': filled_qty,
                                'Quantity': filled_qty,
                                'Symbol': pos.symbols,
                                'Price Per Share': filled_price
                            })
                        logging.info(f"Stop-loss sell recorded for {filled_qty:.4f} shares of {pos.symbols} at ${filled_price:.2f}")
                        print(f"Stop-loss sell recorded for {filled_qty:.4f} shares of {pos.symbols} at ${filled_price:.2f}")
            except Exception as e:
                session.rollback()
                logging.error(f"Error checking stop orders: {e}")
                print(f"Error checking stop orders: {e}")
            finally:
                session.close()
    finally:
        task_running['check_stop_order_status'] = False

def check_price_moves():
    if task_running['check_price_moves']:
        print("check_price_moves already running. Skipping.")
        logging.info("check_price_moves already running. Skipping")
        return
    task_running['check_price_moves'] = True
    try:
        with db_lock:
            session = SessionLocal()
            try:
                positions = session.query(Position).all()
                for pos in positions:
                    current_price = client_get_quote(pos.symbols)
                    if current_price is None:
                        continue
                    pct_change = (current_price - pos.avg_price) / pos.avg_price * 100
                    if abs(pct_change) >= 5:
                        direction = "up" if pct_change > 0 else "down"
                        send_alert(
                            f"{pos.symbols} moved {pct_change:.2f}% {direction} from avg ${pos.avg_price:.2f} to ${current_price:.2f}",
                            subject=f"Price Alert: {pos.symbols}",
                            use_whatsapp=True
                        )
            except Exception as e:
                session.rollback()
                logging.error(f"Error checking price moves: {e}")
                print(f"Error checking price moves: {e}")
            finally:
                session.close()
    finally:
        task_running['check_price_moves'] = False

def poll_order_status(order_id, timeout=300):
    start_time = time.time()
    while time.time() - start_time < timeout:
        status_info = client_get_order_status(order_id)
        if status_info and status_info["status"] in ["FILLED", "CANCELLED", "REJECTED"]:
            return status_info
        time.sleep(5)
    logging.warning(f"Order {order_id} status check timed out after {timeout} seconds.")
    print(f"Order {order_id} status check timed out after {timeout} seconds.")
    return None

def send_alert(message, subject="Trading Bot Alert"):
    """
    Sends a WhatsApp alert using CallMeBot API with environment variables.
    Env vars required:
      - CALLMEBOT_API_KEY
      - CALLMEBOT_PHONE
    """
    api_key = os.getenv("CALLMEBOT_API_KEY")
    phone = os.getenv("CALLMEBOT_PHONE")

    if not api_key or not phone:
        logging.error("Missing CALLMEBOT_API_KEY or CALLMEBOT_PHONE environment variable.")
        print("Missing CALLMEBOT_API_KEY or CALLMEBOT_PHONE environment variable.")
        return

    full_message = f"{subject}: {message}"
    encoded_message = urllib.parse.quote_plus(full_message)

    # Build the full URL with env vars
    url = f"https://api.callmebot.com/whatsapp.php?phone={phone}&text={encoded_message}&apikey={api_key}"

    try:
        response = requests.get(url)
        if response.status_code == 200:
            print(f"WhatsApp alert sent: {subject}")
            logging.info(f"WhatsApp alert sent: {subject}")
        else:
            print(f" Failed to send WhatsApp alert: {response.text}")
            logging.error(f"Failed to send WhatsApp alert: {response.text}")
    except Exception as e:
        logging.error(f"Error sending WhatsApp alert: {e}")
        print(f"Error sending WhatsApp alert: {e}")

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

YF_CALLS_PER_MINUTE = 60
CLIENT_CALLS_PER_MINUTE = 100
ONE_MINUTE = 60

@sleep_and_retry
@limits(calls=CLIENT_CALLS_PER_MINUTE, period=ONE_MINUTE)
def rate_limited_get_quote(sym):
    return client_get_quote(sym)

@sleep_and_retry
@limits(calls=YF_CALLS_PER_MINUTE, period=ONE_MINUTE)
def rate_limited_yf_history(sym, period="20d"):
    return yf.Ticker(sym.replace('.', '-')).history(period=period)

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
        print(f"Total account equity: ${total_equity:.2f}")
        logging.info(f"Total account equity: ${total_equity:.2f}")
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
        print("Filtering valid symbols for buying...")
        logging.info("Filtering valid symbols for buying")
        for sym in symbols_to_buy_list:
            current_price = rate_limited_get_quote(sym)
            if current_price is None:
                print(f"No valid price data for {sym}. Skipping.")
                logging.info(f"No valid price data for {sym}. Skipping")
                continue
            df = rate_limited_yf_history(sym)
            if df.empty or len(df) < 14:
                print(f"Insufficient data for {sym} (daily rows: {len(df)}). Skipping.")
                logging.info(f"Insufficient data for {sym} (daily rows: {len(df)}). Skipping")
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
            dollar_amount = max_new_exposure / len(valid_symbols)
        if dollar_amount <= 0:
            print("Calculated dollar amount for buys is <= 0. Skipping buys.")
            logging.info("Calculated dollar amount for buys is <= 0. Skipping buys.")
            return
        for sym in valid_symbols:
            print(f"\n{'='*60}")
            print(f"Processing {sym}...")
            print(f"{'='*60}")
            logging.info(f"Processing {sym}")
            today_date = datetime.today().date()
            today_date_str = today_date.strftime("%Y-%m-%d")
            current_datetime = datetime.now(eastern)
            current_time_str = current_datetime.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
            print(f"Analysis time: {current_time_str}")
            logging.info(f"Analysis time: {current_time_str}")
            current_price = rate_limited_get_quote(sym)
            if current_price is None:
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
            for interval, delta in interval_map.items():
                with price_history_lock:
                    if current_timestamp - last_stored[sym][interval] >= delta:
                        price_history[sym][interval].append(current_price)
                        last_stored[sym][interval] = current_timestamp
                        print(f"Stored price {current_price} for {sym} at {interval} interval.")
                        logging.info(f"Stored price {current_price} for {sym} at {interval} interval")
            yf_symbol = sym.replace('.', '-')
            print(f"Fetching 20-day historical data for {yf_symbol}...")
            logging.info(f"Fetching 20-day historical data for {yf_symbol}")
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
            lookback_candles = min(20, len(close))
            print(f"Data for {yf_symbol}: rows={len(df)}, close_length={len(close)}, NaN_count={np.isnan(close).sum()}")
            logging.info(f"Data for {yf_symbol}: rows={len(df)}, close_length={len(close)}, NaN_count={np.isnan(close).sum()}")
            try:
                rsi = talib.RSI(close, timeperiod=14)
                latest_rsi = rsi[-1] if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50.00
                latest_rsi = round(latest_rsi, 2)
                if latest_rsi < 50:
                    score += 1
                    print(f"{yf_symbol}: RSI < 50 ({latest_rsi:.2f}): +1 score")
                    logging.info(f"{yf_symbol}: RSI < 50 ({latest_rsi:.2f}): +1 score")
                rsi_display = f"{latest_rsi:.2f}"
                print(f"Latest RSI: {rsi_display}")
                logging.info(f"Latest RSI: {rsi_display}")
            except Exception as e:
                print(f"Error calculating RSI for {yf_symbol}: {e}")
                logging.error(f"Error calculating RSI for {yf_symbol}: {e}")
                latest_rsi = 50.00
                print(f"Latest RSI: 50.00")
                logging.info(f"Latest RSI: 50.00")
            if close[-1] <= close[-2] * 0.997:
                score += 1
                print(f"{yf_symbol}: Price decrease >= 0.3% from previous close: +1 score")
                logging.info(f"{yf_symbol}: Price decrease >= 0.3% from previous close: +1 score")
            print(f"Checking for bullish reversal patterns in {sym}...")
            logging.info(f"Checking for bullish reversal patterns in {sym}")
            bullish_reversal_detected = False
            reversal_candle_index = None
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
            for i in range(-1, -lookback_candles, -1):
                if abs(i) > len(open_valid):
                    continue
                try:
                    for name, func in patterns.items():
                        res = func(open_valid[:i + 1], high_valid[:i + 1], low_valid[:i + 1], close_valid[:i + 1])
                        if res[-1] > 0:
                            detected_patterns.append(name)
                            bullish_reversal_detected = True
                            reversal_candle_index = i
                    if bullish_reversal_detected:
                        score += 1
                        print(f"{yf_symbol}: Detected bullish reversal patterns at candle {reversal_candle_index}: {', '.join(detected_patterns)} (+1 score)")
                        logging.info(f"{yf_symbol}: Detected bullish reversal patterns at candle {reversal_candle_index}: {', '.join(detected_patterns)}")
                        break
                except Exception as e:
                    print(f"Error in candlestick pattern detection for {yf_symbol}: {e}")
                    logging.error(f"Error in candlestick pattern detection for {yf_symbol}: {e}")
                    continue
            if score < 3:
                print(f"{yf_symbol}: Score too low ({score} < 3). Skipping.")
                logging.info(f"{yf_symbol}: Score too low ({score} < 3). Skipping")
                continue
            print(f"Calculating volume metrics for {sym}...")
            logging.info(f"Calculating volume metrics for {sym}")
            recent_avg_volume = df['Volume'].iloc[-5:].mean() if len(df) >= 5 else 0
            prior_avg_volume = df['Volume'].iloc[-10:-5].mean() if len(df) >= 10 else recent_avg_volume
            volume_decrease = recent_avg_volume < prior_avg_volume if len(df) >= 10 else False
            print(f"{yf_symbol}: Recent avg volume = {recent_avg_volume:.0f}, Prior avg volume = {prior_avg_volume:.0f}, Volume decrease = {volume_decrease}")
            logging.info(f"{yf_symbol}: Recent avg volume = {recent_avg_volume:.0f}, Prior avg volume = {prior_avg_volume:.0f}, Volume decrease = {volume_decrease}")
            print(f"Calculating RSI metrics for {sym}...")
            logging.info(f"Calculating RSI metrics for {sym}")
            try:
                rsi_series = talib.RSI(close, timeperiod=14)
                rsi_decrease = False
                recent_avg_rsi = 50.00
                prior_avg_rsi = 50.00
                if len(rsi_series) >= 10:
                    recent_rsi_values = rsi_series[-5:][~np.isnan(rsi_series[-5:])]
                    prior_rsi_values = rsi_series[-10:-5][~np.isnan(rsi_series[-10:-5])]
                    if len(recent_rsi_values) > 0 and len(prior_rsi_values) > 0:
                        recent_avg_rsi = round(np.mean(recent_rsi_values), 2)
                        prior_avg_rsi = round(np.mean(prior_rsi_values), 2)
                        rsi_decrease = recent_avg_rsi < prior_avg_rsi
                    else:
                        recent_avg_rsi = 50.00
                        prior_avg_rsi = 50.00
                print(f"{yf_symbol}: Recent avg RSI = {recent_avg_rsi:.2f}, Prior avg RSI = {prior_avg_rsi:.2f}, RSI decrease = {rsi_decrease}")
                logging.info(f"{yf_symbol}: Recent avg RSI = {recent_avg_rsi:.2f}, Prior avg RSI = {prior_avg_rsi:.2f}, RSI decrease = {rsi_decrease}")
            except Exception as e:
                print(f"Error calculating RSI metrics for {yf_symbol}: {e}")
                logging.error(f"Error calculating RSI metrics for {yf_symbol}: {e}")
                recent_avg_rsi = 50.00
                prior_avg_rsi = 50.00
                rsi_decrease = False
            previous_price = get_previous_price(sym)
            price_increase = current_price > previous_price * 1.005
            print(f"{yf_symbol}: Price increase check: Current = {GREEN if current_price > previous_price else RED}${current_price:.2f}{RESET}, Previous = ${previous_price:.2f}, Increase = {price_increase}")
            logging.info(f"{yf_symbol}: Price increase check: Current = ${current_price:.2f}, Previous = ${previous_price:.2f}, Increase = {price_increase}")
            print(f"Checking price drop for {sym}...")
            logging.info(f"Checking price drop for {sym}")
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
            short_term_trend = None
            with price_history_lock:
                if sym in price_history and '5min' in price_history[sym] and len(price_history[sym]['5min']) >= 2:
                    recent_prices = price_history[sym]['5min'][-2:]
                    short_term_trend = 'up' if recent_prices[-1] > recent_prices[-2] else 'down'
                    print(f"{yf_symbol}: Short-term price trend (5min): {short_term_trend}")
                    logging.info(f"{yf_symbol}: Short-term price trend (5min): {short_term_trend}")
            if detected_patterns and sym in price_history:
                with price_history_lock:
                    for interval, prices in price_history[sym].items():
                        if prices:
                            print(f"{yf_symbol}: Price history at {interval}: {prices[-5:]}")
                            logging.info(f"{yf_symbol}: Price history at {interval}: {prices[-5:]}")
            if not ensure_no_open_orders(sym):
                print(f"Cannot proceed with {sym} due to unresolved open orders.")
                logging.info(f"Cannot proceed with {sym} due to unresolved open orders")
                continue
            in_uptrend = is_in_uptrend(sym)
            print(f"{yf_symbol}: Uptrend = {in_uptrend}")
            logging.info(f"{yf_symbol}: Uptrend = {in_uptrend}")
            if not in_uptrend:
                print(f"{yf_symbol} is not in an uptrend. Skipping buy.")
                logging.info(f"{yf_symbol} is not in an uptrend. Skipping buy")
                continue
            with buy_sell_lock:
                qty = dollar_amount / current_price if FRACTIONAL_BUY_ORDERS else int(dollar_amount / current_price)
                if qty <= 0:
                    print(f"Calculated quantity for {sym} is <= 0. Skipping.")
                    logging.info(f"Calculated quantity for {sym} is <= 0. Skipping")
                    continue
                order_id = client_place_order(
                    symbol=sym,
                    side="BUY",
                    amount=dollar_amount if FRACTIONAL_BUY_ORDERS else None,
                    quantity=qty if not FRACTIONAL_BUY_ORDERS else None,
                    order_type="MARKET"
                )
                if not order_id:
                    print(f"Failed to place buy order for {sym}.")
                    logging.info(f"Failed to place buy order for {sym}")
                    continue
                status_info = poll_order_status(order_id, timeout=300)
                if status_info and status_info["status"] == "FILLED":
                    filled_qty = status_info["filled_qty"]
                    avg_price = status_info["avg_price"] or current_price
                    if filled_qty <= 0:
                        print(f"Buy order for {sym} filled with zero quantity. Skipping.")
                        logging.info(f"Buy order for {sym} filled with zero quantity. Skipping")
                        continue
                    buy_signal += 1
                    print(f"Buy order filled for {filled_qty:.4f} shares of {sym} at ${avg_price:.2f}")
                    logging.info(f"Buy order filled for {filled_qty:.4f} shares of {sym} at ${avg_price:.2f}")
                    with db_lock:
                        session = SessionLocal()
                        try:
                            trade = TradeHistory(
                                symbols=sym,
                                action='buy',
                                quantity=filled_qty,
                                price=avg_price,
                                date=today_date_str
                            )
                            session.add(trade)
                            existing_position = session.query(Position).filter_by(symbols=sym).first()
                            if existing_position:
                                total_qty = existing_position.quantity + filled_qty
                                total_cost = (existing_position.quantity * existing_position.avg_price) + (filled_qty * avg_price)
                                existing_position.avg_price = total_cost / total_qty
                                existing_position.quantity = total_qty
                                existing_position.purchase_date = today_date_str
                            else:
                                position = Position(
                                    symbols=sym,
                                    quantity=filled_qty,
                                    avg_price=avg_price,
                                    purchase_date=today_date_str
                                )
                                session.add(position)
                            order_id_stop, stop_price = place_stop_loss_order(sym, filled_qty, avg_price)
                            if order_id_stop:
                                position = session.query(Position).filter_by(symbols=sym).first()
                                position.stop_order_id = order_id_stop
                                position.stop_price = stop_price
                            session.commit()
                            with open(csv_filename, mode='a', newline='') as csv_file:
                                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                                csv_writer.writerow({
                                    'Date': today_date_str,
                                    'Buy': filled_qty,
                                    'Sell': 0,
                                    'Quantity': filled_qty,
                                    'Symbol': sym,
                                    'Price Per Share': avg_price
                                })
                            send_alert(
                                f"Bought {filled_qty:.4f} shares of {sym} at ${avg_price:.2f}",
                                subject=f"Trade Executed: {sym}",
                                use_whatsapp=True
                            )
                            print(f"Buy recorded in CSV and DB for {filled_qty:.4f} shares of {sym} at ${avg_price:.2f}")
                            logging.info(f"Buy recorded for {filled_qty:.4f} shares of {sym} at ${avg_price:.2f}")
                            symbols_to_remove.append(sym)
                        except Exception as e:
                            session.rollback()
                            logging.error(f"Error saving buy to DB for {sym}: {e}")
                            print(f"Error saving buy to DB for {sym}: {e}")
                        finally:
                            session.close()
            update_previous_price(sym, current_price)
        for sym in symbols_to_remove:
            remove_symbols_from_trade_list(sym)
        print(f"\nBuy signals triggered: {buy_signal}")
        logging.info(f"Buy signals triggered: {buy_signal}")
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
        print(f"Positions to evaluate for selling: {len(positions)}")
        logging.info(f"Positions to evaluate for selling: {len(positions)}")
        if not positions:
            print("No positions to sell.")
            logging.info("No positions to sell.")
            return
        sell_signal = 0
        today_date_str = datetime.today().strftime("%Y-%m-%d")
        for pos in positions:
            sym = pos['symbol']
            qty = pos['qty']
            avg_price = pos['avg_entry_price']
            purchase_date = pos['purchase_date']
            print(f"\n{'='*60}")
            print(f"Processing {sym} for selling...")
            print(f"{'='*60}")
            logging.info(f"Processing {sym} for selling")
            current_price = rate_limited_get_quote(sym)
            if current_price is None:
                print(f"No valid price data for {sym}. Skipping sell.")
                logging.info(f"No valid price data for {sym}. Skipping sell")
                continue
            current_color = GREEN if current_price >= 0 else RED
            print(f"Current price for {sym}: {current_color}${current_price:.2f}{RESET}")
            logging.info(f"Current price for {sym}: ${current_price:.2f}")
            yf_symbol = sym.replace('.', '-')
            df = rate_limited_yf_history(yf_symbol)
            if df.empty or len(df) < 14:
                print(f"Insufficient historical data for {sym} (rows: {len(df)}). Skipping sell.")
                logging.info(f"Insufficient historical data for {sym} (rows: {len(df)}). Skipping sell")
                continue
            df = df.dropna(subset=['Open', 'High', 'Low', 'Close'])
            if len(df) < 14:
                print(f"After cleaning, insufficient data for {yf_symbol} (rows: {len(df)}). Skipping sell.")
                logging.info(f"After cleaning, insufficient data for {yf_symbol} (rows: {len(df)}). Skipping sell")
                continue
            score = 0
            close = df['Close'].values
            try:
                rsi = talib.RSI(close, timeperiod=14)
                latest_rsi = rsi[-1] if len(rsi) > 0 and not np.isnan(rsi[-1]) else 50.00
                latest_rsi = round(latest_rsi, 2)
                rsi_display = f"{latest_rsi:.2f}"
                print(f"Latest RSI for {yf_symbol}: {rsi_display}")
                logging.info(f"Latest RSI for {yf_symbol}: {rsi_display}")
                if latest_rsi > 70:
                    score += 1
                    print(f"{yf_symbol}: RSI > 70 ({latest_rsi:.2f}): +1 score")
                    logging.info(f"{yf_symbol}: RSI > 70 ({latest_rsi:.2f}): +1 score")
            except Exception as e:
                print(f"Error calculating RSI for {yf_symbol}: {e}")
                logging.error(f"Error calculating RSI for {yf_symbol}: {e}")
                latest_rsi = 50.00
                print(f"Latest RSI: 50.00")
                logging.info(f"Latest RSI: 50.00")
            if close[-1] >= close[-2] * 1.003:
                score += 1
                print(f"{yf_symbol}: Price increase >= 0.3% from previous close: +1 score")
                logging.info(f"{yf_symbol}: Price increase >= 0.3% from previous close: +1 score")
            print(f"Checking for bearish reversal patterns in {sym}...")
            logging.info(f"Checking for bearish reversal patterns in {sym}")
            bearish_reversal_detected = False
            detected_patterns = []
            patterns = {
                'Shooting Star': talib.CDLSHOOTINGSTAR,
                'Bearish Engulfing': talib.CDLENGULFING,
                'Evening Star': talib.CDLEVENINGSTAR,
                'Dark Pool Cover': talib.CDLDARKCLOUDCOVER,
                'Three Black Crows': talib.CDL3BLACKCROWS,
                'Hanging Man': talib.CDLHANGINGMAN
            }
            valid_mask = ~np.isnan(df['Open'].values) & ~np.isnan(df['High'].values) & ~np.isnan(df['Low'].values) & ~np.isnan(df['Close'].values)
            if valid_mask.sum() < 2:
                print(f"Insufficient valid data for {sym} after removing NaN values. Skipping candlestick analysis.")
                logging.info(f"Insufficient valid data for {sym} after removing NaN values.")
                continue
            open_valid = np.array(df['Open'].values[valid_mask], dtype=np.float64)
            high_valid = np.array(df['High'].values[valid_mask], dtype=np.float64)
            low_valid = np.array(df['Low'].values[valid_mask], dtype=np.float64)
            close_valid = np.array(df['Close'].values[valid_mask], dtype=np.float64)
            lookback_candles = min(20, len(close_valid))
            for i in range(-1, -lookback_candles, -1):
                if abs(i) > len(open_valid):
                    continue
                try:
                    for name, func in patterns.items():
                        res = func(open_valid[:i + 1], high_valid[:i + 1], low_valid[:i + 1], close_valid[:i + 1])
                        if res[-1] < 0:
                            detected_patterns.append(name)
                            bearish_reversal_detected = True
                    if bearish_reversal_detected:
                        score += 1
                        print(f"{yf_symbol}: Detected bearish reversal patterns: {', '.join(detected_patterns)} (+1 score)")
                        logging.info(f"{yf_symbol}: Detected bearish reversal patterns: {', '.join(detected_patterns)}")
                        break
                except Exception as e:
                    print(f"Error in candlestick pattern detection for {yf_symbol}: {e}")
                    logging.error(f"Error in candlestick pattern detection for {yf_symbol}: {e}")
                    continue
            if score < 2:
                print(f"{yf_symbol}: Score too low ({score} < 2). Skipping sell.")
                logging.info(f"{yf_symbol}: Score too low ({score} < 2). Skipping sell")
                continue
            try:
                rsi_series = talib.RSI(close, timeperiod=14)
                recent_rsi_values = rsi_series[-5:][~np.isnan(rsi_series[-5:])]
                prior_rsi_values = rsi_series[-10:-5][~np.isnan(rsi_series[-10:-5])]
                recent_avg_rsi = np.mean(recent_rsi_values) if len(recent_rsi_values) > 0 else 50.00
                prior_avg_rsi = np.mean(prior_rsi_values) if len(prior_rsi_values) > 0 else 50.00
                rsi_increase = recent_avg_rsi > prior_avg_rsi
                print(f"{yf_symbol}: Recent avg RSI = {recent_avg_rsi:.2f}, Prior avg RSI = {prior_avg_rsi:.2f}, RSI increase = {rsi_increase}")
                logging.info(f"{yf_symbol}: Recent avg RSI = {recent_avg_rsi:.2f}, Prior avg RSI = {prior_avg_rsi:.2f}, RSI increase = {rsi_increase}")
            except Exception as e:
                print(f"Error calculating RSI metrics for {yf_symbol}: {e}")
                logging.error(f"Error calculating RSI metrics for {yf_symbol}: {e}")
                rsi_increase = False
            if not ensure_no_open_orders(sym):
                print(f"Cannot proceed with {sym} due to unresolved open orders.")
                logging.info(f"Cannot proceed with {sym} due to unresolved open orders")
                continue
            # New rule: Only sell if not bought today AND profit ≥ 0.3%
            if purchase_date == today_date_str or current_price < avg_price * 1.003:
                print(f"Skipping {sym}: either bought today or profit less than 0.3%. Avg={avg_price}, Current={current_price}")
                logging.info(f"Skipping {sym}: either bought today or profit less than 0.3%. Avg={avg_price}, Current={current_price}")
                continue
            with buy_sell_lock:
                sell_qty = qty if FRACTIONAL_BUY_ORDERS else int(qty)
                if sell_qty <= 0:
                    print(f"Calculated sell quantity for {sym} is <= 0. Skipping.")
                    logging.info(f"Calculated sell quantity for {sym} is <= 0. Skipping")
                    continue
                order_id = client_place_order(
                    symbol=sym,
                    side="SELL",
                    amount=None,
                    quantity=sell_qty,
                    order_type="MARKET"
                )
                if not order_id:
                    print(f"Failed to place sell order for {sym}.")
                    logging.info(f"Failed to place sell order for {sym}")
                    continue
                status_info = poll_order_status(order_id, timeout=300)
                if status_info and status_info["status"] == "FILLED":
                    filled_qty = status_info["filled_qty"]
                    avg_price = status_info["avg_price"] or current_price
                    if filled_qty <= 0:
                        print(f"Sell order for {sym} filled with zero quantity. Skipping.")
                        logging.info(f"Sell order for {sym} filled with zero quantity. Skipping")
                        continue
                    sell_signal += 1
                    print(f"Sell order filled for {filled_qty:.4f} shares of {sym} at ${avg_price:.2f}")
                    logging.info(f"Sell order filled for {filled_qty:.4f} shares of {sym} at ${avg_price:.2f}")
                    with db_lock:
                        session = SessionLocal()
                        try:
                            trade = TradeHistory(
                                symbols=sym,
                                action='sell',
                                quantity=filled_qty,
                                price=avg_price,
                                date=today_date_str
                            )
                            session.add(trade)
                            position = session.query(Position).filter_by(symbols=sym).first()
                            if position:
                                position.quantity -= filled_qty
                                if position.quantity <= 0:
                                    if position.stop_order_id:
                                        client_cancel_order(position.stop_order_id)
                                    session.delete(position)
                            session.commit()
                            with open(csv_filename, mode='a', newline='') as csv_file:
                                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                                csv_writer.writerow({
                                    'Date': today_date_str,
                                    'Buy': 0,
                                    'Sell': filled_qty,
                                    'Quantity': filled_qty,
                                    'Symbol': sym,
                                    'Price Per Share': avg_price
                                })
                            send_alert(
                                f"Sold {filled_qty:.4f} shares of {sym} at ${avg_price:.2f}",
                                subject=f"Trade Executed: {sym}",
                                use_whatsapp=True
                            )
                            print(f"Sell recorded in CSV and DB for {filled_qty:.4f} shares of {sym} at ${avg_price:.2f}")
                            logging.info(f"Sell recorded for {filled_qty:.4f} shares of {sym} at ${avg_price:.2f}")
                        except Exception as e:
                            session.rollback()
                            logging.error(f"Error saving sell to DB for {sym}: {e}")
                            print(f"Error saving sell to DB for {sym}: {e}")
                        finally:
                            session.close()
            update_previous_price(sym, current_price)
        print(f"\nSell signals triggered: {sell_signal}")
        logging.info(f"Sell signals triggered: {sell_signal}")
    except Exception as e:
        logging.error(f"Error in sell_stocks: {e}")
        print(f"Error in sell_stocks: {e}")
        traceback.print_exc()
    finally:
        task_running['sell_stocks'] = False

def main_loop():
    initialize_csv()
    if not fetch_token_and_account():
        print("Failed to initialize token and account. Exiting.")
        logging.error("Failed to initialize token and account. Exiting.")
        return
    stop_if_stock_market_is_closed()
    symbols_to_buy_list = get_symbols_to_buy()
    symbols_to_sell_dict = load_positions_from_database()
    print(f"\nInitial symbols to buy: {symbols_to_buy_list}")
    logging.info(f"Initial symbols to buy: {symbols_to_buy_list}")
    if PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE:
        print(f"\nInitial symbols to sell from DB: {list(symbols_to_sell_dict.keys())}")
        logging.info(f"Initial symbols to sell from DB: {list(symbols_to_sell_dict.keys())}")
    schedule.every(5).minutes.do(refresh_token_if_needed)
    schedule.every(5).minutes.do(sync_db_with_api)
    schedule.every(1).minutes.do(check_price_moves)
    schedule.every(5).minutes.do(check_stop_order_status)
    schedule.every(5).minutes.do(monitor_stop_losses)
    schedule.every(1).minutes.do(buy_stocks, symbols_to_sell_dict, symbols_to_buy_list, buy_sell_lock)
    schedule.every(5).minutes.do(sell_stocks, symbols_to_sell_dict, buy_sell_lock)
    schedule.every(5).minutes.do(print_database_tables)
    print("\nStarting main trading loop...")
    logging.info("Starting main trading loop")
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            print(f"Error in main loop: {e}")
            traceback.print_exc()
            time.sleep(60)

if __name__ == "__main__":
    main_loop()

