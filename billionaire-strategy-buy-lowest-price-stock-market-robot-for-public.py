import os
import time
import csv
import logging
import threading
import schedule
from uuid import uuid4
from datetime import datetime, timedelta, date
from datetime import time as time2
import pytz
import requests
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

# ANSI color codes for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
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
    symbols = Column(String)
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

# Initialize SQLAlchemy
engine = create_engine('sqlite:///trading_bot.db', connect_args={"check_same_thread": False})
with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
SessionLocal = scoped_session(sessionmaker(bind=engine))
Base.metadata.create_all(engine)

# NYSE Calendar
nyse_cal = mcal.get_calendar('NYSE')

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_cached_data(symbols, data_type, fetch_func, *args, **kwargs):
    print(f"Checking cache for {symbols} {data_type}...")
    key = (symbols, data_type)
    current_time = time.time()
    if key in data_cache and current_time - data_cache[key]['timestamp'] < CACHE_EXPIRY:
        print(f"Using cached {data_type} for {symbols}.")
        return data_cache[key]['data']
    else:
        print(f"Fetching new {data_type} for {symbols}...")
        data = fetch_func(*args, **kwargs)
        data_cache[key] = {'timestamp': current_time, 'data': data}
        print(f"Cached {data_type} for {symbols}.")
        return data

def stop_if_stock_market_is_closed():
    while True:
        current_datetime = datetime.now(eastern)
        current_time_str = current_datetime.strftime("%A, %B %d, %Y, %I:%M:%S %p")
        schedule = nyse_cal.schedule(start_date=current_datetime.date(), end_date=current_datetime.date())
        if not schedule.empty:
            market_open = schedule.iloc[0]['market_open'].astimezone(eastern)
            market_close = schedule.iloc[0]['market_close'].astimezone(eastern)
            pre_market_open = market_open - timedelta(hours=2)
            post_market_close = market_close + timedelta(hours=4)
            if (FRACTIONAL_BUY_ORDERS and market_open <= current_datetime <= market_close) or \
               (not FRACTIONAL_BUY_ORDERS and pre_market_open <= current_datetime <= post_market_close):
                print("Market is open for trading. Proceeding.")
                logging.info(f"{current_time_str}: Market is open for trading.")
                break
            else:
                print(f"\n{current_time_str}: Market closed. Open: {market_open.strftime('%I:%M %p')} - {market_close.strftime('%I:%M %p')}")
                logging.info(f"{current_time_str}: Market closed.")
                time.sleep(60)
        else:
            print(f"\n{current_time_str}: Market closed today (holiday/weekend).")
            logging.info(f"{current_time_str}: Market closed today.")
            time.sleep(60)

def print_database_tables():
    if PRINT_DATABASE:
        session = SessionLocal()
        try:
            positions = client_list_positions()
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
                print(f"{record.symbols} | {record.quantity:.4f} | ${record.avg_price:.2f} | {record.purchase_date} | {color}${current_price:.2f}{RESET} | {color}{percentage_change:.2f}%{RESET}")
        except Exception as e:
            logging.error(f"Error printing database: {e}")
        finally:
            session.close()

def get_symbols_to_buy():
    print("Loading symbols to buy...")
    try:
        with open('electricity-or-utility-stocks-to-buy-list.txt', 'r') as file:
            symbols = [line.strip().upper() for line in file if line.strip()]
        print(f"Loaded {len(symbols)} symbols.")
        return symbols
    except FileNotFoundError:
        print("Error: Symbols file not found.")
        logging.error("Symbols file not found.")
        return []

def remove_symbols_from_trade_list(symbol):
    print(f"Removing {symbol} from trade list...")
    try:
        with open('electricity-or-utility-stocks-to-buy-list.txt', 'r') as file:
            lines = file.readlines()
        with open('electricity-or-utility-stocks-to-buy-list.txt', 'w') as file:
            for line in lines:
                if line.strip() != symbol:
                    file.write(line)
        print(f"Removed {symbol} from trade list.")
    except Exception as e:
        logging.error(f"Error removing {symbol} from trade list: {e}")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_opening_price(symbol):
    print(f"Fetching opening price for {symbol}...")
    yf_symbol = symbol.replace('.', '-')
    stock_data = yf.Ticker(yf_symbol)
    try:
        opening_price = round(float(stock_data.history(period="1d")["Open"].iloc[0]), 4)
        print(f"Opening price for {yf_symbol}: ${opening_price:.4f}")
        return opening_price
    except IndexError:
        logging.error(f"Opening price not found for {yf_symbol}.")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_get_quote(symbol, retries=3):
    print(f"Fetching current price for {symbol} from Public.com...")
    for attempt in range(retries):
        try:
            return get_cached_data(symbol, 'current_price_public', _fetch_current_price_public, symbol)
        except Exception as e:
            logging.error(f"Retry {attempt + 1}/{retries} failed for {symbol}: {e}")
            time.sleep(2 ** attempt)
    logging.error(f"All retries failed for {symbol}, no price data available.")
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
        print(f"Public.com last price for {symbol}: ${last:.4f}")
        return round(last, 4)
    else:
        raise ValueError("No successful quote from Public.com")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_atr_high_price(symbol):
    print(f"Calculating ATR high price for {symbol}...")
    atr_value = get_average_true_range(symbol)
    current_price = client_get_quote(symbol)
    atr_high = round(current_price + 0.40 * atr_value, 4) if current_price and atr_value else None
    return atr_high

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_atr_low_price(symbol):
    print(f"Calculating ATR low price for {symbol}...")
    atr_value = get_average_true_range(symbol)
    current_price = client_get_quote(symbol)
    atr_low = round(current_price - 0.10 * atr_value, 4) if current_price and atr_value else None
    return atr_low

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_average_true_range(symbol):
    print(f"Calculating ATR for {symbol} using yfinance...")
    yf_symbol = symbol.replace('.', '-')
    ticker = yf.Ticker(yf_symbol)
    data = ticker.history(period='30d')
    if data.empty:
        logging.error(f"No data for {yf_symbol}.")
        return None
    try:
        atr = talib.ATR(data['High'].values, data['Low'].values, data['Close'].values, timeperiod=22)
        return atr[-1]
    except Exception as e:
        logging.error(f"Error calculating ATR for {yf_symbol}: {e}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def is_in_uptrend(symbol):
    print(f"Checking if {symbol} is in uptrend using yfinance...")
    yf_symbol = symbol.replace('.', '-')
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period='200d')
    if historical_data.empty or len(historical_data) < 200:
        print(f"Insufficient data for {yf_symbol}.")
        return False
    sma_200 = talib.SMA(historical_data['Close'].values, timeperiod=200)[-1]
    current_price = client_get_quote(symbol)
    in_uptrend = current_price > sma_200 if current_price else False
    print(f"{yf_symbol} {'is' if in_uptrend else 'is not'} in uptrend (Current: {current_price:.2f}, SMA200: {sma_200:.2f})")
    return in_uptrend

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_daily_rsi(symbol):
    print(f"Calculating daily RSI for {symbol} using yfinance...")
    yf_symbol = symbol.replace('.', '-')
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period='30d', interval='1d')
    if historical_data.empty:
        print(f"No daily data for {yf_symbol}.")
        return None
    rsi = talib.RSI(historical_data['Close'], timeperiod=14)[-1]
    return round(rsi, 2) if not np.isnan(rsi) else None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_technical_indicators(symbols, lookback_days=90):
    print(f"Calculating technical indicators for {symbols} using yfinance...")
    yf_symbol = symbols.replace('.', '-')
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period=f'{lookback_days}d')
    if historical_data.empty:
        print(f"No historical data for {yf_symbol}.")
        return historical_data
    short_window = 12
    long_window = 26
    signal_window = 9
    historical_data['macd'], historical_data['signal'], _ = talib.MACD(historical_data['Close'],
                                                                       fastperiod=short_window,
                                                                       slowperiod=long_window,
                                                                       signalperiod=signal_window)
    historical_data['rsi'] = talib.RSI(historical_data['Close'], timeperiod=14)
    historical_data['volume'] = historical_data['Volume']
    return historical_data

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_rsi(symbols, period=14, interval='5m'):
    print(f"Calculating RSI for {symbols} (period={period}, interval={interval}) using yfinance...")
    yf_symbol = symbols.replace('.', '-')
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period='1d', interval=interval, prepost=True)
    if historical_data.empty or len(historical_data['Close']) < period:
        logging.error(f"Insufficient data for RSI calculation for {yf_symbol}.")
        return None
    rsi = talib.RSI(historical_data['Close'], timeperiod=period)
    latest_rsi = round(rsi.iloc[-1], 2) if not rsi.empty else None
    return latest_rsi

def print_technical_indicators(symbols, historical_data):
    print(f"\nTechnical Indicators for {symbols}:\n")
    print(historical_data[['Close', 'macd', 'signal', 'rsi', 'volume']].tail())

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_last_price_within_past_5_minutes(symbols_to_buy_list):
    print("Fetching last prices within past 5 minutes using yfinance...")
    results = {}
    current_datetime = datetime.now(eastern)
    end_time = current_datetime
    start_time = end_time - timedelta(minutes=5)
    for symbol in symbols_to_buy_list:
        print(f"Fetching 5-minute price data for {symbol}...")
        try:
            yf_symbol = symbol.replace('.', '-')
            data = yf.download(yf_symbol, start=start_time, end=end_time, interval='1m', prepost=True)
            if not data.empty:
                last_price = round(float(data['Close'].iloc[-1]), 2)
                results[symbol] = last_price
            else:
                results[symbol] = None
        except Exception as e:
            logging.error(f"Error fetching data for {yf_symbol}: {e}")
            results[symbol] = None
    return results

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_most_recent_purchase_date(symbol):
    print(f"Retrieving most recent purchase date for {symbol}...")
    try:
        session = SessionLocal()
        buy_order = session.query(TradeHistory).filter_by(symbols=symbol, action='buy').order_by(TradeHistory.date.desc()).first()
        if buy_order:
            purchase_date_str = buy_order.date
            print(f"Purchase date for {symbol}: {purchase_date_str}")
        else:
            purchase_date = datetime.now(eastern).date()
            purchase_date_str = purchase_date.strftime("%Y-%m-%d")
            print(f"No buy orders found for {symbol}. Using today: {purchase_date_str}")
        return purchase_date_str
    except Exception as e:
        logging.error(f"Error fetching purchase date for {symbol}: {e}")
        return datetime.now(eastern).strftime("%Y-%m-%d")
    finally:
        session.close()

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def fetch_access_token_and_account_id():
    global secret, access_token, account_id, HEADERS, last_token_fetch_time
    try:
        secret = os.getenv("YOUR_SECRET_KEY")
        if not secret:
            raise ValueError("YOUR_SECRET_KEY not set")
        url = "https://api.public.com/userapiauthservice/personal/access-tokens"
        headers = {"Content-Type": "application/json"}
        request_body = {"validityInMinutes": 1440, "secret": secret}
        response = requests.post(url, headers=headers, json=request_body, timeout=10)
        response.raise_for_status()
        access_token = response.json().get("accessToken")
        if not access_token:
            raise ValueError("No access token returned")
        url = f"{BASE_URL}/trading/account"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        brokerage_account = next((acc for acc in data["accounts"] if acc.get("accountType") == "BROKERAGE"), None)
        if not brokerage_account:
            raise ValueError("No BROKERAGE account found")
        account_id = brokerage_account["accountId"]
        HEADERS = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        last_token_fetch_time = datetime.now()
        logging.info(f"Fetched access token and BROKERAGE account ID: {account_id}")
        return True
    except Exception as e:
        logging.error(f"Error fetching token/account ID: {e}")
        return False

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
                out.append({'symbol': sym, 'qty': qty, 'avg_entry_price': avg, 'purchase_date': date_str})
        return out
    except Exception as e:
        logging.error(f"Positions fetch error: {e}")
        return []

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def client_place_order(symbol, qty, side, order_type="MARKET", limit_price=None, stop_price=None):
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId")
            return None
        order_id = str(uuid4())
        request_body = {
            "orderId": order_id,
            "instrument": {
                "symbol": symbol,
                "type": "EQUITY"
            },
            "orderSide": side.upper(),
            "orderType": order_type.upper(),
            "expiration": {
                "timeInForce": "DAY",
                "expirationTime": (datetime.now(eastern) + timedelta(days=1)).isoformat() + "Z"
            },
            "quantity": str(qty) if qty else None,
            "amount": None,  # Use for dollar-based if needed
            "limitPrice": str(limit_price) if limit_price else None,
            "stopPrice": str(stop_price) if stop_price else None,
            "openCloseIndicator": "OPEN"
        }
        # Remove None values
        request_body = {k: v for k, v in request_body.items() if v is not None}
        url = f"{BASE_URL}/trading/{account_id}/order"
        resp = requests.post(url, headers=HEADERS, json=request_body, timeout=10)
        resp.raise_for_status()
        order_response = resp.json()
        logging.info(f"Order placed: {side} {qty} of {symbol}, Order ID: {order_response.get('orderId')}")
        return order_response.get('orderId')
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

def get_previous_price(symbol):
    if symbol in previous_prices:
        return previous_prices[symbol]
    current_price = client_get_quote(symbol)
    previous_prices[symbol] = current_price
    return current_price

def update_previous_price(symbol, current_price):
    previous_prices[symbol] = current_price

def track_price_changes(symbol):
    current_price = client_get_quote(symbol)
    previous_price = get_previous_price(symbol)
    if symbol not in price_changes:
        price_changes[symbol] = {'increased': 0, 'decreased': 0}
    if current_price > previous_price:
        price_changes[symbol]['increased'] += 1
        print(f"{symbol} price increased | current price: {GREEN}${current_price:.2f}{RESET}")
    elif current_price < previous_price:
        price_changes[symbol]['decreased'] += 1
        print(f"{symbol} price decreased | current price: {RED}${current_price:.2f}{RESET}")
    else:
        print(f"{symbol} price unchanged | current price: ${current_price:.2f}")
    update_previous_price(symbol, current_price)

def poll_order_status(order_id, timeout=60):
    start_time = time.time()
    while time.time() - start_time < timeout:
        status_info = client_get_order_status(order_id)
        if status_info and status_info["status"] in ["FILLED", "CANCELLED", "REJECTED"]:
            return status_info
        time.sleep(2)
    return None

def buy_stocks(symbols_to_sell_dict, symbols_to_buy_list, buy_sell_lock):
    print("Starting buy_stocks function...")
    global price_history, last_stored
    if not symbols_to_buy_list:
        print("No symbols to buy.")
        logging.info("No symbols to buy.")
        return
    symbols_to_remove = []
    buy_signal = 0
    acc = client_get_account()
    total_equity = acc['equity']
    positions = client_list_positions()
    current_exposure = sum(float(p['qty'] * (client_get_quote(p['symbol']) or p['avg_entry_price'])) for p in positions)
    max_new_exposure = total_equity * 0.98 - current_exposure
    if max_new_exposure <= 0:
        print("Portfolio exposure limit reached.")
        logging.info("Portfolio exposure limit reached.")
        return
    valid_symbols = []
    for sym in symbols_to_buy_list:
        current_price = client_get_quote(sym)
        if current_price is None:
            continue
        historical_data = calculate_technical_indicators(sym, lookback_days=5)
        if historical_data.empty:
            continue
        valid_symbols.append(sym)
    if not valid_symbols:
        print("No valid symbols to buy.")
        logging.info("No valid symbols to buy.")
        return
    for sym in valid_symbols:
        print(f"Processing {sym}...")
        current_price = client_get_quote(sym)
        if current_price is None:
            continue
        current_timestamp = time.time()
        if sym not in price_history:
            price_history[sym] = {interval: [] for interval in interval_map}
            last_stored[sym] = {interval: 0 for interval in interval_map}
        for interval, delta in interval_map.items():
            if current_timestamp - last_stored[sym][interval] >= delta:
                price_history[sym][interval].append(current_price)
                last_stored[sym][interval] = current_timestamp
        yf_symbol = sym.replace('.', '-')
        df = yf.Ticker(yf_symbol).history(period="20d")
        if df.empty or len(df) < 3:
            continue
        score = 0
        close = df['Close'].values
        open_ = df['Open'].values
        high = df['High'].values
        low = df['Low'].values
        bullish_patterns = [
            talib.CDLHAMMER, talib.CDLENGULFING, talib.CDLMORNINGSTAR,
            talib.CDLPIERCING, talib.CDL3WHITESOLDIERS, talib.CDLDRAGONFLYDOJI,
            talib.CDLINVERTEDHAMMER, talib.CDLMATCHINGLOW
        ]
        for f in bullish_patterns:
            res = f(open_, high, low, close)
            if res[-1] > 0:
                score += 1
                break
        rsi = talib.RSI(close)
        if rsi[-1] < 50:
            score += 1
        if close[-1] <= close[-2] * 0.997:
            score += 1
        if score < 3:
            continue
        recent_avg_volume = df['Volume'].iloc[-5:].mean() if len(df) >= 5 else 0
        prior_avg_volume = df['Volume'].iloc[-10:-5].mean() if len(df) >= 10 else recent_avg_volume
        volume_decrease = recent_avg_volume < prior_avg_volume if len(df) >= 10 else False
        close_prices = df['Close'].values
        rsi_series = talib.RSI(close_prices, timeperiod=14)
        rsi_decrease = False
        latest_rsi = rsi_series[-1] if len(rsi_series) > 0 else None
        if len(rsi_series) >= 10:
            recent_rsi_values = rsi_series[-5:][~np.isnan(rsi_series[-5:])]
            prior_rsi_values = rsi_series[-10:-5][~np.isnan(rsi_series[-10:-5])]
            if len(recent_rsi_values) > 0 and len(prior_rsi_values) > 0:
                recent_avg_rsi = np.mean(recent_rsi_values)
                prior_avg_rsi = np.mean(prior_rsi_values)
                rsi_decrease = recent_avg_rsi < prior_avg_rsi
        short_window = 12
        long_window = 26
        signal_window = 9
        macd, macd_signal, _ = talib.MACD(close_prices, fastperiod=short_window, slowperiod=long_window, signalperiod=signal_window)
        macd_above_signal = macd[-1] > macd_signal[-1] if len(macd) > 0 else False
        previous_price = get_previous_price(sym)
        price_increase = current_price > previous_price * 1.005
        last_prices = get_last_price_within_past_5_minutes([sym])
        last_price = last_prices.get(sym)
        if last_price is None:
            try:
                last_price = round(float(df['Close'].iloc[-1]), 4)
            except Exception:
                continue
        price_decline_threshold = last_price * (1 - 0.002)
        price_decline = current_price <= price_decline_threshold
        short_term_trend = None
        if sym in price_history and '5min' in price_history[sym] and len(price_history[sym]['5min']) >= 2:
            recent_prices = price_history[sym]['5min'][-2:]
            short_term_trend = 'up' if recent_prices[-1] > recent_prices[-2] else 'down'
        bullish_reversal_detected = False
        detected_patterns = []
        for i in range(-1, -21, -1):
            if len(df) < abs(i):
                continue
            try:
                patterns = {
                    'Hammer': talib.CDLHAMMER(open_[:i + 1], high[:i + 1], low[:i + 1], close[:i + 1])[i] != 0,
                    'Bullish Engulfing': talib.CDLENGULFING(open_[:i + 1], high[:i + 1], low[:i + 1], close[:i + 1])[i] > 0,
                    'Morning Star': talib.CDLMORNINGSTAR(open_[:i + 1], high[:i + 1], low[:i + 1], close[:i + 1])[i] != 0,
                    'Piercing Line': talib.CDLPIERCING(open_[:i + 1], high[:i + 1], low[:i + 1], close[:i + 1])[i] != 0,
                    'Three White Soldiers': talib.CDL3WHITESOLDIERS(open_[:i + 1], high[:i + 1], low[:i + 1], close[:i + 1])[i] != 0,
                    'Dragonfly Doji': talib.CDLDRAGONFLYDOJI(open_[:i + 1], high[:i + 1], low[:i + 1], close[:i + 1])[i] != 0,
                    'Inverted Hammer': talib.CDLINVERTEDHAMMER(open_[:i + 1], high[:i + 1], low[:i + 1], close[:i + 1])[i] != 0,
                    'Tweezer Bottom': talib.CDLMATCHINGLOW(open_[:i + 1], high[:i + 1], low[:i + 1], close[:i + 1])[i] != 0,
                }
                current_detected = [name for name, detected in patterns.items() if detected]
                if current_detected:
                    bullish_reversal_detected = True
                    detected_patterns = current_detected
                    break
            except IndexError:
                continue
        if not is_in_uptrend(sym):
            continue
        daily_rsi = get_daily_rsi(sym)
        if daily_rsi is None or daily_rsi > 50:
            continue
        buy_conditions_met = False
        specific_reason = ""
        if bullish_reversal_detected:
            score += 2
            price_stable = True
            if sym in price_history and '5min' in price_history[sym] and len(price_history[sym]['5min']) >= 2:
                recent_prices = price_history[sym]['5min'][-2:]
                price_stable = abs(recent_prices[-1] - recent_prices[-2]) / recent_prices[-2] < 0.005
                if price_stable:
                    score += 1
            if macd_above_signal:
                score += 1
            if not volume_decrease:
                score += 1
            if rsi_decrease:
                score += 1
            if price_decline:
                score += 1
            for pattern in detected_patterns:
                if pattern == 'Hammer' and latest_rsi < 35 and price_decline >= (last_price * 0.003):
                    score += 1
                elif pattern == 'Bullish Engulfing' and recent_avg_volume > 1.5 * prior_avg_volume:
                    score += 1
                elif pattern == 'Morning Star' and latest_rsi < 40:
                    score += 1
                elif pattern == 'Piercing Line' and recent_avg_rsi < 40:
                    score += 1
                elif pattern == 'Three White Soldiers' and not volume_decrease:
                    score += 1
                elif pattern == 'Dragonfly Doji' and latest_rsi < 30:
                    score += 1
                elif pattern == 'Inverted Hammer' and rsi_decrease:
                    score += 1
                elif pattern == 'Tweezer Bottom' and latest_rsi < 40:
                    score += 1
            if score >= 4:
                buy_conditions_met = True
                specific_reason = f"Score: {score}, patterns: {', '.join(detected_patterns)}"
        if not buy_conditions_met:
            continue
        filled_qty = 0
        filled_price = current_price
        if ALL_BUY_ORDERS_ARE_1_DOLLAR:
            total_cost_for_qty = 1.00
            qty = round(total_cost_for_qty / current_price, 4)
        else:
            atr = get_average_true_range(sym)
            if atr is None:
                continue
            stop_loss_distance = 2 * atr
            risk_per_share = stop_loss_distance
            risk_amount = 0.01 * total_equity
            qty = risk_amount / risk_per_share if risk_per_share > 0 else 0
            total_cost_for_qty = qty * current_price
            with buy_sell_lock:
                cash_available = client_get_account()['buying_power_cash']
                total_cost_for_qty = min(total_cost_for_qty, cash_available - 1.00, max_new_exposure)
                if total_cost_for_qty < 1.00:
                    continue
                qty = round(total_cost_for_qty / current_price, 4)
                estimated_slippage = total_cost_for_qty * 0.001
                total_cost_for_qty -= estimated_slippage
                qty = round(total_cost_for_qty / current_price, 4)
        with buy_sell_lock:
            cash_available = client_get_account()['buying_power_cash']
        if total_cost_for_qty < 1.00 or cash_available < total_cost_for_qty + 1.00:
            continue
        if buy_conditions_met:
            buy_signal = 1
            api_symbols = sym
            reason = f"bullish reversal ({', '.join(detected_patterns)}), {specific_reason}"
            try:
                total_cost_for_qty = round(total_cost_for_qty, 2)
                order_id = client_place_order(api_symbols, qty, "BUY")
                if order_id:
                    current_time_str = datetime.now(eastern).strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
                    status_info = poll_order_status(order_id)
                    if status_info and status_info["status"] == "FILLED":
                        filled_qty = status_info["filled_qty"]
                        filled_price = status_info["avg_price"] or current_price
                        actual_cost = filled_qty * filled_price
                        print(f"{current_time_str}, Order filled for {filled_qty:.4f} shares of {api_symbols} at {GREEN if filled_price > previous_price else RED}${filled_price:.2f}{RESET}, cost: ${actual_cost:.2f}")
                        logging.info(f"{current_time_str} Order filled for {filled_qty:.4f} shares of {api_symbols}, cost: ${actual_cost:.2f}")
                    else:
                        print(f"Order {order_id} not filled within timeout.")
                        logging.info(f"Order {order_id} not filled.")
                        # Optionally cancel if not filled
                        client_cancel_order(order_id)
                        continue
                    with open(csv_filename, mode='a', newline='') as csv_file:
                        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        csv_writer.writerow({
                            'Date': current_time_str,
                            'Buy': 'Buy',
                            'Sell': '',
                            'Quantity': filled_qty,
                            'Symbol': api_symbols,
                            'Price Per Share': filled_price
                        })
                    symbols_to_remove.append((api_symbols, filled_price, today_date_str))
                    # Skip trailing stop for now as not directly supported; can implement manual monitoring
            except Exception as e:
                logging.error(f"Error submitting buy order for {api_symbols}: {e}")
                continue
        update_previous_price(sym, current_price)
        time.sleep(0.8)
    try:
        with buy_sell_lock:
            session = SessionLocal()
            for sym, price, date in symbols_to_remove:
                symbols_to_sell_dict[sym] = (round(price, 4), date)
                symbols_to_buy_list.remove(sym)
                remove_symbols_from_trade_list(sym)
                trade_history = TradeHistory(symbols=sym, action='buy', quantity=filled_qty, price=price, date=date)
                session.add(trade_history)
                db_position = Position(symbols=sym, quantity=filled_qty, avg_price=price, purchase_date=date)
                session.add(db_position)
            session.commit()
            refresh_after_buy()
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database error: {e}")
    finally:
        session.close()

def refresh_after_buy():
    global symbols_to_buy, symbols_to_sell_dict
    print("Refreshing after buy...")
    symbols_to_buy = get_symbols_to_buy()
    symbols_to_sell_dict = update_symbols_to_sell_from_api()
    print("Refresh complete.")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def place_trailing_stop_sell_order(symbol, qty, current_price):
    # Public.com may not support trailing stops directly; approximate with stop market order
    # For now, place a stop market sell at 1% below current
    stop_loss_percent = 1.0
    stop_price = current_price * (1 - stop_loss_percent / 100)
    try:
        if float(qty) != int(qty):
            logging.error(f"Skipped trailing stop for {symbol}: Fractional quantity {qty:.4f} not allowed.")
            return None
        order_id = client_place_order(symbol, int(qty), "SELL", order_type="STOP_MARKET", stop_price=stop_price)
        if order_id:
            logging.info(f"Placed stop market sell order for {qty} shares of {symbol} at stop price {stop_price:.2f}, Order ID: {order_id}")
            return order_id
        return None
    except Exception as e:
        logging.error(f"Error placing stop order for {symbol}: {e}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def update_symbols_to_sell_from_api():
    print("Updating symbols to sell...")
    positions = client_list_positions()
    symbols_to_sell_dict = {}
    session = SessionLocal()
    try:
        for position in positions:
            sym = position['symbol']
            avg_price = position['avg_entry_price']
            qty = position['qty']
            purchase_date = position['purchase_date']
            try:
                db_position = session.query(Position).filter_by(symbols=sym).one()
                db_position.quantity = qty
                db_position.avg_price = avg_price
                db_position.purchase_date = purchase_date
            except NoResultFound:
                db_position = Position(symbols=sym, quantity=qty, avg_price=avg_price, purchase_date=purchase_date)
                session.add(db_position)
            symbols_to_sell_dict[sym] = (avg_price, purchase_date)
        session.commit()
        return symbols_to_sell_dict
    except Exception as e:
        logging.error(f"Error updating symbols to sell: {e}")
        return symbols_to_sell_dict
    finally:
        session.close()

def sell_stocks(symbols_to_sell_dict, buy_sell_lock):
    print("Starting sell_stocks function...")
    symbols_to_remove = []
    now = datetime.now(eastern)
    current_time_str = now.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
    today_date_str = datetime.today().date().strftime("%Y-%m-%d")
    comparison_date = datetime.today().date()
    for symbol, (bought_price, purchase_date) in symbols_to_sell_dict.items():
        try:
            bought_date = datetime.strptime(purchase_date, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue
        if bought_date <= comparison_date:
            current_price = client_get_quote(symbol)
            if current_price is None:
                continue
            try:
                positions = client_list_positions()
                position = next((p for p in positions if p['symbol'] == symbol), None)
                if not position:
                    continue
                bought_price = float(position['avg_entry_price'])
                qty = float(position['qty'])
                # Check for open orders; for simplicity, assume no check or implement list_orders if available
                sell_threshold = bought_price * 1.005
                if current_price >= sell_threshold:
                    order_id = client_place_order(symbol, qty, "SELL")
                    if order_id:
                        status_info = poll_order_status(order_id)
                        if status_info and status_info["status"] == "FILLED":
                            filled_qty = status_info["filled_qty"]
                            filled_price = status_info["avg_price"] or current_price
                            print(f"{current_time_str}, Sold {filled_qty:.4f} shares of {symbol} at {GREEN}${filled_price:.2f}{RESET}")
                            logging.info(f"{current_time_str} Sold {filled_qty:.4f} shares of {symbol} at {filled_price:.2f}.")
                            with open(csv_filename, mode='a', newline='') as csv_file:
                                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                                csv_writer.writerow({
                                    'Date': current_time_str,
                                    'Buy': '',
                                    'Sell': 'Sell',
                                    'Quantity': filled_qty,
                                    'Symbol': symbol,
                                    'Price Per Share': filled_price
                                })
                            symbols_to_remove.append((symbol, filled_qty, filled_price))
                        else:
                            print(f"Sell order {order_id} not filled.")
                            client_cancel_order(order_id)
            except Exception as e:
                logging.error(f"Error processing sell for {symbol}: {e}")
    try:
        with buy_sell_lock:
            session = SessionLocal()
            for symbol, qty, current_price in symbols_to_remove:
                del symbols_to_sell_dict[symbol]
                trade_history = TradeHistory(symbols=symbol, action='sell', quantity=qty, price=current_price, date=today_date_str)
                session.add(trade_history)
                session.query(Position).filter_by(symbols=symbol).delete()
            session.commit()
            refresh_after_sell()
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database error: {e}")
    finally:
        session.close()

def refresh_after_sell():
    global symbols_to_sell_dict
    print("Refreshing after sell...")
    symbols_to_sell_dict = update_symbols_to_sell_from_api()
    print("Refresh complete.")

def load_positions_from_database():
    print("Loading positions from database...")
    session = SessionLocal()
    try:
        positions = session.query(Position).all()
        symbols_to_sell_dict = {p.symbols: (p.avg_price, p.purchase_date) for p in positions}
        return symbols_to_sell_dict
    finally:
        session.close()

def count_day_trades():
    session = SessionLocal()
    try:
        business_days = nyse_cal.schedule(start_date=date.today() - timedelta(days=10), end_date=date.today()).index[-5:].strftime("%Y-%m-%d").tolist()
        trades = session.query(TradeHistory).filter(TradeHistory.date.in_(business_days)).all()
        day_trades = 0
        trades_by_symbol_date = {}
        for trade in trades:
            key = (trade.symbols, trade.date)
            trades_by_symbol_date.setdefault(key, []).append(trade.action)
        for key, actions in trades_by_symbol_date.items():
            if "buy" in actions and "sell" in actions:
                day_trades += 1
        return day_trades
    finally:
        session.close()

def main():
    global symbols_to_buy, symbols_to_sell_dict
    print("Starting trading program...")
    symbols_to_buy = get_symbols_to_buy()
    symbols_to_sell_dict = load_positions_from_database()
    while True:
        try:
            if not fetch_access_token_and_account_id():
                time.sleep(30)
                continue
            stop_if_stock_market_is_closed()
            current_datetime = datetime.now(pytz.timezone('US/Eastern'))
            current_time_str = current_datetime.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
            acc = client_get_account()
            cash_balance = round(acc['buying_power_cash'], 2)
            print("------------------------------------------------------------------------------------")
            print("\n")
            print("*****************************************************")
            print("******** Billionaire Buying Strategy Version ********")
            print("*****************************************************")
            print("2025 Edition of the Advanced Stock Market Trading Robot, Version 8 ")
            print("by https://github.com/CodeProSpecialist")
            print("------------------------------------------------------------------------------------")
            print(f" {current_time_str} Cash Balance: ${cash_balance}")
            day_trade_count = count_day_trades()
            print("\n")
            print(f"Current day trade number: {day_trade_count} out of 3 in 5 business days")
            print("\n")
            print("------------------------------------------------------------------------------------")
            print("\n")
            symbols_to_buy = get_symbols_to_buy()
            if not symbols_to_sell_dict:
                symbols_to_sell_dict = update_symbols_to_sell_from_api()
            print("Starting buy and sell threads...")
            buy_thread = threading.Thread(target=buy_stocks, args=(symbols_to_sell_dict, symbols_to_buy, buy_sell_lock))
            sell_thread = threading.Thread(target=sell_stocks, args=(symbols_to_sell_dict, buy_sell_lock))
            buy_thread.start()
            sell_thread.start()
            buy_thread.join()
            sell_thread.join()
            print("Buy and sell threads completed.")
            if PRINT_SYMBOLS_TO_BUY:
                print("\n")
                print("------------------------------------------------------------------------------------")
                print("\n")
                print("Symbols to Purchase:")
                print("\n")
                for sym in symbols_to_buy:
                    current_price = client_get_quote(sym)
                    print(f"Symbol: {sym} | Current Price: {GREEN if current_price > get_previous_price(sym) else RED}${current_price:.2f}{RESET} ")
                print("\n")
                print("------------------------------------------------------------------------------------")
                print("\n")
            if PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE:
                print_database_tables()
            if DEBUG:
                print("\n")
                print("------------------------------------------------------------------------------------")
                print("\n")
                print("Symbols to Purchase:")
                print("\n")
                for sym in symbols_to_buy:
                    current_price = client_get_quote(sym)
                    atr_low_price = get_atr_low_price(sym)
                    print(f"Symbol: {sym} | Current Price: {GREEN if current_price > get_previous_price(sym) else RED}${current_price:.2f}{RESET} | ATR low buy signal price: ${atr_low_price:.2f}")
                print("\n")
                print("------------------------------------------------------------------------------------")
                print("\n")
                print("\nSymbols to Sell:")
                print("\n")
                for sym, _ in symbols_to_sell_dict.items():
                    current_price = client_get_quote(sym)
                    atr_high_price = get_atr_high_price(sym)
                    print(f"Symbol: {sym} | Current Price: {GREEN if current_price > get_previous_price(sym) else RED}${current_price:.2f}{RESET} | ATR high sell signal profit price: ${atr_high_price:.2f}")
                print("\n")
            print("Waiting 15 seconds before checking price data again........")
            time.sleep(15)
        except Exception as e:
            logging.error(f"Error encountered in main loop: {e}")
            print(f"Error encountered in main loop: {e}")
            time.sleep(120)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Critical error: {e}")
        SessionLocal().close()
