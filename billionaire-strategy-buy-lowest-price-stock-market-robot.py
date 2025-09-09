import threading
import logging
import csv
import os
import time
import schedule
from datetime import datetime, timedelta, date
from datetime import time as time2
import requests
import pytz
import numpy as np
import talib
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from ratelimit import limits, sleep_and_retry
import pandas_market_calendars as mcal

# Load environment variables for Public API
account_id = os.getenv('PUBLIC_ACCOUNT_ID_NUMBER')  # variable for account ID
secret_key = os.getenv('PUBLIC_API_ACCESS_TOKEN')

# Define APIError to match Alpaca's exception
class APIError(Exception):
    pass

class PublicAPI:
    def __init__(self, base_url='https://api.public.com'):
        self.base_url = base_url
        self.access_token = secret_key  # Use the access token directly from environment variable
        if not self.access_token:
            raise APIError("No access token found in PUBLIC_API_ACCESS_TOKEN environment variable")
        self.headers = {'Authorization': f'Bearer {self.access_token}', 'Content-Type': 'application/json'}
        self.account_id = account_id or self._get_account_id()

    def _get_account_id(self):
        url = f"{self.base_url}/accounts"  # Updated endpoint
        logging.info(f"Sending request to {url} with headers {self.headers}")
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()  # Raises an HTTPError for bad responses
            accounts = response.json().get('accounts', [])
            if accounts:
                account_id = accounts[0].get('accountId') or accounts[0].get('id')
                logging.info(f"Retrieved account ID: {account_id}")
                return account_id
            else:
                raise APIError("No accounts found in response")
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error fetching accounts: {e}, Response: {response.text}")
            raise APIError(f"Failed to get accounts: {response.text}")
        except Exception as e:
            logging.error(f"Unexpected error fetching accounts: {e}")
            raise APIError(f"Unexpected error: {str(e)}")

    def get_account(self):
        url = f"{self.base_url}/userapigateway/account/v1/accounts/{self.account_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            acc = response.json()
            # Adjust keys based on assumed response; may need tweaking
            acc['cash'] = str(acc.get('buying_power', 0))
            acc['equity'] = str(acc.get('portfolio_value', 0))
            acc['daytrade_count'] = acc.get('day_trade_count', 0)
            return acc
        else:
            raise APIError(f"Failed to get account: {response.text}")

    def list_positions(self):
        url = f"{self.base_url}/userapigateway/trading/{self.account_id}/portfolio/v2"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            positions = response.json().get('positions', [])
            class Position:
                def __init__(self, d):
                    self.symbol = d.get('symbol')
                    self.qty = str(d.get('quantity'))
                    self.avg_entry_price = str(d.get('avg_price'))
                    self.market_value = str(d.get('market_value'))
            return [Position(p) for p in positions]
        else:
            raise APIError(f"Failed to list positions: {response.text}")

    def submit_order(self, symbol, qty=None, notional=None, side=None, type=None, time_in_force=None, trail_percent=None):
        url = f"{self.base_url}/userapigateway/trading/{self.account_id}/orders/v1/orders"
        data = {
            "symbol": symbol,
            "side": side.upper(),
            "order_type": type.upper(),
            "time_in_force": time_in_force.upper(),
        }
        if notional is not None:
            data["notional"] = notional
        elif qty is not None:
            data["quantity"] = qty
        if type.lower() == 'trailing_stop':
            data["trail_percent"] = trail_percent
        response = requests.post(url, headers=self.headers, json=data)
        if response.status_code in (200, 201):
            order = response.json()
            class Order:
                def __init__(self, d):
                    self.id = d.get('order_id')
                    self.status = d.get('status')
                    self.filled_qty = str(d.get('filled_quantity'))
                    self.filled_avg_price = str(d.get('filled_avg_price'))
                    self.side = d.get('side')
                    self.submitted_at = d.get('submitted_at')
                    self.filled_at = d.get('filled_at')
            return Order(order)
        else:
            raise APIError(response.text)

    def get_position(self, symbol):
        positions = self.list_positions()
        for pos in positions:
            if pos.symbol == symbol:
                return pos
        raise APIError("Position not found")

    def list_orders(self, status='all', nested=False, direction='desc', until=None, limit=None, symbols=None):
        url = f"{self.base_url}/userapigateway/trading/{self.account_id}/orders/v1/orders"
        params = {
            "status": status,
            "direction": direction,
            "limit": limit,
            "until": until,
        }
        if symbols:
            params["symbols"] = ','.join(symbols)
        response = requests.get(url, headers=self.headers, params=params)
        if response.status_code == 200:
            orders = response.json().get('orders', [])
            class Order:
                def __init__(self, d):
                    self.id = d.get('order_id')
                    self.status = d.get('status')
                    self.side = d.get('side')
                    self.filled_at = d.get('filled_at')
                    self.submitted_at = d.get('submitted_at')
                    self.filled_avg_price = str(d.get('filled_avg_price'))
                    self.filled_qty = str(d.get('filled_quantity'))
            return [Order(o) for o in orders]
        else:
            raise APIError(f"Failed to list orders: {response.text}")

    def get_order(self, order_id):
        url = f"{self.base_url}/userapigateway/trading/{self.account_id}/orders/v1/orders/{order_id}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            o = response.json()
            class Order:
                def __init__(self, d):
                    self.status = d.get('status')
                    self.filled_qty = str(d.get('filled_quantity'))
                    self.filled_avg_price = str(d.get('filled_avg_price'))
            return Order(o)
        else:
            raise APIError(f"Failed to get order: {response.text}")

    def get_current_price(self, symbol):
        url = f"{self.base_url}/userapigateway/marketdata/v1/quotes/{symbol}"
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            data = response.json()
            current_price = float(data.get('last_price', 0))
            return round(current_price, 4) if current_price else None
        else:
            logging.error(f"Failed to fetch current price for {symbol}: {response.text}")
            return None

# Initialize the Public API
api = PublicAPI()

# Global variables
global symbols_to_buy, today_date, today_datetime, csv_writer, csv_filename, fieldnames, price_changes, end_time
global current_price, today_date_str, qty
global price_history, last_stored, interval_map

# Configuration flags
PRINT_SYMBOLS_TO_BUY = False
PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE = True
PRINT_DATABASE = True
DEBUG = False
ALL_BUY_ORDERS_ARE_1_DOLLAR = False

# Set the timezone to Eastern
eastern = pytz.timezone('US/Eastern')

# Dictionary to maintain previous prices and price changes
stock_data = {}
previous_prices = {}
price_changes = {}
end_time = 0

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

# Define the API datetime format
api_time_format = '%Y-%m-%dT%H:%M:%S.%f-04:00'

# Thread locks for thread-safe operations
buy_sell_lock = threading.Lock()

# Logging configuration
logging.basicConfig(filename='trading-bot-program-logging-messages.txt', level=logging.INFO)

# Define the CSV file and fieldnames
csv_filename = 'log-file-of-buy-and-sell-signals.csv'
fieldnames = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']

# Initialize CSV file
with open(csv_filename, mode='w', newline='') as csv_file:
    csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    csv_writer.writeheader()

# Define the Database Models
Base = sqlalchemy.orm.declarative_base()

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
engine = create_engine('sqlite:///trading_bot.db')
Session = sessionmaker(bind=engine)
session = Session()

# Create tables if they don't exist
Base.metadata.create_all(engine)

# Add a cache for API data
data_cache = {}
CACHE_EXPIRY = 120

# Rate limit: 60 calls per minute
CALLS = 60
PERIOD = 60

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

def print_database_tables():
    if PRINT_DATABASE:
        positions = api.list_positions()
        show_price_percentage_change = True
        print("\nTrade History In This Robot's Database:")
        print("\n")
        print("Stock | Buy or Sell | Quantity | Avg. Price | Date ")
        print("\n")
        for record in session.query(TradeHistory).all():
            print(f"{record.symbols} | {record.action} | {record.quantity:.4f} | {record.price:.2f} | {record.date}")
        print("----------------------------------------------------------------")
        print("\n")
        print("Positions in the Database To Sell On or After the Date Shown:")
        print("\n")
        print("Stock | Quantity | Avg. Price | Date or The 1st Day This Robot Began Working ")
        print("\n")
        for record in session.query(Position).all():
            symbols_to_sell, quantity, avg_price, purchase_date = record.symbols, record.quantity, record.avg_price, record.purchase_date
            purchase_date_str = purchase_date
            if show_price_percentage_change:
                current_price = api.get_current_price(symbols_to_sell)
                percentage_change = ((current_price - avg_price) / avg_price) * 100 if current_price and avg_price else 0
                print(f"{symbols_to_sell} | {quantity:.4f} | {avg_price:.2f} | {purchase_date_str} | Price Change: {percentage_change:.2f}%")
            else:
                print(f"{symbols_to_sell} | {quantity:.4f} | {avg_price:.2f} | {purchase_date_str}")
        print("\n")

def get_symbols_to_buy():
    print("Loading symbols to buy from file...")
    try:
        with open('electricity-or-utility-stocks-to-buy-list.txt', 'r') as file:
            symbols_to_buy = [line.strip() for line in file.readlines()]
            print(f"Loaded {len(symbols_to_buy)} stock symbols from file.")
        if not symbols_to_buy:
            print("\n")
            print("********************************************************************************************************")
            print("*   Error: The file electricity-or-utility-stocks-to-buy-list.txt doesn't contain any stock symbols.   *")
            print("*   This Robot does not work until you place stock symbols in the file named:                          *")
            print("*       electricity-or-utility-stocks-to-buy-list.txt                                                  *")
            print("********************************************************************************************************")
            print("\n")
        return symbols_to_buy
    except FileNotFoundError:
        print("\n")
        print("****************************************************************************")
        print("*   Error: File not found: electricity-or-utility-stocks-to-buy-list.txt   *")
        print("****************************************************************************")
        print("\n")
        return []

def remove_symbols_from_trade_list(symbols_to_buy):
    print(f"Removing {symbols_to_buy} from trade list...")
    with open('electricity-or-utility-stocks-to-buy-list.txt', 'r') as file:
        lines = file.readlines()
    with open('electricity-or-utility-stocks-to-buy-list.txt', 'w') as file:
        for line in lines:
            if line.strip() != symbols_to_buy:
                file.write(line)
    print(f"Successfully removed {symbols_to_buy} from trade list.")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_opening_price(symbols_to_buy):
    print(f"Fetching opening price for {symbols_to_buy}...")
    try:
        url = f"{api.base_url}/userapigateway/marketdata/v1/bars/{symbols_to_buy}?timeframe=1D"
        response = requests.get(url, headers=api.headers)
        if response.status_code == 200:
            data = response.json()
            bars = data.get('bars', [])
            if bars:
                opening_price = float(bars[0].get('open', 0))
                print(f"Opening price for {symbols_to_buy}: ${opening_price:.4f}")
                return round(opening_price, 4)
            else:
                logging.error(f"No opening price data for {symbols_to_buy}.")
                print(f"No opening price data for {symbols_to_buy}.")
                return None
        else:
            logging.error(f"Failed to fetch opening price for {symbols_to_buy}: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error fetching opening price for {symbols_to_buy}: {e}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_current_price(symbols, retries=3):
    print(f"Attempting to fetch current price for {symbols}...")
    for attempt in range(retries):
        try:
            return get_cached_data(symbols, 'current_price', api.get_current_price, symbols)
        except Exception as e:
            logging.error(f"Retry {attempt + 1}/{retries} failed for {symbols}: {e}")
            print(f"Retry {attempt + 1}/{retries} failed for {symbols}: {e}")
            time.sleep(2 ** attempt)
    print(f"Failed to fetch current price for {symbols} after {retries} attempts.")
    return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_atr_high_price(symbols_to_sell):
    print(f"Calculating ATR high price for {symbols_to_sell}...")
    atr_value = get_average_true_range(symbols_to_sell)
    current_price = get_current_price(symbols_to_sell)
    atr_high = round(current_price + 0.40 * atr_value, 4) if current_price and atr_value else None
    print(f"ATR high price for {symbols_to_sell}: ${atr_high:.4f}" if atr_high else f"Failed to calculate ATR high price for {symbols_to_sell}.")
    return atr_high

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_atr_low_price(symbols_to_buy):
    print(f"Calculating ATR low price for {symbols_to_buy}...")
    atr_value = get_average_true_range(symbols_to_buy)
    current_price = get_current_price(symbols_to_buy)
    atr_low = round(current_price - 0.10 * atr_value, 4) if current_price and atr_value else None
    print(f"ATR low price for {symbols_to_buy}: ${atr_low:.4f}" if atr_low else f"Failed to calculate ATR low price for {symbols_to_buy}.")
    return atr_low

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_average_true_range(symbols):
    print(f"Calculating ATR for {symbols}...")
    try:
        url = f"{api.base_url}/userapigateway/marketdata/v1/bars/{symbols}?timeframe=1D&limit=30"
        response = requests.get(url, headers=api.headers)
        if response.status_code == 200:
            bars = response.json().get('bars', [])
            if len(bars) < 22:
                logging.error(f"Insufficient data for ATR calculation for {symbols}.")
                return None
            high = np.array([float(bar['high']) for bar in bars])
            low = np.array([float(bar['low']) for bar in bars])
            close = np.array([float(bar['close']) for bar in bars])
            atr = talib.ATR(high, low, close, timeperiod=22)[-1]
            print(f"ATR for {symbols}: {atr:.4f}")
            return atr
        else:
            logging.error(f"Failed to fetch ATR data for {symbols}: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error calculating ATR for {symbols}: {e}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def is_in_uptrend(symbols_to_buy):
    print(f"Checking if {symbols_to_buy} is in uptrend (above 200-day SMA)...")
    try:
        url = f"{api.base_url}/userapigateway/marketdata/v1/bars/{symbols_to_buy}?timeframe=1D&limit=200"
        response = requests.get(url, headers=api.headers)
        if response.status_code == 200:
            bars = response.json().get('bars', [])
            if len(bars) < 200:
                print(f"Insufficient data for 200-day SMA for {symbols_to_buy}. Assuming not in uptrend.")
                return False
            close_prices = np.array([float(bar['close']) for bar in bars])
            sma_200 = talib.SMA(close_prices, timeperiod=200)[-1]
            current_price = get_current_price(symbols_to_buy)
            in_uptrend = current_price > sma_200 if current_price else False
            print(f"{symbols_to_buy} {'is' if in_uptrend else 'is not'} in uptrend (Current: {current_price:.2f}, SMA200: {sma_200:.2f})")
            return in_uptrend
        else:
            logging.error(f"Failed to fetch SMA data for {symbols_to_buy}: {response.text}")
            return False
    except Exception as e:
        logging.error(f"Error checking uptrend for {symbols_to_buy}: {e}")
        return False

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_daily_rsi(symbols_to_buy):
    print(f"Calculating daily RSI for {symbols_to_buy}...")
    try:
        url = f"{api.base_url}/userapigateway/marketdata/v1/bars/{symbols_to_buy}?timeframe=1D&limit=30"
        response = requests.get(url, headers=api.headers)
        if response.status_code == 200:
            bars = response.json().get('bars', [])
            if not bars:
                print(f"No daily data for {symbols_to_buy}. RSI calculation failed.")
                return None
            close_prices = np.array([float(bar['close']) for bar in bars])
            rsi = talib.RSI(close_prices, timeperiod=14)[-1]
            rsi_value = round(rsi, 2) if not np.isnan(rsi) else None
            print(f"Daily RSI for {symbols_to_buy}: {rsi_value}")
            return rsi_value
        else:
            logging.error(f"Failed to fetch RSI data for {symbols_to_buy}: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error calculating RSI for {symbols_to_buy}: {e}")
        return None

def status_printer_buy_stocks():
    print(f"\rBuy stocks function is working correctly right now. Checking symbols to buy.....", end='', flush=True)
    print()

def status_printer_sell_stocks():
    print(f"\rSell stocks function is working correctly right now. Checking symbols to sell.....", end='', flush=True)
    print()

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_technical_indicators(symbols, lookback_days=90):
    print(f"Calculating technical indicators for {symbols} over {lookback_days} days...")
    try:
        url = f"{api.base_url}/userapigateway/marketdata/v1/bars/{symbols}?timeframe=1D&limit={lookback_days}"
        response = requests.get(url, headers=api.headers)
        if response.status_code == 200:
            bars = response.json().get('bars', [])
            if not bars:
                print(f"No data for {symbols}. Technical indicators calculation failed.")
                return pd.DataFrame()
            data = pd.DataFrame(bars)
            data['Close'] = data['close'].astype(float)
            data['High'] = data['high'].astype(float)
            data['Low'] = data['low'].astype(float)
            data['Volume'] = data['volume'].astype(float)
            short_window = 12
            long_window = 26
            signal_window = 9
            data['macd'], data['signal'], _ = talib.MACD(data['Close'], fastperiod=short_window, slowperiod=long_window, signalperiod=signal_window)
            rsi_period = 14
            data['rsi'] = talib.RSI(data['Close'], timeperiod=rsi_period)
            data['volume'] = data['Volume']
            print(f"Technical indicators calculated for {symbols}.")
            return data
        else:
            logging.error(f"Failed to fetch technical data for {symbols}: {response.text}")
            return pd.DataFrame()
    except Exception as e:
        logging.error(f"Error calculating technical indicators for {symbols}: {e}")
        return pd.DataFrame()

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_rsi(symbols, period=14, interval='5m'):
    print(f"Calculating RSI for {symbols} (period={period}, interval={interval})...")
    try:
        timeframe = '5M' if interval == '5m' else interval
        url = f"{api.base_url}/userapigateway/marketdata/v1/bars/{symbols}?timeframe={timeframe}&limit=100"
        response = requests.get(url, headers=api.headers)
        if response.status_code == 200:
            bars = response.json().get('bars', [])
            if len(bars) < period:
                logging.error(f"Insufficient data for RSI calculation for {symbols} with {interval} interval.")
                print(f"Insufficient data for RSI calculation for {symbols}.")
                return None
            close_prices = np.array([float(bar['close']) for bar in bars])
            rsi = talib.RSI(close_prices, timeperiod=period)
            latest_rsi = rsi[-1] if len(rsi) > 0 else None
            if latest_rsi is None or not np.isfinite(latest_rsi):
                logging.error(f"Invalid RSI value for {symbols}: {latest_rsi}")
                print(f"Invalid RSI value for {symbols}.")
                return None
            latest_rsi = round(latest_rsi, 2)
            print(f"RSI for {symbols}: {latest_rsi}")
            return latest_rsi
        else:
            logging.error(f"Failed to fetch RSI data for {symbols}: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error calculating RSI for {symbols}: {e}")
        return None

def print_technical_indicators(symbols, historical_data):
    print("")
    print(f"\nTechnical Indicators for {symbols}:\n")
    print(historical_data[['Close', 'macd', 'signal', 'rsi', 'volume']].tail())
    print("")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_cash_on_hand():
    print("Calculating available cash...")
    cash_available = round(float(api.get_account()['cash']), 2)
    print(f"Cash available: ${cash_available:.2f}")
    return cash_available

def calculate_total_symbols(symbols_to_buy_list):
    print("Calculating total symbols to trade...")
    total_symbols = len(symbols_to_buy_list)
    print(f"Total symbols: {total_symbols}")
    return total_symbols

def allocate_cash_equally(cash_available, total_symbols):
    print("Allocating cash equally among symbols...")
    max_allocation_per_symbol = 600.0
    allocation_per_symbol = min(max_allocation_per_symbol, cash_available / total_symbols) if total_symbols > 0 else 0
    allocation = round(allocation_per_symbol, 2)
    print(f"Allocation per symbol: ${allocation:.2f}")
    return allocation

def get_previous_price(symbols):
    print(f"Retrieving previous price for {symbols}...")
    if symbols in previous_prices:
        price = previous_prices[symbols]
        print(f"Previous price for {symbols}: ${price:.4f}")
        return price
    else:
        current_price = get_current_price(symbols)
        previous_prices[symbols] = current_price
        print(f"No previous price for {symbols} was found. Using the current price as the previous price: {current_price}")
        return current_price

def update_previous_price(symbols, current_price):
    print(f"Updating previous price for {symbols} to ${current_price:.4f}")
    previous_prices[symbols] = current_price

def run_schedule():
    print("Running schedule for pending tasks...")
    while not end_time_reached():
        schedule.run_pending()
        time.sleep(1)
    print("Schedule completed.")

def track_price_changes(symbols):
    print(f"Tracking price changes for {symbols}...")
    current_price = get_current_price(symbols)
    previous_price = get_previous_price(symbols)
    print("")
    print_technical_indicators(symbols, calculate_technical_indicators(symbols))
    print("")
    if symbols not in price_changes:
        price_changes[symbols] = {'increased': 0, 'decreased': 0}
    if current_price > previous_price:
        price_changes[symbols]['increased'] += 1
        print(f"{symbols} price just increased | current price: {current_price}")
    elif current_price < previous_price:
        price_changes[symbols]['decreased'] += 1
        print(f"{symbols} price just decreased | current price: {current_price}")
    else:
        print(f"{symbols} price has not changed | current price: {current_price}")
    update_previous_price(symbols, current_price)

def end_time_reached():
    reached = time.time() >= end_time
    print(f"Checking if end time reached: {'Yes' if reached else 'No'}")
    return reached

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_last_price_within_past_5_minutes(symbols_to_buy_list):
    print("Fetching last prices within past 5 minutes for symbols...")
    results = {}
    eastern = pytz.timezone('US/Eastern')
    current_datetime = datetime.now(eastern)
    end_time = current_datetime
    start_time = end_time - timedelta(minutes=5)
    for symbols_to_buy in symbols_to_buy_list:
        print(f"Fetching 5-minute price data for {symbols_to_buy}...")
        try:
            url = f"{api.base_url}/userapigateway/marketdata/v1/bars/{symbols_to_buy}?timeframe=1M&start={start_time.isoformat()}&end={end_time.isoformat()}"
            response = requests.get(url, headers=api.headers)
            if response.status_code == 200:
                bars = response.json().get('bars', [])
                if bars:
                    last_price = round(float(bars[-1]['close']), 2)
                    results[symbols_to_buy] = last_price
                    print(f"Last price for {symbols_to_buy} within 5 minutes: ${last_price:.2f}")
                else:
                    results[symbols_to_buy] = None
                    print(f"No price data found for {symbols_to_buy} within past 5 minutes.")
            else:
                results[symbols_to_buy] = None
                print(f"Failed to fetch price data for {symbols_to_buy}: {response.text}")
        except Exception as e:
            print(f"Error occurred while fetching data for {symbols_to_buy}: {e}")
            logging.error(f"Error occurred while fetching data for {symbols_to_buy}: {e}")
            results[symbols_to_buy] = None
    return results

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_most_recent_purchase_date(symbols_to_sell):
    print(f"Retrieving most recent purchase date for {symbols_to_sell}...")
    try:
        purchase_date_str = None
        order_list = []
        CHUNK_SIZE = 500
        end_time = datetime.now(pytz.UTC).isoformat()
        while True:
            print(f"Fetching orders for {symbols_to_sell} until {end_time}...")
            order_chunk = api.list_orders(
                status='all',
                direction='desc',
                until=end_time,
                limit=CHUNK_SIZE,
                symbols=[symbols_to_sell]
            )
            if order_chunk:
                order_list.extend(order_chunk)
                end_time = (order_chunk[-1].submitted_at - timedelta(seconds=1)).isoformat()
                print(f"Fetched {len(order_chunk)} orders for {symbols_to_sell}.")
            else:
                print(f"No more orders to fetch for {symbols_to_sell}.")
                break
        buy_orders = [
            order for order in order_list
            if order.side == 'buy' and order.status == 'filled' and order.filled_at
        ]
        if buy_orders:
            most_recent_buy = max(buy_orders, key=lambda order: order.filled_at)
            purchase_date = most_recent_buy.filled_at.date()
            purchase_date_str = purchase_date.strftime("%Y-%m-%d")
            print(f"Most recent purchase date for {symbols_to_sell}: {purchase_date_str} (from {len(buy_orders)} buy orders)")
            logging.info(f"Most recent purchase date for {symbols_to_sell}: {purchase_date_str} (from {len(buy_orders)} buy orders)")
        else:
            purchase_date = datetime.now(pytz.UTC).date()
            purchase_date_str = purchase_date.strftime("%Y-%m-%d")
            print(f"No filled buy orders found for {symbols_to_sell}. Using today's date: {purchase_date_str}")
            logging.warning(f"No filled buy orders found for {symbols_to_sell}. Using today's date: {purchase_date_str}.")
        return purchase_date_str
    except Exception as e:
        logging.error(f"Error fetching buy orders for {symbols_to_sell}: {e}")
        purchase_date = datetime.now(pytz.UTC).date()
        purchase_date_str = purchase_date.strftime("%Y-%m-%d")
        print(f"Error fetching buy orders for {symbols_to_sell}: {e}. Using today's date: {purchase_date_str}")
        return purchase_date_str

def buy_stocks(symbols_to_sell_dict, symbols_to_buy_list, buy_sell_lock):
    print("Starting buy_stocks function...")
    global current_price, buy_signal, price_history, last_stored
    if not symbols_to_buy_list:
        print("No symbols to buy.")
        logging.info("No symbols to buy.")
        return
    symbols_to_remove = []
    buy_signal = 0
    account = api.get_account()
    total_equity = float(account['equity'])
    print(f"Total account equity: ${total_equity:.2f}")
    positions = api.list_positions()
    current_exposure = sum(float(pos.market_value) for pos in positions)
    max_new_exposure = total_equity * 0.98 - current_exposure
    if max_new_exposure <= 0:
        print("Portfolio exposure limit reached. No new buys.")
        logging.info("Portfolio exposure limit reached. No new buys.")
        return
    print(f"Current exposure: ${current_exposure:.2f}, Max new exposure: ${max_new_exposure:.2f}")
    processed_symbols = 0
    valid_symbols = []
    print("Filtering valid symbols for buying...")
    for symbols_to_buy in symbols_to_buy_list:
        current_price = get_current_price(symbols_to_buy)
        if current_price is None:
            print(f"No valid price data for {symbols_to_buy}. Skipping.")
            logging.info(f"No valid price data for {symbols_to_buy}.")
            continue
        historical_data = calculate_technical_indicators(symbols_to_buy, lookback_days=5)
        if historical_data.empty:
            print(f"No historical data for {symbols_to_buy}. Skipping.")
            logging.info(f"No historical data for {symbols_to_buy}.")
            continue
        valid_symbols.append(symbols_to_buy)
    print(f"Valid symbols to process: {valid_symbols}")
    if not valid_symbols:
        print("No valid symbols to buy after filtering.")
        logging.info("No valid symbols to buy after filtering.")
        return
    for symbols_to_buy in valid_symbols:
        print(f"Processing {symbols_to_buy}...")
        processed_symbols += 1
        today_date = datetime.today().date()
        today_date_str = today_date.strftime("%Y-%m-%d")
        current_datetime = datetime.now(pytz.timezone('US/Eastern'))
        current_time_str = current_datetime.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
        current_price = get_current_price(symbols_to_buy)
        if current_price is None:
            print(f"No valid price data for {symbols_to_buy}.")
            logging.info(f"No valid price data for {symbols_to_buy}.")
            continue
        current_timestamp = time.time()
        if symbols_to_buy not in price_history:
            price_history[symbols_to_buy] = {interval: [] for interval in interval_map}
            last_stored[symbols_to_buy] = {interval: 0 for interval in interval_map}
        for interval, delta in interval_map.items():
            if current_timestamp - last_stored[symbols_to_buy][interval] >= delta:
                price_history[symbols_to_buy][interval].append(current_price)
                last_stored[symbols_to_buy][interval] = current_timestamp
                print(f"Stored price {current_price} for {symbols_to_buy} at {interval} interval.")
                logging.info(f"Stored price {current_price} for {symbols_to_buy} at {interval} interval.")
        print(f"Fetching 5-day 5-minute historical data for {symbols_to_buy}...")
        historical_data = calculate_technical_indicators(symbols_to_buy, lookback_days=5)
        if historical_data.empty or len(historical_data) < 3:
            print(f"Insufficient historical data for {symbols_to_buy} (rows: {len(historical_data)}). Skipping.")
            logging.info(f"Insufficient historical data for {symbols_to_buy} (rows: {len(historical_data)}).")
            continue
        print(f"Calculating volume metrics for {symbols_to_buy}...")
        recent_avg_volume = historical_data['volume'].iloc[-5:].mean() if len(historical_data) >= 5 else 0
        prior_avg_volume = historical_data['volume'].iloc[-10:-5].mean() if len(historical_data) >= 10 else recent_avg_volume
        volume_decrease = recent_avg_volume < prior_avg_volume if len(historical_data) >= 10 else False
        current_volume = historical_data['volume'].iloc[-1]
        print(f"{symbols_to_buy}: Recent avg volume = {recent_avg_volume:.0f}, Prior avg volume = {prior_avg_volume:.0f}, Volume decrease = {volume_decrease}")
        print(f"Calculating RSI metrics for {symbols_to_buy}...")
        close_prices = historical_data['Close'].values
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
            else:
                recent_avg_rsi = 0
                prior_avg_rsi = 0
        else:
            recent_avg_rsi = 0
            prior_avg_rsi = 0
        print(f"{symbols_to_buy}: Latest RSI = {latest_rsi:.2f}, Recent avg RSI = {recent_avg_rsi:.2f}, Prior avg RSI = {prior_avg_rsi:.2f}, RSI decrease = {rsi_decrease}")
        print(f"Calculating MACD for {symbols_to_buy}...")
        short_window = 12
        long_window = 26
        signal_window = 9
        macd, macd_signal, _ = talib.MACD(close_prices, fastperiod=short_window, slowperiod=long_window, signalperiod=signal_window)
        latest_macd = macd[-1] if len(macd) > 0 else None
        latest_macd_signal = macd_signal[-1] if len(macd_signal) > 0 else None
        macd_above_signal = latest_macd > latest_macd_signal if latest_macd is not None else False
        print(f"{symbols_to_buy}: MACD = {latest_macd:.2f}, Signal = {latest_macd_signal:.2f}, MACD above signal = {macd_above_signal}")
        previous_price = get_previous_price(symbols_to_buy)
        price_increase = current_price > previous_price * 1.005
        print(f"{symbols_to_buy}: Price increase check: Current = ${current_price:.2f}, Previous = ${previous_price:.2f}, Increase = {price_increase}")
        print(f"Checking price drop for {symbols_to_buy}...")
        last_prices = get_last_price_within_past_5_minutes([symbols_to_buy])
        last_price = last_prices.get(symbols_to_buy)
        if last_price is None:
            try:
                last_price = round(float(historical_data['Close'].iloc[-1]), 4)
                print(f"No price found for {symbols_to_buy} in past 5 minutes. Using last closing price: {last_price}")
                logging.info(f"No price found for {symbols_to_buy} in past 5 minutes. Using last closing price: {last_price}")
            except Exception as e:
                print(f"Error fetching last closing price for {symbols_to_buy}: {e}")
                logging.error(f"Error fetching last closing price for {symbols_to_buy}: {e}")
                continue
        price_decline_threshold = last_price * (1 - 0.002)
        price_decline = current_price <= price_decline_threshold
        print(f"{symbols_to_buy}: Price decline check: Current = ${current_price:.2f}, Threshold = ${price_decline_threshold:.2f}, Decline = {price_decline}")
        short_term_trend = None
        if symbols_to_buy in price_history and '5min' in price_history[symbols_to_buy] and len(price_history[symbols_to_buy]['5min']) >= 2:
            recent_prices = price_history[symbols_to_buy]['5min'][-2:]
            short_term_trend = 'up' if recent_prices[-1] > recent_prices[-2] else 'down'
            print(f"{symbols_to_buy}: Short-term price trend (5min): {short_term_trend}")
            logging.info(f"{symbols_to_buy}: Short-term price trend (5min): {short_term_trend}")
        print(f"Checking for bullish reversal patterns in {symbols_to_buy}...")
        open_prices = historical_data['open'].values
        high_prices = historical_data['high'].values
        low_prices = historical_data['low'].values
        close_prices = historical_data['Close'].values
        bullish_reversal_detected = False
        reversal_candle_index = None
        detected_patterns = []
        for i in range(-1, -21, -1):
            if len(historical_data) < abs(i):
                continue
            try:
                patterns = {
                    'Hammer': talib.CDLHAMMER(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1], close_prices[:i + 1])[i] != 0,
                    'Bullish Engulfing': talib.CDLENGULFING(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1], close_prices[:i + 1])[i] > 0,
                    'Morning Star': talib.CDLMORNINGSTAR(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1], close_prices[:i + 1])[i] != 0,
                    'Piercing Line': talib.CDLPIERCING(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1], close_prices[:i + 1])[i] != 0,
                    'Three White Soldiers': talib.CDL3WHITESOLDIERS(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1], close_prices[:i + 1])[i] != 0,
                    'Dragonfly Doji': talib.CDLDRAGONFLYDOJI(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1], close_prices[:i + 1])[i] != 0,
                    'Inverted Hammer': talib.CDLINVERTEDHAMMER(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1], close_prices[:i + 1])[i] != 0,
                    'Tweezer Bottom': talib.CDLMATCHINGLOW(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1], close_prices[:i + 1])[i] != 0,
                }
                current_detected = [name for name, detected in patterns.items() if detected]
                if current_detected:
                    bullish_reversal_detected = True
                    detected_patterns = current_detected
                    reversal_candle_index = i
                    break
            except IndexError as e:
                print(f"IndexError in candlestick pattern detection for {symbols_to_buy}: {e}")
                logging.error(f"IndexError in candlestick pattern detection for {symbols_to_buy}: {e}")
                continue
        if detected_patterns:
            print(f"{symbols_to_buy}: Detected bullish reversal patterns at candle {reversal_candle_index}: {', '.join(detected_patterns)}")
            logging.info(f"{symbols_to_buy}: Detected bullish reversal patterns at candle {reversal_candle_index}: {', '.join(detected_patterns)}")
            if symbols_to_buy in price_history:
                for interval, prices in price_history[symbols_to_buy].items():
                    if prices:
                        print(f"{symbols_to_buy}: Price history at {interval}: {prices[-5:]}")
                        logging.info(f"{symbols_to_buy}: Price history at {interval}: {prices[-5:]}")
        if price_decline:
            print(f"{symbols_to_buy}: Price decline >= 0.2% detected (Current price = {current_price:.2f} <= Threshold = {price_decline_threshold:.2f})")
            logging.info(f"{symbols_to_buy}: Price decline >= 0.2% detected (Current price = {current_price:.2f} <= Threshold = {price_decline_threshold:.2f})")
        if volume_decrease:
            print(f"{symbols_to_buy}: Volume decrease detected (Recent avg = {recent_avg_volume:.0f} < Prior avg = {prior_avg_volume:.0f})")
            logging.info(f"{symbols_to_buy}: Volume decrease detected (Recent avg = {recent_avg_volume:.0f} < Prior avg = {prior_avg_volume:.0f})")
        if rsi_decrease:
            print(f"{symbols_to_buy}: RSI decrease detected (Recent avg = {recent_avg_rsi:.2f} < Prior avg = {prior_avg_rsi:.2f})")
            logging.info(f"{symbols_to_buy}: RSI decrease detected (Recent avg = {recent_avg_rsi:.2f} < Prior avg = {prior_avg_rsi:.2f})")
        if not is_in_uptrend(symbols_to_buy):
            print(f"{symbols_to_buy}: Not in uptrend (below 200-day SMA). Skipping.")
            logging.info(f"{symbols_to_buy}: Not in uptrend. Skipping.")
            continue
        daily_rsi = get_daily_rsi(symbols_to_buy)
        if daily_rsi is None or daily_rsi > 50:
            print(f"{symbols_to_buy}: Daily RSI not oversold ({daily_rsi}). Skipping.")
            logging.info(f"{symbols_to_buy}: Daily RSI not oversold ({daily_rsi}). Skipping.")
            continue
        buy_conditions_met = False
        specific_reason = ""
        score = 0
        if bullish_reversal_detected:
            score += 2
            price_stable = True
            if symbols_to_buy in price_history and '5min' in price_history[symbols_to_buy] and len(price_history[symbols_to_buy]['5min']) >= 2:
                recent_prices = price_history[symbols_to_buy]['5min'][-2:]
                price_stable = abs(recent_prices[-1] - recent_prices[-2]) / recent_prices[-2] < 0.005
                print(f"{symbols_to_buy}: Price stability check (5min): {price_stable}")
                logging.info(f"{symbols_to_buy}: Price stability check (5min): {price_stable}")
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
                if pattern == 'Hammer':
                    if latest_rsi < 35 and price_decline >= (last_price * 0.003):
                        score += 1
                elif pattern == 'Bullish Engulfing':
                    if recent_avg_volume > 1.5 * prior_avg_volume:
                        score += 1
                elif pattern == 'Morning Star':
                    if latest_rsi < 40:
                        score += 1
                elif pattern == 'Piercing Line':
                    if recent_avg_rsi < 40:
                        score += 1
                elif pattern == 'Three White Soldiers':
                    if not volume_decrease:
                        score += 1
                elif pattern == 'Dragonfly Doji':
                    if latest_rsi < 30:
                        score += 1
                elif pattern == 'Inverted Hammer':
                    if rsi_decrease:
                        score += 1
                elif pattern == 'Tweezer Bottom':
                    if latest_rsi < 40:
                        score += 1
            if score >= 3:
                buy_conditions_met = True
                specific_reason = f"Score: {score}, patterns: {', '.join(detected_patterns)}"
        if not buy_conditions_met:
            print(f"{symbols_to_buy}: Buy score too low ({score} < 3). Skipping.")
            logging.info(f"{symbols_to_buy}: Buy score too low ({score} < 3). Skipping.")
            continue
        if ALL_BUY_ORDERS_ARE_1_DOLLAR:
            total_cost_for_qty = 1.00
            qty = round(total_cost_for_qty / current_price, 4)
            print(f"{symbols_to_buy}: Using $1.00 fractional share order mode. Qty = {qty:.4f}")
        else:
            print(f"Calculating position size for {symbols_to_buy}...")
            atr = get_average_true_range(symbols_to_buy)
            if atr is None:
                print(f"No ATR for {symbols_to_buy}. Skipping.")
                continue
            stop_loss_distance = 2 * atr
            risk_per_share = stop_loss_distance
            risk_amount = 0.01 * total_equity
            qty = risk_amount / risk_per_share if risk_per_share > 0 else 0
            total_cost_for_qty = qty * current_price
            with buy_sell_lock:
                cash_available = round(float(api.get_account()['cash']), 2)
                print(f"Cash available for {symbols_to_buy}: ${cash_available:.2f}")
                total_cost_for_qty = min(total_cost_for_qty, cash_available - 1.00, max_new_exposure)
                if total_cost_for_qty < 1.00:
                    print(f"Insufficient risk-adjusted allocation for {symbols_to_buy}.")
                    continue
                qty = round(total_cost_for_qty / current_price, 4)
            estimated_slippage = total_cost_for_qty * 0.001
            total_cost_for_qty -= estimated_slippage
            qty = round(total_cost_for_qty / current_price, 4)
            print(f"{symbols_to_buy}: Adjusted for slippage (0.1%): Notional = ${total_cost_for_qty:.2f}, Qty = {qty:.4f}")
        with buy_sell_lock:
            cash_available = round(float(api.get_account()['cash']), 2)
        if total_cost_for_qty < 1.00:
            print(f"Order amount for {symbols_to_buy} is ${total_cost_for_qty:.2f}, below minimum $1.00")
            logging.info(f"{current_time_str} Did not buy {symbols_to_buy} due to order amount below $1.00")
            continue
        if cash_available < total_cost_for_qty + 1.00:
            print(f"Insufficient cash for {symbols_to_buy}. Available: ${cash_available:.2f}, Required: ${total_cost_for_qty:.2f} + $1.00 minimum")
            logging.info(f"{current_time_str} Did not buy {symbols_to_buy} due to insufficient cash")
            continue
        if buy_conditions_met:
            buy_signal = 1
            api_symbols = symbols_to_buy
            reason = f"bullish reversal ({', '.join(detected_patterns)}), {specific_reason}"
            print(f"Submitting buy order for {api_symbols}...")
            try:
                total_cost_for_qty = round(total_cost_for_qty, 2)
                buy_order = api.submit_order(
                    symbol=api_symbols,
                    notional=total_cost_for_qty,
                    side='buy',
                    type='market',
                    time_in_force='day'
                )
                print(f"{current_time_str}, Submitted buy order for {qty:.4f} shares of {api_symbols} at {current_price:.2f} (notional: ${total_cost_for_qty:.2f}) due to {reason}")
                logging.info(f"{current_time_str} Submitted buy {qty:.4f} shares of {api_symbols} due to {reason}. RSI Decrease={rsi_decrease}, Volume Decrease={volume_decrease}, Bullish Reversal={bullish_reversal_detected}, Price Decline >= 0.2%={price_decline}")
                order_filled = False
                filled_qty = 0
                filled_price = current_price
                for _ in range(30):
                    print(f"Checking order status for {api_symbols}...")
                    order_status = api.get_order(buy_order.id)
                    if order_status.status == 'filled':
                        order_filled = True
                        filled_qty = float(order_status.filled_qty)
                        filled_price = float(order_status.filled_avg_price or current_price)
                        with buy_sell_lock:
                            cash_available = round(float(api.get_account()['cash']), 2)
                        actual_cost = filled_qty * filled_price
                        print(f"Order filled for {filled_qty:.4f} shares of {api_symbols} at ${filled_price:.2f}, actual cost: ${actual_cost:.2f}")
                        logging.info(f"Order filled for {filled_qty:.4f} shares of {api_symbols}, actual cost: ${actual_cost:.2f}")
                        break
                    time.sleep(2)
                if order_filled:
                    print(f"Logging trade for {api_symbols}...")
                    with open(csv_filename, mode='a', newline='') as csv_file:
                        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        csv_writer.writerow({
                            'Date': current_time_str,
                            'Buy': 'Buy',
                            'Quantity': filled_qty,
                            'Symbol': api_symbols,
                            'Price Per Share': filled_price
                        })
                    symbols_to_remove.append((api_symbols, filled_price, today_date_str))
                    if api.get_account()['daytrade_count'] < 3 and not ALL_BUY_ORDERS_ARE_1_DOLLAR:
                        stop_order_id = place_trailing_stop_sell_order(api_symbols, filled_qty, filled_price)
                        if stop_order_id:
                            print(f"Trailing stop sell order placed for {api_symbols} with ID: {stop_order_id}")
                        else:
                            print(f"Failed to place trailing stop sell order for {api_symbols}")
                else:
                    print(f"Buy order not filled for {api_symbols}")
                    logging.info(f"{current_time_str} Buy order not filled for {api_symbols}")
            except APIError as e:
                print(f"Error submitting buy order for {api_symbols}: {e}")
                logging.error(f"Error submitting buy order for {api_symbols}: {e}")
                continue
        else:
            print(f"{symbols_to_buy}: Conditions not met for any detected patterns. Bullish Reversal = {bullish_reversal_detected}, Volume Decrease = {volume_decrease}, RSI Decrease = {rsi_decrease}, Price Decline >= 0.2% = {price_decline}, Price Stable = {price_stable}")
            logging.info(f"{current_time_str} Did not buy {symbols_to_buy} due to Bullish Reversal = {bullish_reversal_detected}, Volume Decrease = {volume_decrease}, RSI Decrease = {rsi_decrease}, Price Decline >= 0.2% = {price_decline}, Price Stable = {price_stable}")
        update_previous_price(symbols_to_buy, current_price)
        time.sleep(0.8)
    try:
        with buy_sell_lock:
            print("Updating database with buy transactions...")
            for symbols_to_sell, price, date in symbols_to_remove:
                symbols_to_sell_dict[symbols_to_sell] = (round(price, 4), date)
                symbols_to_buy_list.remove(symbols_to_sell)
                remove_symbols_from_trade_list(symbols_to_sell)
                trade_history = TradeHistory(
                    symbols=symbols_to_sell,
                    action='buy',
                    quantity=filled_qty,
                    price=price,
                    date=date
                )
                session.add(trade_history)
                db_position = Position(
                    symbols=symbols_to_sell,
                    quantity=filled_qty,
                    avg_price=price,
                    purchase_date=date
                )
                session.add(db_position)
            session.commit()
            print("Database updated successfully.")
            refresh_after_buy()
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Database error: {str(e)}")
        logging.error(f"Database error: {str(e)}")

def refresh_after_buy():
    global symbols_to_buy, symbols_to_sell_dict
    print("Refreshing after buy operation...")
    time.sleep(2)
    symbols_to_buy = get_symbols_to_buy()
    symbols_to_sell_dict = update_symbols_to_sell_from_api()
    print("Refresh complete.")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def place_trailing_stop_sell_order(symbols_to_sell, qty, current_price):
    print(f"Attempting to place trailing stop sell order for {symbols_to_sell}...")
    try:
        if qty != int(qty):
            print(f"Skipping trailing stop sell order for {symbols_to_sell}: Fractional share quantity {qty:.4f} detected.")
            logging.error(f"Skipped trailing stop sell order for {symbols_to_sell}: Fractional quantity {qty:.4f} not allowed.")
            return None
        stop_loss_percent = 1.0
        stop_loss_price = current_price * (1 - stop_loss_percent / 100)
        print(f"Placing trailing stop sell order for {qty} shares of {symbols_to_sell} with trail percent {stop_loss_percent}% (initial stop price: {stop_loss_price:.2f})")
        stop_order = api.submit_order(
            symbol=symbols_to_sell,
            qty=int(qty),
            side='sell',
            type='trailing_stop',
            trail_percent=str(stop_loss_percent),
            time_in_force='gtc'
        )
        print(f"Successfully placed trailing stop sell order for {qty} shares of {symbols_to_sell} with ID: {stop_order.id}")
        logging.info(f"Placed trailing stop sell order for {qty} shares of {symbols_to_sell} with ID: {stop_order.id}")
        return stop_order.id
    except Exception as e:
        print(f"Error placing trailing stop sell order for {symbols_to_sell}: {str(e)}")
        logging.error(f"Error placing trailing stop sell order for {symbols_to_sell}: {str(e)}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def update_symbols_to_sell_from_api():
    print("Updating symbols to sell from Public API...")
    positions = api.list_positions()
    symbols_to_sell_dict = {}
    for position in positions:
        symbols_to_sell = position.symbol
        avg_entry_price = float(position.avg_entry_price)
        quantity = float(position.qty)
        purchase_date_str = get_most_recent_purchase_date(symbols_to_sell)
        try:
            db_position = session.query(Position).filter_by(symbols=symbols_to_sell).one()
            db_position.quantity = quantity
            db_position.avg_price = avg_entry_price
            db_position.purchase_date = purchase_date_str
        except NoResultFound:
            db_position = Position(
                symbols=symbols_to_sell,
                quantity=quantity,
                avg_price=avg_entry_price,
                purchase_date=purchase_date_str
            )
            session.add(db_position)
        symbols_to_sell_dict[symbols_to_sell] = (avg_entry_price, purchase_date_str)
    session.commit()
    print(f"Updated {len(symbols_to_sell_dict)} symbols to sell from API.")
    return symbols_to_sell_dict

def sell_stocks(symbols_to_sell_dict, buy_sell_lock):
    print("Starting sell_stocks function...")
    symbols_to_remove = []
    now = datetime.now(pytz.timezone('US/Eastern'))
    current_time_str = now.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
    today_date_str = datetime.today().date().strftime("%Y-%m-%d")
    comparison_date = datetime.today().date()
    for symbols_to_sell, (bought_price, purchase_date) in symbols_to_sell_dict.items():
        status_printer_sell_stocks()
        try:
            bought_date = datetime.strptime(purchase_date, "%Y-%m-%d").date()
        except (ValueError, TypeError) as e:
            print(f"Error parsing purchase_date for {symbols_to_sell}: {purchase_date}. Skipping. Error: {e}")
            logging.error(f"Error parsing purchase_date for {symbols_to_sell}: {purchase_date}. Error: {e}")
            continue
        print(f"Checking {symbols_to_sell}: Purchase date = {bought_date}, Comparison date = {comparison_date}")
        logging.info(f"Checking {symbols_to_sell}: Purchase date = {bought_date}, Comparison date = {comparison_date}")
        if bought_date <= comparison_date:
            current_price = get_current_price(symbols_to_sell)
            if current_price is None:
                print(f"Skipping {symbols_to_sell}: Could not retrieve current price.")
                logging.error(f"Skipping {symbols_to_sell}: Could not retrieve current price.")
                continue
            try:
                position = api.get_position(symbols_to_sell)
                bought_price = float(position.avg_entry_price)
                qty = float(position.qty)
                open_orders = api.list_orders(status='open')
                symbols_open_orders = [order for order in open_orders if order.symbol == symbols_to_sell]
                print(f"{symbols_to_sell}: Found {len(symbols_open_orders)} open orders.")
                logging.info(f"{symbols_to_sell}: Found {len(symbols_open_orders)} open orders.")
                if symbols_open_orders:
                    print(f"There is an open sell order for {symbols_to_sell}. Skipping sell order.")
                    logging.info(f"{current_time_str} Skipped sell for {symbols_to_sell} due to existing open order.")
                    continue
                sell_threshold = bought_price * 1.005
                print(f"{symbols_to_sell}: Current price = {current_price:.2f}, Bought price = {bought_price:.2f}, Sell threshold = {sell_threshold:.2f}")
                logging.info(f"{symbols_to_sell}: Current price = {current_price:.2f}, Bought price = {bought_price:.2f}, Sell threshold = {sell_threshold:.2f}")
                if current_price >= sell_threshold:
                    print(f"Submitting sell order for {symbols_to_sell}...")
                    api.submit_order(
                        symbol=symbols_to_sell,
                        qty=qty,
                        side='sell',
                        type='market',
                        time_in_force='day'
                    )
                    print(f" {current_time_str}, Sold {qty:.4f} shares of {symbols_to_sell} at {current_price:.2f} based on a higher selling price.")
                    logging.info(f"{current_time_str} Sold {qty:.4f} shares of {symbols_to_sell} at {current_price:.2f}.")
                    with open(csv_filename, mode='a', newline='') as csv_file:
                        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        csv_writer.writerow({
                            'Date': current_time_str,
                            'Sell': 'Sell',
                            'Quantity': qty,
                            'Symbol': symbols_to_sell,
                            'Price Per Share': current_price
                        })
                    symbols_to_remove.append((symbols_to_sell, qty, current_price))
                else:
                    print(f"{symbols_to_sell}: Price condition not met. Current price ({current_price:.2f}) < Sell threshold ({sell_threshold:.2f})")
                    logging.info(f"{symbols_to_sell}: Price condition not met. Current price ({current_price:.2f}) < Sell threshold ({sell_threshold:.2f})")
            except Exception as e:
                print(f"Error processing sell for {symbols_to_sell}: {e}")
                logging.error(f"Error processing sell for {symbols_to_sell}: {e}")
        else:
            print(f"{symbols_to_sell}: Not eligible for sale. Purchase date ({bought_date}) is not on or before comparison date ({comparison_date})")
            logging.info(f"{symbols_to_sell}: Not eligible for sale. Purchase date ({bought_date}) is not on or before comparison date ({comparison_date})")
    try:
        with buy_sell_lock:
            print("Updating database with sell transactions...")
            for symbols_to_sell, qty, current_price in symbols_to_remove:
                del symbols_to_sell_dict[symbols_to_sell]
                trade_history = TradeHistory(
                    symbols=symbols_to_sell,
                    action='sell',
                    quantity=qty,
                    price=current_price,
                    date=today_date_str
                )
                session.add(trade_history)
                session.query(Position).filter_by(symbols=symbols_to_sell).delete()
            session.commit()
            print("Database updated successfully.")
            refresh_after_sell()
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Database error: {str(e)}")
        logging.error(f"Database error: {str(e)}")

def refresh_after_sell():
    global symbols_to_sell_dict
    print("Refreshing after sell operation...")
    symbols_to_sell_dict = update_symbols_to_sell_from_api()
    print("Refresh complete.")

def load_positions_from_database():
    print("Loading positions from database...")
    positions = session.query(Position).all()
    symbols_to_sell_dict = {}
    for position in positions:
        symbols_to_sell = position.symbols
        avg_price = position.avg_price
        purchase_date = position.purchase_date
        symbols_to_sell_dict[symbols_to_sell] = (avg_price, purchase_date)
    print(f"Loaded {len(symbols_to_sell_dict)} positions from database.")
    return symbols_to_sell_dict

def main():
    global symbols_to_buy, symbols_to_sell_dict
    print("Starting main trading program...")
    symbols_to_buy = get_symbols_to_buy()
    symbols_to_sell_dict = load_positions_from_database()
    buy_sell_lock = threading.Lock()
    while True:
        try:
            stop_if_stock_market_is_closed()
            current_datetime = datetime.now(pytz.timezone('US/Eastern'))
            current_time_str = current_datetime.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
            cash_balance = round(float(api.get_account()['cash']), 2)
            print("------------------------------------------------------------------------------------")
            print("\n")
            print("*****************************************************")
            print("******** Billionaire Buying Strategy Version ********")
            print("*****************************************************")
            print("2025 Edition of the Advanced Stock Market Trading Robot, Version 8 ")
            print("by https://github.com/CodeProSpecialist")
            print("------------------------------------------------------------------------------------")
            print(f" {current_time_str} Cash Balance: ${cash_balance}")
            day_trade_count = api.get_account()['daytrade_count']
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
                for symbols_to_buy in symbols_to_buy:
                    current_price = get_current_price(symbols_to_buy)
                    print(f"Symbol: {symbols_to_buy} | Current Price: {current_price} ")
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
                for symbols_to_buy in symbols_to_buy:
                    current_price = get_current_price(symbols_to_buy)
                    atr_low_price = get_atr_low_price(symbols_to_buy)
                    print(f"Symbol: {symbols_to_buy} | Current Price: {current_price} | ATR low buy signal price: {atr_low_price}")
                print("\n")
                print("------------------------------------------------------------------------------------")
                print("\n")
                print("\nSymbols to Sell:")
                print("\n")
                for symbols_to_sell, _ in symbols_to_sell_dict.items():
                    current_price = get_current_price(symbols_to_sell)
                    atr_high_price = get_atr_high_price(symbols_to_sell)
                    print(f"Symbol: {symbols_to_sell} | Current Price: {current_price} | ATR high sell signal profit price: {atr_high_price}")
                print("\n")
            print("Waiting 1 minute before checking price data again........")
            time.sleep(60)
        except Exception as e:
            logging.error(f"Error encountered: {e}")
            print(f"Error encountered in main loop: {e}")
            time.sleep(120)

if __name__ == '__main__':
    try:
        print("Initializing trading bot...")
        main()
    except Exception as e:
        logging.error(f"Error encountered: {e}")
        print(f"Critical error: {e}")
        session.close()
