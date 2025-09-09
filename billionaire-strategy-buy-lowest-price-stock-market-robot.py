```python
import threading
import logging
import csv
import os
import time
import schedule
from datetime import datetime, timedelta, date
from datetime import time as time2
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
import pandas as pd
from public_invest_api import PublicClient

# Load environment variables for Public.com API
username = os.getenv('PUBLIC_USERNAME')
password = os.getenv('PUBLIC_PASSWORD')
mfa_code = os.getenv('PUBLIC_MFA_CODE')  # Optional, for 2FA

# Initialize the Public API client
try:
    client = PublicClient(username=username, password=password, mfa_code=mfa_code)
except Exception as e:
    logging.error(f"Failed to initialize PublicClient: {str(e)}")
    raise

# Global variables
symbols_to_buy = []
today_date = date.today()
today_datetime = datetime.now(pytz.timezone('US/Eastern'))
csv_filename = 'log-file-of-buy-and-sell-signals.csv'
fieldnames = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']
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

# Thread locks for thread-safe operations
buy_sell_lock = threading.Lock()

# Logging configuration
logging.basicConfig(filename='trading-bot-program-logging-messages.txt', level=logging.INFO)

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
        positions = client.get_positions()
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
                current_price = client.get_quote(symbols_to_sell).get('last_price')
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
        bars = client.get_bars(symbols_to_buy, timeframe='1d', limit=1)
        if bars:
            opening_price = round(float(bars[0]['open']), 4)
            print(f"Opening price for {symbols_to_buy}: ${opening_price:.4f}")
            return opening_price
        else:
            logging.error(f"No bars data for {symbols_to_buy}.")
            print(f"Error: No bars data for {symbols_to_buy}.")
            return None
    except Exception as e:
        logging.error(f"Error fetching opening price for {symbols_to_buy}: {e}")
        print(f"Error fetching opening price for {symbols_to_buy}: {e}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_atr_high_price(symbols_to_sell):
    print(f"Calculating ATR high price for {symbols_to_sell}...")
    atr_value = get_average_true_range(symbols_to_sell)
    current_price = client.get_quote(symbols_to_sell).get('last_price')
    atr_high = round(current_price + 0.40 * atr_value, 4) if current_price and atr_value else None
    print(f"ATR high price for {symbols_to_sell}: ${atr_high:.4f}" if atr_high else f"Failed to calculate ATR high price for {symbols_to_sell}.")
    return atr_high

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_atr_low_price(symbols_to_buy):
    print(f"Calculating ATR low price for {symbols_to_buy}...")
    atr_value = get_average_true_range(symbols_to_buy)
    current_price = client.get_quote(symbols_to_buy).get('last_price')
    atr_low = round(current_price - 0.10 * atr_value, 4) if current_price and atr_value else None
    print(f"ATR low price for {symbols_to_buy}: ${atr_low:.4f}" if atr_low else f"Failed to calculate ATR low price for {symbols_to_buy}.")
    return atr_low

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_average_true_range(symbols):
    print(f"Calculating ATR for {symbols}...")
    def _fetch_atr(symbols):
        end = datetime.now(pytz.UTC)
        start = end - timedelta(days=30)
        bars = client.get_bars(symbols, timeframe='1d', start=start.isoformat(), end=end.isoformat(), limit=22)
        if not bars or len(bars) < 22:
            logging.error(f"Insufficient data for ATR calculation for {symbols}.")
            print(f"Insufficient data for ATR calculation for {symbols}.")
            return None
        df = pd.DataFrame(bars)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        atr = talib.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=22)
        atr_value = atr[-1]
        print(f"ATR for {symbols}: {atr_value:.4f}")
        return atr_value
    return get_cached_data(symbols, 'atr', _fetch_atr, symbols)

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def is_in_uptrend(symbols_to_buy):
    print(f"Checking if {symbols_to_buy} is in uptrend (above 200-day SMA)...")
    end = datetime.now(pytz.UTC)
    start = end - timedelta(days=200)
    bars = client.get_bars(symbols_to_buy, timeframe='1d', start=start.isoformat(), end=end.isoformat(), limit=200)
    if not bars or len(bars) < 200:
        print(f"Insufficient data for 200-day SMA for {symbols_to_buy}. Assuming not in uptrend.")
        return False
    df = pd.DataFrame(bars)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    sma_200 = talib.SMA(df['close'].values, timeperiod=200)[-1]
    current_price = client.get_quote(symbols_to_buy).get('last_price')
    in_uptrend = current_price > sma_200 if current_price else False
    print(f"{symbols_to_buy} {'is' if in_uptrend else 'is not'} in uptrend (Current: {current_price:.2f}, SMA200: {sma_200:.2f})")
    return in_uptrend

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_daily_rsi(symbols_to_buy):
    print(f"Calculating daily RSI for {symbols_to_buy}...")
    end = datetime.now(pytz.UTC)
    start = end - timedelta(days=30)
    bars = client.get_bars(symbols_to_buy, timeframe='1d', start=start.isoformat(), end=end.isoformat(), limit=30)
    if not bars:
        print(f"No daily data for {symbols_to_buy}. RSI calculation failed.")
        return None
    df = pd.DataFrame(bars)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    rsi = talib.RSI(df['close'], timeperiod=14)[-1]
    rsi_value = round(rsi, 2) if not np.isnan(rsi) else None
    print(f"Daily RSI for {symbols_to_buy}: {rsi_value}")
    return rsi_value

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
    end = datetime.now(pytz.UTC)
    start = end - timedelta(days=lookback_days)
    bars = client.get_bars(symbols, timeframe='1d', start=start.isoformat(), end=end.isoformat(), limit=lookback_days)
    if not bars:
        print(f"No historical data for {symbols}.")
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    df = df[['open', 'high', 'low', 'close', 'volume']]
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df.index = df.index.tz_convert('US/Eastern')
    short_window = 12
    long_window = 26
    signal_window = 9
    df['macd'], df['signal'], _ = talib.MACD(
        df['Close'], fastperiod=short_window, slowperiod=long_window, signalperiod=signal_window
    )
    rsi_period = 14
    df['rsi'] = talib.RSI(df['Close'], timeperiod=rsi_period)
    df['volume'] = df['Volume']
    print(f"Technical indicators calculated for {symbols}.")
    return df

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_rsi(symbols, period=14, interval='5m'):
    print(f"Calculating RSI for {symbols} (period={period}, interval={interval})...")
    end = datetime.now(pytz.UTC)
    start = end - timedelta(days=5)
    bars = client.get_bars(symbols, timeframe=interval, start=start.isoformat(), end=end.isoformat(), extended=True)
    if not bars or len(bars) < period:
        logging.error(f"Insufficient data for RSI calculation for {symbols} with {interval} interval.")
        print(f"Insufficient data for RSI calculation for {symbols}.")
        return None
    df = pd.DataFrame(bars)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.set_index('timestamp', inplace=True)
    rsi = talib.RSI(df['close'], timeperiod=period)
    latest_rsi = rsi[-1] if len(rsi) > 0 else None
    if latest_rsi is None or not np.isfinite(latest_rsi):
        logging.error(f"Invalid RSI value for {symbols}: {latest_rsi}")
        print(f"Invalid RSI value for {symbols}.")
        return None
    latest_rsi = round(latest_rsi, 2)
    print(f"RSI for {symbols}: {latest_rsi}")
    return latest_rsi

def print_technical_indicators(symbols, historical_data):
    print("")
    print(f"\nTechnical Indicators for {symbols}:\n")
    print(historical_data[['Close', 'macd', 'signal', 'rsi', 'volume']].tail())
    print("")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_cash_on_hand():
    print("Calculating available cash...")
    account = client.get_account()
    cash_available = round(float(account.get('cash', 0)), 2)
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
        current_price = client.get_quote(symbols).get('last_price')
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
    current_price = client.get_quote(symbols).get('last_price')
    previous_price = get_previous_price(symbols)
    print("")
    print_technical_indicators(symbols, calculate_technical_indicators(symbols))
    print("")
    if symbols not in price_changes:
        price_changes[symbols] = {'increased': 0, 'decreased': 0}
    if current_price and previous_price:
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
            bars = client.get_bars(symbols_to_buy, timeframe='1m', start=start_time.isoformat(), end=end_time.isoformat(), extended=True)
            if bars:
                df = pd.DataFrame(bars)
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)
                if not df.empty:
                    last_price = round(float(df['close'].iloc[-1]), 2)
                    results[symbols_to_buy] = last_price
                    print(f"Last price for {symbols_to_buy} within 5 minutes: ${last_price:.2f}")
                else:
                    results[symbols_to_buy] = None
                    print(f"No price data found for {symbols_to_buy} within past 5 minutes.")
            else:
                results[symbols_to_buy] = None
                print(f"No price data found for {symbols_to_buy} within past 5 minutes.")
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
            orders = client.get_orders(status='all', direction='desc', until=end_time, limit=CHUNK_SIZE, symbols=[symbols_to_sell])
            if orders:
                order_list.extend(orders)
                end_time = (pd.to_datetime(orders[-1]['filled_at']) - timedelta(seconds=1)).isoformat()
                print(f"Fetched {len(orders)} orders for {symbols_to_sell}.")
            else:
                print(f"No more orders to fetch for {symbols_to_sell}.")
                break
        buy_orders = [
            order for order in order_list
            if order.get('side') == 'buy' and order.get('status') == 'filled' and order.get('filled_at')
        ]
        if buy_orders:
            most_recent_buy = max(buy_orders, key=lambda order: pd.to_datetime(order['filled_at']))
            purchase_date = pd.to_datetime(most_recent_buy['filled_at']).date()
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
    account = client.get_account()
    total_equity = float(account.get('equity', 0))
    print(f"Total account equity: ${total_equity:.2f}")
    positions = client.get_positions()
    current_exposure = sum(float(pos.get('market_value', 0)) for pos in positions)
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
        current_price = client.get_quote(symbols_to_buy).get('last_price')
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
        current_price = client.get_quote(symbols_to_buy).get('last_price')
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
        end = datetime.now(pytz.UTC)
        start = end - timedelta(days=5)
        historical_data = client.get_bars(symbols_to_buy, timeframe='5m', start=start.isoformat(), end=end.isoformat(), extended=True)
        if not historical_data or len(historical_data) < 3:
            print(f"Insufficient historical data for {symbols_to_buy} (rows: {len(historical_data)}). Skipping.")
            logging.info(f"Insufficient historical data for {symbols_to_buy} (rows: {len(historical_data)}).")
            continue
        df = pd.DataFrame(historical_data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df = df[['open', 'high', 'low', 'close', 'volume']]
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        df.index = df.index.tz_convert('US/Eastern')
        print(f"Calculating volume metrics for {symbols_to_buy}...")
        recent_avg_volume = df['Volume'].iloc[-5:].mean() if len(df) >= 5 else 0
        prior_avg_volume = df['Volume'].iloc[-10:-5].mean() if len(df) >= 10 else recent_avg_volume
        volume_decrease = recent_avg_volume < prior_avg_volume if len(df) >= 10 else False
        current_volume = df['Volume'].iloc[-1]
        print(f"{symbols_to_buy}: Recent avg volume = {recent_avg_volume:.0f}, Prior avg volume = {prior_avg_volume:.0f}, Volume decrease = {volume_decrease}")
        print(f"Calculating RSI metrics for {symbols_to_buy}...")
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
        price_increase = current_price > previous_price * 1.005 if current_price and previous_price else False
        print(f"{symbols_to_buy}: Price increase check: Current = ${current_price:.2f}, Previous = ${previous_price:.2f}, Increase = {price_increase}")
        print(f"Checking price drop for {symbols_to_buy}...")
        last_prices = get_last_price_within_past_5_minutes([symbols_to_buy])
        last_price = last_prices.get(symbols_to_buy)
        if last_price is None:
            try:
                bars = client.get_bars(symbols_to_buy, timeframe='1d', limit=1)
                last_price = round(float(bars[0]['close']), 4)
                print(f"No price found for {symbols_to_buy} in past 5 minutes. Using last closing price: {last_price}")
                logging.info(f"No price found for {symbols_to_buy} in past 5 minutes. Using last closing price: {last_price}")
            except Exception as e:
                print(f"Error fetching last closing price for {symbols_to_buy}: {e}")
                logging.error(f"Error fetching last closing price for {symbols_to_buy}: {e}")
                continue
        price_decline_threshold = last_price * (1 - 0.002)
        price_decline = current_price <= price_decline_threshold if current_price and last_price else False
        print(f"{symbols_to_buy}: Price decline check: Current = ${current_price:.2f}, Threshold = ${price_decline_threshold:.2f}, Decline = {price_decline}")
        short_term_trend = None
        if symbols_to_buy in price_history and '5min' in price_history[symbols_to_buy] and len(price_history[symbols_to_buy]['5min']) >= 2:
            recent_prices = price_history[symbols_to_buy]['5min'][-2:]
            short_term_trend = 'up' if recent_prices[-1] > recent_prices[-2] else 'down'
            print(f"{symbols_to_buy}: Short-term price trend (5min): {short_term_trend}")
            logging.info(f"{symbols_to_buy}: Short-term price trend (5min): {short_term_trend}")
        print(f"Checking for bullish reversal patterns in {symbols_to_buy}...")
        open_prices = df['Open'].values
        high_prices = df['High'].values
        low_prices = df['Low'].values
        close_prices = df['Close'].values
        bullish_reversal_detected = False
        reversal_candle_index = None
        detected_patterns = []
        for i in range(-1, -21, -1):
            if len(df) < abs(i):
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
                cash_available = round(float(client.get_account().get('cash', 0)), 2)
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
            cash_available = round(float(client.get_account().get('cash', 0)), 2)
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
            reason = f"bullish reversal ({', '.join(detected_patterns)}), {specific_reason}"
            print(f"Submitting buy order for {symbols_to_buy}...")
            try:
                total_cost_for_qty = round(total_cost_for_qty, 2)
                buy_order = client.submit_order(
                    symbol=symbols_to_buy,
                    notional=total_cost_for_qty,
                    side='buy',
                    type='market',
                    time_in_force='day'
                )
                print(f"{current_time_str}, Submitted buy order for {qty:.4f} shares of {symbols_to_buy} at {current_price:.2f} (notional: ${total_cost_for_qty:.2f}) due to {reason}")
                logging.info(f"{current_time_str} Submitted buy {qty:.4f} shares of {symbols_to_buy} due to {reason}. RSI Decrease={rsi_decrease}, Volume Decrease={volume_decrease}, Bullish Reversal={bullish_reversal_detected}, Price Decline >= 0.2%={price_decline}")
                order_filled = False
                filled_qty = 0
                filled_price = current_price
                for _ in range(30):
                    print(f"Checking order status for {symbols_to_buy}...")
                    order_status = client.get_order(buy_order.get('order_id'))
                    if order_status.get('status') == 'filled':
                        order_filled = True
                        filled_qty = float(order_status.get('filled_quantity', 0))
                        filled_price = float(order_status.get('average_fill_price', current_price))
                        with buy_sell_lock:
                            cash_available = round(float(client.get_account().get('cash', 0)), 2)
                        actual_cost = filled_qty * filled_price
                        print(f"Order filled for {filled_qty:.4f} shares of {symbols_to_buy} at ${filled_price:.2f}, actual cost: ${actual_cost:.2f}")
                        logging.info(f"Order filled for {filled_qty:.4f} shares of {symbols_to_buy}, actual cost: ${actual_cost:.2f}")
                        break
                    time.sleep(2)
                if order_filled:
                    print(f"Logging trade for {symbols_to_buy}...")
                    with open(csv_filename, mode='a', newline='') as csv_file:
                        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                        csv_writer.writerow({
                            'Date': current_time_str,
                            'Buy': 'Buy',
                            'Quantity': filled_qty,
                            'Symbol': symbols_to_buy,
                            'Price Per Share': filled_price
                        })
                    symbols_to_remove.append((symbols_to_buy, filled_price, today_date_str))
                    if client.get_account().get('daytrade_count', 0) < 3 and not ALL_BUY_ORDERS_ARE_1_DOLLAR:
                        stop_order_id = place_trailing_stop_sell_order(symbols_to_buy, filled_qty, filled_price)
                        if stop_order_id:
                            print(f"Trailing stop sell order placed for {symbols_to_buy} with ID: {stop_order_id}")
                        else:
                            print(f"Failed to place trailing stop sell order for {symbols_to_buy}")
                else:
                    print(f"Buy order not filled for {symbols_to_buy}")
                    logging.info(f"{current_time_str} Buy order not filled for {symbols_to_buy}")
            except Exception as e:
                print(f"Error submitting buy order for {symbols_to_buy}: {e}")
                logging.error(f"Error submitting buy order for {symbols_to_buy}: {e}")
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
        print(f"Placing trailing stop sell order for {qty} shares of {symbols_to_sell} with trail percent {stop_loss_percent}%")
        stop_order = client.submit_order(
            symbol=symbols_to_sell,
            qty=int(qty),
            side='sell',
            type='trailing_stop',
            trail_percent=str(stop_loss_percent),
            time_in_force='gtc'
        )
        print(f"Successfully placed trailing stop sell order for {qty} shares of {symbols_to_sell} with ID: {stop_order.get('order_id')}")
        logging.info(f"Placed trailing stop sell order for {qty} shares of {symbols_to_sell} with ID: {stop_order.get('order_id')}")
        return stop_order.get('order_id')
    except Exception as e:
        print(f"Error placing trailing stop sell order for {symbols_to_sell}: {str(e)}")
        logging.error(f"Error placing trailing stop sell order for {symbols_to_sell}: {str(e)}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def update_symbols_to_sell_from_api():
    print("Updating symbols to sell from Public API...")
    try:
        positions = client.get_positions()
        symbols_to_sell_dict = {}
        for pos in positions:
            symbol = pos.get('symbol')
            qty = float(pos.get('quantity', 0))
            avg_price = float(pos.get('average_price', 0))
            purchase_date = get_most_recent_purchase_date(symbol)
            if qty > 0:
                symbols_to_sell_dict[symbol] = (avg_price, purchase_date)
        print(f"Updated symbols to sell: {list(symbols_to_sell_dict.keys())}")
        return symbols_to_sell_dict
    except Exception as e:
        print(f"Error updating symbols to sell: {e}")
        logging.error(f"Error updating symbols to sell: {e}")
        return {}

def main():
    global symbols_to_buy, symbols_to_sell_dict, end_time
    print("Starting trading bot...")
    stop_if_stock_market_is_closed()
    symbols_to_buy = get_symbols_to_buy()
    symbols_to_sell_dict = update_symbols_to_sell_from_api()
    if PRINT_SYMBOLS_TO_BUY:
        print("\nSymbols to buy:", symbols_to_buy)
    if PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE:
        print_database_tables()
    end_time = time.time() + 60 * 60 * 8  # Run for 8 hours
    schedule.every(1).minutes.do(buy_stocks, symbols_to_sell_dict, symbols_to_buy, buy_sell_lock)
    schedule.every(1).minutes.do(status_printer_buy_stocks)
    schedule.every(1).minutes.do(update_symbols_to_sell_from_api).tag('update_sell')
    run_schedule()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Trading bot stopped by user.")
        logging.info("Trading bot stopped by user.")
    except Exception as e:
        print(f"Fatal error in main: {e}")
        logging.error(f"Fatal error in main: {e}")
    finally:
        session.close()
        print("Database session closed.")
