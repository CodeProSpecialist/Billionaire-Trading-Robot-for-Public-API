import os
import requests
import time
import json
import threading
import logging
import csv
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from uuid import uuid4
import numpy as np
import talib
import yfinance as yf
from ratelimit import limits, sleep_and_retry
from datetime import datetime, timedelta
import pytz

# Load environment variable
secret_key = os.getenv('PUBLIC_API_ACCESS_TOKEN')

if not secret_key:
    raise ValueError("PUBLIC_API_ACCESS_TOKEN environment variable is not set.")

headers = {
    "Authorization": f"Bearer {secret_key}",
    "Content-Type": "application/json"
}

BASE_URL = "https://api.public.com/userapigateway"

# Configuration flags
PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE = True  # Set to True to view database
ALL_BUY_ORDERS_ARE_1_DOLLAR = False  # When True, every buy order is a $1.00 fractional share market day order

# Set the timezone to Eastern
eastern = pytz.timezone('US/Eastern')

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
Base = declarative_base()

class TradeHistory(Base):
    __tablename__ = 'trade_history'
    id = Column(Integer, primary_key=True)
    symbols = Column(String)
    action = Column(String)  # 'buy' or 'sell'
    quantity = Column(Float)  # Changed to Float for fractional shares
    price = Column(Float)
    date = Column(String)

class Position(Base):
    __tablename__ = 'positions'
    symbols = Column(String, primary_key=True)
    quantity = Column(Float)  # Changed to Float for fractional shares
    avg_price = Column(Float)
    purchase_date = Column(String)

# Initialize SQLAlchemy
engine = create_engine('sqlite:///trading_bot.db')
Session = sessionmaker(bind=engine)
session = Session()

# Create tables if they don't exist
Base.metadata.create_all(engine)

# Thread lock for thread-safe operations
buy_sell_lock = threading.Lock()

# Rate limit: 60 calls per minute
CALLS = 60
PERIOD = 60

# Cache for yfinance data
data_cache = {}  # symbols -> {'timestamp': time, 'data_type': data}
CACHE_EXPIRY = 120  # 2 minutes

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_cached_data(symbols, data_type, fetch_func, *args, **kwargs):
    key = (symbols, data_type)
    current_time = time.time()
    if key in data_cache and current_time - data_cache[key]['timestamp'] < CACHE_EXPIRY:
        return data_cache[key]['data']
    else:
        data = fetch_func(*args, **kwargs)
        data_cache[key] = {'timestamp': current_time, 'data': data}
        return data

def get_accounts():
    """Get the list of accounts and return the first account ID."""
    response = requests.get(f"{BASE_URL}/accounts", headers=headers)
    if response.status_code == 200:
        accounts = response.json()
        if accounts:
            return accounts[0]['id']
    print(f"Error getting accounts: {response.status_code} - {response.text}")
    logging.error(f"Error getting accounts: {response.status_code} - {response.text}")
    return None

def get_portfolio(account_id):
    """Get the portfolio details for the specified account."""
    url = f"{BASE_URL}/trading/{account_id}/portfolio/v2"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        portfolio = response.json()
        return portfolio
    else:
        print(f"Error getting portfolio: {response.status_code} - {response.text}")
        logging.error(f"Error getting portfolio: {response.status_code} - {response.text}")
        return None

def get_price(account_id, symbol):
    """Obtain the current price/quote for a symbol from Public.com API."""
    url = f"{BASE_URL}/marketdata/{account_id}/quotes"
    request_body = {
        "instruments": [
            {
                "symbol": symbol,
                "type": "EQUITY"
            }
        ]
    }
    response = requests.post(url, headers=headers, json=request_body)
    if response.status_code == 200:
        data = response.json()
        quote = data.get('quotes', [{}])[0]
        price = quote.get('last') or quote.get('close', 'N/A')
        return float(price) if isinstance(price, (int, float, str)) and price != 'N/A' else None
    else:
        print(f"Error getting price for {symbol}: {response.status_code} - {response.text}")
        logging.error(f"Error getting price for {symbol}: {response.status_code} - {response.text}")
        return None

def place_market_order(account_id, symbol, side, quantity):
    """
    Place a market buy or sell order using the specified Public.com order format.
    side: 'BUY' or 'SELL'
    quantity: Number of shares (as string per API format)
    """
    url = f"{BASE_URL}/trading/{account_id}/order"
    request_body = {
        "orderId": str(uuid4()),
        "instrument": {
            "symbol": symbol,
            "type": "EQUITY"
        },
        "orderSide": side.upper(),
        "orderType": "MARKET",
        "expiration": {
            "timeInForce": "DAY"
        },
        "quantity": str(quantity),
        "amount": "0",
        "limitPrice": "0",
        "stopPrice": "0",
        "openCloseIndicator": "OPEN"
    }

    response = requests.post(url, headers=headers, json=request_body)
    if response.status_code in (200, 201):
        order = response.json()
        print(f"Order placed successfully: {json.dumps(order, indent=2)}")
        logging.info(f"Order placed successfully: {json.dumps(order, indent=2)}")
        return order
    else:
        print(f"Error placing {side} order for {symbol}: {response.status_code} - {response.text}")
        logging.error(f"Error placing {side} order for {symbol}: {response.status_code} - {response.text}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_current_price(symbols):
    yf_symbol = symbols.replace('.', '-')  # Adjust for yfinance compatibility
    stock_data = yf.Ticker(yf_symbol)
    try:
        data = stock_data.history(period='1d', interval='1m', prepost=True)
        if not data.empty:
            current_price = float(data['Close'].iloc[-1])
            return round(current_price, 4)
        else:
            last_close = float(stock_data.history(period='1d')['Close'].iloc[-1])
            return round(last_close, 4)
    except Exception as e:
        logging.error(f"Error fetching current price for {yf_symbol}: {e}")
        print(f"Error fetching current price for {yf_symbol}: {e}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_technical_indicators(symbols, lookback_days=90):
    yf_symbol = symbols.replace('.', '-')  # Adjust for yfinance compatibility
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period=f'{lookback_days}d')
    short_window = 12
    long_window = 26
    signal_window = 9
    historical_data['macd'], historical_data['signal'], _ = talib.MACD(historical_data['Close'],
                                                                       fastperiod=short_window,
                                                                       slowperiod=long_window,
                                                                       signalperiod=signal_window)
    rsi_period = 14
    historical_data['rsi'] = talib.RSI(historical_data['Close'], timeperiod=rsi_period)
    historical_data['volume'] = historical_data['Volume']
    return historical_data

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def is_in_uptrend(symbols_to_buy):
    yf_symbol = symbols_to_buy.replace('.', '-')  # Adjust for yfinance compatibility
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period='200d')
    if historical_data.empty or len(historical_data) < 200:
        return False
    sma_200 = talib.SMA(historical_data['Close'].values, timeperiod=200)[-1]
    current_price = get_current_price(symbols_to_buy)
    return current_price > sma_200 if current_price else False

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_daily_rsi(symbols_to_buy):
    yf_symbol = symbols_to_buy.replace('.', '-')  # Adjust for yfinance compatibility
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period='30d', interval='1d')
    if historical_data.empty:
        return None
    rsi = talib.RSI(historical_data['Close'], timeperiod=14)[-1]
    return round(rsi, 2) if not np.isnan(rsi) else None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_average_true_range(symbols):
    yf_symbol = symbols.replace('.', '-')  # Adjust for yfinance compatibility
    ticker = yf.Ticker(yf_symbol)
    data = ticker.history(period='30d')
    try:
        atr = talib.ATR(data['High'].values, data['Low'].values, data['Close'].values, timeperiod=22)
        return atr[-1]
    except Exception as e:
        logging.error(f"Error calculating ATR for {yf_symbol}: {e}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_last_price_within_past_5_minutes(symbols_to_buy_list):
    results = {}
    eastern = pytz.timezone('US/Eastern')
    current_datetime = datetime.now(eastern)
    end_time = current_datetime
    start_time = end_time - timedelta(minutes=5)

    for symbols_to_buy in symbols_to_buy_list:
        yf_symbol = symbols_to_buy.replace('.', '-')  # Adjust for yfinance compatibility
        try:
            data = yf.download(yf_symbol, start=start_time, end=end_time, interval='1m', prepost=True)
            if not data.empty:
                last_price = round(float(data['Close'].iloc[-1]), 2)
                results[symbols_to_buy] = last_price
            else:
                results[symbols_to_buy] = None
        except Exception as e:
            logging.error(f"Error fetching data for {yf_symbol}: {e}")
            results[symbols_to_buy] = None
    return results

def get_symbols_to_buy():
    try:
        with open('stocks_to_buy_list.txt', 'r') as file:
            symbols_to_buy = [line.strip() for line in file.readlines()]
        return symbols_to_buy
    except FileNotFoundError:
        print("Error: File 'stocks_to_buy_list.txt' not found.")
        logging.error("File 'stocks_to_buy_list.txt' not found.")
        return []

def remove_symbols_from_trade_list(symbols_to_buy):
    with open('stocks_to_buy_list.txt', 'r') as file:
        lines = file.readlines()
    with open('stocks_to_buy_list.txt', 'w') as file:
        for line in lines:
            if line.strip() != symbols_to_buy:
                file.write(line)

def load_positions_from_database():
    positions = session.query(Position).all()
    symbols_to_sell_dict = {}
    for position in positions:
        symbols_to_sell_dict[position.symbols] = (position.avg_price, position.purchase_date)
    return symbols_to_sell_dict

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_cash_on_hand(account_id):
    portfolio = get_portfolio(account_id)
    return float(portfolio.get('cash', 0)) if portfolio else 0

def calculate_total_symbols(symbols_to_buy_list):
    return len(symbols_to_buy_list)

def allocate_cash_equally(cash_available, total_symbols):
    max_allocation_per_symbol = 600.0
    allocation_per_symbol = min(max_allocation_per_symbol, cash_available / total_symbols) if total_symbols > 0 else 0
    return round(allocation_per_symbol, 2)

def buy_stocks(account_id, symbols_to_sell_dict, symbols_to_buy_list, buy_sell_lock):
    if not symbols_to_buy_list:
        print("No symbols to buy.")
        logging.info("No symbols to buy.")
        return
    symbols_to_remove = []

    cash_available = calculate_cash_on_hand(account_id)
    total_equity = cash_available  # Simplified; assumes cash is primary equity for allocation
    current_exposure = sum(float(pos.get('market_value', 0)) for pos in get_portfolio(account_id).get('positions', []))
    max_new_exposure = total_equity * 0.98 - current_exposure
    if max_new_exposure <= 0:
        print("Portfolio exposure limit reached. No new buys.")
        logging.info("Portfolio exposure limit reached. No new buys.")
        return

    valid_symbols = []
    for symbols_to_buy in symbols_to_buy_list:
        current_price = get_current_price(symbols_to_buy)
        if current_price is None:
            continue
        historical_data = calculate_technical_indicators(symbols_to_buy, lookback_days=5)
        if historical_data.empty:
            continue
        valid_symbols.append(symbols_to_buy)

    if not valid_symbols:
        print("No valid symbols to buy after filtering.")
        logging.info("No valid symbols to buy after filtering.")
        return

    for symbols_to_buy in valid_symbols:
        today_date = datetime.today().date()
        today_date_str = today_date.strftime("%Y-%m-%d")
        current_datetime = datetime.now(pytz.timezone('US/Eastern'))
        current_time_str = current_datetime.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")

        current_price = get_current_price(symbols_to_buy)
        if current_price is None:
            continue

        yf_symbol = symbols_to_buy.replace('.', '-')
        stock_data = yf.Ticker(yf_symbol)
        historical_data = stock_data.history(period='5d', interval='5m', prepost=True)
        if historical_data.empty or len(historical_data) < 3:
            continue

        recent_avg_volume = historical_data['Volume'].iloc[-5:].mean() if len(historical_data) >= 5 else 0
        prior_avg_volume = historical_data['Volume'].iloc[-10:-5].mean() if len(historical_data) >= 10 else recent_avg_volume
        volume_decrease = recent_avg_volume < prior_avg_volume if len(historical_data) >= 10 else False

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

        short_window = 12
        long_window = 26
        signal_window = 9
        macd, macd_signal, _ = talib.MACD(close_prices, fastperiod=short_window, slowperiod=long_window, signalperiod=signal_window)
        latest_macd = macd[-1] if len(macd) > 0 else None
        latest_macd_signal = macd_signal[-1] if len(macd_signal) > 0 else None
        macd_above_signal = latest_macd > latest_macd_signal if latest_macd is not None else False

        last_prices = get_last_price_within_past_5_minutes([symbols_to_buy])
        last_price = last_prices.get(symbols_to_buy)
        if last_price is None:
            try:
                last_price = round(float(stock_data.history(period='1d')['Close'].iloc[-1]), 4)
            except Exception:
                continue
        price_decline_threshold = last_price * (1 - 0.002)
        price_decline = current_price <= price_decline_threshold

        open_prices = historical_data['Open'].values
        high_prices = historical_data['High'].values
        low_prices = historical_data['Low'].values
        close_prices = historical_data['Close'].values

        bullish_reversal_detected = False
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
                    break
            except IndexError:
                continue

        if not is_in_uptrend(symbols_to_buy):
            continue

        daily_rsi = get_daily_rsi(symbols_to_buy)
        if daily_rsi is None or daily_rsi > 50:
            continue

        buy_conditions_met = False
        score = 0
        if bullish_reversal_detected:
            score += 2
            price_stable = True
            if abs(current_price - last_price) / last_price >= 0.005:
                price_stable = False
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

            if score >= 3:
                buy_conditions_met = True

        if not buy_conditions_met:
            continue

        if ALL_BUY_ORDERS_ARE_1_DOLLAR:
            total_cost_for_qty = 1.00
            qty = round(total_cost_for_qty / current_price, 4)
        else:
            atr = get_average_true_range(symbols_to_buy)
            if atr is None:
                continue
            stop_loss_distance = 2 * atr
            risk_per_share = stop_loss_distance
            risk_amount = 0.01 * total_equity
            qty = risk_amount / risk_per_share if risk_per_share > 0 else 0
            total_cost_for_qty = qty * current_price

            with buy_sell_lock:
                cash_available = calculate_cash_on_hand(account_id)
                total_cost_for_qty = min(total_cost_for_qty, cash_available - 1.00, max_new_exposure)
                if total_cost_for_qty < 1.00:
                    continue
                qty = round(total_cost_for_qty / current_price, 4)

            estimated_slippage = total_cost_for_qty * 0.001
            total_cost_for_qty -= estimated_slippage
            qty = round(total_cost_for_qty / current_price, 4)

        with buy_sell_lock:
            cash_available = calculate_cash_on_hand(account_id)
        if total_cost_for_qty < 1.00 or cash_available < total_cost_for_qty + 1.00:
            continue

        if buy_conditions_met:
            order = place_market_order(account_id, symbols_to_buy, "BUY", qty)
            if order:
                filled_qty = qty  # Public.com API may not provide immediate fill status; assume filled for simplicity
                filled_price = current_price
                actual_cost = filled_qty * filled_price
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

    try:
        with buy_sell_lock:
            for symbols_to_sell, price, date in symbols_to_remove:
                symbols_to_sell_dict[symbols_to_sell] = (round(price, 4), date)
                symbols_to_buy_list.remove(symbols_to_sell.replace('.', '-'))
                remove_symbols_from_trade_list(symbols_to_sell.replace('.', '-'))
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
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database error: {str(e)}")

def sell_stocks(account_id, symbols_to_sell_dict, buy_sell_lock):
    symbols_to_remove = []
    now = datetime.now(pytz.timezone('US/Eastern'))
    current_time_str = now.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
    today_date_str = datetime.today().date().strftime("%Y-%m-%d")
    comparison_date = datetime.today().date()

    for symbols_to_sell, (bought_price, purchase_date) in symbols_to_sell_dict.items():
        try:
            bought_date = datetime.strptime(purchase_date, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            continue

        if bought_date <= comparison_date:
            current_price = get_current_price(symbols_to_sell)
            if current_price is None:
                continue

            portfolio = get_portfolio(account_id)
            positions = portfolio.get('positions', []) if portfolio else []
            position = next((p for p in positions if p.get('symbol') == symbols_to_sell), None)
            if not position:
                continue
            qty = float(position.get('quantity', 0))
            bought_price = float(position.get('avg_price', bought_price))

            sell_threshold = bought_price * 1.005
            if current_price >= sell_threshold:
                order = place_market_order(account_id, symbols_to_sell, "SELL", qty)
                if order:
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

    try:
        with buy_sell_lock:
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
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database error: {str(e)}")

def trading_robot(symbols=["AAPL"], interval=60):
    account_id = get_accounts()
    if not account_id:
        print("Cannot proceed without account ID.")
        logging.error("Cannot proceed without account ID.")
        return

    symbols_to_buy = symbols if symbols else get_symbols_to_buy()
    symbols_to_sell_dict = load_positions_from_database()

    while True:
        try:
            portfolio = get_portfolio(account_id)
            if portfolio is None:
                time.sleep(interval)
                continue

            cash_available = portfolio.get('cash', 0) if portfolio else 0
            positions = portfolio.get('positions', []) if portfolio else []
            print(f"Starting trading robot. Cash: ${cash_available}, Positions: {len(positions)}")

            buy_thread = threading.Thread(target=buy_stocks, args=(account_id, symbols_to_sell_dict, symbols_to_buy, buy_sell_lock))
            sell_thread = threading.Thread(target=sell_stocks, args=(account_id, symbols_to_sell_dict, buy_sell_lock))

            buy_thread.start()
            sell_thread.start()

            buy_thread.join()
            sell_thread.join()

            if PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE:
                print("\nTrade History:")
                for record in session.query(TradeHistory).all():
                    print(f"{record.symbols} | {record.action} | {record.quantity:.4f} | {record.price:.2f} | {record.date}")
                print("\nPositions to Sell:")
                for record in session.query(Position).all():
                    current_price = get_current_price(record.symbols)
                    percentage_change = ((current_price - record.avg_price) / record.avg_price) * 100 if current_price and record.avg_price else 0
                    print(f"{record.symbols} | {record.quantity:.4f} | {record.avg_price:.2f} | {record.purchase_date} | Price Change: {percentage_change:.2f}%")

            time.sleep(interval)

        except KeyboardInterrupt:
            print("Trading robot stopped by user.")
            logging.info("Trading robot stopped by user.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            logging.error(f"Unexpected error: {e}")
            time.sleep(interval)

if __name__ == "__main__":
    trading_robot()
