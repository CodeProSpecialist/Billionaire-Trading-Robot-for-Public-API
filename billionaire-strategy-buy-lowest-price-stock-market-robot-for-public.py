#!/usr/bin/env python3
import os
import time
import csv
import logging
import threading
from uuid import uuid4
from datetime import datetime, timedelta
import pytz
import requests
import yfinance as yf
import talib
import pandas_market_calendars as mcal
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import SQLAlchemyError
from requests.exceptions import HTTPError, ConnectionError, Timeout
from ratelimit import limits, sleep_and_retry
import numpy as np

# ANSI color codes for terminal output
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"

# Global variables
YOUR_SECRET_KEY = "YOUR_SECRET_KEY"
secret = None
access_token = None
account_id = None
last_token_fetch_time = None
BASE_URL = "https://api.public.com/userapigateway"
HEADERS = None

# --- Flags ---
FRACTIONAL_BUY_ORDERS = True
ALL_BUY_ORDERS_ARE_1_DOLLAR = False
PRINT_DB_TRADES = True

# --- Timezone ---
eastern = pytz.timezone('US/Eastern')

# --- CSV ---
CSV_FILENAME = 'log-file-of-buy-and-sell-signals.csv'
CSV_FIELDS = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']
with open(CSV_FILENAME, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
    writer.writeheader()

# --- Logging ---
logging.basicConfig(filename='trading-bot.log', level=logging.INFO, 
                    format='%(asctime)s %(levelname)s:%(message)s')

# --- DB ---
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

engine = create_engine('sqlite:///trading_bot.db', connect_args={"check_same_thread": False})
with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
SessionLocal = scoped_session(sessionmaker(bind=engine))
Base.metadata.create_all(engine)

SYMBOLS_FILE = "electricity-or-utility-stocks-to-buy-list.txt"

# Additional globals from Alpaca script
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
}  # intervals in seconds

def load_symbols_from_file(file_path=SYMBOLS_FILE):
    symbols = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                s = line.strip().upper()
                if s:
                    symbols.append(s)
        logging.info(f"Loaded {len(symbols)} symbols from {file_path}")
    except Exception as e:
        logging.error(f"Error loading symbols file: {e}")
    return symbols

nyse_cal = mcal.get_calendar('NYSE')

def get_last_5_business_days():
    """Get the last 5 NYSE business days as YYYY-MM-DD strings."""
    today = datetime.now(eastern).date()
    schedule = nyse_cal.schedule(start_date=today - timedelta(days=10), end_date=today)
    business_days = schedule.index[-5:].strftime("%Y-%m-%d").tolist()
    return business_days

def count_day_trades():
    """Count day trades in the last 5 business days."""
    session = SessionLocal()
    try:
        business_days = get_last_5_business_days()
        trades = session.query(TradeHistory).filter(TradeHistory.date.in_(business_days)).all()
        day_trades = 0
        trades_by_symbol_date = {}
        for trade in trades:
            key = (trade.symbols, trade.date)
            if key not in trades_by_symbol_date:
                trades_by_symbol_date[key] = []
            trades_by_symbol_date[key].append(trade.action)
        for key, actions in trades_by_symbol_date.items():
            if "buy" in actions and "sell" in actions:
                day_trades += 1
        return day_trades
    except Exception as e:
        logging.error(f"Error counting day trades: {e}")
        return 0
    finally:
        session.close()

def robot_can_run():
    """
    Determines if trading actions (buy/sell) are allowed:
    - Fractional buys only during regular market hours
    - Whole shares allowed in pre-market or post-market
    """
    now = datetime.now(eastern)
    schedule = nyse_cal.schedule(start_date=now.date(), end_date=now.date())
    if schedule.empty:
        return False, "Market closed today"

    market_open = schedule.iloc[0]['market_open'].tz_convert(eastern)
    market_close = schedule.iloc[0]['market_close'].tz_convert(eastern)
    pre_market_open = market_open - timedelta(hours=2)
    post_market_close = market_close + timedelta(hours=4)

    if market_open <= now <= market_close:
        return True, "Market open - trading allowed for all shares"
    if pre_market_open <= now <= post_market_close and not FRACTIONAL_BUY_ORDERS:
        return True, "Extended hours - trading allowed for whole shares"
    else:
        return False, f"Outside extended hours ({pre_market_open.strftime('%I:%M %p')} - {post_market_close.strftime('%I:%M %p')})"

def fetch_access_token_and_account_id():
    """Fetch a new access token and BROKERAGE account ID."""
    global secret, access_token, account_id, HEADERS, last_token_fetch_time
    try:
        secret = os.getenv("YOUR_SECRET_KEY")
        if not secret:
            raise ValueError("YOUR_SECRET_KEY environment variable not set")

        # Authorization
        url = "https://api.public.com/userapiauthservice/personal/access-tokens"
        headers = {
            "Content-Type": "application/json"
        }
        request_body = {
            "validityInMinutes": 1440,
            "secret": secret
        }
        response = requests.post(url, headers=headers, json=request_body, timeout=10)
        response.raise_for_status()
        access_token = response.json().get("accessToken")
        if not access_token:
            raise ValueError("No access token returned")

        # Account Information
        url = f"{BASE_URL}/trading/account"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Filter for BROKERAGE account
        brokerage_account = next((acc for acc in data["accounts"] if acc.get("accountType") == "BROKERAGE"), None)
        if not brokerage_account:
            raise ValueError("No BROKERAGE account found")
        account_id = brokerage_account["accountId"]

        # Update HEADERS
        HEADERS = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        last_token_fetch_time = datetime.now()
        logging.info(f"Successfully fetched access token and BROKERAGE account ID: {account_id}")
        return True
    except HTTPError as e:
        logging.error(f"HTTP error fetching token or account ID: {e}, Response: {response.text if 'response' in locals() else 'No response'}")
        time.sleep(30)
        return False
    except (ConnectionError, Timeout) as e:
        logging.error(f"Network error fetching token or account ID: {e}")
        time.sleep(30)
        return False
    except Exception as e:
        logging.error(f"Unexpected error fetching token or account ID: {e}")
        time.sleep(30)
        return False

def client_get_account():
    """Fetch account details for BROKERAGE account."""
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId available")
            return {'equity': 0.0, 'buying_power_cash': 0.0, 'cash_only_buying_power': 0.0, 'cash_on_hand': 0.0, 'accountId': None, 'raw': {}}
        
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
            'accountId': account.get('accountId', account_id),
            'raw': account
        }
    except (HTTPError, ConnectionError, Timeout) as e:
        logging.error(f"Account fetch error for BROKERAGE account {account_id}: {e}")
        return {'equity': 0.0, 'buying_power_cash': 0.0, 'cash_only_buying_power': 0.0, 'cash_on_hand': 0.0, 'accountId': account_id, 'raw': {}}
    except Exception as e:
        logging.error(f"Unexpected error fetching BROKERAGE account {account_id}: {e}")
        return {'equity': 0.0, 'buying_power_cash': 0.0, 'cash_only_buying_power': 0.0, 'cash_on_hand': 0.0, 'accountId': account_id, 'raw': {}}

def client_list_positions():
    """Fetch current positions for BROKERAGE account."""
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId available")
            return []

        resp = requests.get(f"{BASE_URL}/trading/{account_id}/portfolio/v2", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        logging.info(f"Portfolio data for BROKERAGE account {account_id}: {data}")
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
                out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'purchase_date': date_str})
        return out
    except (HTTPError, ConnectionError, Timeout) as e:
        logging.error(f"Positions fetch error for BROKERAGE account {account_id}: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error fetching positions for BROKERAGE account {account_id}: {e}")
        return []

def client_place_order(symbol, qty, side, price=None):
    """Place a buy or sell order for BROKERAGE account."""
    try:
        if not account_id:
            logging.error("No BROKERAGE accountId available")
            return None
        
        order_type = "market" if price is None else "limit"
        payload = {
            "symbol": symbol,
            "quantity": qty,
            "side": side,
            "type": order_type,
            "time_in_force": "day",
            "accountId": account_id
        }
        if price:
            payload["price"] = round(price, 2)

        resp = requests.post(f"{BASE_URL}/v1/orders", json=payload, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        order = resp.json()
        logging.info(f"Order placed for BROKERAGE account {account_id}: {side} {qty} of {symbol} at {order_type} price")
        return order
    except (HTTPError, ConnectionError, Timeout) as e:
        logging.error(f"Order placement error for {symbol} on BROKERAGE account {account_id}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error placing order for {symbol} on BROKERAGE account {account_id}: {e}")
        return None

@sleep_and_retry
@limits(calls=60, period=60)
def client_get_quote(symbol):
    """Fetch latest quote using yfinance, limited to 60 calls per minute."""
    try:
        yf_symbol = symbol.replace('.', '-')  # Adjust for yfinance compatibility
        ticker = yf.Ticker(yf_symbol)
        data = ticker.history(period="1d", interval="1m", prepost=True)
        if data.empty:
            logging.error(f"No price data for {yf_symbol} from yfinance")
            return None
        price = round(float(data['Close'].iloc[-1]), 2)
        logging.info(f"Fetched price ${price:.2f} for {yf_symbol} from yfinance")
        return price
    except Exception as e:
        logging.error(f"Quote fetch error for {symbol}: {e}")
        return None

def calculate_technical_indicators(symbols, lookback_days=90):
    """Calculate technical indicators using yfinance data."""
    print(f"Calculating technical indicators for {symbols} over {lookback_days} days...")
    yf_symbol = symbols.replace('.', '-')  # Adjust for yfinance compatibility
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period=f'{lookback_days}d')
    if historical_data.empty:
        print(f"No historical data for {yf_symbol} from yfinance")
        logging.error(f"No historical data for {yf_symbol} from yfinance")
        return historical_data
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
    print(f"Technical indicators calculated for {yf_symbol}.")
    return historical_data

def get_average_true_range(symbols):
    """Calculate the Average True Range (ATR) using yfinance."""
    print(f"Calculating ATR for {symbols}...")
    yf_symbol = symbols.replace('.', '-')  # Adjust for yfinance compatibility
    ticker = yf.Ticker(yf_symbol)
    data = ticker.history(period='30d')
    if data.empty:
        print(f"No data for {yf_symbol} from yfinance for ATR calculation")
        logging.error(f"No data for {yf_symbol} from yfinance for ATR calculation")
        return None
    try:
        atr = talib.ATR(data['High'].values, data['Low'].values, data['Close'].values, timeperiod=22)
        atr_value = round(atr[-1], 2)
        print(f"ATR for {yf_symbol}: {atr_value:.2f}")
        return atr_value
    except Exception as e:
        logging.error(f"Error calculating ATR for {yf_symbol}: {e}")
        print(f"Error calculating ATR for {yf_symbol}: {e}")
        return None

def is_in_uptrend(symbols_to_buy):
    """Check if stock is in uptrend using yfinance (above 200-day SMA)."""
    print(f"Checking if {symbols_to_buy} is in uptrend (above 200-day SMA)...")
    yf_symbol = symbols_to_buy.replace('.', '-')  # Adjust for yfinance compatibility
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period='200d')
    if historical_data.empty or len(historical_data) < 200:
        print(f"Insufficient data for 200-day SMA for {yf_symbol} from yfinance")
        logging.error(f"Insufficient data for 200-day SMA for {yf_symbol} from yfinance")
        return False
    sma_200 = round(talib.SMA(historical_data['Close'].values, timeperiod=200)[-1], 2)
    current_price = client_get_quote(symbols_to_buy)
    in_uptrend = current_price > sma_200 if current_price else False
    print(
        f"{yf_symbol} {'is' if in_uptrend else 'is not'} in uptrend (Current: {current_price:.2f}, SMA200: {sma_200:.2f})")
    return in_uptrend

def get_daily_rsi(symbols_to_buy):
    """Calculate daily RSI using yfinance."""
    print(f"Calculating daily RSI for {symbols_to_buy}...")
    yf_symbol = symbols_to_buy.replace('.', '-')  # Adjust for yfinance compatibility
    stock_data = yf.Ticker(yf_symbol)
    historical_data = stock_data.history(period='30d', interval='1d')
    if historical_data.empty:
        print(f"No daily data for {yf_symbol} from yfinance")
        logging.error(f"No daily data for {yf_symbol} from yfinance")
        return None
    rsi = talib.RSI(historical_data['Close'], timeperiod=14)[-1]
    rsi_value = round(rsi, 2) if not np.isnan(rsi) else None
    print(f"Daily RSI for {yf_symbol}: {rsi_value}")
    return rsi_value

def get_last_price_within_past_5_minutes(symbols_to_buy_list):
    """Fetch last price within past 5 minutes using yfinance."""
    print("Fetching last prices within past 5 minutes for symbols...")
    results = {}
    eastern = pytz.timezone('US/Eastern')
    current_datetime = datetime.now(eastern)
    end_time = current_datetime
    start_time = end_time - timedelta(minutes=5)

    for symbols_to_buy in symbols_to_buy_list:
        print(f"Fetching 5-minute price data for {symbols_to_buy}...")
        try:
            yf_symbol = symbols_to_buy.replace('.', '-')  # Adjust for yfinance compatibility
            data = yf.download(yf_symbol, start=start_time, end=end_time, interval='1m', prepost=True, auto_adjust=False)
            if not data.empty:
                last_price = round(float(data['Close'].iloc[-1]), 2)
                results[symbols_to_buy] = last_price
                print(f"Last price for {yf_symbol} within 5 minutes: ${last_price:.2f}")
                logging.info(f"Last price for {yf_symbol} within 5 minutes: ${last_price:.2f}")
            else:
                results[symbols_to_buy] = None
                print(f"No price data found for {yf_symbol} within past 5 minutes from yfinance")
                logging.error(f"No price data found for {yf_symbol} within past 5 minutes from yfinance")
        except Exception as e:
            print(f"Error fetching data for {yf_symbol} from yfinance: {e}")
            logging.error(f"Error fetching data for {yf_symbol} from yfinance: {e}")
            results[symbols_to_buy] = None

    return results

def print_database():
    """Print trade history and positions from the database, including current price and % change."""
    if not PRINT_DB_TRADES:
        return

    session = SessionLocal()
    try:
        print("\nTrade History In This Robot's Database:")
        print("\nStock | Buy or Sell | Quantity | Avg. Price | Date")
        print("---------------------------------------------------")
        trades = session.query(TradeHistory).all()
        if not trades:
            print("No trade history found.")
        for record in trades:
            print(f"{record.symbols} | {record.action.capitalize()} | {record.quantity:.4f} | ${record.price:.2f} | {record.date}")

        print("\n---------------------------------------------------")
        print("Positions in the Database To Sell On or After the Date Shown:")
        print("\nStock | Quantity | Avg. Price | Date | Current Price | % Change")
        print("---------------------------------------------------")
        positions = session.query(Position).all()
        if not positions:
            print("No positions held.")
        for record in positions:
            current_price = client_get_quote(record.symbols)
            percentage_change = round(((current_price - record.avg_price) / record.avg_price * 100), 2) if current_price and record.avg_price else 0
            color = GREEN if percentage_change >= 0 else RED
            print(f"{record.symbols} | {record.quantity:.4f} | ${record.avg_price:.2f} | {record.purchase_date} | {color}${current_price:.2f}{RESET} | {color}{percentage_change:.2f}%{RESET}")
        print("\n")
    except Exception as e:
        logging.error(f"Error printing database: {e}")
        print(f"Error printing database: {e}")
    finally:
        session.close()

def buy_stocks(symbols):
    print("Starting buy_stocks function...")
    global price_history, last_stored
    if not symbols:
        print("No symbols to buy.")
        logging.info("No symbols to buy.")
        return
    symbols_to_remove = []

    # Get total equity for risk calculations
    print("Fetching account equity for risk calculations...")
    acc = client_get_account()
    total_equity = acc['equity']
    print(f"Total account equity: ${total_equity:.2f}")

    # Track open positions for portfolio risk cap (max 98% equity in open positions)
    print("Checking current portfolio exposure...")
    positions = client_list_positions()
    current_exposure = 0.0
    for p in positions:
        cur_price = client_get_quote(p['symbol']) or p['avg_price']
        current_exposure += p['qty'] * cur_price
    current_exposure = round(current_exposure, 2)
    max_new_exposure = round(total_equity * 0.98 - current_exposure, 2)
    if max_new_exposure <= 0:
        print("Portfolio exposure limit reached. No new buys.")
        logging.info("Portfolio exposure limit reached. No new buys.")
        return
    print(f"Current exposure: ${current_exposure:.2f}, Max new exposure: ${max_new_exposure:.2f}")

    # Track processed symbols for dynamic allocation
    processed_symbols = 0
    valid_symbols = []

    # First pass: Filter valid symbols to avoid wasting allocations
    print("Filtering valid symbols for buying...")
    for sym in symbols:
        current_price = client_get_quote(sym)
        if current_price is None:
            print(f"No valid price data for {sym} from yfinance. Skipping.")
            logging.info(f"No valid price data for {sym} from yfinance.")
            continue
        historical_data = calculate_technical_indicators(sym, lookback_days=5)
        if historical_data.empty:
            print(f"No historical data for {sym} from yfinance. Skipping.")
            logging.info(f"No historical data for {sym} from yfinance.")
            continue
        valid_symbols.append(sym)
    print(f"Valid symbols to process: {valid_symbols}")

    # Check if there are valid symbols
    if not valid_symbols:
        print("No valid symbols to buy after filtering.")
        logging.info("No valid symbols to buy after filtering.")
        return

    today_str = datetime.now(eastern).strftime("%Y-%m-%d")
    # Process each valid symbol
    for sym in valid_symbols:
        print(f"Processing {sym}...")
        processed_symbols += 1
        current_datetime = datetime.now(eastern)
        current_time_str = current_datetime.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")

        # Fetch current data
        current_price = client_get_quote(sym)
        if current_price is None:
            print(f"No valid price data for {sym} from yfinance.")
            logging.info(f"No valid price data for {sym} from yfinance.")
            continue

        # Update price history for the symbol at specified intervals
        current_timestamp = time.time()
        if sym not in price_history:
            price_history[sym] = {interval: [] for interval in interval_map}
            last_stored[sym] = {interval: 0 for interval in interval_map}
        for interval, delta in interval_map.items():
            if current_timestamp - last_stored[sym][interval] >= delta:
                price_history[sym][interval].append(current_price)
                last_stored[sym][interval] = current_timestamp
                print(f"Stored price ${current_price:.2f} for {sym} at {interval} interval.")
                logging.info(f"Stored price ${current_price:.2f} for {sym} at {interval} interval.")

        # Get historical data for volume, RSI, and candlesticks
        yf_symbol = sym.replace('.', '-')  # Adjust for yfinance compatibility
        print(f"Fetching 20-day historical data for {yf_symbol}...")
        df = yf.Ticker(yf_symbol).history(period="20d")
        if df.empty or len(df) < 3:
            print(f"Insufficient historical data for {sym} from yfinance (rows: {len(df)}). Skipping.")
            logging.info(f"Insufficient historical data for {sym} from yfinance (rows: {len(df)}).")
            continue

        # --- Score calculation ---
        score = 0
        close = df['Close'].values
        open_ = df['Open'].values
        high = df['High'].values
        low = df['Low'].values

        # Candlestick bullish reversal patterns
        bullish_patterns = [
            talib.CDLHAMMER, talib.CDLHANGINGMAN, talib.CDLENGULFING,
            talib.CDLPIERCING, talib.CDLMORNINGSTAR, talib.CDLINVERTEDHAMMER,
            talib.CDLDRAGONFLYDOJI
        ]
        for f in bullish_patterns:
            res = f(open_, high, low, close)
            if res[-1] > 0:
                score += 1
                break

        # RSI decrease
        rsi = talib.RSI(close)
        if rsi[-1] < 50:
            score += 1

        # Price decrease 0.3%
        if close[-1] <= close[-2] * 0.997:
            score += 1

        if score < 3:
            print(f"{yf_symbol}: Score too low ({score} < 3). Skipping.")
            logging.info(f"{yf_symbol}: Score too low ({score} < 3). Skipping.")
            continue

        # Calculate volume decrease
        print(f"Calculating volume metrics for {sym}...")
        recent_avg_volume = df['Volume'].iloc[-5:].mean() if len(df) >= 5 else 0
        prior_avg_volume = df['Volume'].iloc[-10:-5].mean() if len(df) >= 10 else recent_avg_volume
        volume_decrease = recent_avg_volume < prior_avg_volume if len(df) >= 10 else False
        current_volume = df['Volume'].iloc[-1]
        print(
            f"{yf_symbol}: Recent avg volume = {recent_avg_volume:.0f}, Prior avg volume = {prior_avg_volume:.0f}, Volume decrease = {volume_decrease}")

        # Calculate RSI decrease
        print(f"Calculating RSI metrics for {sym}...")
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
        print(
            f"{yf_symbol}: Latest RSI = {latest_rsi:.2f}, Recent avg RSI = {recent_avg_rsi:.2f}, Prior avg RSI = {prior_avg_rsi:.2f}, RSI decrease = {rsi_decrease}")

        # Calculate MACD
        print(f"Calculating MACD for {sym}...")
        short_window = 12
        long_window = 26
        signal_window = 9
        macd, macd_signal, _ = talib.MACD(close_prices, fastperiod=short_window, slowperiod=long_window,
                                          signalperiod=signal_window)
        latest_macd = round(macd[-1], 2) if len(macd) > 0 else None
        latest_macd_signal = round(macd_signal[-1], 2) if len(macd_signal) > 0 else None
        macd_above_signal = latest_macd > latest_macd_signal if latest_macd is not None else False
        print(
            f"{yf_symbol}: MACD = {latest_macd:.2f}, Signal = {latest_macd_signal:.2f}, MACD above signal = {macd_above_signal}")

        # Check price increase (for logging)
        previous_price = current_price  # Simplified, as no previous_prices dict
        price_increase = current_price > previous_price * 1.005
        print(
            f"{yf_symbol}: Price increase check: Current = ${current_price:.2f}, Previous = ${previous_price:.2f}, Increase = {price_increase}")

        # Check price drop
        print(f"Checking price drop for {sym}...")
        last_prices = get_last_price_within_past_5_minutes([sym])
        last_price = last_prices.get(sym)
        if last_price is None:
            try:
                df_last = yf.Ticker(yf_symbol).history(period="1d")
                last_price = round(float(df_last['Close'].iloc[-1]), 2)
                print(f"No price found for {yf_symbol} in past 5 minutes from yfinance. Using last closing price: ${last_price:.2f}")
                logging.info(f"No price found for {yf_symbol} in past 5 minutes from yfinance. Using last closing price: ${last_price:.2f}")
            except Exception as e:
                print(f"Error fetching last closing price for {yf_symbol} from yfinance: {e}")
                logging.error(f"Error fetching last closing price for {yf_symbol} from yfinance: {e}")
                continue

        price_decline_threshold = round(last_price * (1 - 0.002), 2)
        price_decline = current_price <= price_decline_threshold
        print(
            f"{yf_symbol}: Price decline check: Current = ${current_price:.2f}, Threshold = ${price_decline_threshold:.2f}, Decline = {price_decline}")

        # Calculate short-term price trend
        short_term_trend = None
        if sym in price_history and '5min' in price_history[sym] and len(price_history[sym]['5min']) >= 2:
            recent_prices = price_history[sym]['5min'][-2:]
            short_term_trend = 'up' if recent_prices[-1] > recent_prices[-2] else 'down'
            print(f"{yf_symbol}: Short-term price trend (5min): {short_term_trend}")
            logging.info(f"{yf_symbol}: Short-term price trend (5min): {short_term_trend}")

        # Detect bullish reversal candlestick patterns
        print(f"Checking for bullish reversal patterns in {sym}...")
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
                    'Hammer': talib.CDLHAMMER(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1],
                                              close_prices[:i + 1])[i] != 0,
                    'Bullish Engulfing':
                        talib.CDLENGULFING(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1],
                                           close_prices[:i + 1])[i] > 0,
                    'Morning Star': talib.CDLMORNINGSTAR(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1],
                                                         close_prices[:i + 1])[i] != 0,
                    'Piercing Line': talib.CDLPIERCING(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1],
                                                       close_prices[:i + 1])[i] != 0,
                    'Three White Soldiers':
                        talib.CDL3WHITESOLDIERS(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1],
                                                close_prices[:i + 1])[i] != 0,
                    'Dragonfly Doji':
                        talib.CDLDRAGONFLYDOJI(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1],
                                               close_prices[:i + 1])[i] != 0,
                    'Inverted Hammer':
                        talib.CDLINVERTEDHAMMER(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1],
                                                close_prices[:i + 1])[i] != 0,
                    'Tweezer Bottom': talib.CDLMATCHINGLOW(open_prices[:i + 1], high_prices[:i + 1], low_prices[:i + 1],
                                                           close_prices[:i + 1])[i] != 0,
                }
                current_detected = [name for name, detected in patterns.items() if detected]
                if current_detected:
                    bullish_reversal_detected = True
                    detected_patterns = current_detected
                    reversal_candle_index = i
                    break
            except IndexError as e:
                print(f"IndexError in candlestick pattern detection for {yf_symbol}: {e}")
                logging.error(f"IndexError in candlestick pattern detection for {yf_symbol}: {e}")
                continue

        if detected_patterns:
            print(
                f"{yf_symbol}: Detected bullish reversal patterns at candle {reversal_candle_index}: {', '.join(detected_patterns)}")
            logging.info(
                f"{yf_symbol}: Detected bullish reversal patterns at candle {reversal_candle_index}: {', '.join(detected_patterns)}")
            if sym in price_history:
                for interval, prices in price_history[sym].items():
                    if prices:
                        print(f"{yf_symbol}: Price history at {interval}: {[round(p, 2) for p in prices[-5:]]}")
                        logging.info(f"{yf_symbol}: Price history at {interval}: {[round(p, 2) for p in prices[-5:]]}")
        if price_decline:
            print(
                f"{yf_symbol}: Price decline >= 0.2% detected (Current price = ${current_price:.2f}, Threshold = ${price_decline_threshold:.2f})")
            logging.info(
                f"{yf_symbol}: Price decline >= 0.2% detected (Current price = ${current_price:.2f}, Threshold = ${price_decline_threshold:.2f})")
        if volume_decrease:
            print(
                f"{yf_symbol}: Volume decrease detected (Recent avg = {recent_avg_volume:.0f}, Prior avg = {prior_avg_volume:.0f})")
            logging.info(
                f"{yf_symbol}: Volume decrease detected (Recent avg = {recent_avg_volume:.0f}, Prior avg = {prior_avg_volume:.0f})")
        if rsi_decrease:
            print(
                f"{yf_symbol}: RSI decrease detected (Recent avg = {recent_avg_rsi:.2f}, Prior avg = {prior_avg_rsi:.2f})")
            logging.info(
                f"{yf_symbol}: RSI decrease detected (Recent avg = {recent_avg_rsi:.2f}, Prior avg = {prior_avg_rsi:.2f})")

        # Add trend filter
        if not is_in_uptrend(sym):
            print(f"{yf_symbol}: Not in uptrend (below 200-day SMA). Skipping.")
            logging.info(f"{yf_symbol}: Not in uptrend. Skipping.")
            continue

        # Add multi-timeframe confirmation
        daily_rsi = get_daily_rsi(sym)
        if daily_rsi is None or daily_rsi > 50:
            print(f"{yf_symbol}: Daily RSI not oversold ({daily_rsi}). Skipping.")
            logging.info(f"{yf_symbol}: Daily RSI not oversold ({daily_rsi}). Skipping.")
            continue

        # Pattern-specific buy conditions with scoring
        buy_conditions_met = False
        specific_reason = ""
        if bullish_reversal_detected:
            score += 2
            price_stable = True
            if sym in price_history and '5min' in price_history[sym] and len(price_history[sym]['5min']) >= 2:
                recent_prices = price_history[sym]['5min'][-2:]
                price_stable = abs(recent_prices[-1] - recent_prices[-2]) / recent_prices[-2] < 0.005
                print(f"{yf_symbol}: Price stability check (5min): {price_stable}")
                logging.info(f"{yf_symbol}: Price stability check (5min): {price_stable}")
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

            if score >= 4:
                buy_conditions_met = True
                specific_reason = f"Score: {score}, patterns: {', '.join(detected_patterns)}"

        if not buy_conditions_met:
            print(f"{yf_symbol}: Buy score too low ({score} < 4). Skipping.")
            logging.info(f"{yf_symbol}: Buy score too low ({score} < 4). Skipping.")
            continue

        # Determine position sizing
        filled_qty = 0
        filled_price = current_price
        if ALL_BUY_ORDERS_ARE_1_DOLLAR:
            total_cost_for_qty = 1.00
            qty = round(total_cost_for_qty / current_price, 4)
            print(f"{yf_symbol}: Using $1.00 fractional share order mode. Qty = {qty:.4f}")
        else:
            # Volatility-based position sizing
            print(f"Calculating position size for {sym}...")
            atr = get_average_true_range(sym)
            if atr is None:
                print(f"No ATR for {yf_symbol} from yfinance. Skipping.")
                continue
            stop_loss_distance = round(2 * atr, 2)
            risk_per_share = stop_loss_distance
            risk_amount = round(0.01 * total_equity, 2)
            qty = risk_amount / risk_per_share if risk_per_share > 0 else 0
            total_cost_for_qty = qty * current_price

            # Cap by available cash and portfolio exposure
            cash = acc['buying_power_cash']
            print(f"Cash available for {yf_symbol}: ${cash:.2f}")
            total_cost_for_qty = round(min(total_cost_for_qty, cash - 1.00, max_new_exposure), 2)
            if total_cost_for_qty < 1.00:
                print(f"Insufficient risk-adjusted allocation for {yf_symbol}.")
                continue
            qty = round(total_cost_for_qty / current_price, 4)

            # Estimate slippage
            estimated_slippage = round(total_cost_for_qty * 0.001, 2)
            total_cost_for_qty = round(total_cost_for_qty - estimated_slippage, 2)
            qty = round(total_cost_for_qty / current_price, 4)
            print(f"{yf_symbol}: Adjusted for slippage (0.1%): Notional = ${total_cost_for_qty:.2f}, Qty = {qty:.4f}")

        # Unified cash checks
        cash = acc['buying_power_cash']
        if total_cost_for_qty < 1.00:
            print(f"Order amount for {yf_symbol} is ${total_cost_for_qty:.2f}, below minimum $1.00")
            logging.info(f"{current_time_str} Did not buy {yf_symbol} due to order amount below $1.00")
            continue
        if cash < total_cost_for_qty + 1.00:
            print(
                f"Insufficient cash for {yf_symbol}. Available: ${cash:.2f}, Required: ${total_cost_for_qty:.2f} + $1.00 minimum")
            logging.info(f"{current_time_str} Did not buy {yf_symbol} due to insufficient cash")
            continue

        if buy_conditions_met:
            api_symbols = sym
            reason = f"bullish reversal ({', '.join(detected_patterns)}), {specific_reason}"
            print(f"Submitting buy order for {api_symbols}...")
            try:
                # Ensure notional value is rounded to 2 decimal places
                total_cost_for_qty = round(total_cost_for_qty, 2)
                order = client_place_order(api_symbols, qty, "buy")
                if order:
                    print(
                        f"{current_time_str}, Submitted buy order for {qty:.4f} shares of {api_symbols} at ${current_price:.2f} (notional: ${total_cost_for_qty:.2f}) due to {reason}")
                    logging.info(
                        f"{current_time_str} Submitted buy {qty:.4f} shares of {api_symbols} due to {reason}. RSI Decrease={rsi_decrease}, Volume Decrease={volume_decrease}, Bullish Reversal={bullish_reversal_detected}, Price Decline >= 0.2%={price_decline}")

                    # Assume filled for simplicity, as no order status polling in current setup
                    filled_qty = qty
                    filled_price = round(current_price, 2)
                    actual_cost = round(filled_qty * filled_price, 2)
                    print(
                        f"Order filled for {filled_qty:.4f} shares of {api_symbols} at ${filled_price:.2f}, actual cost: ${actual_cost:.2f}")
                    logging.info(
                        f"Order filled for {filled_qty:.4f} shares of {api_symbols}, actual cost: ${actual_cost:.2f}")

                    # Skip trailing stop as not implemented for Public.com

                    # Record CSV
                    with open(CSV_FILENAME, 'a', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                        writer.writerow({
                            'Date': current_time_str,
                            'Buy': 'Buy',
                            'Sell': '',
                            'Quantity': filled_qty,
                            'Symbol': api_symbols,
                            'Price Per Share': filled_price
                        })

                    symbols_to_remove.append((api_symbols, filled_price, today_str))

            except Exception as e:
                print(f"Error submitting buy order for {api_symbols}: {e}")
                logging.error(f"Error submitting buy order for {api_symbols}: {e}")
                continue

        else:
            print(
                f"{yf_symbol}: Conditions not met for any detected patterns. Bullish Reversal = {bullish_reversal_detected}, Volume Decrease = {volume_decrease}, RSI Decrease = {rsi_decrease}, Price Decline >= 0.2% = {price_decline}, Price Stable = {price_stable}")
            logging.info(
                f"{current_time_str} Did not buy {yf_symbol} due to Bullish Reversal = {bullish_reversal_detected}, Volume Decrease = {volume_decrease}, RSI Decrease = {rsi_decrease}, Price Decline >= 0.2% = {price_decline}, Price Stable = {price_stable}")

        time.sleep(0.8)

    try:
        session = SessionLocal()
        print("Updating database with buy transactions...")
        for sym, price, date in symbols_to_remove:
            # Record in DB
            trade = TradeHistory(
                symbols=sym,
                action="buy",
                quantity=filled_qty,
                price=price,
                date=date
            )
            session.add(trade)
            db_position = Position(
                symbols=sym,
                quantity=filled_qty,
                avg_price=price,
                purchase_date=date
            )
            session.add(db_position)
        session.commit()
        print("Database updated successfully.")
        session.close()
    except SQLAlchemyError as e:
        if session:
            session.rollback()
        logging.error(f"DB error saving trade for {sym} on BROKERAGE account {account_id}: {e}")

def sell_stocks():
    account = client_get_account()
    positions = client_list_positions()
    today_str = datetime.now(eastern).strftime("%Y-%m-%d")
    day_trades = count_day_trades()

    for p in positions:
        # Skip if bought today and day trades >= 3
        if p['purchase_date'] == today_str and day_trades >= 3:
            logging.info(f"Skipping sell of {p['symbol']} (bought today, day trades={day_trades})")
            print(f"  {p['symbol']}: Skipped sell (bought today, day trades={day_trades})")
            continue
        # Skip if bought today
        if p['purchase_date'] == today_str:
            continue
        cur_price = client_get_quote(p['symbol'])
        if cur_price is None:
            print(f"No price data for {p['symbol']} from yfinance. Skipping sell.")
            logging.info(f"No price data for {p['symbol']} from yfinance. Skipping sell.")
            continue

        # Sell if profit >= 0.5%
        if cur_price >= 1.005 * p['avg_price']:
            order = client_place_order(p['symbol'], p['qty'], "sell")
            if order:
                logging.info(f"Selling {p['qty']} of {p['symbol']} at ${cur_price:.2f} for BROKERAGE account {account_id}")
                with open(CSV_FILENAME, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                    writer.writerow({
                        'Date': today_str,
                        'Buy': '',
                        'Sell': 'Sell',
                        'Quantity': p['qty'],
                        'Symbol': p['symbol'],
                        'Price Per Share': cur_price
                    })

                # Update DB
                session = SessionLocal()
                try:
                    trade = TradeHistory(
                        symbols=p['symbol'],
                        action="sell",
                        quantity=p['qty'],
                        price=cur_price,
                        date=today_str
                    )
                    session.add(trade)
                    session.query(Position).filter(Position.symbols == p['symbol']).delete()
                    session.commit()
                except SQLAlchemyError as e:
                    logging.error(f"DB error saving trade for {p['symbol']} on BROKERAGE account {account_id}: {e}")
                    session.rollback()
                finally:
                    session.close()

def trading_robot(interval=30):
    global secret, access_token, account_id, HEADERS, last_token_fetch_time
    symbols = load_symbols_from_file()
    if not symbols:
        print("No symbols loaded.")
        return

    while True:
        try:
            # Fetch new access token and BROKERAGE account ID
            current_time = datetime.now()
            if (access_token is None or 
                last_token_fetch_time is None or 
                (current_time - last_token_fetch_time).total_seconds() >= 86400):
                if not fetch_access_token_and_account_id():
                    logging.error("Failed to fetch access token or BROKERAGE account ID, retrying in main loop")
                    time.sleep(30)
                    continue

            # Print today's date
            today_str = datetime.now(eastern).strftime("%Y-%m-%d")
            print(f"Today's Date: {today_str}")

            # Print current time
            current_time = datetime.now(eastern)
            print(f"Current Time: {current_time.strftime('%I:%M:%S %p %Z')}")

            # Check if time is past 8:00 PM EDT
            stop_time = current_time.replace(hour=20, minute=0, second=0, microsecond=0)
            if current_time >= stop_time:
                print("Stopping bot: Current time is at or past 8:00 PM EDT")
                logging.info("Bot stopped at 8:00 PM EDT")
                break

            can_trade, msg = robot_can_run()
            print(msg)

            # Print account summary
            acc = client_get_account()
            print(f"Equity: ${acc['equity']:.2f} | Account Buying Power Cash: ${acc['buying_power_cash']:.2f} | Cash Only Buying Power: ${acc['cash_only_buying_power']:.2f} | Cash on Hand: ${acc['cash_on_hand']:.2f}")

            # Print day trades
            day_trades = count_day_trades()
            print(f"Day Trades (Last 5 Business Days): {day_trades}")

            # Print owned positions with current price and % change
            positions = client_list_positions()
            print("Owned Positions:")
            if not positions:
                print("  No positions held.")
            for p in positions:
                cur_price = client_get_quote(p['symbol']) or 0
                price_color = GREEN if cur_price > p['avg_price'] else RED if cur_price < p['avg_price'] else RESET
                pct = round((cur_price - p['avg_price']) / p['avg_price'] * 100, 2) if p['avg_price'] else 0
                print(f"  {p['symbol']} | Qty: {p['qty']:.4f} | Avg Price: ${p['avg_price']:.2f} | Current Price: {price_color}${cur_price:.2f}{RESET} | Change: {price_color}{pct:.2f}%{RESET}")

            print("Buy Analysis:")
            if not can_trade:
                print("  Trading not allowed, skipping buy analysis.")
                time.sleep(30)
                continue

            # Sell first
            sell_stocks()
            # Then buy
            buy_stocks(symbols)

            # Print database (trade history and positions)
            if PRINT_DB_TRADES:
                print("\n------------------------------------------------------------------------------------")
                print_database()
                print("------------------------------------------------------------------------------------\n")

            # Print waiting message
            print(f"Waiting 15 seconds to continue...")
            time.sleep(interval)
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(15)

if __name__ == "__main__":
    trading_robot(interval=15)
    
