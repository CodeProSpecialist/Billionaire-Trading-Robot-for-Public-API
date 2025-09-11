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
from requests.exceptions import HTTPError, ConnectionError, Timeout
from ratelimit import limits, sleep_and_retry

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
            return {'equity': 0.0, 'cash': 0.0, 'accountId': None, 'raw': {}}
        
        resp = requests.get(f"{BASE_URL}/v1/accounts/{account_id}", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        account = resp.json()
        print(account)
        return {
            'equity': float(account.get('equity', 0)),
            'cash': float(account.get('cash', 0)),
            'accountId': account.get('accountId', account_id),
            'raw': account
        }
    except (HTTPError, ConnectionError, Timeout) as e:
        logging.error(f"Account fetch error for BROKERAGE account {account_id}: {e}")
        return {'equity': 0.0, 'cash': 0.0, 'accountId': account_id, 'raw': {}}
    except Exception as e:
        logging.error(f"Unexpected error fetching BROKERAGE account {account_id}: {e}")
        return {'equity': 0.0, 'cash': 0.0, 'accountId': account_id, 'raw': {}}

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
            avg = float(p.get('costBasis', {}).get('unitCost', 0))
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
            payload["price"] = price

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
        df = yf.Ticker(symbol.replace('.', '-')).history(period="1d", interval="1m")
        return float(df['Close'].iloc[-1])
    except Exception as e:
        logging.error(f"Quote fetch error for {symbol}: {e}")
        return None

def buy_stocks(symbols):
    account = client_get_account()
    cash = account['cash']
    positions = client_list_positions()
    today_str = datetime.now(eastern).strftime("%Y-%m-%d")

    for sym in symbols:
        current_price = client_get_quote(sym)
        if current_price is None or cash < 3.0:
            print(f"  {sym}: Skipped (No price data or insufficient cash)")
            continue

        # --- Score calculation ---
        score = 0
        df = yf.Ticker(sym.replace('.', '-')).history(period="20d")
        close = df['Close'].values
        open_ = df['Open'].values
        high = df['High'].values
        low = df['Low'].values
        volume = df['Volume'].values

        # Candlestick bullish reversal patterns
        bullish_patterns = [
            talib.CDLHAMMER, talib.CDLHANGINGMAN, talib.CDLENGULFING,
            talib.CDLPIERCING, talib.CDLMORNINGSTAR, talib.CDLINVERTEDHAMMER,
            talib.CDLDRAGONFLYDOJI, talib.CDLBULLISHENGULFING
        ]
        for f in bullish_patterns:
            res = f(open_, high, low, close)
            if res[-1] > 0:
                score += 2
                break

        # RSI oversold bounce
        rsi = talib.RSI(close)
        if len(rsi) >= 2 and rsi[-2] < 30 and rsi[-1] > rsi[-2]:
            score += 1

        # Price decrease 0.3%
        if close[-1] <= close[-2] * 0.997:
            score += 2

        # Low volume (below 10-day SMA)
        if len(volume) >= 10:
            avg_vol = talib.SMA(volume, timeperiod=10)[-1]
            if volume[-1] < avg_vol:
                score += 1

        # Favorable MACD (MACD > signal)
        macd, signal, _ = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        if len(macd) >= 1 and macd[-1] > signal[-1]:
            score += 1

        # Calculate % change from previous close
        price_change_pct = ((close[-1] - close[-2]) / close[-2] * 100) if len(close) >= 2 else 0
        price_color = "\033[92m" if price_change_pct > 0 else "\033[91m" if price_change_pct < 0 else "\033[0m"
        reset = "\033[0m"
        print(f"  {sym}: Score={score}, Price={price_color}${close[-1]:.2f}{reset} ({price_color}{price_change_pct:.2f}%{reset})")

        if score < 4:
            continue

        # Determine buy quantity
        qty = round((cash - 2.0) / current_price, 2) if FRACTIONAL_BUY_ORDERS else int((cash - 2.0) / current_price)
        if ALL_BUY_ORDERS_ARE_1_DOLLAR:
            qty = round(1.0 / current_price, 2) if FRACTIONAL_BUY_ORDERS else 1
        if qty <= 0:
            continue

        # Place buy order
        order = client_place_order(sym, qty, "buy")
        if order:
            logging.info(f"Buying {qty} of {sym} at {current_price} for BROKERAGE account {account_id}")
            cash -= qty * current_price

            # Record CSV
            with open(CSV_FILENAME, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writerow({
                    'Date': today_str,
                    'Buy': 'Buy',
                    'Sell': '',
                    'Quantity': qty,
                    'Symbol': sym,
                    'Price Per Share': round(current_price, 2)
                })

            # Record in DB
            session = SessionLocal()
            try:
                trade = TradeHistory(
                    symbols=sym,
                    action="buy",
                    quantity=qty,
                    price=current_price,
                    date=today_str
                )
                session.add(trade)
                session.commit()
            except SQLAlchemyError as e:
                logging.error(f"DB error saving trade for {sym} on BROKERAGE account {account_id}: {e}")
                session.rollback()
            finally:
                session.close()

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
            continue

        # Sell if profit >= 0.5%
        if cur_price >= 1.005 * p['avg_price']:
            order = client_place_order(p['symbol'], p['qty'], "sell")
            if order:
                logging.info(f"Selling {p['qty']} of {p['symbol']} at {cur_price} for BROKERAGE account {account_id}")
                with open(CSV_FILENAME, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                    writer.writerow({
                        'Date': today_str,
                        'Buy': '',
                        'Sell': 'Sell',
                        'Quantity': p['qty'],
                        'Symbol': p['symbol'],
                        'Price Per Share': round(cur_price, 2)
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
            print(f"Equity: ${acc['equity']:.2f} | Cash: ${acc['cash']:.2f}")

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
                price_color = "\033[92m" if cur_price > p['avg_price'] else "\033[91m" if cur_price < p['avg_price'] else "\033[0m"
                reset = "\033[0m"
                pct = (cur_price - p['avg_price']) / p['avg_price'] * 100 if p['avg_price'] else 0
                print(f"  {p['symbol']} | Qty: {p['qty']} | Avg Price: ${p['avg_price']:.2f} | Current Price: {price_color}${cur_price:.2f}{reset} | Change: {price_color}{pct:.2f}%{reset}")

            print("Buy Analysis:")
            if not can_trade:
                print("  Trading not allowed, skipping buy analysis.")
                time.sleep(30)
                continue

            # Sell first
            sell_stocks()
            # Then buy
            buy_stocks(symbols)

            time.sleep(interval)
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    trading_robot(interval=30)
