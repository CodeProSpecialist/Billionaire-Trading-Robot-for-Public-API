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

# Core API code (must match exactly)
secret = os.getenv("YOUR_SECRET_KEY")

HEADERS = {"Authorization": f"Bearer {secret_key}", "Content-Type": "application/json"}
BASE_URL = "https://api.public.com/userapigateway"

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
    action = Column(String)  # buy or sell
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

def robot_can_run():
    """
    Determines if trading actions (buy/sell) are allowed:
    - Fractional buys only during regular market hours
    - Whole shares allowed in pre-market or post-market
    Returns: (can_trade: bool, message: str)
    """
    now = datetime.now(eastern)
    schedule = nyse_cal.schedule(start_date=now.date(), end_date=now.date())
    if schedule.empty:
        return False, "Market closed today"

    market_open = schedule.iloc[0]['market_open'].tz_convert(eastern)
    market_close = schedule.iloc[0]['market_close'].tz_convert(eastern)
    pre_market_open = market_open - timedelta(hours=2)  # 7:00 AM ET
    post_market_close = market_close + timedelta(hours=4) # 8:00 PM ET

    if market_open <= now <= market_close:
        return True, "Market open - trading allowed for all shares"
    if pre_market_open <= now <= post_market_close and not FRACTIONAL_BUY_ORDERS:
        return True, "Extended hours - trading allowed for whole shares"
    else:
        return False, f"Outside extended hours ({pre_market_open.strftime('%I:%M %p')} - {post_market_close.strftime('%I:%M %p')})"

def client_get_account():
    """
    Fetch account details from Public.com API.
    Adjust endpoint and response parsing based on actual Public.com API documentation.
    """
    try:
        resp = requests.get(f"{BASE_URL}/v1/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        account = data.get('accounts', [{}])[0]
        return {
            'equity': float(account.get('equity', 0)),
            'cash': float(account.get('cash', 0)),
            'accountId': account.get('accountId', ''),  # Extract accountId
            'raw': account
        }
    except (HTTPError, ConnectionError, Timeout) as e:
        logging.error(f"Account fetch error: {e}")
        return {'equity': 0.0, 'cash': 0.0, 'accountId': '', 'raw': {}}
    except Exception as e:
        logging.error(f"Unexpected error fetching account: {e}")
        return {'equity': 0.0, 'cash': 0.0, 'accountId': '', 'raw': {}}

def client_list_positions():
    """
    Fetch current positions from Public.com API using portfolio/v2 endpoint.
    """
    try:
        # Get accountId from client_get_account
        account = client_get_account()
        account_id = account.get('accountId')
        if not account_id:
            logging.error("No accountId found in account data")
            return []

        # Fetch positions from portfolio/v2 endpoint
        resp = requests.get(f"{BASE_URL}/trading/{account_id}/portfolio/v2", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        pos_list = data.get('positions', [])
        out = []
        for p in pos_list:
            sym = p.get('instrument', {}).get('symbol')
            qty = float(p.get('quantity', 0))
            avg = float(p.get('costBasis', {}).get('unitCost', 0))
            # Convert openedAt to YYYY-MM-DD in Eastern Time
            opened_at = p.get('openedAt', datetime.now(eastern).strftime("%Y-%m-%d"))
            try:
                date_str = datetime.fromisoformat(opened_at.replace('Z', '+00:00')).astimezone(eastern).strftime("%Y-%m-%d")
            except ValueError:
                date_str = datetime.now(eastern).strftime("%Y-%m-%d")
            if sym and qty > 0:  # Only include valid positions
                out.append({'symbol': sym, 'qty': qty, 'avg_price': avg, 'purchase_date': date_str})
        return out
    except (HTTPError, ConnectionError, Timeout) as e:
        logging.error(f"Positions fetch error: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error fetching positions: {e}")
        return []

def client_place_order(symbol, qty, side, price=None):
    """
    Place a buy or sell order via Public.com API.
    Adjust endpoint and payload based on actual Public.com API documentation.
    """
    try:
        order_type = "market" if price is None else "limit"
        payload = {
            "symbol": symbol,
            "quantity": qty,
            "side": side,  # 'buy' or 'sell'
            "type": order_type,
            "time_in_force": "day"
        }
        if price:
            payload["price"] = price

        resp = requests.post(f"{BASE_URL}/v1/orders", json=payload, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        order = resp.json()
        logging.info(f"Order placed: {side} {qty} of {symbol} at {order_type} price")
        return order
    except (HTTPError, ConnectionError, Timeout) as e:
        logging.error(f"Order placement error for {symbol}: {e}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error placing order for {symbol}: {e}")
        return None

def client_get_quote(symbol):
    """
    Fetch latest quote using yfinance.
    """
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
        if current_price is None or cash < 3.0:  # leave $2 buffer
            continue

        # --- Score calculation ---
        score = 0
        df = yf.Ticker(sym.replace('.', '-')).history(period="20d")
        close = df['Close'].values
        open_ = df['Open'].values
        high = df['High'].values
        low = df['Low'].values

        # Candlestick bullish reversal patterns
        bullish_patterns = [
            talib.CDLHAMMER, talib.CDLHANGINGMAN, talib.CDLENGULFING,
            talib.CDLPIERCING, talib.CDLMORNINGSTAR, talib.CDLINVERTEDHAMMER,
            talib.CDLDRAGONFLYDOJI, talib.CDLBULLISHENGULFING
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
            logging.info(f"Buying {qty} of {sym} at {current_price}")
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
                logging.error(f"DB error saving trade for {sym}: {e}")
                session.rollback()
            finally:
                session.close()

def sell_stocks():
    account = client_get_account()
    positions = client_list_positions()
    today_str = datetime.now(eastern).strftime("%Y-%m-%d")

    for p in positions:
        # Only sell if bought before today
        if p['purchase_date'] == today_str:
            continue
        cur_price = client_get_quote(p['symbol'])
        if cur_price is None:
            continue

        # Sell if profit >= 0.5%
        if cur_price >= 1.005 * p['avg_price']:
            order = client_place_order(p['symbol'], p['qty'], "sell")
            if order:
                logging.info(f"Selling {p['qty']} of {p['symbol']} at {cur_price}")
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
                    logging.error(f"DB error saving trade for {p['symbol']}: {e}")
                    session.rollback()
                finally:
                    session.close()

def trading_robot(interval=120):
    symbols = load_symbols_from_file()
    if not symbols:
        print("No symbols loaded.")
        return

    while True:
        try:
            # Print current date and time in Eastern Time
            current_time = datetime.now(eastern)
            print(f"Current Time: {current_time.strftime('%Y-%m-%d %I:%M:%S %p %Z')}")

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

            # Print owned positions with color based on current price vs avg price
            positions = client_list_positions()
            print("Owned Positions:")
            if not positions:
                print("  No positions held.")
            for p in positions:
                cur_price = client_get_quote(p['symbol']) or 0
                price_color = "\033[92m" if cur_price > p['avg_price'] else "\033[91m"
                reset = "\033[0m"
                pct = (cur_price - p['avg_price']) / p['avg_price'] * 100 if p['avg_price'] else 0
                print(f"  {p['symbol']} | Qty: {p['qty']} | Avg Price: ${p['avg_price']:.2f} | Current Price: {price_color}${cur_price:.2f}{reset} | Change: {pct:.2f}%")

            if not can_trade:
                time.sleep(300)
                continue

            # Sell first
            sell_stocks()
            # Then buy
            buy_stocks(symbols)

            time.sleep(interval)
        except Exception as e:
            logging.error(f"Main loop error: {e}")
            time.sleep(120)

if __name__ == "__main__":
    trading_robot(interval=120)
