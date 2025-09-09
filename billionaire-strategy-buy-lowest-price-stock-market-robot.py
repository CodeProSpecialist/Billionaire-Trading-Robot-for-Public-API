#!/usr/bin/env python3
"""
Integrated trading bot:
- PublicClient for all trading operations
- yfinance for historical candles & TA indicators
- SQLAlchemy (SQLite WAL + scoped_session) for trade history & positions
- pandas-market-calendars for market hours
- Detailed logging + CSV of trades
- Reads symbols from electricity-or-utility-stocks-to-buy-list.txt
- Auto-restart main loop on error after 2 minutes
"""

import os, time, csv, logging, threading
from uuid import uuid4
from datetime import datetime, timedelta
import pytz
import requests
import numpy as np
import talib
import yfinance as yf
import pandas_market_calendars as mcal
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# ---------------------- Configuration & Environment ---------------------- #
secret_key = os.getenv('PUBLIC_API_ACCESS_TOKEN')
if not secret_key:
    raise ValueError("PUBLIC_API_ACCESS_TOKEN environment variable is not set.")

HEADERS = {"Authorization": f"Bearer {secret_key}", "Content-Type": "application/json"}
BASE_URL = "https://api.public.com/userapigateway"

PRINT_ROBOT_STORED_BUY_AND_SELL_LIST_DATABASE = True
ALL_BUY_ORDERS_ARE_1_DOLLAR = False
eastern = pytz.timezone('US/Eastern')

CSV_FILENAME = 'log-file-of-buy-and-sell-signals.csv'
CSV_FIELDS = ['Date', 'Buy', 'Sell', 'Quantity', 'Symbol', 'Price Per Share']
LOGFILE = 'trading-bot-program-logging-messages.txt'

with open(CSV_FILENAME, 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
    writer.writeheader()

logging.basicConfig(filename=LOGFILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

# ---------------------- SQLAlchemy ---------------------- #
Base = declarative_base()

class TradeHistory(Base):
    __tablename__ = 'trade_history'
    id = Column(Integer, primary_key=True)
    symbols = Column(String, index=True)
    action = Column(String)  # 'buy' or 'sell'
    quantity = Column(Float)
    price = Column(Float)
    date = Column(String)

class Position(Base):
    __tablename__ = 'positions'
    symbols = Column(String, primary_key=True)
    quantity = Column(Float)
    avg_price = Column(Float)
    purchase_date = Column(String)

engine = create_engine('sqlite:///trading_bot.db',
                       connect_args={"check_same_thread": False},
                       pool_pre_ping=True, future=True)
with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
    conn.execute(text("PRAGMA synchronous=NORMAL;"))

Base.metadata.create_all(engine)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False))
def get_db_session():
    return SessionLocal()

# ---------------------- Public API Client ---------------------- #
try:
    from public_invest_api import PublicClient
    try:
        client = PublicClient(api_key=secret_key)
    except TypeError:
        client = PublicClient()
    use_client = True
    logging.info("PublicClient instantiated.")
except Exception as e:
    client = None
    use_client = False
    logging.info(f"PublicClient not available/importable: {e}. Falling back to REST calls.")

_account_id_cache = None
def get_account_id():
    global _account_id_cache
    if _account_id_cache:
        return _account_id_cache
    try:
        if use_client and hasattr(client, 'account'):
            acc = client.account()
            _account_id_cache = acc.get('id') or acc.get('account_id')
            return _account_id_cache
        resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        arr = resp.json()
        if isinstance(arr, list) and arr:
            _account_id_cache = arr[0].get('id')
            return _account_id_cache
    except Exception as e:
        logging.error(f"Error getting account id: {e}")
    return None

def client_get_account():
    try:
        if use_client and hasattr(client, 'account'):
            acc = client.account()
            return {'equity': float(acc.get('equity',0)), 'cash': float(acc.get('cash',0)), 'raw': acc}
        else:
            resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
            resp.raise_for_status()
            a = resp.json()[0] if isinstance(resp.json(), list) else resp.json()
            return {'equity': float(a.get('equity',0)), 'cash': float(a.get('cash',0)), 'raw': a}
    except Exception as e:
        logging.error(f"Error fetching account: {e}")
        return {'equity':0.0, 'cash':0.0, 'raw':{}}

def client_list_positions():
    out=[]
    try:
        if use_client and hasattr(client,'positions'):
            positions = client.positions() or []
            for p in positions:
                sym = p.get('symbol') or p.get('instrument',{}).get('symbol')
                qty = float(p.get('quantity') or p.get('qty') or 0)
                avg = float(p.get('avg_price') or p.get('avg_entry_price') or 0)
                buy_date = datetime.now().date()
                out.append({'symbol':sym,'qty':qty,'avg_price':avg,'buy_date':buy_date})
        else:
            resp = requests.get(f"{BASE_URL}/trading/accounts/positions", headers=HEADERS, timeout=10)
            resp.raise_for_status()
            for p in resp.json().get('positions',[]):
                sym = p.get('symbol')
                qty = float(p.get('quantity',0))
                avg = float(p.get('avg_price',0))
                buy_date = datetime.now().date()
                out.append({'symbol':sym,'qty':qty,'avg_price':avg,'buy_date':buy_date})
    except Exception as e:
        logging.error(f"Error fetching positions: {e}")
    return out

def client_get_quote(symbol):
    try:
        if use_client and hasattr(client,'quote'):
            q = client.quote(symbol)
            price = q.get('last') or q.get('last_price') or q.get('close')
            return float(price) if price else None
        resp = requests.post(f"{BASE_URL}/marketdata/quotes", headers=HEADERS,
                             json={"instruments":[{"symbol":symbol,"type":"EQUITY"}]}, timeout=10)
        resp.raise_for_status()
        quotes = resp.json().get('quotes',[])
        if quotes: return float(quotes[0].get('last') or quotes[0].get('close') or 0)
    except Exception as e:
        logging.error(f"Error fetching quote for {symbol}: {e}")
    return None

def client_place_market_order(symbol, side, quantity):
    try:
        if use_client:
            if hasattr(client,'buy') and side.upper()=='BUY': return client.buy(symbol, quantity)
            if hasattr(client,'sell') and side.upper()=='SELL': return client.sell(symbol, quantity)
            if hasattr(client,'create_order'): return client.create_order(symbol=symbol, side=side.upper(), order_type='market', quantity=str(quantity))
        acct = get_account_id()
        if not acct: return None
        url = f"{BASE_URL}/trading/{acct}/order"
        body = {"orderId":str(uuid4()), "instrument":{"symbol":symbol,"type":"EQUITY"},
                "orderSide":side.upper(),"orderType":"MARKET","expiration":{"timeInForce":"DAY"},
                "quantity":str(quantity)}
        resp = requests.post(url, headers=HEADERS, json=body, timeout=15)
        if resp.status_code in (200,201): return resp.json()
    except Exception as e:
        logging.error(f"Exception placing order: {e}")
    return None

# ---------------------- yfinance helpers ---------------------- #
def yf_symbol(sym): return sym.replace('.', '-')
def calculate_technical_indicators(symbol, lookback_days=5, interval='5m'):
    try:
        symbol = yf_symbol(symbol)
        df = yf.Ticker(symbol).history(period=f'{lookback_days}d', interval=interval, prepost=True)
        return df if df is not None else None
    except Exception as e:
        logging.error(f"yfinance error for {symbol}: {e}")
        return None
def get_average_true_range(symbol):
    df = calculate_technical_indicators(symbol, lookback_days=30, interval='1d')
    if df is None or df.empty: return None
    atr = talib.ATR(df['High'].values, df['Low'].values, df['Close'].values, timeperiod=14)
    return float(atr[-1]) if len(atr)>0 else None
def is_in_uptrend(symbol):
    df = calculate_technical_indicators(symbol, lookback_days=250, interval='1d')
    if df is None or len(df)<200: return False
    sma200 = talib.SMA(df['Close'].values, timeperiod=200)
    return float(df['Close'].iloc[-1]) > float(sma200[-1])
def get_daily_rsi(symbol):
    df = calculate_technical_indicators(symbol, lookback_days=60, interval='1d')
    if df is None or len(df)<14: return None
    rsi = talib.RSI(df['Close'].values, timeperiod=14)
    return float(rsi[-1])

# ---------------------- Price storage ---------------------- #
price_history = {}
last_stored = {}
interval_map = {'5min':5*60, '1min':60}

# ---------------------- File helpers ---------------------- #
SYMBOLS_FILE = "electricity-or-utility-stocks-to-buy-list.txt"
def load_symbols_from_file(file_path=SYMBOLS_FILE):
    symbols=[]
    try:
        with open(file_path,'r') as f:
            for line in f:
                s=line.strip().upper()
                if s: symbols.append(s)
        logging.info(f"Loaded {len(symbols)} symbols")
    except Exception as e:
        logging.error(f"Error loading symbols: {e}")
    return symbols

# ---------------------- Market hours ---------------------- #
nyse_cal = mcal.get_calendar('NYSE')
def market_is_open():
    now=datetime.now(eastern)
    schedule=nyse_cal.schedule(start_date=now.date(),end_date=now.date())
    if schedule.empty: return False
    open_time = schedule.iloc[0]['market_open'].tz_convert(eastern)
    close_time = schedule.iloc[0]['market_close'].tz_convert(eastern)
    return open_time <= now <= close_time

# ---------------------- Trade logging ---------------------- #
def log_trade_csv(date_str, buy, sell, qty, symbol, price):
    with open(CSV_FILENAME,'a',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=CSV_FIELDS)
        writer.writerow({'Date':date_str,'Buy':buy,'Sell':sell,'Quantity':qty,'Symbol':symbol,'Price Per Share':price})

def record_db_trade(action, symbol, qty, price, date_str):
    session=get_db_session()
    try:
        th=TradeHistory(symbols=symbol, action=action, quantity=qty, price=price, date=date_str)
        if action=='buy': session.add(Position(symbols=symbol, quantity=qty, avg_price=price, purchase_date=date_str))
        elif action=='sell': session.query(Position).filter_by(symbols=symbol).delete()
        session.add(th); session.commit()
    except SQLAlchemyError as e:
        logging.error(f"DB error: {e}"); session.rollback()
    finally: session.close()

# ---------------------- Main trading robot ---------------------- #
def trading_robot(interval=60):
    symbols_to_buy_list=load_symbols_from_file()
    if not symbols_to_buy_list: return
    account_id=get_account_id()
    while True:
        try:
            if market_is_open():
                # sell yesterday's positions (simplified)
                db=get_db_session()
                try:
                    yesterday=(datetime.now(eastern)-timedelta(days=1)).strftime("%Y-%m-%d")
                    for p in db.query(Position).all():
                        if p.purchase_date==yesterday:
                            current_price=client_get_quote(p.symbols)
                            if current_price>=float(p.avg_price)*1.005:
                                client_place_market_order(p.symbols,'SELL',p.quantity)
                                now_str=datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
                                log_trade_csv(now_str,'','Sell',p.quantity,p.symbols,current_price)
                                record_db_trade('sell',p.symbols,p.quantity,current_price,datetime.now(eastern).strftime("%Y-%m-%d"))
                finally: db.close()
                # buy pass
                account=client_get_account()
                cash_available=float(account.get('cash',0))
                for sym in symbols_to_buy_list:
                    price=client_get_quote(sym)
                    if price is None: continue
                    atr=get_average_true_range(sym)
                    if atr is None: continue
                    qty=max(0.001, round((cash_available*0.01)/(2*atr),4))
                    if qty*price>cash_available: qty=cash_available/price
                    if qty<=0: continue
                    client_place_market_order(sym,'BUY',qty)
                    nowstamp=datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
                    log_trade_csv(nowstamp,'Buy','',qty,sym,price)
                    record_db_trade('buy',sym,qty,price,datetime.now(eastern).strftime("%Y-%m-%d"))
            time.sleep(interval)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.error(f"Unexpected error: {e}", exc_info=True)
            time.sleep(120)

if __name__=="__main__":
    trading_robot(interval=120)
