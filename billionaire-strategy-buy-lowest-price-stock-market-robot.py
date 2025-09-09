#!/usr/bin/env python3
"""
Complete Trading Bot:
- PublicClient (public_invest_api) for trading operations
- yfinance for historical candles and technical indicators
- SQLAlchemy (SQLite WAL + scoped_session) for trade history & positions
- pandas-market-calendars for market hours
- Detailed logging + CSV of trades
- Reads symbols from electricity-or-utility-stocks-to-buy-list.txt
"""

import os
import time
import json
import csv
import logging
import threading
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

# ------------------------- Configuration & Environment ------------------------- #
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

with open(CSV_FILENAME, mode='w', newline='') as csv_f:
    writer = csv.DictWriter(csv_f, fieldnames=CSV_FIELDS)
    writer.writeheader()

LOGFILE = 'trading-bot-program-logging-messages.txt'
logging.basicConfig(filename=LOGFILE, level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

# ------------------------- DB: SQLAlchemy setup ------------------------- #
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
                       pool_pre_ping=True,
                       future=True)

with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
    conn.execute(text("PRAGMA synchronous=NORMAL;"))

Base.metadata.create_all(engine)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False))

def get_db_session():
    return SessionLocal()

# ------------------------- PublicClient wrapper ------------------------- #
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
        if isinstance(arr, list) and len(arr) > 0:
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
            arr = resp.json()
            a = arr[0] if isinstance(arr,list) and arr else arr
            return {'equity': float(a.get('equity',0)), 'cash': float(a.get('cash',0)), 'raw': a}
    except Exception as e:
        logging.error(f"Error fetching account: {e}")
        return {'equity':0.0,'cash':0.0,'raw':{}}

def client_list_positions():
    out=[]
    try:
        if use_client and hasattr(client, 'positions'):
            positions = client.positions() or []
            for p in positions:
                sym = p.get('symbol') or p.get('instrument', {}).get('symbol')
                qty = float(p.get('quantity') or p.get('qty') or 0)
                avg = float(p.get('avg_price') or p.get('avg_entry_price') or 0)
                created_at = p.get('created_at') or p.get('purchase_date') or None
                buy_date = datetime.now().date()
                if created_at:
                    try:
                        buy_date = datetime.fromisoformat(created_at).date()
                    except Exception:
                        pass
                out.append({'symbol':sym,'qty':qty,'avg_price':avg,'buy_date':buy_date})
        else:
            resp = requests.get(f"{BASE_URL}/trading/accounts/positions", headers=HEADERS, timeout=10)
            resp.raise_for_status()
            positions = resp.json().get('positions', [])
            for p in positions:
                sym = p.get('symbol') or p.get('instrument',{}).get('symbol')
                qty = float(p.get('quantity') or 0)
                avg = float(p.get('avg_price') or 0)
                buy_date = datetime.now().date()
                out.append({'symbol':sym,'qty':qty,'avg_price':avg,'buy_date':buy_date})
    except Exception as e:
        logging.error(f"Error fetching positions: {e}")
    return out

def client_get_quote(symbol):
    try:
        if use_client and hasattr(client,'quote'):
            q=client.quote(symbol)
            price=q.get('last') or q.get('last_price') or q.get('close')
            return float(price) if price else None
        else:
            url=f"{BASE_URL}/marketdata/quotes"
            resp=requests.post(url,headers=HEADERS,json={"instruments":[{"symbol":symbol,"type":"EQUITY"}]},timeout=10)
            if resp.status_code==200:
                payload=resp.json().get('quotes',[])
                if payload:
                    p=payload[0].get('last') or payload[0].get('close')
                    return float(p) if p else None
    except Exception as e:
        logging.error(f"Error fetching quote for {symbol}: {e}")
    return None

def client_place_market_order(symbol, side, quantity):
    try:
        if use_client:
            if hasattr(client,'buy') and side.upper()=='BUY':
                return client.buy(symbol,quantity)
            if hasattr(client,'sell') and side.upper()=='SELL':
                return client.sell(symbol,quantity)
            if hasattr(client,'order'):
                return client.order(symbol=symbol, side=side.upper(), type='market', quantity=str(quantity))
        acct = get_account_id()
        if not acct:
            logging.error("No account id for REST order")
            return None
        url=f"{BASE_URL}/trading/{acct}/order"
        body={
            "orderId": str(uuid4()),
            "instrument": {"symbol":symbol,"type":"EQUITY"},
            "orderSide": side.upper(),
            "orderType": "MARKET",
            "expiration":{"timeInForce":"DAY"},
            "quantity": str(quantity),
            "amount":"0","limitPrice":"0","stopPrice":"0","openCloseIndicator":"OPEN"
        }
        resp=requests.post(url,headers=HEADERS,json=body,timeout=15)
        if resp.status_code in (200,201):
            return resp.json()
        else:
            logging.error(f"Order error ({side} {symbol}) status={resp.status_code} body={resp.text}")
            return None
    except Exception as e:
        logging.error(f"Exception placing order: {e}")
        return None

# ------------------------- Utility Wrappers ------------------------- #
def get_account_info(): return client_get_account()
def list_positions_wrapper(): return client_list_positions()
def get_price(account_id_unused,symbol): return client_get_quote(symbol)
def place_market_order(account_id_unused,symbol,side,quantity): return client_place_market_order(symbol,side,quantity)

# ------------------------- yfinance helpers ------------------------- #
def yf_symbol(sym): return sym.replace('.','-')

def calculate_technical_indicators(symbols,lookback_days=5,interval='5m'):
    symbol=yf_symbol(symbols)
    try:
        df=yf.Ticker(symbol).history(period=f'{lookback_days}d',interval=interval,prepost=True)
        return df
    except Exception as e:
        logging.error(f"yfinance error for {symbol}: {e}")
        return None

def get_average_true_range(symbols):
    symbol=yf_symbol(symbols)
    try:
        df=yf.Ticker(symbol).history(period='30d',interval='1d')
        if df is None or df.empty: return None
        atr=talib.ATR(df['High'].values,df['Low'].values,df['Close'].values,timeperiod=14)
        return float(atr[-1]) if len(atr)>0 else None
    except Exception as e:
        logging.error(f"ATR error for {symbols}: {e}")
        return None

def is_in_uptrend(symbols_to_buy):
    symbol=yf_symbol(symbols_to_buy)
    try:
        df=yf.Ticker(symbol).history(period='250d',interval='1d')
        if df is None or len(df)<200: return False
        sma200=talib.SMA(df['Close'].values,timeperiod=200)
        return float(df['Close'].iloc[-1])>float(sma200[-1])
    except Exception as e:
        logging.error(f"is_in_uptrend error for {symbols_to_buy}: {e}")
        return False

def get_daily_rsi(symbols_to_buy):
    symbol=yf_symbol(symbols_to_buy)
    try:
        df=yf.Ticker(symbol).history(period='60d',interval='1d')
        if df is None or len(df)<14: return None
        rsi=talib.RSI(df['Close'].values,timeperiod=14)
        return float(rsi[-1])
    except Exception as e:
        logging.error(f"get_daily_rsi error for {symbols_to_buy}: {e}")
        return None

def get_previous_close(symbols_to_buy):
    symbol=yf_symbol(symbols_to_buy)
    try:
        df=yf.Ticker(symbol).history(period='3d',interval='1d')
        if df is None or len(df)<2: return None
        return float(df['Close'].iloc[-2])
    except Exception as e:
        logging.error(f"get_previous_close error for {symbols_to_buy}: {e}")
        return None

def get_last_price_within_past_5_minutes(symbols_to_buy_list):
    results={}
    end_time=datetime.now(eastern)
    start_time=end_time - timedelta(minutes=6)
    for s in symbols_to_buy_list:
        symbol=yf_symbol(s)
        try:
            df=yf.download(symbol,start=start_time.strftime('%Y-%m-%d %H:%M:%S'),
                           end=end_time.strftime('%Y-%m-%d %H:%M:%S'),
                           interval='1m',progress=False)
            if df is None or df.empty:
                results[s]=None
            else:
                results[s]=float(df['Close'].iloc[-1])
        except Exception as e:
            logging.error(f"Error fetching 1m data for {s}: {e}")
            results[s]=None
    return results

# ------------------------- Price History Storage ------------------------- #
price_history={}
last_stored={}
interval_map={'5min':5*60,'1min':60}

# ------------------------- Load symbols ------------------------- #
SYMBOLS_FILE="electricity-or-utility-stocks-to-buy-list.txt"
def load_symbols_from_file(file_path=SYMBOLS_FILE):
    symbols=[]
    try:
        with open(file_path,'r') as f:
            for line in f:
                s=line.strip().upper()
                if s: symbols.append(s)
        logging.info(f"Loaded {len(symbols)} symbols from {file_path}")
    except Exception as e:
        logging.error(f"Error loading symbols file: {e}")
    return symbols

# ------------------------- Market Hours ------------------------- #
nyse_cal=mcal.get_calendar('NYSE')
def market_is_open():
    now=datetime.now(eastern)
    schedule=nyse_cal.schedule(start_date=now.date(),end_date=now.date())
    if schedule.empty:
        print("Market closed today (weekend or holiday).")
        return False
    market_open=schedule.iloc[0]['market_open'].tz_convert(eastern)
    market_close=schedule.iloc[0]['market_close'].tz_convert(eastern)
    if market_open<=now<=market_close:
        return True
    else:
        print(f"Market is closed. Regular hours today: {market_open.strftime('%I:%M %p')} - {market_close.strftime('%I:%M %p')} ET")
        return False

# ------------------------- Trade Logging ------------------------- #
def log_trade_csv(date_str,buy,sell,qty,symbol,price):
    with open(CSV_FILENAME,mode='a',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=CSV_FIELDS)
        writer.writerow({'Date':date_str,'Buy':buy,'Sell':sell,'Quantity':qty,'Symbol':symbol,'Price Per Share':price})

def record_db_trade(action,symbol,qty,price,date_str):
    session=get_db_session()
    try:
        th=TradeHistory(symbols=symbol,action=action,quantity=qty,price=price,date=date_str)
        if action=='buy':
            pos=Position(symbols=symbol,quantity=qty,avg_price=price,purchase_date=date_str)
            session.add(pos)
        elif action=='sell':
            session.query(Position).filter_by(symbols=symbol).delete()
        session.add(th)
        session.commit()
    except SQLAlchemyError as e:
        logging.error(f"DB error in record_db_trade: {e}")
        session.rollback()
    finally:
        session.close()

# ------------------------- Sell Logic ------------------------- #
def sell_yesterdays_positions_if_up(account_id):
    now=datetime.now(eastern)
    yesterday=(now.date()-timedelta(days=1)).strftime("%Y-%m-%d")
    db=get_db_session()
    try:
        positions=db.query(Position).all()
        for p in positions:
            sym=p.symbols
            if p.purchase_date==yesterday:
                current_price=client_get_quote(sym)
                if current_price is None:
                    continue
                if current_price>=p.avg_price*1.005:
                    logging.info(f"Selling {sym} bought {p.purchase_date} at {current_price}")
                    order=client_place_market_order(sym,'SELL',p.quantity)
                    now_str=now.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
                    if order:
                        log_trade_csv(now_str,'','Sell',p.quantity,sym,current_price)
                        record_db_trade('sell',sym,p.quantity,current_price,now.date().strftime("%Y-%m-%d"))
    except Exception as e:
        logging.error(f"Error in sell_yesterdays_positions_if_up: {e}")
    finally:
        db.close()

# ------------------------- Buy Logic (Full Version) ------------------------- #
def buy_stocks(account_id,symbols_to_buy_list):
    if not symbols_to_buy_list: return
    account=client_get_account()
    cash_available=float(account.get('cash',0))
    portfolio_positions=client_list_positions()
    current_exposure=sum((p.get('qty',0)*(client_get_quote(p.get('symbol')) or 0)) for p in portfolio_positions)
    total_equity=cash_available+current_exposure
    max_new_exposure=total_equity*0.98-current_exposure
    if max_new_exposure<=0: return
    valid_symbols=[]
    for sym in symbols_to_buy_list:
        q=client_get_quote(sym)
        hist=calculate_technical_indicators(sym,lookback_days=5,interval='5m')
        if q is not None and hist is not None and not hist.empty:
            valid_symbols.append(sym)
    for sym in valid_symbols:
        now=datetime.now(eastern)
        now_str=now.strftime("Eastern Time | %I:%M:%S %p | %m-%d-%Y |")
        today_date_str=now.date().strftime("%Y-%m-%d")
        current_price=client_get_quote(sym)
        if current_price is None: continue
        ts=time.time()
        if sym not in price_history:
            price_history[sym]={k:[] for k in interval_map}
            last_stored[sym]={k:0 for k in interval_map}
        for k,delta in interval_map.items():
            if ts-last_stored[sym][k]>=delta:
                price_history[sym][k].append(current_price)
                last_stored[sym][k]=ts
        hist=calculate_technical_indicators(sym,lookback_days=5,interval='5m')
        if hist is None or hist.empty: continue
        close_prices=hist['Close'].values
        open_prices=hist['Open'].values
        high_prices=hist['High'].values
        low_prices=hist['Low'].values
        macd,macd_signal,_=talib.MACD(close_prices,fastperiod=12,slowperiod=26,signalperiod=9)
        rsi_series=talib.RSI(close_prices,timeperiod=14)
        latest_macd=float(macd[-1]) if len(macd)>0 else None
        latest_macd_signal=float(macd_signal[-1]) if len(macd_signal)>0 else None
        latest_rsi=float(rsi_series[-1]) if len(rsi_series)>0 else None
        if latest_macd is None or latest_macd_signal is None or latest_rsi is None:
            continue
        if latest_macd>latest_macd_signal and latest_rsi<65:
            qty=round(max_new_exposure/current_price,2)
            if qty<1 and ALL_BUY_ORDERS_ARE_1_DOLLAR: qty=1
            if qty<0.01: continue
            logging.info(f"Placing buy order: {sym} x {qty} at {current_price}")
            order=client_place_market_order(sym,'BUY',qty)
            if order:
                log_trade_csv(now_str,'Buy','',qty,sym,current_price)
                record_db_trade('buy',sym,qty,current_price,today_date_str)

# ------------------------- Main Bot Loop ------------------------- #
def run_bot_loop():
    symbols_list=load_symbols_from_file()
    while True:
        try:
            if market_is_open():
                account_id=get_account_id()
                sell_yesterdays_positions_if_up(account_id)
                buy_stocks(account_id,symbols_list)
            else:
                print("Waiting for market to open...")
            time.sleep(60)
        except Exception as e:
            logging.error(f"Main loop exception: {e}", exc_info=True)
            print(f"Unexpected error: {e}. Retrying in 2 minutes...")
            time.sleep(120)

# ------------------------- Entry Point ------------------------- #
if __name__=="__main__":
    run_bot_loop()
