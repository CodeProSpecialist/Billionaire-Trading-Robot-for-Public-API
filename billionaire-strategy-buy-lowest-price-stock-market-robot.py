#!/usr/bin/env python3
"""
Full trading bot with detailed buy/sell logic:
- PublicClient for trading operations + REST fallback
- yfinance for OHLCV, TA indicators (MACD, RSI, ATR)
- Candlestick patterns detection (Hammer, Bullish Engulfing)
- SQLAlchemy SQLite WAL + scoped_session for trade history & positions
- pandas-market-calendars for market hours
- CSV trade logging
- Auto-loop with error handling and exposure/cash management
"""

import os, time, csv, logging, threading
from uuid import uuid4
from datetime import datetime, timedelta
import pytz, requests, numpy as np, talib, yfinance as yf, pandas_market_calendars as mcal
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy.exc import SQLAlchemyError

# ---------------------- Configuration ---------------------- #
secret_key = os.getenv('PUBLIC_API_ACCESS_TOKEN')
if not secret_key: raise ValueError("PUBLIC_API_ACCESS_TOKEN not set.")

HEADERS = {"Authorization": f"Bearer {secret_key}", "Content-Type": "application/json"}
BASE_URL = "https://api.public.com/userapigateway"

PRINT_DB = True
ALL_BUY_ORDERS_ARE_1_DOLLAR = False
eastern = pytz.timezone('US/Eastern')

CSV_FILENAME = 'log-file-of-buy-and-sell-signals.csv'
CSV_FIELDS = ['Date','Buy','Sell','Quantity','Symbol','Price Per Share']
LOGFILE = 'trading-bot-program-logging-messages.txt'

with open(CSV_FILENAME,'w',newline='') as f:
    writer=csv.DictWriter(f,fieldnames=CSV_FIELDS)
    writer.writeheader()

logging.basicConfig(filename=LOGFILE, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# ---------------------- Database ---------------------- #
Base = declarative_base()

class TradeHistory(Base):
    __tablename__='trade_history'
    id=Column(Integer, primary_key=True)
    symbols=Column(String,index=True)
    action=Column(String)  # 'buy'/'sell'
    quantity=Column(Float)
    price=Column(Float)
    date=Column(String)

class Position(Base):
    __tablename__='positions'
    symbols=Column(String, primary_key=True)
    quantity=Column(Float)
    avg_price=Column(Float)
    purchase_date=Column(String)

engine = create_engine('sqlite:///trading_bot.db', connect_args={"check_same_thread":False}, pool_pre_ping=True, future=True)
with engine.connect() as conn:
    conn.execute(text("PRAGMA journal_mode=WAL;"))
    conn.execute(text("PRAGMA synchronous=NORMAL;"))

Base.metadata.create_all(engine)
SessionLocal = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False))
def get_db_session(): return SessionLocal()

# ---------------------- Public Client ---------------------- #
try:
    from public_invest_api import PublicClient
    try: client = PublicClient(api_key=secret_key)
    except TypeError: client = PublicClient()
    use_client = True
    logging.info("PublicClient instantiated.")
except Exception as e:
    client = None; use_client = False
    logging.info(f"PublicClient not importable: {e}")

_account_id_cache = None
def get_account_id():
    global _account_id_cache
    if _account_id_cache: return _account_id_cache
    try:
        if use_client and hasattr(client,'account'):
            acc=client.account(); _account_id_cache=acc.get('id') or acc.get('account_id'); return _account_id_cache
        resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status(); arr = resp.json()
        if isinstance(arr,list) and arr: _account_id_cache=arr[0].get('id'); return _account_id_cache
    except Exception as e: logging.error(f"Error getting account id: {e}")
    return None

def client_get_account():
    try:
        if use_client and hasattr(client,'account'):
            acc = client.account()
            return {'equity':float(acc.get('equity',0)),'cash':float(acc.get('cash',0)),'raw':acc}
        resp = requests.get(f"{BASE_URL}/accounts", headers=HEADERS, timeout=10)
        resp.raise_for_status(); a = resp.json()[0] if isinstance(resp.json(),list) else resp.json()
        return {'equity':float(a.get('equity',0)),'cash':float(a.get('cash',0)),'raw':a}
    except Exception as e: logging.error(f"Error fetching account: {e}"); return {'equity':0.0,'cash':0.0,'raw':{}}

def client_list_positions():
    out=[]
    try:
        if use_client and hasattr(client,'positions'):
            for p in client.positions() or []:
                sym=p.get('symbol') or p.get('instrument',{}).get('symbol')
                qty=float(p.get('quantity') or 0)
                avg=float(p.get('avg_price') or 0)
                out.append({'symbol':sym,'qty':qty,'avg_price':avg,'buy_date':datetime.now().date()})
        else:
            resp = requests.get(f"{BASE_URL}/trading/accounts/positions", headers=HEADERS, timeout=10)
            resp.raise_for_status()
            for p in resp.json().get('positions',[]):
                sym=p.get('symbol'); qty=float(p.get('quantity',0)); avg=float(p.get('avg_price',0))
                out.append({'symbol':sym,'qty':qty,'avg_price':avg,'buy_date':datetime.now().date()})
    except Exception as e: logging.error(f"Error fetching positions: {e}")
    return out

def client_get_quote(symbol):
    try:
        if use_client and hasattr(client,'quote'): q=client.quote(symbol); price=q.get('last') or q.get('last_price') or q.get('close'); return float(price) if price else None
        resp = requests.post(f"{BASE_URL}/marketdata/quotes", headers=HEADERS, json={"instruments":[{"symbol":symbol,"type":"EQUITY"}]}, timeout=10)
        resp.raise_for_status(); quotes = resp.json().get('quotes',[]); 
        if quotes: return float(quotes[0].get('last') or quotes[0].get('close') or 0)
    except Exception as e: logging.error(f"Error fetching quote for {symbol}: {e}")
    return None

def client_place_market_order(symbol, side, quantity):
    try:
        if use_client:
            if hasattr(client,'buy') and side.upper()=='BUY': return client.buy(symbol, quantity)
            if hasattr(client,'sell') and side.upper()=='SELL': return client.sell(symbol, quantity)
            if hasattr(client,'create_order'): return client.create_order(symbol=symbol, side=side.upper(), order_type='market', quantity=str(quantity))
        acct=get_account_id(); 
        if not acct: return None
        url=f"{BASE_URL}/trading/{acct}/order"
        body={"orderId":str(uuid4()),"instrument":{"symbol":symbol,"type":"EQUITY"},"orderSide":side.upper(),"orderType":"MARKET","expiration":{"timeInForce":"DAY"},"quantity":str(quantity)}
        resp=requests.post(url, headers=HEADERS, json=body, timeout=15)
        if resp.status_code in (200,201): return resp.json()
    except Exception as e: logging.error(f"Exception placing order: {e}")
    return None

# ---------------------- Helpers ---------------------- #
def yf_symbol(sym): return sym.replace('.','-')
def calculate_technical_indicators(symbol, lookback_days=5, interval='5m'):
    try: df=yf.Ticker(yf_symbol(symbol)).history(period=f'{lookback_days}d', interval=interval, prepost=True); return df if df is not None else None
    except Exception as e: logging.error(f"yfinance error for {symbol}: {e}"); return None
def get_average_true_range(symbol):
    df=calculate_technical_indicators(symbol, lookback_days=30, interval='1d')
    if df is None or df.empty: return None
    atr=talib.ATR(df['High'].values, df['Low'].values, df['Close'].values, timeperiod=14)
    return float(atr[-1]) if len(atr)>0 else None
def is_in_uptrend(symbol):
    df=calculate_technical_indicators(symbol, lookback_days=250, interval='1d')
    if df is None or len(df)<200: return False
    sma200=talib.SMA(df['Close'].values,timeperiod=200)
    return float(df['Close'].iloc[-1])>float(sma200[-1])
def get_daily_rsi(symbol):
    df=calculate_technical_indicators(symbol, lookback_days=60, interval='1d')
    if df is None or len(df)<14: return None
    rsi=talib.RSI(df['Close'].values,timeperiod=14)
    return float(rsi[-1])
def get_last_price_within_past_5_minutes(symbols_list):
    results={}
    end=datetime.now(eastern); start=end-timedelta(minutes=6)
    for s in symbols_list:
        try:
            df=yf.download(yf_symbol(s), start=start.strftime('%Y-%m-%d %H:%M:%S'), end=end.strftime('%Y-%m-%d %H:%M:%S'), interval='1m', progress=False)
            results[s]=float(df['Close'].iloc[-1]) if df is not None and not df.empty else None
        except Exception as e: logging.error(f"Error fetching 1m data for {s}: {e}"); results[s]=None
    return results

# ---------------------- CSV / DB logging ---------------------- #
def log_trade_csv(date_str,buy,sell,qty,symbol,price):
    with open(CSV_FILENAME,'a',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=CSV_FIELDS)
        writer.writerow({'Date':date_str,'Buy':buy,'Sell':sell,'Quantity':qty,'Symbol':symbol,'Price Per Share':price})

def record_db_trade(action,symbol,qty,price,date_str):
    session=get_db_session()
    try:
        th=TradeHistory(symbols=symbol,action=action,quantity=qty,price=price,date=date_str)
        if action=='buy': session.add(Position(symbols=symbol,quantity=qty,avg_price=price,purchase_date=date_str))
        elif action=='sell': session.query(Position).filter_by(symbols=symbol).delete()
        session.add(th); session.commit()
    except SQLAlchemyError as e: logging.error(f"DB error: {e}"); session.rollback()
    finally: session.close()

# ---------------------- Market Hours ---------------------- #
nyse_cal=mcal.get_calendar('NYSE')
def market_is_open():
    now=datetime.now(eastern); schedule=nyse_cal.schedule(start_date=now.date(), end_date=now.date())
    if schedule.empty: return False
    mo=schedule.iloc[0]['market_open'].tz_convert(eastern); mc=schedule.iloc[0]['market_close'].tz_convert(eastern)
    return mo<=now<=mc

# ---------------------- Buy / Sell Logic ---------------------- #
price_history={}; last_stored={}; interval_map={'5min':300,'1min':60}

def load_symbols_from_file(file_path="electricity-or-utility-stocks-to-buy-list.txt"):
    symbols=[]
    try:
        with open(file_path,'r') as f:
            for line in f: s=line.strip().upper(); symbols.append(s) if s else None
        logging.info(f"Loaded {len(symbols)} symbols from {file_path}")
    except Exception as e: logging.error(f"Error loading symbols file: {e}")
    return symbols

def sell_yesterdays_positions_if_up(account_id):
    now=datetime.now(eastern); yesterday=(now.date()-timedelta(days=1)).strftime("%Y-%m-%d")
    db=get_db_session()
    try:
        for p in db.query(Position).all():
            if p.purchase_date==yesterday:
                cur_price=client_get_quote(p.symbols)
                if cur_price is None: continue
                if cur_price>=float(p.avg_price)*1.005:
                    logging.info(f"Selling {p.symbols} yesterday's buy at {cur_price}")
                    client_place_market_order(p.symbols,'SELL',p.quantity)
                    now_str=now.strftime("%Y-%m-%d %H:%M:%S")
                    log_trade_csv(now_str,'','Sell',p.quantity,p.symbols,cur_price)
                    record_db_trade('sell',p.symbols,p.quantity,cur_price,now.strftime("%Y-%m-%d"))
    except Exception as e: logging.error(f"Error in sell_yesterdays_positions_if_up: {e}")
    finally: db.close()

def buy_stocks(account_id,symbols_to_buy_list):
    if not symbols_to_buy_list: return
    account=client_get_account(); cash_available=float(account.get('cash',0))
    portfolio_positions=client_list_positions(); current_exposure=sum((p.get('qty',0)*(client_get_quote(p.get('symbol')) or 0)) for p in portfolio_positions)
    total_equity=cash_available+current_exposure; max_new_exposure=total_equity*0.98-current_exposure
    if max_new_exposure<=0: return

    valid_symbols=[]
    for sym in symbols_to_buy_list:
        q=client_get_quote(sym)
        hist=calculate_technical_indicators(sym,lookback_days=5,interval='5m')
        if q is None or hist is None or hist.empty or len(hist)<5: continue
        valid_symbols.append(sym)
    for sym in valid_symbols:
        try:
            current_price=client_get_quote(sym)
            if current_price is None: continue

            ts=time.time()
            if sym not in price_history: price_history[sym]={k:[] for k in interval_map}; last_stored[sym]={k:0 for k in interval_map}
            for k,delta in interval_map.items():
                if ts-last_stored[sym][k]>=delta:
                    price_history[sym][k].append(current_price)
                    last_stored[sym][k]=ts

            hist=calculate_technical_indicators(sym,lookback_days=5,interval='5m')
            close_prices=hist['Close'].values; open_prices=hist['Open'].values; high_prices=hist['High'].values; low_prices=hist['Low'].values
            macd,macd_signal,_=talib.MACD(close_prices,fastperiod=12,slowperiod=26,signalperiod=9)
            rsi_series=talib.RSI(close_prices,timeperiod=14)
            latest_macd=float(macd[-1]) if len(macd)>0 else None
            latest_macd_signal=float(macd_signal[-1]) if len(macd_signal)>0 else None
            latest_rsi=float(rsi_series[-1]) if len(rsi_series)>0 else None

            recent_avg_volume=hist['Volume'].iloc[-5:].mean() if len(hist)>=5 else 0
            prior_avg_volume=hist['Volume'].iloc[-10:-5].mean() if len(hist)>=10 else recent_avg_volume
            volume_decrease=recent_avg_volume<prior_avg_volume if len(hist)>=10 else False

            last5=get_last_price_within_past_5_minutes([sym]).get(sym)
            last5=last5 or current_price
            price_decline_threshold=last5*(1-0.002)
            price_decline=current_price<=price_decline_threshold

            bullish_reversal_detected=False; detected_patterns=[]
            for i in range(-1,-21,-1):
                if len(hist)<abs(i): continue
                try:
                    patterns={'Hammer':talib.CDLHAMMER(open_prices[:i+1],high_prices[:i+1],low_prices[:i+1],close_prices[:i+1])[i]!=0,
                              'Bullish Engulfing':talib.CDLENGULFING(open_prices[:i+1],high_prices[:i+1],low_prices[:i+1],close_prices[:i+1])[i]>0}
                    current_detected=[name for name,det in patterns.items() if det]
                    if current_detected: bullish_reversal_detected=True; detected_patterns=current_detected; break
                except Exception: continue

            if not is_in_uptrend(sym): continue
            daily_rsi=get_daily_rsi(sym)
            if daily_rsi is None or daily_rsi>50: continue

            score=0
            if bullish_reversal_detected: score+=2
            price_stable=True
            if sym in price_history and '5min' in price_history[sym] and len(price_history[sym]['5min'])>=2:
                recent_prices=price_history[sym]['5min'][-2:]
                price_stable=abs(recent_prices[-1]-recent_prices[-2])/recent_prices[-2]<0.005
            if price_stable: score+=1
            if latest_macd is not None and latest_macd_signal is not None and latest_macd>latest_macd_signal: score+=1
            if not volume_decrease: score+=1
            if latest_rsi is not None and latest_rsi<40: score+=1
            for pat in detected_patterns:
                if pat=='Bullish Engulfing' and recent_avg_volume>1.5*prior_avg_volume: score+=1
                if pat=='Hammer' and latest_rsi is not None and latest_rsi<35: score+=1
            if score<3: continue

            if ALL_BUY_ORDERS_ARE_1_DOLLAR: total_cost=1.0; qty=round(total_cost/current_price,4)
            else:
                atr=get_average_true_range(sym)
                if atr is None or atr<=0: continue
                stop_loss_distance=2*atr; risk_amount=0.01*max(1.0,total_equity)
                qty=risk_amount/stop_loss_distance if stop_loss_distance>0 else 0
                total_cost=qty*current_price
                account_now=client_get_account(); cash_now=float(account_now.get('cash',0))
                total_cost=min(total_cost, cash_now-1.0 if cash_now>1.0 else 0, max_new_exposure)
                if total_cost<1.0: continue
                qty=round(total_cost/current_price,4)
            if qty<=0: continue

            client_place_market_order(sym,'BUY',qty)
            now_str=datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")
            log_trade_csv(now_str,'Buy','',qty,sym,current_price)
            record_db_trade('buy',sym,qty,current_price,datetime.now(eastern).strftime("%Y-%m-%d"))
            logging.info(f"Bought {qty} shares of {sym} at {current_price}")

        except Exception as e: logging.error(f"Error evaluating {sym}: {e}")

def run_bot_loop():
    symbols_list=load_symbols_from_file()
    while True:
        try:
            if market_is_open():
                account_id=get_account_id()
                sell_yesterdays_positions_if_up(account_id)
                buy_stocks(account_id,symbols_list)
            else: logging.info("Market is closed, waiting...")
            time.sleep(60)
        except Exception as e: logging.error(f"Main loop exception: {e}")
        time.sleep(5)

if __name__=="__main__":
    logging.info("Starting trading bot loop...")
    run_bot_loop()
