import threading
import logging
from logging.handlers import RotatingFileHandler
import csv
import os
import time
import schedule
from datetime import datetime, timedelta, date
import pytz
import requests
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
import json
import pickle
import pandas as pd
from backoff import on_exception, expo

# Logging configuration with rotation
handler = RotatingFileHandler('trading-bot-program-logging-messages.txt', maxBytes=1000000, backupCount=5)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[handler]
)

# Load environment variables
account_id = os.getenv('PUBLIC_ACCOUNT_ID_NUMBER')
secret_key = os.getenv('PUBLIC_API_ACCESS_TOKEN')
username = os.getenv('PUBLIC_USERNAME')
password = os.getenv('PUBLIC_PASSWORD')

# Validate environment variables (prioritize token-based authentication)
if not (account_id and secret_key) and not (username and password):
    logging.error("Missing required environment variables for authentication")
    raise ValueError("Must provide either PUBLIC_ACCOUNT_ID_NUMBER and PUBLIC_API_ACCESS_TOKEN or PUBLIC_USERNAME and PUBLIC_PASSWORD")

# Define APIError
class APIError(Exception):
    pass

# Endpoints class for Public.com API
class Endpoints:
    def __init__(self):
        self.baseurl = "https://public.com"
        self.prodapi = "https://prod-api.154310543964.hellopublic.com"
        self.ordergateway = f"{self.prodapi}/customerordergateway"
        self.userservice = f"{self.baseurl}/userservice"

    def login_url(self) -> str:
        return f"{self.userservice}/public/web/login"

    def mfa_url(self) -> str:
        return f"{self.userservice}/public/web/verify-two-factor"

    def refresh_url(self) -> str:
        return f"{self.userservice}/public/web/token-refresh"

    def portfolio_url(self, account_uuid: str) -> str:
        return f"{self.prodapi}/hstier1service/account/{account_uuid}/portfolio"

    def account_history_url(self, account_uuid: str) -> str:
        return f"{self.prodapi}/hstier2service/history?accountUuids={account_uuid}"

    def get_quote_url(self, symbol: str) -> str:
        return f"{self.prodapi}/tradingservice/quote/equity/{symbol}"

    def get_bars_url(self, symbol: str) -> str:
        return f"{self.prodapi}/tradingservice/bars/equity/{symbol}"

    def preflight_order_url(self, account_uuid: str) -> str:
        return f"{self.ordergateway}/accounts/{account_uuid}/orders/preflight"

    def build_order_url(self, account_uuid: str) -> str:
        return f"{self.ordergateway}/accounts/{account_uuid}/orders"

    def submit_put_order_url(self, account_uuid: str, order_id: str) -> str:
        return f"{self.ordergateway}/accounts/{account_uuid}/orders/{order_id}"

    def submit_get_order_url(self, account_uuid: str, order_id: str) -> str:
        return f"{self.prodapi}/hstier1service/account/{account_uuid}/order/{order_id}"

    def get_pending_orders_url(self, account_uuid: str) -> str:
        return f"{self.prodapi}/hstier2service/history?status=PENDING&type=ALL&accountUuids={account_uuid}"

    def cancel_pending_order_url(self, account_uuid: str, order_id: str) -> str:
        return f"{self.ordergateway}/accounts/{account_uuid}/orders/{order_id}"

    def contract_details_url(self, option_symbol: str) -> str:
        return f"{self.prodapi}/hstier1service/contract-details/{option_symbol}/BUY"

    def build_headers(self, auth: bool = None, prodApi: bool = False) -> dict:
        headers = {
            "authority": "public.com",
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.8",
            "content-type": "application/json",
            "origin": "https://public.com",
            "sec-ch-ua": '"Not A(Brand";v="8", "Chromium";v="132", "Brave";v="132"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "sec-gpc": "1",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "x-app-version": "web-1.0.11",
        }
        if auth is not None:
            headers["authorization"] = auth
        if prodApi:
            headers["authority"] = self.prodapi.replace("https://", "")
            headers["sec-fetch-site"] = "cross-site"
        return headers

    @staticmethod
    def build_payload(email: str, password: str, code: str = None) -> str:
        payload = {
            "email": email,
            "password": password,
        }
        if code is None:
            payload["stayLoggedIn"] = True
        else:
            payload["verificationCode"] = code
        return json.dumps(payload)

# Public.com API client class
class Public:
    def __init__(self, filename: str = "public_credentials.pkl", path: str = None):
        self.session = requests.Session()
        self.endpoints = Endpoints()
        self.session.headers.update(self.endpoints.build_headers())
        self.access_token = secret_key or None
        self.account_uuid = account_id or None
        self.account_number = None
        self.all_login_info = None
        self.timeout = 10
        self.expires_at = None
        self.filename = filename
        self.path = path
        if not self.access_token or not self.account_uuid:
            if not username or not password:
                raise APIError("Must provide PUBLIC_USERNAME and PUBLIC_PASSWORD if PUBLIC_API_ACCESS_TOKEN or PUBLIC_ACCOUNT_ID_NUMBER is missing")
            self.login(username, password, wait_for_2fa=True)
        else:
            self.session.headers.update(self.endpoints.build_headers(self.access_token))
        self._load_cookies()

    def _save_cookies(self) -> None:
        filename = self.filename
        if self.path is not None:
            filename = os.path.join(self.path, filename)
        if self.path is not None and not os.path.exists(self.path):
            os.makedirs(self.path)
        with open(filename, "wb") as f:
            pickle.dump(self.session.cookies, f)

    def _load_cookies(self) -> bool:
        filename = self.filename
        if self.path is not None:
            filename = os.path.join(self.path, filename)
        if not os.path.exists(filename):
            return False
        with open(filename, "rb") as f:
            self.session.cookies.update(pickle.load(f))
        return True

    def _clear_cookies(self) -> None:
        filename = self.filename
        if self.path is not None:
            filename = os.path.join(self.path, filename)
        if os.path.exists(filename):
            os.remove(filename)
        self.session.cookies.clear()

    def login(self, username: str = None, password: str = None, wait_for_2fa: bool = True, code: str = None) -> dict:
        if username is None or password is None:
            raise APIError("Username or password not provided")
        refresh_success = False
        try:
            response = self._refresh_token()
            refresh_success = True
        except Exception:
            pass
        headers = self.session.headers
        need_2fa = True
        if not refresh_success:
            payload = self.endpoints.build_payload(username, password)
            self._load_cookies()
            response = self.session.post(
                self.endpoints.login_url(),
                headers=headers,
                data=payload,
                timeout=self.timeout,
            )
            if response.status_code != 200:
                self._clear_cookies()
                response = self.session.post(
                    self.endpoints.login_url(),
                    headers=headers,
                    data=payload,
                    timeout=self.timeout,
                )
            if response.status_code != 200:
                logging.error(f"Login failed: {response.text}")
                raise APIError(f"Login failed: {response.text}")
            response = response.json()
            if response.get("twoFactorResponse") is not None:
                self._clear_cookies()
                phone = response["twoFactorResponse"]["maskedPhoneNumber"]
                logging.info(f"2FA required, code sent to phone number {phone}")
                code = input("Enter 2FA code: ")
            else:
                need_2fa = False
        if need_2fa and not refresh_success:
            payload = self.endpoints.build_payload(username, password, code)
            response = self.session.post(
                self.endpoints.mfa_url(),
                headers=headers,
                data=payload,
                timeout=self.timeout,
            )
            if response.status_code != 200:
                logging.error(f"MFA login failed: {response.text}")
                raise APIError(f"MFA login failed: {response.text}")
            response = self.session.post(
                self.endpoints.login_url(),
                headers=headers,
                data=payload,
                timeout=self.timeout,
            )
            if response.status_code != 200:
                logging.error(f"Login failed after MFA: {response.text}")
                raise APIError(f"Login after MFA failed: {response.text}")
            response = response.json()
        if "loginResponse" in response:
            response = response["loginResponse"]
        self.access_token = response["accessToken"]
        self.account_uuid = response["accounts"][0]["accountUuid"]
        self.account_number = response["accounts"][0]["account"]
        self.expires_at = (int(response["serverTime"]) / 1000) + int(response["expiresIn"])
        self.all_login_info = response
        self.session.headers.update(self.endpoints.build_headers(self.access_token))
        self._save_cookies()
        return response

    def _refresh_token(self) -> dict:
        headers = self.session.headers
        response = self.session.post(
            self.endpoints.refresh_url(), headers=headers, timeout=self.timeout
        )
        if response.status_code != 200:
            logging.error(f"Token refresh failed: {response.text}")
            raise APIError(f"Token refresh failed: {response.text}")
        response = response.json()
        self.access_token = response["accessToken"]
        self.expires_at = (int(response["serverTime"]) / 1000) + int(response["expiresIn"])
        self.account_uuid = response["accounts"][0]["accountUuid"]
        self.session.headers.update(self.endpoints.build_headers(self.access_token))
        self._save_cookies()
        return response

    @on_exception(expo, APIError, max_tries=3)
    def get_account(self):
        headers = self.endpoints.build_headers(self.access_token, prodApi=True)
        portfolio = self.session.get(
            self.endpoints.portfolio_url(self.account_uuid),
            headers=headers,
            timeout=self.timeout,
        )
        if portfolio.status_code != 200:
            logging.error(f"Portfolio request failed: {portfolio.status_code} {portfolio.text}")
            raise APIError(f"Portfolio request failed: {portfolio.text}")
        acc = portfolio.json()
        acc['cash'] = str(acc.get('equity', {}).get('cash', 0))
        acc['equity'] = str(acc.get('equity', {}).get('total', 0))
        acc['daytrade_count'] = acc.get('dayTradeCount', 0)
        return acc

    def list_positions(self):
        account_info = self.get_account()
        positions = account_info.get('positions', [])
        class Position:
            def __init__(self, d):
                self.symbol = d.get('instrument', {}).get('symbol')
                self.qty = str(d.get('quantity'))
                self.avg_entry_price = str(d.get('averagePrice'))
                self.market_value = str(d.get('marketValue'))
        return [Position(p) for p in positions]

    @on_exception(expo, APIError, max_tries=3)
    def submit_order(self, symbol, qty=None, notional=None, side=None, type=None, time_in_force=None, trail_percent=None):
        headers = self.endpoints.build_headers(self.access_token, prodApi=True)
        symbol = symbol.upper()
        side = side.upper()
        type = type.upper()
        time_in_force = time_in_force.upper()
        if side not in ["BUY", "SELL"]:
            raise APIError(f"Invalid side: {side}")
        if type not in ["MARKET", "LIMIT", "STOP", "TRAILING_STOP"]:
            raise APIError(f"Invalid order type: {type}")
        if time_in_force not in ["DAY", "GTC", "IOC", "FOK"]:
            raise APIError(f"Invalid time in force: {time_in_force}")
        payload = {
            "symbol": symbol,
            "orderSide": side,
            "type": type,
            "timeInForce": time_in_force,
        }
        if notional is not None:
            with buy_sell_lock:
                current_price = self.get_current_price(symbol)
            payload["quantity"] = notional / current_price if current_price else 0
        elif qty is not None:
            payload["quantity"] = qty
        if type == "TRAILING_STOP":
            payload["trailPercent"] = trail_percent
        preflight = self.session.post(
            self.endpoints.preflight_order_url(self.account_uuid),
            headers=headers,
            json=payload,
            timeout=self.timeout,
        )
        if preflight.status_code != 200:
            logging.error(f"Preflight failed: {preflight.text}")
            raise APIError(f"Preflight failed: {preflight.text}")
        build_response = self.session.post(
            self.endpoints.build_order_url(self.account_uuid),
            headers=headers,
            json=payload,
            timeout=self.timeout,
        )
        if build_response.status_code != 200:
            logging.error(f"Build order failed: {build_response.text}")
            raise APIError(f"Build order failed: {build_response.text}")
        build_response = build_response.json()
        order_id = build_response.get("orderId")
        if not order_id:
            logging.error(f"No order ID: {build_response}")
            raise APIError(f"No order ID: {build_response}")
        submit_response = self.session.put(
            self.endpoints.submit_put_order_url(self.account_uuid, order_id),
            headers=headers,
            timeout=self.timeout,
        )
        if submit_response.status_code != 200:
            logging.error(f"Submit order failed: {submit_response.text}")
            raise APIError(f"Submit order failed: {submit_response.text}")
        class Order:
            def __init__(self, d):
                self.id = d.get('orderId')
                self.status = d.get('status')
                self.filled_qty = str(d.get('filledQuantity', 0))
                self.filled_avg_price = str(d.get('averageFillPrice', 0))
                self.side = d.get('orderSide')
                self.submitted_at = d.get('createdAt')
                self.filled_at = d.get('filledAt')
        check_response = self.session.get(
            self.endpoints.submit_get_order_url(self.account_uuid, order_id),
            headers=headers,
            timeout=self.timeout,
        )
        if check_response.status_code != 200:
            logging.error(f"Order check failed: {check_response.text}")
            raise APIError(f"Order check failed: {check_response.text}")
        return Order(check_response.json())

    def get_position(self, symbol):
        positions = self.list_positions()
        for pos in positions:
            if pos.symbol == symbol:
                return pos
        raise APIError(f"Position not found for {symbol}")

    def list_orders(self, status='all', nested=False, direction='desc', until=None, limit=None, symbols=None):
        headers = self.endpoints.build_headers(self.access_token)
        params = {
            "status": status.upper(),
            "type": "ALL",
            "accountUuids": self.account_uuid,
        }
        if direction:
            params["direction"] = direction.upper()
        if limit:
            params["limit"] = limit
        if until:
            params["until"] = until
        if symbols:
            params["symbols"] = ','.join(symbols)
        response = self.session.get(
            self.endpoints.get_pending_orders_url(self.account_uuid),
            headers=headers,
            params=params,
            timeout=self.timeout,
        )
        if response.status_code != 200:
            logging.error(f"List orders failed: {response.text}")
            raise APIError(f"List orders failed: {response.text}")
        orders = response.json().get('orders', [])
        class Order:
            def __init__(self, d):
                self.id = d.get('orderId')
                self.status = d.get('status')
                self.side = d.get('orderSide')
                self.filled_at = d.get('filledAt')
                self.submitted_at = d.get('createdAt')
                self.filled_avg_price = str(d.get('averageFillPrice', 0))
                self.filled_qty = str(d.get('filledQuantity', 0))
        return [Order(o) for o in orders]

    def get_order(self, order_id):
        headers = self.endpoints.build_headers(self.access_token)
        response = self.session.get(
            self.endpoints.submit_get_order_url(self.account_uuid, order_id),
            headers=headers,
            timeout=self.timeout,
        )
        if response.status_code != 200:
            logging.error(f"Get order failed: {response.text}")
            raise APIError(f"Get order failed: {response.text}")
        o = response.json()
        class Order:
            def __init__(self, d):
                self.status = d.get('status')
                self.filled_qty = str(d.get('filledQuantity', 0))
                self.filled_avg_price = str(d.get('averageFillPrice', 0))
        return Order(o)

    @on_exception(expo, APIError, max_tries=3)
    def get_current_price(self, symbol, retries=3):
        for attempt in range(retries):
            headers = self.endpoints.build_headers(self.access_token)
            url = self.endpoints.get_quote_url(symbol)
            response = self.session.get(url, headers=headers, timeout=self.timeout)
            if response.status_code == 200:
                data = response.json()
                if not data:
                    logging.error(f"Empty response from quotes API for {symbol}: {response.text}")
                    bars_url = f"{self.endpoints.get_bars_url(symbol)}?timeframe=1m&limit=1"
                    response = self.session.get(bars_url, headers=headers, timeout=self.timeout)
                    if response.status_code == 200:
                        bars = response.json().get('bars', [])
                        if bars:
                            current_price = float(bars[-1].get('close', 0))
                            return round(current_price, 4) if current_price else None
                        logging.error(f"No bars data for {symbol}: {response.json()}")
                        time.sleep(2 ** attempt)
                        continue
                    logging.error(f"Bars API failed for {symbol}: {response.status_code} {response.text}")
                    time.sleep(2 ** attempt)
                    continue
                current_price = float(data.get('last', 0))
                if not current_price:
                    logging.error(f"No 'last' price in response for {symbol}: {data}")
                    time.sleep(2 ** attempt)
                    continue
                return round(current_price, 4)
            logging.error(f"Quotes API failed for {symbol}: {response.status_code} {response.text}")
            time.sleep(2 ** attempt)
        logging.error(f"Failed to fetch current price for {symbol} after {retries} attempts")
        return None

    @on_exception(expo, APIError, max_tries=3)
    def get_historical_data(self, symbol, period, interval, prepost=True):
        headers = self.endpoints.build_headers(self.access_token)
        timeframe_map = {
            '1m': '1m',
            '5m': '5m',
            '1d': '1d',
            '200d': '1d',
            '30d': '1d',
            '5d': '5m',
        }
        timeframe = timeframe_map.get(interval, '1d')
        limit = {
            '1m': 1000,
            '5m': 1000,
            '1d': 200 if period == '200d' else 30 if period == '30d' else 5,
        }.get(timeframe, 100)
        url = f"{self.endpoints.get_bars_url(symbol)}?timeframe={timeframe}&limit={limit}"
        if prepost:
            url += "&extended=true"
        response = self.session.get(url, headers=headers, timeout=self.timeout)
        if response.status_code != 200:
            logging.error(f"Historical data request failed for {symbol}: {response.status_code} {response.text}")
            return pd.DataFrame()
        data = response.json().get('bars', [])
        if not data:
            logging.error(f"No historical data for {symbol}")
            return pd.DataFrame()
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.set_index('timestamp', inplace=True)
        df = df[['open', 'high', 'low', 'close', 'volume']]
        df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
        df.index = df.index.tz_convert('US/Eastern')
        return df

# Initialize the Public API
try:
    api = Public()
except APIError as e:
    logging.error(f"Failed to initialize API: {str(e)}")
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

# Thread lock
buy_sell_lock = threading.Lock()

# Initialize CSV file
try:
    with open(csv_filename, mode='w', newline='') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        csv_writer.writeheader()
except Exception as e:
    logging.error(f"Error initializing CSV file: {e}")
    raise

# Define Database Models
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
engine = create_engine('sqlite:///trading_bot.db')
Session = sessionmaker(bind=engine)
session = Session()
Base.metadata.create_all(engine)

# Cache for API data
data_cache = {}
CACHE_EXPIRY = 120
CALLS = 60
PERIOD = 60

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_cached_data(symbols, data_type, fetch_func, *args, **kwargs):
    logging.info(f"Checking cache for {symbols} {data_type}...")
    key = (symbols, data_type)
    current_time = time.time()
    if key in data_cache and current_time - data_cache[key]['timestamp'] < CACHE_EXPIRY:
        logging.info(f"Using cached {data_type} for {symbols}.")
        return data_cache[key]['data']
    data = fetch_func(*args, **kwargs)
    data_cache[key] = {'timestamp': current_time, 'data': data}
    logging.info(f"Cached {data_type} for {symbols}.")
    return data

def stop_if_stock_market_is_closed():
    nyse = mcal.get_calendar('NYSE')
    while True:
        eastern = pytz.timezone('US/Eastern')
        current_datetime = datetime.now(eastern)
        current_time_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        schedule = nyse.schedule(start_date=current_datetime.date(), end_date=current_datetime.date())
        if not schedule.empty:
            market_open = schedule.iloc[0]['market_open'].astimezone(eastern)
            market_close = schedule.iloc[0]['market_close'].astimezone(eastern)
            if market_open <= current_datetime <= market_close:
                logging.info(f"{current_time_str}: Market is open. Proceeding with trading operations.")
                break
            else:
                logging.info(f"{current_time_str}: Market is closed. Waiting for market open.")
                time.sleep(60)
        else:
            logging.info(f"{current_time_str}: Market is closed today (holiday or weekend).")
            time.sleep(60)

def print_database_tables():
    if PRINT_DATABASE:
        positions = api.list_positions()
        logging.info("Printing database tables...")
        print("\nTrade History In This Robot's Database:")
        print("Stock | Buy or Sell | Quantity | Avg. Price | Date ")
        for record in session.query(TradeHistory).all():
            print(f"{record.symbols} | {record.action} | {record.quantity:.4f} | {record.price:.2f} | {record.date}")
        print("----------------------------------------------------------------")
        print("\nPositions in the Database To Sell On or After the Date Shown:")
        print("Stock | Quantity | Avg. Price | Date or The 1st Day This Robot Began Working ")
        for record in session.query(Position).all():
            symbols_to_sell, quantity, avg_price, purchase_date = record.symbols, record.quantity, record.avg_price, record.purchase_date
            current_price = api.get_current_price(symbols_to_sell)
            percentage_change = ((current_price - avg_price) / avg_price) * 100 if current_price and avg_price else 0
            print(f"{symbols_to_sell} | {quantity:.4f} | {avg_price:.2f} | {purchase_date} | Price Change: {percentage_change:.2f}%")
        print("\n")

def get_symbols_to_buy():
    logging.info("Loading symbols to buy from file...")
    try:
        with open('electricity-or-utility-stocks-to-buy-list.txt', 'r') as file:
            symbols_to_buy = [line.strip().upper() for line in file.readlines() if line.strip()]
            logging.info(f"Loaded {len(symbols_to_buy)} stock symbols from file.")
            if not symbols_to_buy:
                logging.warning("No stock symbols found in electricity-or-utility-stocks-to-buy-list.txt")
            return symbols_to_buy
    except FileNotFoundError:
        logging.error("File not found: electricity-or-utility-stocks-to-buy-list.txt")
        return []
    except Exception as e:
        logging.error(f"Error reading symbols file: {e}")
        return []

def remove_symbols_from_trade_list(symbols_to_buy):
    logging.info(f"Removing {symbols_to_buy} from trade list...")
    try:
        with open('electricity-or-utility-stocks-to-buy-list.txt', 'r') as file:
            lines = file.readlines()
        with open('electricity-or-utility-stocks-to-buy-list.txt', 'w') as file:
            for line in lines:
                if line.strip() != symbols_to_buy:
                    file.write(line)
        logging.info(f"Successfully removed {symbols_to_buy} from trade list.")
    except Exception as e:
        logging.error(f"Error removing {symbols_to_buy} from trade list: {e}")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_opening_price(symbols_to_buy):
    logging.info(f"Fetching opening price for {symbols_to_buy}...")
    historical_data = api.get_historical_data(symbols_to_buy, '1d', '1d')
    try:
        opening_price = round(float(historical_data['Open'].iloc[0]), 4)
        logging.info(f"Opening price for {symbols_to_buy}: ${opening_price:.4f}")
        return opening_price
    except (IndexError, KeyError):
        logging.error(f"Opening price not found for {symbols_to_buy}.")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_atr_high_price(symbols_to_sell):
    logging.info(f"Calculating ATR high price for {symbols_to_sell}...")
    atr_value = get_average_true_range(symbols_to_sell)
    with buy_sell_lock:
        current_price = api.get_current_price(symbols_to_sell)
    atr_high = round(current_price + 0.40 * atr_value, 4) if current_price and atr_value else None
    logging.info(f"ATR high price for {symbols_to_sell}: ${atr_high:.4f}" if atr_high else f"Failed to calculate ATR high price for {symbols_to_sell}.")
    return atr_high

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_atr_low_price(symbols_to_buy):
    logging.info(f"Calculating ATR low price for {symbols_to_buy}...")
    atr_value = get_average_true_range(symbols_to_buy)
    with buy_sell_lock:
        current_price = api.get_current_price(symbols_to_buy)
    atr_low = round(current_price - 0.10 * atr_value, 4) if current_price and atr_value else None
    logging.info(f"ATR low price for {symbols_to_buy}: ${atr_low:.4f}" if atr_low else f"Failed to calculate ATR low price for {symbols_to_buy}.")
    return atr_low

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_average_true_range(symbols):
    logging.info(f"Calculating ATR for {symbols}...")
    def _fetch_atr(symbols):
        historical_data = api.get_historical_data(symbols, '30d', '1d')
        if historical_data.empty or len(historical_data) < 22:
            logging.warning(f"Insufficient data for ATR calculation for {symbols}. Returning 0.")
            return 0.0
        atr = talib.ATR(historical_data['High'].values, historical_data['Low'].values, historical_data['Close'].values, timeperiod=22)
        atr_value = atr[-1] if len(atr) > 0 and not np.isnan(atr[-1]) else 0.0
        logging.info(f"ATR for {symbols}: {atr_value:.4f}")
        return atr_value
    return get_cached_data(symbols, 'atr', _fetch_atr, symbols)

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def is_in_uptrend(symbols_to_buy):
    logging.info(f"Checking if {symbols_to_buy} is in uptrend (above 200-day SMA)...")
    historical_data = api.get_historical_data(symbols_to_buy, '200d', '1d')
    if historical_data.empty or len(historical_data) < 200:
        logging.info(f"Insufficient data for 200-day SMA for {symbols_to_buy}. Assuming not in uptrend.")
        return False
    sma_200 = talib.SMA(historical_data['Close'].values, timeperiod=200)[-1]
    with buy_sell_lock:
        current_price = api.get_current_price(symbols_to_buy)
    in_uptrend = current_price > sma_200 if current_price else False
    logging.info(f"{symbols_to_buy} {'is' if in_uptrend else 'is not'} in uptrend (Current: {current_price:.2f}, SMA200: {sma_200:.2f})")
    return in_uptrend

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_daily_rsi(symbols_to_buy):
    logging.info(f"Calculating daily RSI for {symbols_to_buy}...")
    historical_data = api.get_historical_data(symbols_to_buy, '30d', '1d')
    if historical_data.empty:
        logging.error(f"No daily data for {symbols_to_buy}. RSI calculation failed.")
        return None
    rsi = talib.RSI(historical_data['Close'], timeperiod=14)[-1]
    rsi_value = round(rsi, 2) if not np.isnan(rsi) else None
    logging.info(f"Daily RSI for {symbols_to_buy}: {rsi_value}")
    return rsi_value

def status_printer_buy_stocks():
    logging.info("Buy stocks function is working correctly right now. Checking symbols to buy...")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_technical_indicators(symbols, lookback_days=90):
    logging.info(f"Calculating technical indicators for {symbols} over {lookback_days} days...")
    historical_data = api.get_historical_data(symbols, f'{lookback_days}d', '1d')
    if historical_data.empty:
        logging.error(f"No historical data for {symbols}.")
        return pd.DataFrame()
    short_window = 12
    long_window = 26
    signal_window = 9
    historical_data['macd'], historical_data['signal'], _ = talib.MACD(
        historical_data['Close'], fastperiod=short_window, slowperiod=long_window, signalperiod=signal_window
    )
    rsi_period = 14
    historical_data['rsi'] = talib.RSI(historical_data['Close'], timeperiod=rsi_period)
    historical_data['volume'] = historical_data['Volume']
    logging.info(f"Technical indicators calculated for {symbols}.")
    return historical_data

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_rsi(symbols, period=14, interval='5m'):
    logging.info(f"Calculating RSI for {symbols} (period={period}, interval={interval})...")
    historical_data = api.get_historical_data(symbols, '5d', interval, prepost=True)
    if historical_data.empty or len(historical_data['Close']) < period:
        logging.error(f"Insufficient data for RSI calculation for {symbols} with {interval} interval.")
        return None
    rsi = talib.RSI(historical_data['Close'], timeperiod=period)
    latest_rsi = rsi[-1] if len(rsi) > 0 else None
    if latest_rsi is None or not np.isfinite(latest_rsi):
        logging.error(f"Invalid RSI value for {symbols}: {latest_rsi}")
        return None
    latest_rsi = round(latest_rsi, 2)
    logging.info(f"RSI for {symbols}: {latest_rsi}")
    return latest_rsi

def print_technical_indicators(symbols, historical_data):
    logging.info(f"Technical Indicators for {symbols}:")
    print(f"\nTechnical Indicators for {symbols}:\n")
    print(historical_data[['Close', 'macd', 'signal', 'rsi', 'volume']].tail())
    print("")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def calculate_cash_on_hand():
    logging.info("Calculating available cash...")
    with buy_sell_lock:
        cash_available = round(float(api.get_account()['cash']), 2)
    logging.info(f"Cash available: ${cash_available:.2f}")
    return cash_available

def calculate_total_symbols(symbols_to_buy_list):
    logging.info("Calculating total symbols to trade...")
    total_symbols = len(symbols_to_buy_list)
    logging.info(f"Total symbols: {total_symbols}")
    return total_symbols

def allocate_cash_equally(cash_available, total_symbols):
    logging.info("Allocating cash equally among symbols...")
    max_allocation_per_symbol = 600.0
    allocation_per_symbol = min(max_allocation_per_symbol, cash_available / total_symbols) if total_symbols > 0 else 0
    allocation = round(allocation_per_symbol, 2)
    logging.info(f"Allocation per symbol: ${allocation:.2f}")
    return allocation

def get_previous_price(symbols):
    logging.info(f"Retrieving previous price for {symbols}...")
    if symbols in previous_prices:
        price = previous_prices[symbols]
        logging.info(f"Previous price for {symbols}: ${price:.4f}")
        return price
    with buy_sell_lock:
        current_price = api.get_current_price(symbols)
    previous_prices[symbols] = current_price
    logging.info(f"No previous price for {symbols}. Using current price: {current_price}")
    return current_price

def update_previous_price(symbols, current_price):
    logging.info(f"Updating previous price for {symbols} to ${current_price:.4f}")
    previous_prices[symbols] = current_price

def run_schedule():
    logging.info("Running schedule for pending tasks...")
    while not end_time_reached():
        schedule.run_pending()
        time.sleep(1)
    logging.info("Schedule completed.")

def track_price_changes(symbols):
    logging.info(f"Tracking price changes for {symbols}...")
    with buy_sell_lock:
        current_price = api.get_current_price(symbols)
    previous_price = get_previous_price(symbols)
    print_technical_indicators(symbols, calculate_technical_indicators(symbols))
    if symbols not in price_changes:
        price_changes[symbols] = {'increased': 0, 'decreased': 0}
    if current_price and previous_price:
        if current_price > previous_price:
            price_changes[symbols]['increased'] += 1
            logging.info(f"{symbols} price increased | current price: {current_price}")
        elif current_price < previous_price:
            price_changes[symbols]['decreased'] += 1
            logging.info(f"{symbols} price decreased | current price: {current_price}")
        else:
            logging.info(f"{symbols} price unchanged | current price: {current_price}")
        update_previous_price(symbols, current_price)

def end_time_reached():
    reached = time.time() >= end_time
    logging.info(f"Checking if end time reached: {'Yes' if reached else 'No'}")
    return reached

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_last_price_within_past_5_minutes(symbols_to_buy_list):
    logging.info("Fetching last prices within past 5 minutes for symbols...")
    results = {}
    eastern = pytz.timezone('US/Eastern')
    current_datetime = datetime.now(eastern)
    end_time = current_datetime
    start_time = end_time - timedelta(minutes=5)
    for symbols_to_buy in symbols_to_buy_list:
        logging.info(f"Fetching 5-minute price data for {symbols_to_buy}...")
        try:
            historical_data = api.get_historical_data(symbols_to_buy, '5d', '1m', prepost=True)
            if not historical_data.empty:
                data = historical_data[start_time:end_time]
                if not data.empty:
                    last_price = round(float(data['Close'].iloc[-1]), 2)
                    results[symbols_to_buy] = last_price
                    logging.info(f"Last price for {symbols_to_buy} within 5 minutes: ${last_price:.2f}")
                else:
                    results[symbols_to_buy] = None
                    logging.info(f"No price data for {symbols_to_buy} within past 5 minutes.")
            else:
                results[symbols_to_buy] = None
                logging.info(f"No price data for {symbols_to_buy} within past 5 minutes.")
        except Exception as e:
            logging.error(f"Error fetching data for {symbols_to_buy}: {e}")
            results[symbols_to_buy] = None
    return results

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def get_most_recent_purchase_date(symbols_to_sell):
    logging.info(f"Retrieving most recent purchase date for {symbols_to_sell}...")
    try:
        purchase_date_str = None
        order_list = []
        CHUNK_SIZE = 500
        end_time = datetime.now(pytz.UTC).isoformat()
        while True:
            logging.info(f"Fetching orders for {symbols_to_sell} until {end_time}...")
            order_chunk = api.list_orders(
                status='all',
                direction='desc',
                until=end_time,
                limit=CHUNK_SIZE,
                symbols=[symbols_to_sell]
            )
            if order_chunk:
                order_list.extend(order_chunk)
                end_time = (pd.to_datetime(order_chunk[-1].submitted_at) - timedelta(seconds=1)).isoformat()
                logging.info(f"Fetched {len(order_chunk)} orders for {symbols_to_sell}.")
            else:
                logging.info(f"No more orders to fetch for {symbols_to_sell}.")
                break
        buy_orders = [
            order for order in order_list
            if order.side == 'buy' and order.status == 'filled' and order.filled_at
        ]
        if buy_orders:
            most_recent_buy = max(buy_orders, key=lambda order: pd.to_datetime(order.filled_at))
            purchase_date = pd.to_datetime(most_recent_buy.filled_at).date()
            purchase_date_str = purchase_date.strftime("%Y-%m-%d")
            logging.info(f"Most recent purchase date for {symbols_to_sell}: {purchase_date_str}")
        else:
            purchase_date = datetime.now(pytz.UTC).date()
            purchase_date_str = purchase_date.strftime("%Y-%m-%d")
            logging.warning(f"No filled buy orders found for {symbols_to_sell}. Using today's date: {purchase_date_str}")
        return purchase_date_str
    except Exception as e:
        logging.error(f"Error fetching buy orders for {symbols_to_sell}: {e}")
        purchase_date = datetime.now(pytz.UTC).date()
        purchase_date_str = purchase_date.strftime("%Y-%m-%d")
        logging.info(f"Error fetching buy orders for {symbols_to_sell}. Using today's date: {purchase_date_str}")
        return purchase_date_str

def buy_stocks(symbols_to_sell_dict, symbols_to_buy_list, buy_sell_lock):
    global current_price, buy_signal, price_history, last_stored
    logging.info("Starting buy_stocks function...")
    if not symbols_to_buy_list:
        logging.info("No symbols to buy.")
        return
    symbols_to_remove = []
    buy_signal = 0
    with buy_sell_lock:
        account = api.get_account()
    total_equity = float(account['equity'])
    logging.info(f"Total account equity: ${total_equity:.2f}")
    positions = api.list_positions()
    current_exposure = sum(float(pos.market_value) for pos in positions)
    max_new_exposure = total_equity * 0.98 - current_exposure
    if max_new_exposure <= 0:
        logging.info("Portfolio exposure limit reached. No new buys.")
        return
    logging.info(f"Current exposure: ${current_exposure:.2f}, Max new exposure: ${max_new_exposure:.2f}")
    valid_symbols = []
    logging.info("Filtering valid symbols for buying...")
    for symbols_to_buy in symbols_to_buy_list:
        with buy_sell_lock:
            current_price = api.get_current_price(symbols_to_buy)
        if current_price is None:
            logging.info(f"No valid price data for {symbols_to_buy}. Skipping.")
            continue
        historical_data = calculate_technical_indicators(symbols_to_buy, lookback_days=5)
        if historical_data.empty:
            logging.info(f"No historical data for {symbols_to_buy}. Skipping.")
            continue
        valid_symbols.append(symbols_to_buy)
    if not valid_symbols:
        logging.info("No valid symbols to buy after filtering.")
        return
    for symbols_to_buy in valid_symbols:
        logging.info(f"Processing {symbols_to_buy}...")
        today_date = datetime.today().date()
        today_date_str = today_date.strftime("%Y-%m-%d")
        current_datetime = datetime.now(pytz.timezone('US/Eastern'))
        current_time_str = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
        with buy_sell_lock:
            current_price = api.get_current_price(symbols_to_buy)
        if current_price is None:
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
                logging.info(f"Stored price {current_price} for {symbols_to_buy} at {interval} interval.")
        historical_data = api.get_historical_data(symbols_to_buy, '5d', '5m', prepost=True)
        if historical_data.empty or len(historical_data) < 3:
            logging.info(f"Insufficient historical data for {symbols_to_buy} (rows: {len(historical_data)}). Skipping.")
            continue
        logging.info(f"Calculating volume metrics for {symbols_to_buy}...")
        recent_avg_volume = historical_data['Volume'].iloc[-5:].mean() if len(historical_data) >= 5 else 0
        prior_avg_volume = historical_data['Volume'].iloc[-10:-5].mean() if len(historical_data) >= 10 else recent_avg_volume
        volume_decrease = recent_avg_volume < prior_avg_volume if len(historical_data) >= 10 else False
        logging.info(f"{symbols_to_buy}: Recent avg volume = {recent_avg_volume:.0f}, Prior avg volume = {prior_avg_volume:.0f}, Volume decrease = {volume_decrease}")
        logging.info(f"Calculating RSI metrics for {symbols_to_buy}...")
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
        logging.info(f"{symbols_to_buy}: Latest RSI = {latest_rsi:.2f}, Recent avg RSI = {recent_avg_rsi:.2f}, Prior avg RSI = {prior_avg_rsi:.2f}, RSI decrease = {rsi_decrease}")
        logging.info(f"Calculating MACD for {symbols_to_buy}...")
        short_window = 12
        long_window = 26
        signal_window = 9
        macd, macd_signal, _ = talib.MACD(close_prices, fastperiod=short_window, slowperiod=long_window, signalperiod=signal_window)
        latest_macd = macd[-1] if len(macd) > 0 else None
        latest_macd_signal = macd_signal[-1] if len(macd_signal) > 0 else None
        macd_above_signal = latest_macd > latest_macd_signal if latest_macd is not None else False
        logging.info(f"{symbols_to_buy}: MACD = {latest_macd:.2f}, Signal = {latest_macd_signal:.2f}, MACD above signal = {macd_above_signal}")
        previous_price = get_previous_price(symbols_to_buy)
        price_increase = current_price > previous_price * 1.005 if current_price and previous_price else False
        logging.info(f"{symbols_to_buy}: Price increase check: Current = ${current_price:.2f}, Previous = ${previous_price:.2f}, Increase = {price_increase}")
        logging.info(f"Checking price drop for {symbols_to_buy}...")
        last_prices = get_last_price_within_past_5_minutes([symbols_to_buy])
        last_price = last_prices.get(symbols_to_buy)
        if last_price is None:
            try:
                historical_data = api.get_historical_data(symbols_to_buy, '1d', '1d')
                last_price = round(float(historical_data['Close'].iloc[-1]), 4)
                logging.info(f"No price found for {symbols_to_buy} in past 5 minutes. Using last closing price: {last_price}")
            except Exception as e:
                logging.error(f"Error fetching last closing price for {symbols_to_buy}: {e}")
                continue
        price_decline_threshold = last_price * (1 - 0.002)
        price_decline = current_price <= price_decline_threshold if current_price and last_price else False
        logging.info(f"{symbols_to_buy}: Price decline check: Current = ${current_price:.2f}, Threshold = ${price_decline_threshold:.2f}, Decline = {price_decline}")
        short_term_trend = None
        if symbols_to_buy in price_history and '5min' in price_history[symbols_to_buy] and len(price_history[symbols_to_buy]['5min']) >= 2:
            recent_prices = price_history[symbols_to_buy]['5min'][-2:]
            short_term_trend = 'up' if recent_prices[-1] > recent_prices[-2] else 'down'
            logging.info(f"{symbols_to_buy}: Short-term price trend (5min): {short_term_trend}")
        logging.info(f"Checking for bullish reversal patterns in {symbols_to_buy}...")
        open_prices = historical_data['Open'].values
        high_prices = historical_data['High'].values
        low_prices = historical_data['Low'].values
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
                logging.error(f"IndexError in candlestick pattern detection for {symbols_to_buy}: {e}")
                continue
        if detected_patterns:
            logging.info(f"{symbols_to_buy}: Detected bullish reversal patterns at candle {reversal_candle_index}: {', '.join(detected_patterns)}")
            if symbols_to_buy in price_history:
                for interval, prices in price_history[symbols_to_buy].items():
                    if prices:
                        logging.info(f"{symbols_to_buy}: Price history at {interval}: {prices[-5:]}")
        if price_decline:
            logging.info(f"{symbols_to_buy}: Price decline >= 0.2% detected (Current price = {current_price:.2f} <= Threshold = {price_decline_threshold:.2f})")
        if volume_decrease:
            logging.info(f"{symbols_to_buy}: Volume decrease detected (Recent avg = {recent_avg_volume:.0f} < Prior avg = {prior_avg_volume:.0f})")
        if rsi_decrease:
            logging.info(f"{symbols_to_buy}: RSI decrease detected (Recent avg = {recent_avg_rsi:.2f} < Prior avg = {prior_avg_rsi:.2f})")
        if not is_in_uptrend(symbols_to_buy):
            logging.info(f"{symbols_to_buy}: Not in uptrend (below 200-day SMA). Skipping.")
            continue
        daily_rsi = get_daily_rsi(symbols_to_buy)
        if daily_rsi is None or daily_rsi > 50:
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
            logging.info(f"{symbols_to_buy}: Buy score too low ({score} < 3). Skipping.")
            continue
        if ALL_BUY_ORDERS_ARE_1_DOLLAR:
            total_cost_for_qty = 1.00
            qty = round(total_cost_for_qty / current_price, 4)
            logging.info(f"{symbols_to_buy}: Using $1.00 fractional share order mode. Qty = {qty:.4f}")
        else:
            logging.info(f"Calculating position size for {symbols_to_buy}...")
            atr = get_average_true_range(symbols_to_buy)
            if atr is None:
                logging.info(f"No ATR for {symbols_to_buy}. Skipping.")
                continue
            stop_loss_distance = 2 * atr
            risk_per_share = stop_loss_distance
            risk_amount = 0.01 * total_equity
            qty = risk_amount / risk_per_share if risk_per_share > 0 else 0
            total_cost_for_qty = qty * current_price
            with buy_sell_lock:
                cash_available = round(float(api.get_account()['cash']), 2)
                logging.info(f"Cash available for {symbols_to_buy}: ${cash_available:.2f}")
                total_cost_for_qty = min(total_cost_for_qty, cash_available - 1.00, max_new_exposure)
                if total_cost_for_qty < 1.00:
                    logging.info(f"Insufficient risk-adjusted allocation for {symbols_to_buy}.")
                    continue
                qty = round(total_cost_for_qty / current_price, 4)
            estimated_slippage = total_cost_for_qty * 0.001
            total_cost_for_qty -= estimated_slippage
            qty = round(total_cost_for_qty / current_price, 4)
            logging.info(f"{symbols_to_buy}: Adjusted for slippage (0.1%): Notional = ${total_cost_for_qty:.2f}, Qty = {qty:.4f}")
        with buy_sell_lock:
            cash_available = round(float(api.get_account()['cash']), 2)
        if total_cost_for_qty < 1.00:
            logging.info(f"Order amount for {symbols_to_buy} is ${total_cost_for_qty:.2f}, below minimum $1.00")
            continue
        if cash_available < total_cost_for_qty + 1.00:
            logging.info(f"Insufficient cash for {symbols_to_buy}. Available: ${cash_available:.2f}, Required: ${total_cost_for_qty:.2f}")
            continue
        if buy_conditions_met:
            buy_signal = 1
            reason = f"bullish reversal ({', '.join(detected_patterns)}), {specific_reason}"
            logging.info(f"Submitting buy order for {symbols_to_buy}...")
            try:
                total_cost_for_qty = round(total_cost_for_qty, 2)
                buy_order = api.submit_order(
                    symbol=symbols_to_buy,
                    notional=total_cost_for_qty,
                    side='buy',
                    type='market',
                    time_in_force='day'
                )
                logging.info(f"{current_time_str} Submitted buy order for {qty:.4f} shares of {symbols_to_buy} at {current_price:.2f} (notional: ${total_cost_for_qty:.2f}) due to {reason}")
                order_filled = False
                filled_qty = 0
                filled_price = current_price
                for _ in range(30):
                    logging.info(f"Checking order status for {symbols_to_buy}...")
                    order_status = api.get_order(buy_order.id)
                    if order_status.status == 'filled':
                        order_filled = True
                        filled_qty = float(order_status.filled_qty)
                        filled_price = float(order_status.filled_avg_price or current_price)
                        with buy_sell_lock:
                            cash_available = round(float(api.get_account()['cash']), 2)
                        actual_cost = filled_qty * filled_price
                        logging.info(f"Order filled for {filled_qty:.4f} shares of {symbols_to_buy} at ${filled_price:.2f}, actual cost: ${actual_cost:.2f}")
                        break
                    time.sleep(2)
                if order_filled:
                    logging.info(f"Logging trade for {symbols_to_buy}...")
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
                    if api.get_account()['daytrade_count'] < 3 and not ALL_BUY_ORDERS_ARE_1_DOLLAR:
                        stop_order_id = place_trailing_stop_sell_order(symbols_to_buy, filled_qty, filled_price)
                        if stop_order_id:
                            logging.info(f"Trailing stop sell order placed for {symbols_to_buy} with ID: {stop_order_id}")
                        else:
                            logging.info(f"Failed to place trailing stop sell order for {symbols_to_buy}")
                else:
                    logging.info(f"Buy order not filled for {symbols_to_buy}")
            except APIError as e:
                logging.error(f"Error submitting buy order for {symbols_to_buy}: {e}")
                continue
        else:
            logging.info(f"{symbols_to_buy}: Conditions not met for any detected patterns.")
        update_previous_price(symbols_to_buy, current_price)
        time.sleep(0.8)
    try:
        with buy_sell_lock:
            logging.info("Updating database with buy transactions...")
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
            logging.info("Database updated successfully.")
            refresh_after_buy()
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"Database error: {str(e)}")

def refresh_after_buy():
    global symbols_to_buy, symbols_to_sell_dict
    logging.info("Refreshing after buy operation...")
    time.sleep(2)
    symbols_to_buy = get_symbols_to_buy()
    symbols_to_sell_dict = update_symbols_to_sell_from_api()
    logging.info("Refresh complete.")

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def place_trailing_stop_sell_order(symbols_to_sell, qty, current_price):
    logging.info(f"Attempting to place trailing stop sell order for {symbols_to_sell}...")
    try:
        if qty != int(qty):
            logging.error(f"Skipped trailing stop sell order for {symbols_to_sell}: Fractional quantity {qty:.4f} not allowed.")
            return None
        stop_loss_percent = 1.0
        logging.info(f"Placing trailing stop sell order for {qty} shares of {symbols_to_sell} with trail percent {stop_loss_percent}%")
        stop_order = api.submit_order(
            symbol=symbols_to_sell,
            qty=int(qty),
            side='sell',
            type='trailing_stop',
            trail_percent=str(stop_loss_percent),
            time_in_force='gtc'
        )
        logging.info(f"Placed trailing stop sell order for {qty} shares of {symbols_to_sell} with ID: {stop_order.id}")
        return stop_order.id
    except APIError as e:
        logging.error(f"Error placing trailing stop sell order for {symbols_to_sell}: {str(e)}")
        return None

@sleep_and_retry
@limits(calls=CALLS, period=PERIOD)
def update_symbols_to_sell_from_api():
    logging.info("Updating symbols to sell from Public API...")
    try:
        positions = api.list_positions()
        symbols_to_sell_dict = {}
        for pos in positions:
            symbol = pos.symbol
            qty = float(pos.qty)
            avg_price = float(pos.avg_entry_price)
            purchase_date = get_most_recent_purchase_date(symbol)
            if qty > 0:
                symbols_to_sell_dict[symbol] = (avg_price, purchase_date)
        logging.info(f"Updated symbols to sell: {list(symbols_to_sell_dict.keys())}")
        return symbols_to_sell_dict
    except Exception as e:
        logging.error(f"Error updating symbols to sell: {e}")
        return {}

# Global variables for price tracking
previous_prices = {}
price_changes = {}

def main():
    global symbols_to_buy, symbols_to_sell_dict, end_time
    logging.info("Starting trading bot...")
    stop_if_stock_market_is_closed()
    symbols_to_buy = get_symbols_to_buy()
    symbols_to_sell_dict = update_symbols_to_sell_from_api()
    if PRINT_SYMBOLS_TO_BUY:
        logging.info(f"Symbols to buy: {symbols_to_buy}")
        print(f"Symbols to buy: {symbols_to_buy}")
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
        logging.info("Trading bot stopped by user.")
        print("Trading bot stopped by user.")
    except Exception as e:
        logging.error(f"Fatal error in main: {e}")
        print(f"Fatal error in main: {e}")
    finally:
        session.close()
