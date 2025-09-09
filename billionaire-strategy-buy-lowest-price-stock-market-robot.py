import os
import requests
import time
import json
from uuid import uuid4

# Load environment variable
secret_key = os.getenv('PUBLIC_API_ACCESS_TOKEN')

if not secret_key:
    raise ValueError("PUBLIC_API_ACCESS_TOKEN environment variable is not set.")

headers = {
    "Authorization": f"Bearer {secret_key}",
    "Content-Type": "application/json"
}

BASE_URL = "https://api.public.com/userapigateway"

def get_accounts():
    """Get the list of accounts and return the first account ID."""
    response = requests.get(f"{BASE_URL}/accounts", headers=headers)
    if response.status_code == 200:
        accounts = response.json()
        if accounts:
            return accounts[0]['id']  # Assuming the first account is the primary one
    print(f"Error getting accounts: {response.status_code} - {response.text}")
    return None

def get_price(symbol):
    """Obtain the current price/quote for a symbol from Public.com API."""
    response = requests.get(f"{BASE_URL}/quotes/{symbol}", headers=headers)
    if response.status_code == 200:
        quote = response.json()
        price = quote.get('last') or quote.get('close', 'N/A')
        return price
    else:
        print(f"Error getting price for {symbol}: {response.status_code} - {response.text}")
        return None

def place_market_order(account_id, symbol, side, quantity):
    """
    Place a market buy or sell order using the specified Public.com order format.
    side: 'BUY' or 'SELL'
    quantity: Number of shares (as string per API format)
    """
    url = f"{BASE_URL}/trading/{account_id}/order"
    request_body = {
        "orderId": str(uuid4()),  # Generate unique order ID
        "instrument": {
            "symbol": symbol,
            "type": "EQUITY"
        },
        "orderSide": side.upper(),
        "orderType": "MARKET",
        "expiration": {
            "timeInForce": "DAY"  # Day order for market orders
        },
        "quantity": str(quantity),
        "amount": "0",  # Not used for market orders
        "limitPrice": "0",  # Not used for market orders
        "stopPrice": "0",  # Not used for market orders
        "openCloseIndicator": "OPEN"
    }

    response = requests.post(url, headers=headers, json=request_body)
    if response.status_code in (200, 201):
        order = response.json()
        print(f"Order placed successfully: {json.dumps(order, indent=2)}")
        return order
    else:
        print(f"Error placing {side} order for {symbol}: {response.status_code} - {response.text}")
        return None

def trading_robot(symbol="AAPL", quantity=1, buy_threshold=150.0, sell_threshold=200.0, interval=60):
    """
    Simple trading bot that monitors the price of a symbol.
    Buys if price < buy_threshold, sells if price > sell_threshold.
    Runs indefinitely, checking every 'interval' seconds.
    WARNING: This is for educational purposes. Do not use in production without proper testing and risk controls.
    """
    account_id = get_accounts()
    if not account_id:
        print("Cannot proceed without account ID.")
        return

    print(f"Starting trading robot for {symbol}. Buy below ${buy_threshold}, Sell above ${sell_threshold}.")
    print("Press Ctrl+C to stop.")

    while True:
        try:
            price = get_price(symbol)
            if price is None:
                time.sleep(interval)
                continue

            print(f"Current price of {symbol}: ${price}")

            if price < buy_threshold:
                print(f"Price ${price} < ${buy_threshold}. Placing buy order.")
                place_market_order(account_id, symbol, "BUY", quantity)
            elif price > sell_threshold:
                print(f"Price ${price} > ${sell_threshold}. Placing sell order.")
                place_market_order(account_id, symbol, "SELL", quantity)
            else:
                print(f"Price ${price} within range. Holding.")

            time.sleep(interval)

        except KeyboardInterrupt:
            print("Trading robot stopped by user.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}")
            time.sleep(interval)

if __name__ == "__main__":
    trading_robot()
