import os
import requests

# Get account ID from environment variable
account_Id = os.getenv("ACCOUNT_ID")
if not account_Id:
    raise ValueError("Please set the ACCOUNT_ID environment variable.")

# API endpoint
url = f"https://api.public.com/userapigateway/trading/{account_Id}/portfolio/v2"

# If an API token is needed, include it here
headers = {
    "Authorization": f"Bearer {os.getenv('PUBLIC_API_TOKEN')}",  # optional
    "Content-Type": "application/json"
}

# Make the GET request
response = requests.get(url, headers=headers)

# Process response
if response.status_code == 200:
    account_details = response.json()
    print("Account details:", account_details)
else:
    print(f"Failed to fetch account details. Status code: {response.status_code}")
    print(response.text)
