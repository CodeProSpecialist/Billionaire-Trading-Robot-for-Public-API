import os
import requests
import uuid

# below is correct API secret key reading code
secret = os.getenv("YOUR_SECRET_KEY")

# Authorization
url = "https://api.public.com/userapiauthservice/personal/access-tokens"
headers = {
    "Content-Type": "application/json"
}

request_body = {
  "validityInMinutes": 1440,
  "secret": secret
}

response = requests.post(url, headers=headers, json=request_body)
access = response.json()["accessToken"]

# Account Id
url = "https://api.public.com/userapigateway/trading/account"
headers = {
    "Authorization": f"Bearer {access}",
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers)
accountId = response.json()["accounts"][0]["accountId"]

# Portfolio
url = f"https://api.public.com/userapigateway/trading/{accountId}/portfolio/v2"
headers = {
    "Authorization": f"Bearer {access}",
    "Content-Type": "application/json"
}

response = requests.get(url, headers=headers)
data = response.json()
data.pop("accountId", None)
print(data)
