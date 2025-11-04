import time
import requests
from decimal import Decimal

def ray_to_percent(ray_value: str):
    return float(Decimal(ray_value) / Decimal(10**27) * 100)

def fetch_interest_rate_history(chain: str, token: str, base_url: str = "https://api.hyperlend.finance"):
    url = f"{base_url}/data/interestRateHistory"
    resp = requests.get(url, params={"chain": chain, "token": token}, timeout=20)
    resp.raise_for_status()
    return resp.json(), token

def get_price(coin="HYPE", interval="1m"):
    url = "https://api.hyperliquid.xyz/info"
    now = int(time.time() * 1000)
    # request last 1 interval worth of candles
    start = now - 60 * 1000  # 1 minute ago
    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin": coin,
            "interval": interval,
            "startTime": start,
            "endTime": now
        }
    }

    headers = {"Content-Type": "application/json"}
    r = requests.post(url, json=payload, headers=headers)
    data = r.json()
    if not data:
        raise Exception("No candle data returned")

    last_candle = data[-1]
    price = float(last_candle["c"])
    return price