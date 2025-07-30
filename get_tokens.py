import requests

BASE_URL = "https://www.okx.com"

def get_okx_symbols():
    r = requests.get(f"{BASE_URL}/api/v5/public/instruments?instType=SWAP").json()
    symbols = [s["instId"] for s in r["data"]]
    return symbols

print(get_okx_symbols())


import requests

BASE_URL = "https://fapi.binance.com"

def get_binance_symbols():
    r = requests.get(f"{BASE_URL}/fapi/v1/exchangeInfo").json()
    symbols = [s["symbol"] for s in r["symbols"] if s["contractType"] == "PERPETUAL"]
    return symbols

print(get_binance_symbols())
