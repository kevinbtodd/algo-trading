import requests
import ast
import time

URL = "https://api.bitfinex.com/v1/pubticker/btcusd"
URL_ID = 3

def get_tick():

    while True:

        try:
            response = requests.get(URL)

        except:
            time.sleep(1)
            continue

        if not (response.status_code == requests.codes.ok):
            time.sleep(1)
            continue

        try:
            tick = ast.literal_eval(response.text)
        except:
            time.sleep(1)
            continue

        tick['timestamp'] = int(round(float(tick['timestamp'])))

        tick['id'] = URL_ID

        return tick
