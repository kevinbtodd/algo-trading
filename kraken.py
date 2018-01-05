import requests
import time
import ast

URL = "https://api.kraken.com/0/public/Ticker"
URL_ID = 4

def get_tick():

    pairs = {
        'pair': 'XBTUSD'
    }

    while True:


        timestamp = time.time()

        try:
            response = requests.get(URL, params = pairs)
        except:
            time.sleep(1)
            continue

        if not (response.status_code == requests.codes.ok):
            time.sleep(1)
            continue

        # approx timestamp = unix timestamp when request sent + response time
        try:
            timestamp += response.elapsed.total_seconds()
            timestamp = int(round(timestamp))
        except:
            time.sleep(1)
            continue

        try:
            tick = ast.literal_eval(response.text)
        except:
            time.sleep(1)
            continue

        try:
            data = tick['result']

            data = data['XXBTZUSD']

            ask = data['a']
            ask = ask[0]

            bid = data['b']
            bid = bid[0]

            tick2 = {'bid': bid, 'ask': ask, 'timestamp': timestamp}
            tick2['id'] = URL_ID
        except:
            time.sleep(1)
            continue

        return tick2

