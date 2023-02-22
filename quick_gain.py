import asyncio
import websockets
import json
import time
import pandas as pd
import gate_api
from datetime import datetime
import telegram_send
import api_keys

key = api_keys.gate_key
secret = api_keys.gate_secret
configuration = gate_api.Configuration(
  host = "https://api.gateio.ws/api/v4",
  key=key,
  secret=secret
)
api_client = gate_api.ApiClient(configuration)
ga_client = gate_api.SpotApi(api_client)


all_pairs = ga_client.list_tickers()
good_pairs = [x.currency_pair for x in all_pairs if float(x.quote_volume) < 80000 and float(x.quote_volume)>1000]
good_pairs = [pair for pair in good_pairs if '5L' not in pair]
good_pairs = [pair for pair in good_pairs if '5S' not in pair]
good_pairs = [pair for pair in good_pairs if '3S' not in pair]
good_pairs = [pair for pair in good_pairs if '3L' not in pair]


# getting past 5 min candles

five_mins = {}
means = {}
rising_dict = {}
bought_pairs = []

for pair in good_pairs:
  candle_data = ga_client.list_candlesticks(pair, limit=11, interval='5m')
  temp_list = [float(x[2]) for x in candle_data][:10]
  five_mins[pair] = temp_list
  means[pair] = sum(temp_list)/len(temp_list)
  rising_dict[pair] = (temp_list[-1]/means[pair]) - 1 > 0.01
  time.sleep(0.01)



async def update_five():
  global five_mins
  global means
  global bought_pairs
  global rising_dict

  while True:

    await asyncio.sleep(60*5)
    tickers = ga_client.list_tickers()
    for x in tickers:
      if x.currency_pair in good_pairs:
        temp_list = five_mins[x.currency_pair]
        _ = temp_list.pop(0)
        temp_list.append(float(x.last))
        five_mins[x.currency_pair] = temp_list
        means[x.currency_pair] = sum(temp_list)/len(temp_list)
        rising_dict[x.currency_pair] = (float(x.last)/means[x.currency_pair]) -1 > 0.01



async def handle_ga_socket():
  global five_mins
  global rising_dict
  ga_uri = "wss://api.gateio.ws/ws/v4/"
  async with websockets.connect(ga_uri) as websocket:
    await websocket.send(json.dumps({
                                    "time": int(time.time()),
                                    "channel": "spot.trades",
                                    "event": "subscribe",
                                    "payload": good_pairs
                                    }))
    async for message in websocket:
      message = json.loads(message)
      if message['event'] == 'update':
        pair = message['result']['currency_pair']
        if pair not in bought_pairs:
          price = float(message['result']['price'])
          pair_prev = five_mins[pair][-1]
          perc = (price/pair_prev) -1
          if perc > 0.035 and rising_dict[pair]:
            bought_pairs.append(pair)
            telegram_send.send(messages=[f"TOP GAINERS\n{pair}\nPrice: {str(price)}\nRise: {perc}"])

async def handler():
  await asyncio.wait([update_five(), handle_ga_socket()])




print("starting")
asyncio.get_event_loop().run_until_complete(handler())