import asyncio
from pickle import GLOBAL
import websockets
import json
import time
import pandas as pd
import gate_api
from binance.client import Client
from datetime import datetime
import telegram_send
import api_keys

bi_client = Client(None, None)

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
good_pairs = [x.currency_pair for x in all_pairs if float(x.quote_volume) > 20000 and float(x.quote_volume) < 5000000]
good_pairs = [pair for pair in good_pairs if '5L' not in pair]
good_pairs = [pair for pair in good_pairs if '5S' not in pair]
good_pairs = [pair for pair in good_pairs if '3S' not in pair]
good_pairs = [pair for pair in good_pairs if '3L' not in pair]

bi_pairs = bi_client.get_all_tickers()
bi_pairs = [x['symbol'] for x in bi_pairs if 'USDT' in x['symbol']]
bi_pairs = [x.replace('USDT', '_USDT') for x in bi_pairs]

shared_pairs = []


for x in bi_pairs:
  if x in good_pairs:
    shared_pairs.append(x)

print(len(shared_pairs))

# shared_pairs=shared_pairs[:50]


base_uri = 'wss://stream.binance.com:9443/stream?streams='
for pair in shared_pairs:
  base_uri = f"{base_uri}{pair.replace('_', '').lower()}@bookTicker/"

base_uri = base_uri[:-1]

lock = asyncio.Lock()
gate_prices = {}
bi_prices = {}
exclude_pairs = []

for pair in all_pairs:
  if float(pair.change_percentage) > 30:
    exclude_pairs.append(pair.currency_pair)

day_prices =  {x.currency_pair:float(x.last)*(100/(100+float(x.change_percentage))) for x in all_pairs}

ga_uri = "wss://api.gateio.ws/ws/v4/"

bi_connections = set()
# bi_connections.add('wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker/ethusdt@bookTicker')
bi_connections.add(base_uri)


async def handle_bi_socket(uri, ):
  global bi_prices
  async with websockets.connect(uri) as websocket:
    async for message in websocket:
      message = json.loads(message)
      bi_prices[message['data']['s'].replace('USDT', '_USDT')] = float(message['data']['a'])
      # idx = message['data']['s'].replace('USDT', '_USDT')
      # price = float(message['data']['a'])
      # all_prices.loc[idx, 'binance'] = price
      # ga_price = all_prices.loc[idx, 'gate']
      # if ga_price:
      #   all_prices.loc[idx, 'difference'] = (price/ga_price - 1) * 100


async def handle_ga_socket(uri, ):
  global gate_prices
  async with websockets.connect(uri) as websocket:
    await websocket.send(json.dumps({
                                    "time": int(time.time()),
                                    "channel": "spot.book_ticker",
                                    "event": "subscribe",
                                    "payload": shared_pairs
                                    }))
    async for message in websocket:
      message = json.loads(message)
      if message['event'] == 'update':
        gate_prices[message["result"]["s"]] = float(message["result"]["a"])





async def checker():
  # global 
  global gate_prices
  global bi_prices
  global exclude_pairs
  global day_prices
  while True:
    df = pd.concat([pd.Series(gate_prices, dtype='float32'), pd.Series(bi_prices, dtype='float32')], axis=1)
    df = df.rename({0:'gate', 1:'binance'},axis=1)
    df['difference'] = (df['binance']/df['gate']-1)*100
    df = df.dropna().sort_values(by=['difference'], ascending=False)
    df = df[df['difference'] > 0.2]
    for index, row in df.iterrows():
      if index not in exclude_pairs and row['gate']>1.1*(day_prices[index]):
        telegram_send.send(messages=[f"{index}\nPrice: {row['gate']}\nArbitrage: {'{:0.2f}'.format(row['difference'])}%"])
        exclude_pairs.append(index)

    await asyncio.sleep(0.01)

async def clear_exclude():
  global exclude_pairs
  while True:
    await asyncio.sleep(60*60*24)
    exclude_pairs = []

async def update_day():
  global day_prices
  while True:
    all_pairs = ga_client.list_tickers()
    day_prices = {x.currency_pair:float(x.last)*(100/(100+float(x.change_percentage))) for x in all_pairs}
    telegram_send.send(messages=["Still working"])
    await asyncio.sleep(60*60*1)

async def handler():
  async with lock:
    await asyncio.wait([handle_ga_socket(ga_uri), checker(), clear_exclude(), update_day()] + [handle_bi_socket(uri) for uri in bi_connections])


print("Starting")
asyncio.get_event_loop().run_until_complete(handler())