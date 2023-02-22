import time
import pandas as pd
import telegram_send
import gate_api
import logging
from datetime import datetime, timedelta
from gate_api.exceptions import ApiException, GateApiException
from logging.handlers import RotatingFileHandler
import traceback
import api_keys

rfh = RotatingFileHandler(
    filename='/home/ubuntu/gacha/logs/orderbook_scanner.log', 
    mode='a',
    maxBytes=5*1024*1024,
    backupCount=2,
    encoding=None,
    delay=0
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)-8s %(levelname)-8s %(message)s",
    datefmt="%y-%m-%d %H:%M:%S",
    handlers=[
        rfh
    ]
)
logger = logging.getLogger('main')


key = api_keys.gate_key
secret = api_keys.gate_secret


configuration = gate_api.Configuration(
  host = "https://api.gateio.ws/api/v4",
  key=key,
  secret=secret
)

api_client = gate_api.ApiClient(configuration)
client = gate_api.SpotApi(api_client)

url = "https://api.gateio.ws/api/v4/spot/tickers"

def find_orderbooks(pairs:list):
  df = pd.DataFrame()
  for pair in pairs:
    ticker = client.list_tickers(currency_pair=pair)[0].to_dict()
    last = float(ticker['last'])
    growth = float(ticker['change_percentage'])
    if growth > 0:
      orderbook = client.list_order_book(pair, limit=500).to_dict()

      asks = orderbook['asks']
      bids = orderbook['bids']

      ask_end = last*1.2
      bid_end = last*0.95

      ask_volume = 0
      bid_volume = 0

      #Getting ask volume
      ask_index = 0
      ask_check_flag = last
      while ask_check_flag <= ask_end and ask_index<len(asks):
        ask_price = float(asks[ask_index][0])
        ask_coins = float(asks[ask_index][1])
        ask_volume += ask_price*ask_coins
        ask_index += 1
        ask_check_flag = ask_price

      bid_index = 0
      bid_check_flag = last
      while bid_check_flag >= bid_end and bid_index<len(bids):
        bid_price = float(bids[bid_index][0])
        bid_coins = float(bids[bid_index][1])
        bid_volume += bid_price*bid_coins
        bid_index += 1
        bid_check_flag = bid_price

      if bid_index < 500 and ask_volume*10 < bid_volume:
        df.loc[pair, 'time'] = datetime.now()
        df.loc[pair, 'price'] = last
        df.loc[pair, 'ask_volume'] = ask_volume
        df.loc[pair, 'bid_volume'] = bid_volume

  return(df)


if __name__ == "__main__":
  try:
    all_currency_pairs = client.list_currency_pairs()
    pairs = [x.to_dict()['id'] for x in all_currency_pairs]
    pairs = [x for x in pairs if "USDT" in x]
    pairs = [x for x in pairs if "3S" not in x]
    pairs = [x for x in pairs if "3L" not in x]
    pairs = [x for x in pairs if "5L" not in x]
    pairs = [x for x in pairs if "5S" not in x]
    pairs = [x for x in pairs if "BEAR" not in x]
    pairs = [x for x in pairs if "BULL" not in x]

    df = find_orderbooks(pairs)

    time.sleep(20)

    df = find_orderbooks(list(df.index))
    df = df[df['ask_volume']*10 < df['bid_volume']]

    if len(df) > 0:
      for index, row in df.iterrows():
        telegram_send.send(messages=[f"Orderbook scanner - {index}\nPrice: {row['price']}\tTime: {datetime.now()}"])
    logger.info("Working Fine...")
  except Exception as e:
    telegram_send.send(messages=["Orderbook Scanner failed"])
    logger.error(f"{e}\n {traceback.format_exc()}")
  



