import json
import time
import requests
import gate_api
import telegram_send
import traceback
import logging
from logging.handlers import RotatingFileHandler
import api_keys

rfh = RotatingFileHandler(
    filename='./logs/sell_kucoin.log', 
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
gaclient = gate_api.SpotApi(api_client)


url ="https://www.kucoin.com/_api/cms/articles?page=1&pageSize=10&category=listing&lang=en_US"

headers = {
  "accept": "application/json",
  "accept-encoding": "gzip, deflate, br",
  "accept-language": "en-US,en-GB;q=0.9,en;q=0.8",
  "cache-control": "no-cache",
  "pragma": "no-cache",
  "referer": "https://www.kucoin.com/news/categories/listing",
  "sec-ch-ua": '" Not A;Brand";v="99", "Chromium";v="96", "Google Chrome";v="96"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": "Windows",
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "same-origin",
  "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36"}


if __name__ == "__main__":

  while True:
    try:
      with open("./data/kucoin_tokens.json") as read_file:
        kucoin_tokens_dict = json.load(read_file)
      kucoin_tokens = kucoin_tokens_dict.keys()
      
      r = requests.get(url=url, headers=headers)

      for token in kucoin_tokens:
        if token in r.text:
          pair = token+"_USDT"

          #getting available tokens in format acceptable for sale
          balances = gaclient.list_spot_accounts()
          raw_token = next(x for x in balances if x.to_dict()['currency'] == token).to_dict()
          new_token = float(raw_token['available'])
      
          bids = gaclient.list_order_book(currency_pair=pair).to_dict()['bids']
          coin_float_length = len(bids[-1][1].split(".")[-1])

          base_subtractor = "0."
          for x in range(coin_float_length-1):
            base_subtractor = base_subtractor + "0"
          subtractor = float(base_subtractor+"1")
          new_tokens = f"%.{coin_float_length}f"%(new_token-subtractor)
          bid = bids[-3][0]
          float_length = len(bid.split('.')[-1])
          raw_sell_price = kucoin_tokens_dict[token]
          sell_price = f"%.{float_length}f"%(float(raw_sell_price)*1.1)

          # if new_tokens > 0:
          sell_order_dict = {
                    "text": "t-kucoin_listed",
                    "currency_pair": pair,
                    "type": "limit",
                    "account": "spot",
                    "side": "sell",
                    "iceberg": "0",
                    "amount": new_tokens,
                    "price": str(sell_price),
                    "time_in_force": "gtc",
                    "auto_borrow": "false"
                  }

          sell_order = gaclient.create_order(order=sell_order_dict)

          telegram_send.send(messages=[f"Attempting to sell {pair} at {sell_price}..."])

          #allowing time for limit order to be filled
          time.sleep(20)
          
          #checking if sold and starting loop to sell at market price
          sell_order_details = gaclient.get_order(order_id=sell_order.to_dict()['id'], currency_pair=pair)
          if float(sell_order_details.to_dict()['left'])/float(sell_order_details.to_dict()['amount']) > 0.05:
            cancel_order = gaclient.cancel_orders(currency_pair=pair)
            raw_token = next(x for x in balances if x.to_dict()['currency'] == token).to_dict()
            new_token = float(raw_token['available'])
            new_tokens = f"%.{coin_float_length}f"%(float(new_token-subtractor))
            try_sell = True
            
            time.sleep(0.2)
            while try_sell:
              cancel_order = gaclient.cancel_orders(currency_pair=pair)
              try:
                balances = gaclient.list_spot_accounts()
                new_token = float(next(x for x in balances if x.to_dict()['currency'] == token).to_dict()['available'])
                base_subtractor = "0."
                for x in range(coin_float_length-1):
                  base_subtractor = base_subtractor + "0"
                subtractor = float(base_subtractor+"1")
                new_tokens = f"%.{coin_float_length}f"%(float(new_token-subtractor))
              except StopIteration:
                new_tokens = 0

              if float(new_tokens) < 1:
                telegram_send.send(messages=["SOLD!"])
                try_sell = False
                break

              if float(new_tokens) >= 1:
                bids = gaclient.list_order_book(currency_pair=pair).to_dict()['bids']
                bid = bids[-3][0]
                sell_order_dict = {
                        "text": "t-kucoin_listed",
                        "currency_pair": pair,
                        "type": "limit",
                        "account": "spot",
                        "side": "sell",
                        "iceberg": "0",
                        "amount": new_tokens,
                        "price": bid,
                        "time_in_force": "gtc",
                        "auto_borrow": "false"
                      }

                sell_order = gaclient.create_order(order=sell_order_dict)
                time.sleep(5)

                #checking if sold
                sell_order_details = gaclient.get_order(currency_pair=pair, order_id=sell_order.to_dict()['id'])
                if float(sell_order_details.to_dict()['left'])/float(sell_order_details.to_dict()['amount']) <0.1:
                  telegram_send.send(messages=[f"Sold all of {token}"])
                  try_sell = False

            kucoin_tokens_dict.pop(token)
            with open("./data/kucoin_tokens.json", 'w') as write_file:
              json.dump(kucoin_tokens_dict, write_file)
          else:
            #FILLED ORIGINAL sell order complete details here

            telegram_send.send(messages=[f"Sold all of {token}"])
      logger.info("Working Fine")
    except Exception as e:
      logger.error(f"{e}\n {traceback.format_exc()}")
      telegram_send.send(messages=[f"Error with sell_kucoin.py. Please check logs\n{e}"])
    time.sleep(20)