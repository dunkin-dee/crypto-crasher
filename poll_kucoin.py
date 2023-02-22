from cmath import log
import copy
import math
import time
import json
import pandas as pd
import logging
import gate_api
import telegram_send
import traceback
from kucoin.client import Client
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from gate_api.exceptions import ApiException, GateApiException
import api_keys

with open('data/rex_ip.json') as r_file:
  ip_dict = json.load(r_file)

rex_address = ip_dict['rex']

rfh = RotatingFileHandler(
    filename='./logs/poll_kucoin.log', 
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


client = Client(None, None, None)

key = api_keys.gate_key
secret = api_keys.gate_secret

configuration = gate_api.Configuration(
  host = "https://api.gateio.ws/api/v4",
  key=key,
  secret=secret
)

api_client = gate_api.ApiClient(configuration)
gaclient = gate_api.SpotApi(api_client)


def get_profit(pair, buy_orders, sell_orders):
  total_out = 0
  for orderId in buy_orders:
    order_details = gaclient.get_order(currency_pair=pair, order_id=orderId).to_dict()
    total_out = total_out + float(order_details["filled_total"])
    total_out = total_out + float(order_details["fee"])
    time.sleep(0.1)

  total_in = 0
  for orderId in sell_orders:
    order_details = gaclient.get_order(currency_pair=pair, order_id=orderId).to_dict()
    total_in = total_in + float(order_details["filled_total"])
    total_out = total_out + float(order_details["fee"])
    time.sleep(0.1)

  return(total_in-total_out)

def get_prices() -> pd.DataFrame:
  raw_list = gaclient.list_tickers()
  tickers = [x.to_dict() for x in raw_list]
  df = pd.DataFrame(tickers)
  df.set_index('currency_pair', inplace=True)
  df = df[['last']]
  return df


if __name__ == "__main__":

  balances = gaclient.list_spot_accounts()
  usdt = next(x for x in balances if x.to_dict()['currency'] == 'USDT').to_dict()
  funds = float(usdt['available'])
  error_count = 0
  gate_currencies = gaclient.list_currencies()
  gate_tokens = [x.to_dict()['currency'] for x in gate_currencies]

  
  #getting all current pairs to compare against new ones
  raw_currencies = client.get_currencies()
  currencies = list(set(value["currency"] for value in raw_currencies))
  current_currencies = copy.deepcopy(currencies)
  current_length = len(current_currencies)
  print(current_length)

  #getting price dataframe

  price_df = get_prices()
  loop_counter =  0
  after_maintenance = False
  print('before loop')
  while True:
    try:
      #updating snapshot of funds every 10 loops
      loop_counter += 1
      if loop_counter == 1000:
        balances = gaclient.list_spot_accounts()
        usdt = next(x for x in balances if x.to_dict()['currency'] == 'USDT').to_dict()
        funds = float(usdt['available'])
        
        gate_currencies = gaclient.list_currencies()
        gate_tokens = [x.to_dict()['currency'] for x in gate_currencies]
        
        price_df = get_prices()
        
        logger.info("Working fine.")
        loop_counter = 0
        after_maintenance = True

      raw_currencies = client.get_currencies()
      currencies = list(set(value["currency"] for value in raw_currencies))

      if len(currencies) > current_length:
        new_currencies = list(set(currencies) - set(current_currencies))
        trade = new_currencies[-1]
        
        #updating current_symbols
        current_currencies = copy.deepcopy(currencies)
        current_length = len(current_currencies)
        #############################################
        if trade in gate_tokens:
        
          if funds > 20:
            buy_orders = []
            sell_orders = []
            #Gettin pair and buy price details
            pair = trade + "_USDT"
            ask = price_df.loc[pair, 'last']

            float_length = len(ask.split('.')[-1])
            buy_price = f"%.{float_length}f"%(float(ask)*1.1)

            usable_funds = funds
            amount = "%.2f"%((usable_funds/float(buy_price))-0.01)
            #making buy order
            order_dict = {
                        "text": "t-kucoin_listed",
                        "currency_pair": pair,
                        "type": "limit",
                        "account": "spot",
                        "side": "buy",
                        "iceberg": "0",
                        "amount": amount,
                        "price": buy_price,
                        "time_in_force": "ioc",
                        "auto_borrow": "false"
                      }
            buy_order = gaclient.create_order(order=order_dict)
            # coin_float_length = len(asks[-1][1].split(".")[-1])
            time.sleep(1)

            #Getting details of filled buy_order
            order_details = gaclient.get_order(order_id=buy_order.to_dict()['id'], currency_pair=pair)
            if float(order_details.to_dict()['filled_total']) > 0:
              bought_price = float(order_details.to_dict()['filled_total'])/float(order_details.to_dict()['amount'])
          
              with open("./data/kucoin_tokens.json") as read_file:
                kucoin_tokens = json.load(read_file)
              
              kucoin_tokens[trade] = bought_price
              
              with open("./data/kucoin_tokens.json", "w") as dump_file:
                json.dump(kucoin_tokens, dump_file)
              telegram_send.send(messages=[f"{trade} now listed on Kucoin!! \nBought {pair} at {bought_price}\nOrder ID: {buy_order.to_dict()['id']}\nSent at - {datetime.now()}"])
              if after_maintenance:
                telegram_send.send(messages=[f"Bought immediately after maintenance delay"])


            else:
              telegram_send.send(messages=[f"{trade} now listed on Kucoin!! \nOrder sent with buy price: {buy_price} but not filled."])
          else:
            telegram_send.send(messages=[f"{trade} now listed on Kucoin!! \nHowever not enough funds..."])
        else:
          telegram_send.send(messages=[f"{trade} now listed on Kucoin!! \nHowever not available on gate.io..."])

      error_count = 0
    except Exception as e:
      error_count += 1
      logger.error(f"{e}\n {traceback.format_exc()}")
      telegram_send.send(messages=[f"Error with poll_kucoin.py. Please check logs\n{e}"])
      still_down = True
      while still_down:
        try:
          with open('data/rex_ip.json') as r_file:
            ip_dict = json.load(r_file)
          still_down = False
        except Exception as e:
          logger.error(f"{e}\n {traceback.format_exc()}")
          time.sleep(60)
          continue

      time.sleep(1)
      if error_count > 5:
        telegram_send.send(messages=["TOO MANY ERRORS IN A ROW! CHECK LOGS!\nQuitting..."])
      continue
    after_maintenance = False
