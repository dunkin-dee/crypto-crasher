import time
import pandas as pd
import telegram_send
import gate_api
import json
import logging
from datetime import datetime, timedelta
from gate_api.exceptions import ApiException, GateApiException
from logging.handlers import RotatingFileHandler
import traceback
import api_keys

rfh = RotatingFileHandler(
    filename='logs/autobots.log', 
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


if __name__ == "__main__":
  #Collecting settings
  with open("data/autobots_settings.json") as read_file:
    settings = json.load(read_file)
  with open("data/autobots_coins.json", 'r') as read_file:
    all_coins = json.load(read_file)
  #Getting float lengths
  all_currencies = client.list_currency_pairs()
  currency_dict = {}
  for currency in all_currencies:
    currency_info = currency.to_dict()
    temp_dict = {
      'coin_float_length': currency_info['amount_precision'],
      'float_length': currency_info['precision']
    }
    currency_dict[currency_info['id']] = temp_dict
  counter = 0

  #Loop forever
  while True:
    try:
      if settings['autotrade_on']:
        return_list = []
        coin_name_list = []
        for coin_dict in all_coins:
          return_coin = True
          pair = coin_dict['pair']
          coin_name_list.append(pair)
          stop_loss = settings['stop_loss']*float(coin_dict['buy_price'])
          take_profit = settings['take_profit']*float(coin_dict['buy_price'])
          coin_buy_time = datetime.fromtimestamp(coin_dict['buy_time'])
          ticker = client.list_tickers(currency_pair=pair)[0].to_dict()
          last = float(ticker['last'])
          sell = False
          wait = 0
          
          if last >= take_profit:
            sell_price = take_profit
            sell = True 
            wait = 5
            sell_string = "take profit"
          if last <= stop_loss:
            sell_price = stop_loss
            sell = True
            sell_string = "stop loss"
          if datetime.now() - coin_buy_time > timedelta(days=1):
            sell_price = stop_loss
            sell = True
            sell_string = "time out"

          if sell:
            return_coin = False
            float_length = currency_dict[pair]['float_length']
            coin_float_length = currency_dict[pair]['coin_float_length']
            balances = client.list_spot_accounts()
            pair1_wallet = next(x for x in balances if x.to_dict()['currency'] == pair.replace('_USDT','')).to_dict()
            coins = float(pair1_wallet['available'])*0.999
            final_sell_price = f"%.{float_length}f"%sell_price
            sellable_coins = f"%.{coin_float_length}f"%coins

            sell_order_dict = {
              "text": f"t-autotrade",
              "currency_pair": pair,
              "type": "limit",
              "account": "spot",
              "side": "sell",
              "iceberg": "0",
              "amount": sellable_coins,
              "price": final_sell_price,
              "time_in_force": "gtc",
              "auto_borrow": "false"
            }
            sell_order = client.create_order(order=sell_order_dict)
            sell_order_id = sell_order.to_dict()['id']
            time.sleep(wait)

            cancel = client.cancel_order(order_id=sell_order_id, currency_pair=pair)
            time.sleep(1)

            balances = client.list_spot_accounts()
            try:
              pair1_wallet = next(x for x in balances if x.to_dict()['currency'] == pair.replace('_USDT','')).to_dict()
              coins = float(pair1_wallet['available'])*0.999
            except Exception:
              coins = 0
            sellable_coins = f"%.{coin_float_length}f"%coins

            if float(sellable_coins)*float(end_sell_price) > 2:
              bids = client.list_order_book(currency_pair = pair).to_dict()['bids']
              end_sell_price = bids[-1][0]
              end_sell_order_dict = {
                          "text": f"t-sellpump",
                          "currency_pair": pair,
                          "type": "limit",
                          "account": "spot",
                          "side": "sell",
                          "iceberg": "0",
                          "amount": sellable_coins,
                          "price": end_sell_price,
                          "time_in_force": "gtc",
                          "auto_borrow": "false"
                        }
              sell_order = client.create_order(order=end_sell_order_dict)
            telegram_send.send(messages=[f"Sold All of {pair.replace('_USDT','')} at {sell_string}"])

          if return_coin:
            return_list.append(coin_dict)
        telegram_send.send(messages=[return_list])
        with open("data/autobots_coins.json", 'w') as write_file:
          json.dump(return_list, write_file)

      else:
        with open("data/autobots_coins.json", 'w') as write_file:
          logger.info("Dumped Empty list")
          json.dump([], write_file)
      counter += 1
      time.sleep(2)
      #regular update of key information and logging data
      if counter >= 30:
        counter = 0
        currencies_string = " ".join(coin_name_list)
        logger.info(f"Working Fine. Coins in list are {currencies_string}")
        #building pairing info dict i.e float_length
        all_currencies = client.list_currency_pairs()
        currency_dict = {}
        for currency in all_currencies:
          currency_info = currency.to_dict()
          temp_dict = {
            'coin_float_length': currency_info['amount_precision'],
            'float_length': currency_info['precision']
          }
          currency_dict[currency_info['id']] = temp_dict
        #Updating settings
        with open("data/autobots_settings.json") as read_file:
          settings = json.load(read_file)
        with open("data/autobots_coins.json", 'r') as read_file:
          all_coins = json.load(read_file)

    except Exception as e:
      logger.error(f"{e}\n {traceback.format_exc()}")
      telegram_send.send(messages=[f"autobots.py failed. Check logs."])
      quit()
    