from locale import currency
import time
import copy
import math
import pandas as pd
import telegram_send
import gate_api
import requests
import json
import logging
from datetime import datetime, timedelta
from gate_api.exceptions import ApiException, GateApiException
from logging.handlers import RotatingFileHandler
import traceback
import api_keys

rfh = RotatingFileHandler(
    filename='gacha/logs/top_gainers.log', 
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

def get_bid_ask_ratio(pair, last):

    orderbook = client.list_order_book(pair, limit=500).to_dict()

    asks = orderbook['asks']
    bids = orderbook['bids']

    ask_end = last*1.1
    bid_end = last*0.9

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

    return bid_volume/ask_volume

def get_min_candlesticks(pair: str):
    columns = ["time", "volume", "close", "high", "low", "open", "dunno"]
    cols = ["open", "close", "high", "low", "volume"]
    now = datetime.now()
    start = (now - timedelta(days=1)).replace(second=0, microsecond=0)
    mid1 = start + timedelta(hours=14)
    mid2 = start + timedelta(hours=13)

    end_stamp = int(datetime.timestamp(now))
    start_stamp = int(datetime.timestamp(start))
    mid1_stamp = int(datetime.timestamp(mid1))
    mid2_stamp = int(datetime.timestamp(mid2))

    klines1 = client.list_candlesticks(pair, interval='1m', _from=start_stamp, to=mid1_stamp)
    klines2 = client.list_candlesticks(pair, interval='1m', _from=mid2_stamp, to=end_stamp)

    df_1 = pd.DataFrame(klines1, columns=columns)
    df_2 = pd.DataFrame(klines2, columns=columns)
    df = df_1.append(df_2, ignore_index=True)
    df["time"] = pd.to_datetime(df["time"], unit='s')
    df[cols] = df[cols].apply(pd.to_numeric)
    df.drop_duplicates(subset=['time'], inplace=True)

    return(df.sort_values(by=['time'], ascending=True))

if __name__ == "__main__":
    try:
        #Read from all json files
        with open("/home/ubuntu/gacha/data/autobots_coins.json") as read_file:
            all_coins = json.load(read_file)
        all_coins_copy = copy.deepcopy(all_coins)

        all_pairs = client.list_currency_pairs()
        with open('/home/ubuntu/gacha/data/top_gain_exclude.json') as jfile:
            excluded = json.load(jfile)
        with open('/home/ubuntu/gacha/data/top_gainer_values.json') as jfile:
            settings = json.load(jfile)

        with open('/home/ubuntu/gacha/data/topgainers_datadump.json', 'r') as jfile:
            data_dump = json.load(jfile)
        data_dump_copy = copy.deepcopy(data_dump)
        
        
        excluded_copy = copy.deepcopy(excluded)
        for k, v in excluded_copy.items():
            if datetime.now() > (datetime.fromtimestamp(v) + timedelta(hours=24)):
                del excluded[k]

        raw_pairs = client.list_currency_pairs()
        pairs = [pair.to_dict()['id'] for pair in raw_pairs]
        pairs = [pair for pair in pairs if '_USDT' in pair]
        pairs = [pair for pair in pairs if '3S' not in pair]
        pairs = [pair for pair in pairs if '3L' not in pair]
        pairs = [pair for pair in pairs if '5S' not in pair]
        pairs = [pair for pair in pairs if '5L' not in pair]
        pairs = [pair for pair in pairs if pair not in excluded]
        temp_df = pd.DataFrame()
        og_columns = ['last','change_percentage', 'quote_volume']
        og_df = pd.DataFrame(requests.get(url).json())
        og_df = og_df.set_index('currency_pair', drop=True)
        og_df = og_df[og_df.index.isin(pairs)]
        og_df[og_columns] = og_df[og_columns].astype('float')

        temp_df = og_df[og_df['quote_volume'] < 500000]
        temp_df = temp_df[temp_df['quote_volume'] > 80000]
        temp_df = temp_df[temp_df['change_percentage']>20]

        final_df = pd.DataFrame()

        for pair in temp_df.index:
            df = get_min_candlesticks(pair)
            if df[-500:-1]['volume'].sum() > df[:-500]['volume'].sum():
                volume = df['volume'].sum()
                growth = ((df.iloc[-1]['close'] - df.iloc[0]['open'])/df.iloc[0]['open'])*100
                final_df.loc[pair, 'volume'] = volume
                final_df.loc[pair, 'growth'] = growth
                final_df.loc[pair, 'last'] = temp_df.loc[pair, 'last']
        if len(final_df) > 0:
            final_df = final_df[final_df['growth']>=settings['growth']]
            final_df = final_df[final_df['volume']<500000]
            final_df = final_df[final_df['volume']>=80000]

        if len(final_df) > 0:
            balances = client.list_spot_accounts()
            usdt = next(x for x in balances if x.to_dict()['currency'] == 'USDT').to_dict()
            fund_limit = float(usdt['available'])

            final_df = final_df.sort_values(by=['growth'], ascending=False)
            for index,row in final_df.iterrows():
                pair = index
                if get_bid_ask_ratio(pair, row['last']) > 2.5:
                    if fund_limit > settings['amount']:
                        fund_limit = settings['amount']

                    if not settings['buy']:
                        last_price = og_df.loc[pair, 'last']
                        telegram_send.send(messages=[f"Top Gainers\n{pair.replace('_USDT', '')}\nCurrent Volume of {int(final_df.iloc[0]['volume'])} USDT\nCurrent Gain of {int(final_df.iloc[0]['growth'])}%\nPrice: {last_price}"], conf="gacha/telegram_send.conf")
                    elif fund_limit < 5:
                        telegram_send.send(messages=[f"Top Gainers\nAttempted to buy {pair.replace('_USDT', '')} but not enough funds\nCurrent Volume of {final_df.iloc[0]['volume']}\nCurrent Gain of {final_df.iloc[0]['growth']}"])
                        quit()

                    else:
                        asks = client.list_order_book(currency_pair = pair).to_dict()['asks']
                        ask = asks[-1][0]


                        for x in all_pairs:
                            x_dict = x.to_dict()
                            if x_dict['id'] == pair:
                                coin_float_length = x_dict['amount_precision']
                                float_length = x_dict['precision']
                        buy_price = f"%.{float_length}f"%(float(ask)*1.05)
                        usable_funds = fund_limit
                        amount = str(math.trunc(usable_funds/float(buy_price)))
                        order_dict = {
                                    "text": f"t-none",
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
                        buy_order = client.create_order(order=order_dict)
                        telegram_send.send(messages=[f"Top Gainers: Buy Order for {pair} sent at {datetime.now()}\nCurrent Volume of {final_df.iloc[0]['volume']}\nCurrent Gain of {final_df.iloc[0]['growth']}\nPrice: {buy_price}"])

                        time.sleep(2)
                        pair1 = pair.replace('_USDT', '')

                        balances = client.list_spot_accounts()
                        try:
                            pair1_wallet = next(x for x in balances if x.to_dict()['currency'] == pair1).to_dict()
                            coins = float(pair1_wallet['available'])
                        except Exception:
                            coins = 0

                        order_details = client.get_order(order_id=buy_order.to_dict()['id'], currency_pair=pair)
                        total_bought_price = float(order_details.to_dict()["filled_total"])
                        bought_price = total_bought_price/coins

                        coin_dict = {}
                        coin_dict["pair"] = pair
                        coin_dict["buy_time"] = int(datetime.timestamp(datetime.now()))
                        coin_dict["buy_price"] = f"%.{float_length}f"%bought_price

                        all_coins_copy.append(coin_dict)

                        with open("/home/ubuntu/gacha/data/autobots_coins.json", 'w') as write_file:
                            json.dump(all_coins_copy, write_file)

                        with open("/home/ubuntu/gacha/data/autobots_coins2.json", 'w') as write_file:
                            json.dump(all_coins, write_file)

                        telegram_send.send(messages=[f"Bought {coins} coins at {bought_price}"])




                # sellable_coins =  f"%.{coin_float_length}f"%coins
                # sell_multiplier = settings['sell_multiplier']
                # sell_price = float(bought_price*sell_multiplier)
                # final_sell_price = f"%.{float_length}f"%sell_price
                
                # sell_order_dict = {
                #             "text": f"t-sellpump",
                #             "currency_pair": pair,
                #             "type": "limit",
                #             "account": "spot",
                #             "side": "sell",
                #             "iceberg": "0",
                #             "amount": sellable_coins,
                #             "price": final_sell_price,
                #             "time_in_force": "gtc",
                #             "auto_borrow": "false"
                #             }
                # sell_order = client.create_order(order=sell_order_dict)
                # telegram_send.send(messages=[f"Sell order for {pair} sent at price: {final_sell_price}..."])
                excluded[pair] = int(datetime.timestamp(datetime.now()))
                with open('/home/ubuntu/gacha/data/top_gain_exclude.json', 'w') as jfile:
                    json.dump(excluded, jfile)
            
            # data_dump_copy.append({
            #     'pair': pair,
            #     'growth': final_df.iloc[0]['growth'],
            #     'volume': final_df.iloc[0]['volume'],
            #     'order_book': client.list_order_book(currency_pair=pair, limit=200).to_dict()
            # })
            # with open('/home/ubuntu/gacha/data/topgainers_datadump.json', 'w') as jfile:
            #     json.dump(data_dump_copy, jfile)
        logger.info("Working Fine...")

    except Exception as e:
        logger.error(f"{e}\n {traceback.format_exc()}")
        telegram_send.send(messages=[f"top_gainers.py failed. Check logs."])
