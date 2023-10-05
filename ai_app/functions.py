from binance.client import Client
from datetime import datetime
import pandas as pd
import schedule, time, threading, os
from apscheduler.schedulers.background import BackgroundScheduler

api_key = 'xQ6ucxBVyApc2fsQ3uGIdn8Rp87YAekx1hzP9W8ZqzWw0orYasOij7RhFVU9NHVE'
api_sec = "API KEY"

client = Client(api_key, api_sec)


def get_top_traders():
    # get the symbol for the trading pair you are interested in (e.g., BTCUSDT).
    symbol = 'BTCUSDT'

    end_time = int(time.time() * 1000)
    start_time = end_time - (3 * 24 * 60 * 60 * 1000)

    # fetching the trading data for the symbol.
    klines = client.futures_klines(symbol=symbol,
                                   interval=Client.KLINE_INTERVAL_1HOUR,
                                   limit=1000,
                                   startTime=start_time,
                                   endTime=end_time)

    # calculating the trading volume, profit percentage, profit gain, buying price, and selling price for each trader.
    trader_data = {}

    for kline in klines:
        timestamp = int(kline[0]) // 1000  # converting milliseconds to seconds.
        trader_id = kline[5]  # the field containing the trader's ID.
        product_traded = symbol
        volume = float(kline[9])  # the field containing trading volume.
        close_price = float(kline[4])  # the field containing the closing price.
        open_price = float(kline[1])  # the field containing the opening price.
        datetime_obj = datetime.fromtimestamp(timestamp)

        if trader_id not in trader_data:
            trader_data[trader_id] = {
                'Trader ID': trader_id,
                'Date': datetime_obj.strftime('%Y-%m-%d'),
                'Time': datetime_obj.strftime('%H:%M:%S'),
                'Product Traded': product_traded,
                'Trading Volume (Second)': 0,
                'Trading Volume (Minute)': 0,
                'Total Earned': 0,
                'Profit Percentage': 0,
                'Profit Gain': 0,
                'Buying Price': 0,
                'Selling Price': 0,
                'last_close_price': close_price

            }

        # Update trading volume for different time intervals.
        current_time = timestamp % 60  # Second
        trader_data[trader_id]['Trading Volume (Second)'] += volume
        if current_time == 0:
            trader_data[trader_id]['Trading Volume (Minute)'] += volume

        current_time = timestamp % 3600  # Hour
        # if current_time == 0:
        # trader_data[trader_id]['Trading Volume (Hour)'] += volume

        # Calculate profit percentage, profit gain, buying price, and selling price.
        # trader_data[trader_id]['Profit Gain'] = round((close_price - open_price), 2)
        trader_data[trader_id]['Buying Price'] = open_price
        trader_data[trader_id]['Selling Price'] = close_price

        profit_gain = ((close_price - open_price) / open_price) * 100
        trader_data[trader_id]['Profit Gain'] += round(profit_gain, 2)
        # trader_data[trader_id]['Total Earned'].append(trader_data[trader_id]['Total Earned'][-1] + profit_gain)

    for trader_id, data in trader_data.items():
        open_price = float(klines[0][1])
        close_price = float(klines[-1][4])
        profit_percentage = ((close_price - open_price) / open_price) * 100
        data['Profit Percentage'] = round(profit_percentage, 2)

    # sort traders by trading volume and profit percentage and get the top 7.
    sorted_traders = sorted(trader_data.values(), key=lambda x: (x['Trading Volume (Second)'], x['Profit Percentage']),
                            reverse=True)[:7]

    return sorted_traders


def fetch_live_data():
    global live_data
    live_data = get_top_traders()


def scheduled_task():
    fetch_live_data()


# Schedule the task to run every 2 minutes.
schedule.every(2).minutes.do(scheduled_task)