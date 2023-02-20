import numpy as np
import pandas as pd
import ccxt
import time
import datetime
from collections import defaultdict
from datetime import datetime, timezone

def timeframe_1m(ticker, limit):
    """
    the function returns a 1-minute timeframe in the DataFrame format of the specified length
    """
    limit += 1
    exchange_data = ccxt.binance({ 'enableRateLimit': True })
    candles = exchange_data.fetch_ohlcv(ticker, '1m', limit=limit)
    columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    data = pd.DataFrame(candles)
    data.drop(index=[limit-1], axis=0, inplace = True)
    data.columns = columns
    data['timestamp'] = data['timestamp'].apply(lambda x: datetime.fromtimestamp(x/1000))

    return data

    
def custom_frame(data, timeframe):
    
    """
    the function converts 1 minute timeframe to the specified timeframe,
    for example 1 minute to 2,3,4...10 minutes and so on up to 59
    """

    frames_dict = defaultdict(list)

    for i in data.columns:
        series = data[i]
        flag = 0
        left = 0
        right = timeframe
        
        while flag != len(series)//timeframe:
            if i == 'timestamp':
                frames_dict[i].append(min(series[left:right]))
            elif i == 'open':
                frames_dict[i].append(np.array(series[left:right])[0])
            elif i == 'high':
                frames_dict[i].append(max(series[left:right]))
            elif i == 'low':
                frames_dict[i].append(min(series[left:right]))
            elif i == 'close':
                frames_dict[i].append(np.array(series[left:right])[-1])
            elif i == 'volume':
                frames_dict[i].append(sum(series[left:right]))
            flag += 1
            left += timeframe
            right += timeframe
                
    time_frame = pd.DataFrame(frames_dict)

    return time_frame

def mfi(data, length):
    """
    the function calculates the money flow index of the specified length   
    """
    typical = (data['high'].to_numpy() + data['low'].to_numpy() + data['close'].to_numpy()) / 3 * data['volume'].to_numpy()[::-1]

    negative = 0
    positive = 0
    left = 0
    right = length
    flag = 0
    mfi_time = 0
    mfi_volume = 0

    while flag != len(typical)//length:
        try:
            for index, val in enumerate(typical[left:right]):
                try:
                    if typical[index] > typical[index+1]:
                        positive += val
                    else:
                        negative += val 
                except:
                    break

            if negative != 0:
                money_ratio = positive / negative
                mfi = 100 - (100 / (1 + money_ratio))
                mfi_time = data['timestamp'].to_numpy()[::-1][left:right][0]
                mfi_volume = mfi

            else:
                mfi = 100
                mfi_time = data['timestamp'].to_numpy()[::-1][left:right][0]
                mfi_volume = mfi

            right += length
            left += length
            flag += 1

        except:
            break

    return mfi_time, mfi_volume

def volume_repalce(list):

    for i in range(0, (len(list)-1)):
        list[(len(list)-2-i)] = list[-1-i]

    return list

a = timeframe_1m('BTC/USDT', 24)

b = custom_frame(a, 4)

c = mfi(b, 5)

print(a)
print(b)