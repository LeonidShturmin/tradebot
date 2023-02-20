import numpy as np
import pandas as pd
import talib as ta
import ccxt
from datetime import datetime, timezone
import time
import socket

from functions import timeframe_1m, custom_frame, mfi, volume_repalce

ticker = 'BTC/USD'
timeframe = 4

length_mfi = 4
length = timeframe * length_mfi + timeframe

mfi_time = []
mfi_volume = []
mfi_volume_2 = []

bars = custom_frame(timeframe_1m(ticker, length), timeframe)

flag = True
time.sleep(60)

while True:

    if flag == True:
        bars = bars.append(timeframe_1m(ticker, 1), ignore_index = True)
        bars.drop(labels=0, axis = 0, inplace = True)
        mfi_time.append(mfi(bars, length_mfi)[0])
        mfi_volume.append(mfi(bars, length_mfi)[1])
        flag = False
        print(mfi_volume)
    else:
        bars = bars.append(timeframe_1m(ticker, 1), ignore_index = True)
        index = bars.shape[0]
        temp = bars.iloc[(index - 2):]
        bars.drop(labels=[(index - 2), (index - 1)], axis = 0, inplace = True)
        bars = bars.append(custom_frame(temp, temp.shape[0]), ignore_index = True)
        mfi_point = mfi(bars, length_mfi)
        mfi_time.append(mfi_point[0])
        mfi_volume.append(mfi_point[1])
        mfi_volume = volume_repalce(mfi_volume)
        print(mfi_volume)
        if int((bars['timestamp'][(bars.shape[0]-1)] - bars['timestamp'][(bars.shape[0]-2)]).total_seconds()) == timeframe * 60:
            print(mfi_volume)
            mfi_volume_2.append(mfi_volume)
            mfi_volume = []
            flag = True

    time.sleep(60)

