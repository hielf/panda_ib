import pandas as pd
import numpy as np
import talib as ta
from .set_label import set_label
import sys

# 整理数据

def flag1(row):
    return 1
    
def set_buyY(row):
    if  np.log(row['future_hh'] / row['sma15']) > 0.0008: #and ta.LOG10(row['future_ll'] / row['close']) > - 0.005:
        return 1
    else:
        return 0

def set_saleY(row):
     if np.log(row['future_ll'] / row['sma15']) < -0.0008: # and ta.LOG10(row['future_hh'] / row['close'])  < 0.005:
        return 1
     else:
        return 0

_mask = [
    'minute', 'hour','day_of_week', 'quarter', 'day_of_month',
    'hl_pct', 'log_pct','log_pct5','log_pct15', 'stochrsi', 'stochrsi_std', 'stochrsi_skew',
                    'log_sema5', 'log_sema30', 'up', 'down', 'aroon',
                    'log_sma5','log_sma30',
                    'log_ema5','log_ema30',
                    'jq_std','jq_skew', 'jq_kurt',
                    'hl_std','hl_skew', 'hl_kurt',
                    'jq_std_log_sema15',  'jq_skew_log_sema15', 'jq_kurt_log_sema15',
                    'jq_std_log_sema60', 'jq_skew_log_sema60', 'jq_kurt_log_sema60',]
def my_mask():
    return _mask
def comb_data(_df, mask= _mask):
    _df.dropna(inplace=True)

    rsi_period = 30 
    rsi_window = 10
    n1 = 10
    n2 = 6

    #aroon
    down, up = ta.AROON(_df['high'], _df['low'], timeperiod=60)
    aroon = up - down
    _df['down'], _df['up'], _df['aroon'] = down, up, aroon

    _df['hl_pct'] = (_df['high'] - _df['low']) / _df['close'] 

    _df['pct_change'] = _df['close'].pct_change() 
    _df['pct_change5'] = _df['close'].pct_change(5) 
    _df['pct_change15'] = _df['close'].pct_change(15)
    _df['log_pct'] = ta.LOG10(_df['pct_change']+1)
    _df['log_pct5'] = ta.LOG10(_df['pct_change5']+1)
    _df['log_pct15'] = ta.LOG10(_df['pct_change15']+1)

    _df['rsi'] = ta.RSI(_df['close'], timeperiod=rsi_period)
    rsi_ll = _df['rsi'].rolling(window=rsi_window).min()
    rsi_hh = _df['rsi'].rolling(window=rsi_window).max()
    _df['stochrsi'] = (_df['rsi'] - rsi_ll) / (rsi_hh - rsi_ll)

    _df['stochrsi_std'] = _df['stochrsi'].rolling(window=n1*n2).std()
    _df['stochrsi_mean'] = _df['stochrsi'].rolling(window=n1*n2).mean()
    _df['stochrsi_skew'] = _df['stochrsi'].rolling(window= n1* n2 ).skew()
    _df['stochrsi_kurt'] = _df['stochrsi'].rolling(window= n1* n2 ).kurt()

    _df['sma5'] = ta.SMA(_df['close'], timeperiod=5) 
    _df['sma15'] = ta.SMA(_df['close'], timeperiod=15)
    _df['sma30'] = ta.SMA(_df['close'], timeperiod=30)
    _df['sma60'] = ta.SMA(_df['close'], timeperiod=60)
    # _df['sma120'] = ta.SMA(_df['close'], timeperiod=120)
    # _df['sma600'] = ta.SMA(_df['close'], timeperiod=600)

    _df['ema5'] = ta.EMA(_df['close'], timeperiod=5) 
    _df['ema15'] = ta.EMA(_df['close'], timeperiod=15)
    _df['ema30'] = ta.EMA(_df['close'], timeperiod=30)
    _df['ema60'] = ta.EMA(_df['close'], timeperiod=60)
    # _df['ema120'] = ta.EMA(_df['close'], timeperiod=120)
    # _df['ema600'] = ta.EMA(_df['close'], timeperiod=600)

    _df['log_sma5'] = ta.LOG10(_df['sma5'] / _df['close'])
    _df['log_sma15'] = ta.LOG10(_df['sma15'] / _df['close'])
    _df['log_sma30'] = ta.LOG10(_df['sma30'] / _df['close'])
    _df['log_sma60'] = ta.LOG10(_df['sma60'] / _df['close'])
    # _df['log_sma120'] = ta.LOG10(_df['sma120'] / _df['close'])
    # _df['log_sma600'] = ta.LOG10(_df['sma600'] / _df['close'])

    _df['log_ema5'] = ta.LOG10(_df['ema5'] / _df['close'])
    _df['log_ema15'] = ta.LOG10(_df['ema15'] / _df['close'])
    _df['log_ema30'] = ta.LOG10(_df['ema30'] / _df['close'])
    _df['log_ema60'] = ta.LOG10(_df['ema60'] / _df['close'])
    # _df['log_ema120'] = ta.LOG10(_df['ema120'] / _df['close'])
    # _df['log_ema600'] = ta.LOG10(_df['ema600'] / _df['close'])

    _df['log_sema5'] = ta.LOG10(_df['ema5'] / _df['sma5'])
    _df['log_sema15'] = ta.LOG10(_df['ema15'] / _df['sma15'])
    _df['log_sema30'] = ta.LOG10(_df['ema30'] / _df['sma30'])
    _df['log_sema60'] = ta.LOG10(_df['ema60'] / _df['sma60'])

    _df['jq_std'] = _df['log_pct'].rolling(window=n1*n2).std()
    _df['jq_mean'] = _df['log_pct'].rolling(window=n1*n2).mean()
    _df['jq_median'] = _df['log_pct'].rolling(window=n1*n2).median()
    _df['jq_skew'] = _df['log_pct'].rolling(window=n1*n2).skew()
    _df['jq_kurt'] = _df['log_pct'].rolling(window=n1*n2).kurt()

    _df['hl_std'] = _df['hl_pct'].rolling(window=n1*n2).std()
    _df['hl_mean'] = _df['hl_pct'].rolling(window=n1*n2).mean()
    _df['hl_median'] = _df['hl_pct'].rolling(window=n1*n2).median()
    _df['hl_skew'] = _df['hl_pct'].rolling(window=n1*n2).skew()
    _df['hl_kurt'] = _df['hl_pct'].rolling(window=n1*n2).kurt()

    _df['jq_std_log_sema15'] = _df['log_sema15'].rolling(window=n1*n2).std()
    _df['jq_mean_log_sema15'] = _df['log_sema15'].rolling(window=n1*n2).mean()
    _df['jq_median_log_sema15'] = _df['log_sema15'].rolling(window=n1*n2).median()
    _df['jq_skew_log_sema15'] = _df['log_sema15'].rolling(window=n1*n2).skew()
    _df['jq_kurt_log_sema15'] = _df['log_sema15'].rolling(window=n1*n2).kurt()

    _df['jq_std_log_sema60'] = _df['log_sema60'].rolling(window=n1*n2).std()
    _df['jq_mean_log_sema60'] = _df['log_sema60'].rolling(window=n1*n2).mean()
    _df['jq_median_log_sema60'] = _df['log_sema60'].rolling(window=n1*n2).median()
    _df['jq_skew_log_sema60'] = _df['log_sema60'].rolling(window=n1*n2).skew()
    _df['jq_kurt_log_sema60'] = _df['log_sema60'].rolling(window=n1*n2).kurt()

    return _df


def make_datetime_feature(_df, cname, d_lst=[]):
    for item in d_lst:
        if item == 'minute':
            _df['minute'] = _df[cname].dt.minute
        if item == 'hour':
            _df['hour'] = _df[cname].dt.hour
        if item == 'day_of_week':
            _df['day_of_week'] = _df[cname].dt.dayofweek
        if item == 'quarter':
            _df['quarter'] = _df[cname].dt.quarter
        if item == 'month':
            _df['month'] = _df[cname].dt.month
        if item == 'day_of_year':
            _df['day_of_year'] = _df[cname].dt.dayofyear
        if item == 'day_of_month':
            _df['day_of_month'] = _df[cname].dt.day
        if item == 'week_of_year':
            _df['week_of_year'] = _df[cname].dt.weekofyear


    return _df
