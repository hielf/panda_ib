# -*- coding: utf-8 -*-

# 策略框架来自 003-atr-2

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import time
import uuid
import pandas as pd
import backtrader as bt
import numpy as np
from sklearn.linear_model import LinearRegression
import joblib
import os
import csv
import yaml
import json
from tqdm import tqdm
import sys
import talib as ta
from backtrader.feeds import PandasData  # 用于扩展DataFeed

import pytz
pytz.common_timezones[-8:]

tz = pytz.timezone('Asia/Shanghai')

def get_yaml_data(yaml_file):

    # 打开yaml文件
    print("***获取yaml文件数据***")
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()

    print(file_data)
    print("类型：", type(file_data))

    # 将字符串转化为字典或列表
    print("***转化yaml数据为字典或列表***")
    data = yaml.load(file_data)
    print(data)
    print("类型：", type(data))
    return data


# from_date = datetime.datetime.strptime(config_params['from_date'],'%Y-%m-%d %H:%M:%S')
# to_date = datetime.datetime.strptime(config_params['end_date'],'%Y-%m-%d %H:%M:%S')


def format_data(dataframe, period='1D', localize_zone='Asia/Shanghai', convert_zone= 'US/Eastern'):
    '''
        预处理格式, 1分钟和昨天1天合并
    '''

    # 设置日期索引后才能做resample
    dataframe['datetime'] = pd.to_datetime(dataframe.index)
    dataframe.set_index('datetime', inplace=True)

    # spx需要做时区转换
    # ts_utc = dataframe.index.tz_localize(localize_zone)
    # dataframe.index = ts_utc.tz_convert(convert_zone)

    df1 = dataframe
    df1.sort_index(inplace=True)

    df1= df1.resample('15S').agg({'open': 'first',
                                'high': 'max',
                                'low': 'min',
                                'close': 'last', 'volume': 'sum'})
    df1 = df1.dropna(axis=0)
    df1['openinterest'] = 0

    df2= df1.resample(period, closed='right',label='right').agg({'open': 'first',
                                'high': 'max',
                                'low': 'min',
                                'close': 'last', 'volume': 'sum'})

    df3 = df2.dropna(axis=0) # 缺失值处理

    df3['atr'] = ta.ATR(df3['high'] , df3['low'], df3['close'], timeperiod=7)

    print(df3.atr.describe())

    df3['tr'] = df3['high'] - df3['low']
    df3['tr_1'] = df3['tr'].shift(1)
    df3['tr_1'].fillna(0, inplace=True)
    df3['hb'] = df3['high'] + df3['tr_1'] * config_params['base_line']
    df3['hh'] = df3['hb'] + df3['tr_1'] * config_params['break_line']

    df3['hb'] = df3['high'] + df3['atr'] * config_params['base_line']
    df3['hh'] = df3['hb'] + df3['atr'] * config_params['break_line']

    df3['lb'] =  df3['low'] - df3['atr'] * config_params['base_line']
    df3['ll'] =  df3['lb'] - df3['atr'] * config_params['break_line']
    df3['lb'] =  df3['low'] - df3['tr_1'] * config_params['base_line']
    df3['ll'] =  df3['lb'] - df3['tr_1'] * config_params['break_line']


    for item in [ 'hb', 'hh', 'lb', 'll', 'high', 'low', 'atr', 'close']:
        df3[item+'_1'] = df3[item].shift(1)

    df3 = df3[[ 'hb', 'hh', 'lb', 'll', 'hb_1', 'hh_1', 'lb_1', 'll_1', 'high_1', 'low_1', 'atr_1', 'close_1']]
    df3.dropna(inplace=True)

    df3 = df3.asfreq(freq='15S').ffill()

    df3.reset_index(inplace=True)

    df1.reset_index(inplace=True)
    df1['ema'] = ta.MA(df1['close'], timeperiod = 120)

    df4 = pd.merge(df1, df3)
    print(df4.head())

    return df4

class PandasDataExtend(PandasData):
    # 增加线
    lines = ('hb', 'hh', 'lb', 'll', 'hb_1', 'hh_1', 'lb_1', 'll_1','high_1', 'low_1', 'atr_1', 'close_1', 'ema',)

    # 第几列, 或者直接给列名
    params = (   ( 'hb', 'hb'),
                 ( 'hh',  'hh'),
                 ( 'lb',  'lb'),
                 ( 'll',  'll'),
                 ( 'hb_1',  'hb_1'),
                 ( 'hh_1',  'hh_1'),
                 ( 'lb_1',  'lb_1'),
                 ( 'll_1',  'll_1'),
                 ( 'high_1',  'high_1'),
                 ( 'low_1',  'low_1'),
                 ( 'atr_1',  'atr_1'),
                 ('close_1',  'close_1'),
                 ('ema',  'ema'),
                   )  # 上市天数

# Create a Stratey
class MyStrategy(bt.Strategy):
    params = (
        ('maperiod', 60),
        ('printlog', True),
        ('max_price', 0),
        ('min_price', 0),
        ('hb_price', 0),
        ('hh_price', 0),
        ('lb_price', 0),
        ('ll_price', 0),


    )

    def log(self, txt, dt=None, doprint=False):
        ''' Logging function fot this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.datetime(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataopen = self.datas[0].open
        self.dataclose = self.datas[0].close
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low

        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.sellprice = None
        self.sellcomm = None

        self.overcross = False
        self.overcross_price = None
        self.win_loss = 0

        self.lines.atr2 = bt.indicators.ATR(self.data, period=12)

        # self.buy_sig = bt.indicators.CrossOver(self.datas[0].high_1, self.dataopen)
        # self.sell_sig = bt.indicators.CrossOver(self.datas[0].low_1, self.dataopen)

    def start(self):
        print("the world call me!")

    def prenext(self):
        print("not mature")

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enougth cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    'BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm

            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))
                self.sellprice = order.executed.price
                self.sellcomm = order.executed.comm


            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            pass
            #self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, COMM %.2f, GROSS %.2f, NET %.2f \n\n' %
                 (trade.commission, trade.pnl, trade.pnlcomm))

        if trade.pnlcomm < 0:
            self.win_loss += 1

    def next(self):

        # 9:45 - 15:45
        if self.data.datetime.time() > datetime.time(15, 55) or self.data.datetime.time() < datetime.time(9, 20) :
            self.win_loss = 0
            if self. position.size > 0:
                self.order = self.sell()
                self.log('BUY Close by Day end, %.4f' % self.dataclose[0])
                trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            if self. position.size < 0:
                self.order = self.buy()
                self.log('Sale Close by Day end, %.4f' % self.dataclose[0])
                trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            return


        if self.order:
            return

        # Check if we are in the market

        if not self.position :#and self.win_loss < 3:

            if  self.datahigh[0] > self.datas[0].hb_1[0] :#and self.lines.atr2[0] < self.lines.atr2[-2]:
                 self.log('BUY CREATE, %.4f' % (self.dataclose[0]))
                 self.order = self.buy()
                 self.max_price = self.dataclose[0] - self.datas[0].atr_1[-2]*3
                 self.overcross = False
                 self.overcross_price = None
                 trades.append({'order': 'buy', 'time': self.data.datetime.time().strftime('%H:%M:%S')})


            elif self.datalow[0] < self.datas[0].lb_1[0] :#and self.lines.atr2[0] < self.lines.atr2[-2]:
                 self.log('SELL CREATE, %.4f' % self.dataclose[0])
                 self.order = self.sell()
                 self.min_price = self.dataclose[0] + self.datas[0].atr_1[-2]*3
                 self.overcross = False
                 self.overcross_price = None
                 trades.append({'order': 'sell', 'time': self.data.datetime.time().strftime('%H:%M:%S')})


        else:
            if self. position.size > 0:
                close_sig = False
                self.max_price = max(self.max_price, self.dataclose[0] - self.datas[0].atr_1[-2]*2)


                if self.datahigh[0] > self.datas[0].hh_1[0]:
                    self.overcross = True

                    if not self.overcross_price:
                        self.overcross_price = self.datas[0].hh_1[0]

                if self.overcross_price:

                    if self.overcross and self.datalow[0] < self.overcross_price:
                        self.log('BUY CLOSE A, %.4f, %4f' % (self.overcross_price, self.dataclose[0]))
                        close_sig = True

                    if self.datas[0].hh_1[-1] != self.datas[0].hh_1[-2]:
                        self.overcross = False
                        self.overcross_price = None

                if self.dataclose[0] < self.max_price:
                    self.log('BUY CLOSE B, %.4f' % self.dataclose[0])
                    close_sig = True

                if close_sig:
                    self.order = self.sell()
                    self.max_price = None
                    trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})



            if self. position.size < 0:
                close_sig = False
                self.min_price = min(self.min_price, self.dataclose[0] + self.datas[0].atr_1[-2]*2)


                if self.datalow[0] < self.datas[0].ll_1[0]:
                    self.overcross = True

                    if not self.overcross_price:
                        self.overcross_price = self.datas[0].ll_1[0]

                if self.overcross_price:

                    if self.overcross and self.datahigh[0] > self.overcross_price:
                        self.log('SELL CLOSE A, %.4f, %4f' % (self.overcross_price, self.dataclose[0]))
                        close_sig = True

                    if self.datas[0].ll_1[-1] != self.datas[0].ll_1[-2]:
                        self.overcross = False
                        self.overcross_price = None

                if self.dataclose[0] > self.min_price:
                    self.log('SELL CLOSE B, %.4f' % self.dataclose[0])
                    close_sig = True

                if close_sig:
                    self.order = self.buy()
                    self.min_price = None
                    trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})




    def stop(self):
        print("death")

if __name__ == '__main__':
    csv_path = sys.argv[1]
    json_path = sys.argv[2]
    begin_time = sys.argv[3]
    end_time = sys.argv[4]
    configfile = sys.argv[5]

    current_path = os.path.abspath(".")
    yaml_path = os.path.join(current_path, configfile)
    config_params = get_yaml_data(yaml_path)

    begin_time = datetime.datetime.strptime(begin_time, '%Y-%m-%d %H:%M:%S +0800')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S +0800')

    date_handler = lambda obj: (
        obj.isoformat()
        if isinstance(obj, (datetime.datetime, datetime.date))
        else None
    )

    trades = []
    # Create a cerebro entity
    cerebro = bt.Cerebro()
    # Add a strategy
    cerebro.addstrategy(MyStrategy)
    # 本地数据，笔者用Wind获取的东风汽车数据以csv形式存储在本地。
    dataframe = pd.read_csv(csv_path, index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])
    dataframe = format_data(dataframe, period=config_params['period'])
    print(dataframe.shape)
    data = PandasDataExtend(
            dataname=dataframe,
            datetime=0,  # 日期列
            open=1,  # 开盘价所在列
            high=2,  # 最高价所在列
            low=3,  # 最低价所在列
            close=4,  # 收盘价价所在列
            volume=5,  # 成交量所在列
            openinterest=6,
            fromdate=begin_time,  # 起始日2002, 4, 1
            todate=end_time,  # 结束日 2015, 12, 31
            plot=False
        )


    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    # cerebro.addwriter(bt.WriterFile, csv = True, out='results_%s.csv' % str(sys.argv[4]))
    # Set our desired cash start
    cerebro.broker.setcash(1000000.0)
    # 设置每笔交易交易的股票数量
    cerebro.addsizer(bt.sizers.FixedSize, stake=4)
    # Set the commission
    cerebro.broker.setcommission(
        commission=30,
        commtype = bt.CommInfoBase.COMM_FIXED, # 固定手续费
        automargin = 5, # 保证金10% , 这里5是因为hsi 指数 一个点50元, 10%保证金, 交易一次30元
        mult = 50 # 利润乘数, hsi 是1个点50
        )
    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name = 'SharpeRatio', timeframe=bt.TimeFrame.Months)
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='myannual')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name = 'TR', timeframe=bt.TimeFrame.Months)

    with open(csv_path + '.csv', 'w') as f:
        headers=['TIME','action', 'price', 'comm', 'pnl']
        writer = csv.writer(f)
        writer.writerow(headers)

        results = cerebro.run()

    # endtime = time.time()
    print('='*5, 'program running time', '='*5)
    print('==== 2 bar ====')
    # print ('time:', (endtime - starttime), 'seconds')
    print('='*5, 'program running time', '='*5)


    strat = results[0]
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    print('SR:', strat.analyzers.SharpeRatio.get_analysis())
    print('DW:', strat.analyzers.DW.get_analysis())
    print('AN:', strat.analyzers.myannual.get_analysis())
    print('TimeReturn')
    for date, value in  results[0].analyzers.TR.get_analysis().items():
        print(date, value)

    print(trades)
    with open(json_path, 'w') as f:
        json.dump(trades, f)

    # cerebro.plot()
