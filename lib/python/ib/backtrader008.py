# -*- coding: utf-8 -*-

# 策略框架来自 003-atr-2

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import time
import pandas as pd
import backtrader as bt
import numpy as np
from sklearn.linear_model import LinearRegression
import joblib
import csv
from tqdm import tqdm
import sys
import talib as ta
from backtrader.feeds import PandasData  # 用于扩展DataFeed

import pytz
pytz.common_timezones[-8:]

tz = pytz.timezone('Asia/Shanghai')

starttime = time.time()
data_source = sys.argv[1]
from_date = datetime.datetime.strptime(sys.argv[2],'%Y-%m-%d %H:%M:%S')
to_date = datetime.datetime.strptime(sys.argv[3],'%Y-%m-%d %H:%M:%S')


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

    df1= df1.resample('1T').agg({'open': 'first',
                                'high': 'max',
                                'low': 'min',
                                'close': 'last', 'volume': 'sum'})
    df1 = df1.dropna(axis=0)
    df1['openinterest'] = 0

    df2= df1.resample(period).agg({'open': 'first',
                                'high': 'max',
                                'low': 'min',
                                'close': 'last', 'volume': 'sum'})

    df3 = df2.dropna(axis=0) # 缺失值处理

    df3['atr'] = ta.ATR(df3['high'] , df3['low'], df3['close'], timeperiod=15)

    df3['tr'] = df3['high'] - df3['low']

    df3['hb'] = df3['high'] + df3['tr']/3
    df3['hh'] = df3['hb'] + df3['atr']*1

    df3['lb'] =  df3['low'] - df3['tr']/3
    df3['ll'] =  df3['lb'] - df3['atr']*1

    for item in [ 'hb', 'hh', 'lb', 'll', 'high', 'low', 'atr', 'close']:
        df3[item+'_1'] = df3[item].shift(1)

    up, middle, low=ta.BBANDS(df3['close'],matype=ta.MA_Type.T3, timeperiod=15, nbdevup=2, nbdevdn=2)

    df3['up'] = up
    df3['middle'] = middle
    df3['low'] = low
    # df3 = df3[['hb', 'hh', 'lb', 'll', 'hb_1', 'hh_1', 'lb_1', 'll_1', 'high_1', 'low_1', 'atr_1', 'close_1', 'up', 'middle', 'low']]
    print(df3.tail())
    df3.dropna(inplace=True)

    df3['openinterest'] = 0

    # df3 = df3.resample('1T').agg({  'hb': 'last',
    #                                 'hh': 'last',
    #                                 'lb': 'last',
    #                                 'll': 'last',
    #                                 'hb_1': 'last',
    #                                 'hh_1': 'last',
    #                                 'lb_1': 'last',
    #                                 'll_1': 'last',
    #                                 'high_1': 'last',
    #                                 'low_1': 'last',
    #                                 'atr_1': 'last',
    #                                 'close_1': 'last',
    #                                 'up': 'last',
    #                                 'middle': 'last',
    #                                 'low': 'last',
    #                                 })


    # df3.ffill(inplace=True)
    df3.reset_index(inplace=True)

    # df1.reset_index(inplace=True)

    # df4 = pd.merge(df1, df3, on='datetime')
    # print(df1.shape, df3.shape, df4.shape)

    return df3

class PandasDataExtend(PandasData):
    # 增加线
    lines = ('hb', 'hh', 'lb', 'll', 'hb_1', 'hh_1', 'lb_1', 'll_1','high_1', 'low_1', 'atr_1', 'close_1', 'up', 'middle', 'low')

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
                 ('up',  'up'),
                 ('middle',  'middle'),
                 ('low',  'low'),

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

        self.lines.atr = bt.indicators.ATR(self.data, period=12)

        self.lines.top=bt.indicators.BollingerBands(self.datas[0],period=120).top
        self.lines.bot=bt.indicators.BollingerBands(self.datas[0],period=120).bot


        self.buy_sig = bt.indicators.CrossOver(self.lines.top, self.dataopen)
        self.sell_sig = bt.indicators.CrossOver(self.lines.bot, self.dataopen)

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

    def next(self):

        # 9:45 - 15:45
        if self.data.datetime.time() > datetime.time(15, 50) or self.data.datetime.time() < datetime.time(9, 20) :

            if self. position.size > 0:
                self.order = self.sell()
                self.log('BUY Close by Day end, %.4f' % self.dataclose[0])

            if self. position.size < 0:
                self.order = self.buy()
                self.log('Sale Close by Day end, %.4f' % self.dataclose[0])

            return

        if self.order:
            return

        # Check if we are in the market





        if not self.position:
            if self.broker.getvalue() > 600000:
                self.sizer.p.stake = 2
            if self.broker.getvalue() > 700000:
                self.sizer.p.stake = 3
            if self.broker.getvalue() > 900000:
                self.sizer.p.stake = 4
            if self.broker.getvalue() > 1000000:
                self.sizer.p.stake = 4
            if self.broker.getvalue() > 1100000:
                self.sizer.p.stake = 5
            if self.broker.getvalue() > 1200000:
                self.sizer.p.stake = 5
            if self.broker.getvalue() > 1300000:
                self.sizer.p.stake = 6
            if self.broker.getvalue() < 500000:
                self.sizer.p.stake = 2
            if self.broker.getvalue() <= 400000:
                self.sizer.p.stake = 1


            if  self.datas[0].up[0] < self.dataclose[0] and self.lines.atr[0] > self.lines.atr[-2] :
                 self.log('BUY CREATE, %.4f' % (self.dataclose[0]))
                 self.order = self.sell()
                 self.max_price = self.dataclose[0]
                 self.overcross = False


            elif (self.datas[0].low[0] > self.dataclose[0] and self.lines.atr[0] > self.lines.atr[-2]):
                 self.log('SELL CREATE, %.4f' % self.dataclose[0])
                 self.order = self.buy()
                 self.overcross = False
                 self.min_price = self.dataclose[0]


        else:
            if self. position.size < 0:
                # close_sig = False
                # self.max_price = min(self.max_price, self.datas[0].lb_1[0])

                # if self.datalow[0] < self.datas[0].ll_1[0]:
                #     self.overcross = True

                # if self.overcross and self.datahigh[0] > self.datas[0].ll_1[0]:
                #     self.log('BUY CLOSE A, %.4f' % self.datas[0].hh_1[0])
                #     close_sig = True

                # if self.dataclose[0] > self.max_price:
                #     self.log('BUY CLOSE B, %.4f' % self.dataclose[0])
                #     close_sig = True

                if self.datas[0].middle[0] - self.lines.atr[0] > self.dataclose[0] or self.dataclose[0] > self.sellprice + self.lines.atr[0]:
                    self.order = self.buy()
                    self.max_price = None



            if self. position.size > 0:
            #     close_sig = False
            #     self.min_price = min(self.min_price, self.datas[0].lb_1[0])

            #     if self.datalow[0] < self.datas[0].ll_1[0]:
            #         self.overcross = True

            #     if self.overcross and self.datahigh[0] > self.datas[0].ll_1[0]:
            #         self.log('SELL CLOSE A, %.4f' % self.datas[0].ll_1[0])
            #         close_sig = True

            #     if self.dataclose[0] > self.min_price:
            #         self.log('SELL CLOSE B, %.4f' % self.dataclose[0])
            #         close_sig = True

                if self.datas[0].middle[0] + self.lines.atr[0]< self.dataclose[0] or self.dataclose[0] < self.buyprice - self.lines.atr[0]:
                    self.order = self.sell()
                    self.max_price = None




    def stop(self):
        print("death")

if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()
    # Add a strategy
    cerebro.addstrategy(MyStrategy)
    # 本地数据，笔者用Wind获取的东风汽车数据以csv形式存储在本地。
    dataframe = pd.read_csv(data_source, index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])
    dataframe = format_data(dataframe, period=sys.argv[4])
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
            fromdate=from_date,  # 起始日2002, 4, 1
            todate=to_date,  # 结束日 2015, 12, 31
            plot=False
        )


    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    cerebro.addwriter(bt.WriterFile, csv = True, out='results_%s.csv' % str(sys.argv[4]))
    # Set our desired cash start
    cerebro.broker.setcash(400000.0)
    # 设置每笔交易交易的股票数量
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)
    # Set the commission
    cerebro.broker.setcommission(
        commission=30,
        commtype = bt.CommInfoBase.COMM_FIXED, # 固定手续费
        automargin = 5, # 保证金10% , 这里5是因为hsi 指数 一个点50元, 10%保证金, 交易一次30元
        mult = 50 # 利润乘数, hsi 是1个点50
        )
    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name = 'SharpeRatio')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='myannual')

    results = cerebro.run()

    endtime = time.time()
    print('='*5, 'program running time', '='*5)
    print('==== 2 bar ====')
    print ('time:', (endtime - starttime), 'seconds')
    print('='*5, 'program running time', '='*5)


    strat = results[0]
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    print('SR:', strat.analyzers.SharpeRatio.get_analysis())
    print('DW:', strat.analyzers.DW.get_analysis())
    print('AN:', strat.analyzers.myannual.get_analysis())


    # cerebro.plot()
