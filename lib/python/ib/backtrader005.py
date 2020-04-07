# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # For datetime objects
import time
import pandas as pd
import backtrader as bt
import numpy as np
from sklearn.linear_model import LinearRegression
import joblib
import xgboost as xgb

from tqdm import tqdm

import pandaAI.tools005 as ptools
from sklearn.ensemble import VotingClassifier
starttime = time.time()

buy_clf = joblib.load('reg_buy02.pkl')
sell_clf = joblib.load('reg_sell02.pkl')

exec_len = 5

class PandasData(bt.feeds.PandasData):
    lines = ('buy_dice','sell_dice',)
    params = (
        ('datetime', None),
        ('open',-1),
        ('high',-1),
        ('low',-1),
        ('close',-1),
        ('volume',-1),
        ('openinterest',None),
        ('buy_dice',-1),
        ('sell_dice',-1),
    )


class MyStrategy(bt.Strategy):
    params = (
        ('maperiod', 24),
        ('printlog', True),
        ('max_price', 0),
        ('min_price', 0),
        ('exec_len', exec_len),
    )

    def log(self, txt, dt=None, doprint=False):
        ''' Logging function fot this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.datetime(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low

        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.sellprice = None
        self.sellcomm = None

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

        #9:45 - 16:15
        if self.data.datetime.time() > datetime.time(16, 25) or self.data.datetime.time() < datetime.time(9, 45):
            if self. position.size > 0:
                self.log('long Day End, %.2f' % self.dataclose[0])
                self.order = self.sell()

            if self. position.size < 0:
                self.log('short Day End, %.2f' % self.dataclose[0])
                self.order = self.buy()

            return

        if self.order:
            return

        # Check if we are in the market
        if not self.position:
            if self.data.buy_dice[0] > 0.55 :
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                self.order = self.buy()

            if self.data.sell_dice[0] > 0.55 :
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                self.order = self.sell()
        else:
            '''
            > 0 is long (you have taken)
            == 0 is no position
            < 0 is short (you have given)
            '''
            if self. position.size > 0: #buy
                if self.params.max_price < self.dataclose[0]:
                    self.params.max_price = self.dataclose[0]

                if self.data.buy_dice[0] > 0.45:
                    self.params.exec_len += 2

                if len(self) < (self.bar_executed + self.params.exec_len):

                    # 冲高回落
                    if np.log(self.dataclose[0] / self.params.max_price) < -0.0003:
                        self.log('BUY CLOSE CUTWIN, %.2f' % self.dataclose[0])
                        self.order = self.sell()
                        self.params.max_price = 0
                        self.params.exec_len = exec_len
                    # 移动平仓
                    elif np.log(self.dataclose[0] / self.buyprice) < -0.0006 + self.params.exec_len / 10000:

                        self.log('BUY CLOSE CUTMOV, %.2f' % self.dataclose[0])
                        self.order = self.sell()
                        self.params.max_price = 0
                        self.params.exec_len = exec_len

                else:
                    if np.log(self.dataclose[0] / self.buyprice) < 0.0008 + (len(self) - self.bar_executed)/5000:
                        self.log('BUY CLOSE TIMEOUT, %.2f' % self.dataclose[0])
                        self.order = self.sell()
                        self.params.max_price = 0
                        self.params.exec_len = exec_len




            if self. position.size < 0: #sell
                if -self.params.min_price > -self.dataclose[0]:
                    self.params.min_price = self.dataclose[0]

                if self.data.sell_dice[0] > 0.45:
                    self.params.exec_len += 2

                if len(self) < (self.bar_executed + self.params.exec_len):

                    # 冲高回落
                    if np.log(self.dataclose[0] / self.params.min_price) > 0.0003:
                        self.log('SELL CLOSE CUTWIN, %.2f' % self.dataclose[0])
                        self.order = self.buy()
                        self.params.min_price = 0
                        self.params.exec_len = exec_len
                    # 移动平仓
                    elif np.log(self.dataclose[0] / self.sellprice) > 0.0006 - self.params.exec_len / 10000:

                        self.log('SELL CLOSE CUTMOV, %.2f' % self.dataclose[0])
                        self.order = self.buy()
                        self.params.min_price = 0
                        self.params.exec_len = exec_len

                else:
                    if np.log(self.dataclose[0] / self.sellprice) > -0.0008 - (len(self) - self.bar_executed)/5000:
                        self.log('SELL CLOSE TIMEOUT, %.2f' % self.dataclose[0])
                        self.order = self.buy()
                        self.params.min_price = 0
                        self.params.exec_len = exec_len

    def stop(self):
        print("death")

if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()
    # Add a strategy
    cerebro.addstrategy(MyStrategy)
    df = pd.read_csv('./data/hsi202003.csv', index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])
    #dataframe = pd.read_hdf('./data/data.h5', 'df4')
    dataframe = ptools.comb_data(df)
    dataframe['datetime'] = pd.to_datetime(dataframe.index)
    dataframe = ptools.make_datetime_feature(dataframe, 'datetime', ['minute', 'hour','day_of_week', 'quarter', 'day_of_month'])

    dataframe.dropna(inplace=True)
    mask = ptools.my_mask()
    pred_buy  = buy_clf.predict_proba(dataframe.loc[:, mask])[:,1]
    pred_sell  = sell_clf.predict_proba(dataframe.loc[:, mask])[:,1]

    dataframe['buy_dice'] = pred_buy
    dataframe['sell_dice'] = pred_sell
    dataframe['openinterest'] = 0

    #dataframe.to_csv('./m0120.csv')
    #dataframe['datetime'] = pd.to_datetime(dataframe.index)

    # data = bt.feeds.PandasData(dataname=dataframe,
    #                         fromdate = datetime.datetime(2020, 3, 1, 9, 45),
    #                         todate = datetime.datetime(2020, 4, 3, 10,15)
    #                         ) # 年月日, 小时, 分钟, 实盘就传参数吧
    data=PandasData(    dataname=dataframe,
                        fromdate = datetime.datetime(2019, 2, 4),
                        todate = datetime.datetime(2020, 12, 28)
    )

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    # Set our desired cash start
    cerebro.broker.setcash(350000.0)
    # 设置每笔交易交易的股票数量
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)
    # Set the commission
    cerebro.broker.setcommission(
        commission=30,
        commtype = bt.CommInfoBase.COMM_FIXED, # 固定手续费
        automargin = 5, # 保证金10% , 这里5是因为hsi 指数 一个点50元, 10%保证金, 交易一次30元
        mult = 50  # 利润乘数, hsi 是1个点50
        )
    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name = 'SharpeRatio')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')
    results = cerebro.run()

    endtime = time.time()
    print('='*5, 'program running time', '='*5)
    print ('time:', (endtime - starttime), 'seconds')
    print('='*5, 'program running time', '='*5)

    strat = results[0]
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    print('SR:', strat.analyzers.SharpeRatio.get_analysis())
    print('DW:', strat.analyzers.DW.get_analysis())

    # cerebro.plot()
