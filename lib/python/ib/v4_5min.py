# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import sys
import datetime  # For datetime objects
from dateutil.relativedelta import relativedelta
import time
import pandas as pd
import backtrader as bt
import numpy as np
from sklearn.linear_model import LinearRegression
import joblib
import json

from tqdm import tqdm

starttime = time.time()

reg_buy_open = joblib.load('hsi_buy_open05.pkl')
reg_buy_break = joblib.load('hsi_sale_break05.pkl')
reg_sale_open = joblib.load('hsi_buy_open05.pkl')
reg_sale_break = joblib.load('hsi_sale_break05.pkl')



class PandasData(bt.feeds.PandasData):
    lines = ('dual_buy_open','dual_buy_break','dual_sale_open','dual_sale_break',)
    params = (
        ('datetime', None),
        ('open',-1),
        ('high',-1),
        ('low',-1),
        ('close',-1),
        ('volume',-1),
        ('openinterest',None),
        ('dual_buy_open',-1),
        ('dual_buy_break',-1),
        ('dual_sale_open',-1),
        ('dual_sale_break',-1),
    )

# Create a Stratey
class MyStrategy(bt.Strategy):
    params = (
        ('maperiod', 12),
        ('printlog', True),
        ('dual_window',12),
        ('dual_period', '02T'),
        ('max_price', 0),
        ('min_price', 0)
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

        # 9:45 - 15:45
        if self.data.datetime.time() > datetime.time(15, 45) or self.data.datetime.time() < datetime.time(9, 45):
            if self. position.size > 0:
                self.order = self.sell()

            if self. position.size < 0:
                self.order = self.buy()

            return

        if self.order:
            return

        # Check if we are in the market
        if not self.position:

            if self.dataclose[0] > self.data.dual_buy_open[0]:
                 self.log('BUY CREATE, %.2f' % self.dataclose[0])
                 self.order = self.buy()
                 trades.append({'order': 'buy', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            elif self.dataclose[0] < self.data.dual_sale_open[0]:
                 self.log('SELL CREATE, %.2f' % self.dataclose[0])
                 self.order = self.sell()
                 trades.append({'order': 'sell', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

        else:
            '''
            > 0 is long (you have taken)
            == 0 is no position
            < 0 is short (you have given)
            '''
            if self. position.size > 0:
                if len(self) >= (self.bar_executed + 2):
                    if self.params.max_price < self.dataclose[0]:
                        self.params.max_price = self.dataclose[0]
                    # 冲高回落
                    if self.params.max_price > self.data.dual_buy_break[-1] and self.dataclose[0] < self.data.dual_buy_open[-1]:
                        self.log('BUY CLOSE HIT, %.2f' % self.dataclose[0])
                        self.order = self.sell()
                        self.params.max_price = 0
                        trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

                    # # 移动平仓
                    elif self.dataclose[0] < self.dataclose[-1]:
                        self.log('BUY CLOSE MOV, %.2f' % self.dataclose[0])
                        self.order = self.sell()
                        self.params.max_price = 0
                        trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})
                else:
                    if self.dataclose[-0] < self.dataclose[-2]:
                        self.log('BUY CLOSE MOV2, %.2f' % self.dataclose[0])
                        self.order = self.sell()
                        self.params.max_price = 0
                        trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            if self. position.size < 0:
                if len(self) >= (self.bar_executed + 2):
                    if self.params.min_price > -self.dataclose[0]:
                        self.params.min_price = -self.dataclose[0]
                    # 冲低回升
                    if abs(self.params.min_price) < self.data.dual_sale_break[-1] and self.dataclose[0] > self.data.dual_sale_open[-1]:
                        self.log('SALE CLOSE HIT, %.2f' % self.dataclose[0])
                        self.order = self.buy()
                        self.params.min_price = 0
                        trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

                    # 移动平仓
                    elif self.dataclose[0] > self.data.close[-1]:
                        self.log('SALE CLOSE MOV, %.2f' % self.dataclose[0])
                        self.order = self.buy()
                        self.params.min_price = 0
                        trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})
                else:
                    if self.dataclose[0] > self.data.close[-2]:
                        self.log('SALE CLOSE MOV2, %.2f' % self.dataclose[0])
                        self.order = self.buy()
                        self.params.min_price = 0
                        trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

    def stop(self):
        print("death")

if __name__ == '__main__':
    csv_path = sys.argv[1]
    json_path = sys.argv[2]
    begin_time = sys.argv[3]
    end_time = sys.argv[4]

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
    # parase_dates = True是为了读取csv为dataframe的时候能够自动识别datetime格式的字符串，big作为index
    # 注意，这里最后的pandas要符合backtrader的要求的格式
    #dataframe = pd.read_csv('./data/hsi202003.csv', index_col=0, parse_dates=True)
    dataframe = pd.read_csv(csv_path, index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])

    dataframe= dataframe.resample('5T').agg({'open': 'first',
                                'high': 'max',
                                'low': 'min',
                                'close': 'last', 'volume': 'sum'})
    dataframe['datetime'] = pd.to_datetime(dataframe.index)
    dataframe.dropna(inplace=True)
    #period_data = period_data[['open', 'high', 'hh', 'low', 'll', 'close' ]]
    dataframe['hh'] = dataframe['high']
    dataframe['ll'] = dataframe['low']
    pred_data = dataframe[['open', 'high', 'hh', 'low', 'll', 'close' ]]

    dataframe['dual_buy_open'] = reg_buy_open.predict(pred_data)
    dataframe['dual_buy_break'] = reg_buy_break.predict(pred_data)
    dataframe['dual_sale_open'] = reg_sale_open.predict(pred_data)
    dataframe['dual_sale_break'] = reg_sale_break.predict(pred_data)
    dataframe['openinterest'] = 0
    data = bt.feeds.PandasData(dataname=dataframe,
                            fromdate = begin_time,
                            todate = end_time
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
    print('from ' + str(begin_time) + ' to ' + str(end_time) + '', '+4')
    print ('time:', (endtime - starttime), 'seconds')
    print('='*5, 'program running time', '='*5)

    strat = results[0]
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    print('SR:', strat.analyzers.SharpeRatio.get_analysis())
    print('DW:', strat.analyzers.DW.get_analysis())

    print(trades)
    with open(json_path, 'w') as f:
        json.dump(trades, f)
    # cerebro.plot()
