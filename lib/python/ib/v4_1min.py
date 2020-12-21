# -*- coding: utf-8 -*-

# 用法
# python backtrader003.py ./data/hsi_1m.csv '2020-11-01 09:00:00' '2020-11-01 15:55:00'

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
starttime = time.time()

buyAI = joblib.load('buyAI006.pkl')
sellAI = joblib.load('sellAI006.pkl')

# def set_label(row):
#     if row['future_ema'] - row['current_ema']>row['atr10']*1.5:
#         return 1
#     else:
#         return 0

def rebuild_data(df3):
    column_names = []

    for i in range(1,16,5):

        df3['feature1_'+str(i)] = df3['ema-sma5'].shift(i*1)
        df3['feature2_'+str(i)] = df3['ema-sma10'].shift(i*2)
        df3['feature3_'+str(i)] = df3['ema-sma15'].shift(i*3)
        df3['feature4_'+str(i)] = df3['ema-sma30'].shift(i*6)
        df3['feature5_'+str(i)] = df3['ema-sma60'].shift(i*30)
        df3['feature6_'+str(i)] = df3['atr120'].shift(i*30)
        df3['feature7_'+str(i)] = df3['rsi'].shift(i*30)
        df3['feature8_'+str(i)] = df3['kama'].shift(i*2)
        df3['feature9_'+str(i)] = df3['slowk'].shift(i*5)
        df3['feature10_'+str(i)] = df3['slowd'].shift(i*5)
        for j in range(1,11):
            column_names.append('feature'+str(j) +'_'+str(i))

    return df3, column_names

def format_data(df3):
    df3['ema5']=ta.EMA(df3['close'], timeperiod=5)/df3['close']
    df3['sma5']=ta.SMA(df3['close'], timeperiod=5)/df3['close']
    df3['ema10']=ta.EMA(df3['close'], timeperiod=10)/df3['close']
    df3['sma10']=ta.SMA(df3['close'], timeperiod=10)/df3['close']
    df3['ema15']=ta.EMA(df3['close'], timeperiod=15)/df3['close']
    df3['sma15']=ta.SMA(df3['close'], timeperiod=15)/df3['close']
    df3['ema30']=ta.EMA(df3['close'], timeperiod=30)/df3['close']
    df3['sma30']=ta.SMA(df3['close'], timeperiod=30)/df3['close']
    df3['ema60']=ta.EMA(df3['close'], timeperiod=60)/df3['close']
    df3['sma60']=ta.SMA(df3['close'], timeperiod=60)/df3['close']
    df3['ema120']=ta.EMA(df3['close'], timeperiod=120)/df3['close']
    df3['kama']=ta.KAMA(df3['close'], timeperiod=60)/df3['close']
    df3['rsi']=ta.RSI(df3['close'], timeperiod=15)

    df3['atr60'] = ta.ATR(df3['high'], df3['low'], df3['close'], timeperiod=60)
    df3['atr120'] = ta.ATR(df3['high'], df3['low'], df3['close'], timeperiod=120)

    df3['ema-sma5']=df3['ema5']/df3['sma5']
    df3['ema-sma10']=df3['ema10']/df3['sma10']
    df3['ema-sma15']=df3['ema15']/df3['sma15']
    df3['ema-sma30']=df3['ema30']/df3['sma30']
    df3['ema-sma60']=df3['ema60']/df3['sma60']

    slowk, slowd = ta.STOCHRSI(df3['close'], timeperiod=240, fastk_period=120, fastd_period=60, fastd_matype=0)

    df3['slowk'] =slowk
    df3['slowd'] =slowd

    df3['current_ema'] = (df3['ema5']  - df3['ema30']) * df3['close']
    df3['future_ema'] = df3['current_ema'].rolling(15).mean().shift(-20)

    #60分钟后ema最大值大于当前close+atr*4 大概是50点
    # df3['ll']=df3.apply(set_label,axis=1)

    df3, column_names = rebuild_data(df3)

    df3.dropna(inplace=True)

    df5= df3[['close','atr120']+column_names + ['volume']]
    print(df5.shape)

    y_expect = buyAI.predict(df5)
    df3['buy_ll']=y_expect

    y_expect = sellAI.predict(df5)
    df3['sell_ll']=y_expect


    df3.reset_index(inplace=True)
    df3.rename(columns={"date": "datetime"}, inplace=True)

    return df3


class PandasDataExtend(PandasData):
    # 增加线
    lines = ('buy_status', 'sell_status', 'atr120', )

    # 第几列, 或者直接给列名
    params = (  ('buy_status', 'buy_ll'),
                ('sell_status', 'sell_ll'),
                ('atr120', 'atr120')

                )  # 上市天数

class MyStrategy(bt.Strategy):
    params = (
        ('maperiod', 24),
        ('printlog', True),
        ('max_price', None),
        ('min_price', None),
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
        self.clock1 = None



        #self.data = dual_trust(dual_window=self.params.dual_window, dual_period= self.params.dual_period, subplot=False)
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

                row_list = [self.data.datetime.time(), 'buy', self.buyprice, self.buycomm, 0]
                writer.writerow(row_list)

            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))
                self.sellprice = order.executed.price
                self.sellcomm = order.executed.comm

                row_list = [self.data.datetime.time(), 'sell', self.sellprice, self.sellcomm, 0]
                writer.writerow(row_list)

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
        row_list = [self.data.datetime.time(), 'profit', 0, 0, trade.pnlcomm]
        writer.writerow(row_list)

    def next(self):

        # 9:45 - 15:45
        if self.data.datetime.time() > datetime.time(15, 35) or self.data.datetime.time() < datetime.time(9, 40):
            if self. position.size > 0:
                self.order = self.sell()
                self.log('BUY Close by Day end, %.2f' % self.dataclose[0])
                trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            if self. position.size < 0:
                self.order = self.buy()
                self.log('Sale Close by Day end, %.2f' % self.dataclose[0])
                trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})
            return

        if self.order:
            return

        # Check if we are in the market
        if not self.position and self.data.datetime.time() < datetime.time(15, 35):
            if  self.data.buy_status[0] == 1 and self.dataclose[0] - self.dataclose[-1] > self.data.atr120[0]*0.5:
                 self.log('BUY CREATE %.2f' % (self.dataclose[0]))
                 self.order = self.buy()
                 self.params.max_price = self.dataclose[0]
                 trades.append({'order': 'buy', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            elif self.data.sell_status[0] == 1 and self.dataclose[0] - self.dataclose[-1] < -self.data.atr120[0]*0.5:
                 self.log('SELL CREATE, %.2f' % self.dataclose[0])
                 self.order = self.sell()
                 self.params.min_price = self.dataclose[0]
                 trades.append({'order': 'sell', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

        else:
            if self.clock1 == None:
                self.clock1 = self.bar_executed

            if self.data.buy_status[0] == 1:
                self.clock1 = len(self)

            if self. position.size > 0:
                self.params.max_price = max(self.datahigh[-5], self.params.max_price)

                if self.dataclose[0] - self.data.atr120[0] > self.params.max_price:
                    self.params.max_price = max(self.dataclose[0], self.params.max_price)

                # 冲高回落
                if (self.dataclose[0] + self.data.atr120[0]*2 < self.params.max_price
                        or self.dataclose[0] - self.buyprice < -self.data.atr120[0]*1
                        ):
                    self.log('BUY CLOSE HIT, %.2f' % self.dataclose[0])
                    self.order = self.sell()
                    self.params.max_price = None
                    self.clock1 = None
                    trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})



            if self. position.size < 0:
                if self.params.min_price == None:
                    self.params.min_price = self.dataclose[0]
                self.params.min_price = min(self.datalow[-5], self.params.min_price)

                if self.dataclose[0] - self.data.atr120[0] < self.params.min_price:
                    self.params.min_price = min(self.dataclose[0], self.params.min_price)

            #     # 冲高回落
                if self.dataclose[0] - self.sellprice > self.data.atr120[0] * 1 or self.dataclose[0] - self.params.min_price  > self.data.atr120[0] * 2:
                    self.log('SELL  CLOSE HIT, %.2f' % self.dataclose[0])
                    self.order = self.buy()
                    self.params.min_price = None
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
    # dataframe = pd.read_csv('./20201113/hsi_15secs_202011131546.csv', index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])
    dataframe = pd.read_csv(csv_path, index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])
    dataframe['openinterest'] =0
    dataframe = format_data(dataframe)

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

    with open(csv_path + '.csv', 'w') as f:
        headers=['TIME','action', 'price', 'comm', 'pnl']
        writer = csv.writer(f)
        writer.writerow(headers)

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

    print(trades)
    with open(json_path, 'w') as f:
        json.dump(trades, f)

    # print("夏普比例:", results[0].analyzers.sharpe.get_analysis()["sharperatio"])
    # print("年化收益率:", results[0].analyzers.AR.get_analysis())
    # print("最大回撤:%.2f，最大回撤周期%d" % (results[0].analyzers.DD.get_analysis().max.drawdown, results[0].analyzers.DD.get_analysis().max.len))
    # print("总收益率:%.2f" % (results[0].analyzers.RE.get_analysis()["rtot"]))
    # results[0].analyzers.TA.print()

    cerebro.plot()
