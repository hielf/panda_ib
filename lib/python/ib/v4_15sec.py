# -*- coding: utf-8 -*-

from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import sys
import talib
from backtrader.feeds import PandasData  # 用于扩展DataFeed
import datetime  # For datetime objects
from dateutil.relativedelta import relativedelta
import time
import pandas as pd
import backtrader as bt
import numpy as np
from sklearn.linear_model import LinearRegression
import joblib
import json
import csv

from tqdm import tqdm

starttime = time.time()

reg_buy_open = joblib.load('hsi_buy_open06.pkl')
reg_buy_break = joblib.load('hsi_buy_break06.pkl')
reg_sale_open = joblib.load('hsi_sale_open06.pkl')
reg_sale_break = joblib.load('hsi_sale_break06.pkl')

def format_data(dataframe):
    dataframe['datetime'] = pd.to_datetime(dataframe.index)

    dataframe= dataframe.resample('5T').agg({'open': 'first',
                                'high': 'max',
                                'low': 'min',
                                'close': 'last', 'volume': 'sum'})
    dataframe.dropna(inplace=True)
    dataframe['openinterest'] = 0
    dataframe['hh'] = dataframe['high']
    dataframe['ll'] = dataframe['low']

    pred_data = dataframe[['open', 'high', 'hh', 'low', 'll', 'close' ]]
    dataframe['dual_buy_open'] = reg_buy_open.predict(pred_data)
    dataframe['dual_buy_break'] = reg_buy_break.predict(pred_data)
    dataframe['dual_sale_open'] = reg_sale_open.predict(pred_data)
    dataframe['dual_sale_break'] = reg_sale_break.predict(pred_data)
    dataframe['atr'] = talib.ATR(dataframe['high'],dataframe['low'], dataframe['close'], timeperiod=6)
    dataframe['macd'], dataframe['macdsignal'], dataframe['macdhist'] = talib.MACD(dataframe['close'], fastperiod=4, slowperiod=24, signalperiod=2)
    dataframe['macd2'], dataframe['macdsignal2'], dataframe['macdhist2'] = talib.MACD(dataframe['close'], fastperiod=6, slowperiod=24, signalperiod=3)
    dataframe.reset_index(inplace=True)

    return dataframe


class PandasDataExtend(PandasData):
    # 增加线
    lines = ('atr', 'macd','macd2', 'macdsignal', 'macdsignal2','macdhist','dual_buy_open','dual_buy_break','dual_sale_open','dual_sale_break',)

    # 第几列, 或者直接给列名
    params = (  ('atr', 'atr'),
                ('macd', 'macd'),
                ('macd2', 'macd2'),
                ('macdhist', 'macdhist'),
                ('macdsignal', 'macdsignal'),
                ('macdsignal2', 'macdsignal2'),
                ('dual_buy_open','dual_buy_open'),
                ('dual_buy_break','dual_buy_break'),
                ('dual_sale_open','dual_sale_open'),
                ('dual_sale_break','dual_sale_break'),                 )  # 上市天数

class dual_trust(bt.Indicator):
    lines = (   'dual_buy_open','dual_buy_break','dual_sale_open','dual_sale_break', 'close_resample',
                'high_resample',
                'low_resample', )
    def __init__(self, dual_window,  dual_period):
        self.params.dual_window = dual_window
        # addminperiod 一个5day的均线，那么开始五天是没有indicator的，这个时候，
        # 策略会调用prenext方法。而在indicator中，这个函数就是告诉strategy，我需要几天才能成熟
        self.addminperiod(self.params.dual_window)
        self.params.dual_period = dual_period

        self.iteration_progress = tqdm(desc='Total runs', total=(self.datas[0].close.buflen()))

    def DUAL(self, df2, period, bar_num):

        period_data = df2.resample(period).last()

        period_data['open'] = df2['open'].resample(period).first()
        # 处理最高价和最低价
        period_data['high'] = df2['high'].resample(period).max()
        # 最低价
        period_data['low'] = df2['low'].resample(period).min()

        period_data['hh'] = period_data['high'].rolling(bar_num).max()
        period_data['ll'] = period_data['low'].rolling(bar_num).min()

        period_data = period_data[['open', 'high', 'hh', 'low', 'll', 'close' ]]

        period_data.dropna(inplace=True)

        return period_data

    def next(self):

        self.iteration_progress.update()
        self.iteration_progress.set_description("Dual trust Processing {} out of {}".format(len(self.datas[0].close), self.datas[0].close.buflen()))

        data_serial_open = self.data.open.get(size=self.params.dual_window)
        data_serial_high = self.data.high.get(size=self.params.dual_window)
        data_serial_low = self.data.low.get(size=self.params.dual_window)
        data_serial_close = self.data.close.get(size=self.params.dual_window)

        dt_date = self.datas[0].datetime.datetime()

        dt1 = pd.date_range( end= dt_date, periods=self.params.dual_window, freq="min")

        dt = pd.DataFrame({})

        dt['datetime'] = dt1
        dt['open'] = pd.DataFrame(data_serial_open)
        dt['close'] = pd.DataFrame(data_serial_close)
        dt['high'] = pd.DataFrame(data_serial_high)
        dt['low'] = pd.DataFrame(data_serial_low)

        dt.set_index('datetime', inplace= True)
        pred_data = self.DUAL(dt, self.params.dual_period, 1)

        # lines 0 是当前, 1是未来, -1 是上一个 和pandas 不一样, pd 是 -1 为当前时间
        self.lines.close_resample[0] = pred_data.close[-2] # 当前价格和上一个close 价格比较

        prenext_num = -1
        #reg_buy_open = joblib.load('reg_buy_open.pkl')
        self.lines.dual_buy_open[0] = int(reg_buy_open.predict(pred_data)[prenext_num] * 100)/100

        #reg_buy_break = joblib.load('reg_buy_break.pkl')
        self.lines.dual_buy_break[0] = int(reg_buy_break.predict(pred_data)[prenext_num] * 100)/100

        #reg_sale_open = joblib.load('reg_sale_open.pkl')
        self.lines.dual_sale_open[0] = int(reg_sale_open.predict(pred_data)[prenext_num] * 100)/100

        #reg_sale_break = joblib.load('reg_buy_break.pkl')
        self.lines.dual_sale_break[0] = int(reg_sale_break.predict(pred_data)[prenext_num] * 100)/100

        return self.lines
# Create a Stratey
class MyStrategy(bt.Strategy):
    params = (
        ('maperiod', 24),
        ('printlog', True),
        ('dual_window',80),
        ('dual_period', '05T'),
        ('max_price', 0),
        ('min_price', 0),
        ('record_id',0)
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
        self.openstate = False
        self.buy_open = None
        self.buy_break = None
        self.sale_open = None
        self.sale_break = None

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
        if self.data.datetime.time() > datetime.time(15, 45) or self.data.datetime.time() < datetime.time(9, 45):
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
        if not self.position:

            if  self.data.macdhist[-1] > self.data.macdhist[-2] - 0.01 and self.dataclose[0] > self.data.high[-1] + self.data.atr[-1]*0.25:
                 self.log('BUY CREATE, %.2f, MACD %.2f, macd2 %.2f' % (self.dataclose[0], self.data.macd[0], self.data.macd2[0]))
                 self.order = self.buy()
                 self.params.max_price = self.dataclose[0]
                 self.buy_open = self.data.dual_buy_open[-1]
                 self.buy_break = self.data.dual_buy_break[-1]
                 # self.log('\n *buy move price %.2f\n' % (self.buy_open))
                 # moves.append({'order': 'sell', 'price': self.buy_open, 'time': self.data.datetime.time().strftime('%H:%M:%S')})
                 trades.append({'order': 'buy', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            elif  self.data.macdhist[-1] < self.data.macdhist[-2] + 0.01 and self.dataclose[0] < self.data.low[-1] - self.data.atr[-1]*0.25:
                 self.log('SELL CREATE, %.2f' % self.dataclose[0])
                 self.order = self.sell()
                 self.params.min_price = self.dataclose[0]
                 self.sale_open = self.data.dual_sale_open[-1]
                 self.sale_break = self.data.dual_sale_break[-1]
                 # self.log('\n *sell move price %.2f\n' % (self.sale_open))
                 # moves.append({'order': 'buy', 'price': self.sale_open, 'time': self.data.datetime.time().strftime('%H:%M:%S')})
                 trades.append({'order': 'sell', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

        else:
            '''
            > 0 is long (you have taken)
            == 0 is no position
            < 0 is short (you have given)
            '''
            if self. position.size > 0:
                if self.params.max_price < self.datahigh[0]:
                    self.params.max_price = self.datahigh[0]

                if self.buy_break < self.data.dual_buy_break[-1]:
                    self.buy_break = self.data.dual_buy_break[-1]
                    self.buy_open = self.data.dual_buy_open[-1]
                    # self.log('\n *buy move price %.2f\n' % (self.buy_open))
                    # moves.append({'order': 'sell', 'price': self.buy_open, 'time': self.data.datetime.time().strftime('%H:%M:%S')})

                if len(self) >= (self.bar_executed + 1): # 开仓后大于2分钟

                    # 冲高回落
                    if self.params.max_price > self.buy_break and self.dataclose[0] < self.buy_open:
                        self.log('BUY CLOSE HIT, %.2f' % self.dataclose[0])
                        self.order = self.sell()
                        self.params.max_price = None
                        self.buy_break = None
                        self.buy_open = None
                        trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

                    # # 移动平仓
                    elif self.dataclose[0] + self.data.atr[-1]/2 < self.dataclose[-1]:
                        self.log('BUY CLOSE MOV, %.2f' % self.dataclose[0])
                        self.order = self.sell()
                        self.params.max_price = None
                        self.buy_break = None
                        self.buy_open = None
                        trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            if self. position.size < 0:
                if self.params.min_price > self.datalow[0]:
                        self.params.min_price = self.datalow[0]

                if self.sale_break > self.data.dual_sale_break[-1]:
                    self.sale_break = self.data.dual_sale_break[-1]
                    self.sale_open = self.data.dual_sale_open[-1]
                    # self.log('\n *sell move price %.2f\n' % (self.sale_open))
                    # moves.append({'order': 'buy', 'price': self.sale_open, 'time': self.data.datetime.time().strftime('%H:%M:%S')})

                if len(self) >= (self.bar_executed + 1):

                    # 冲低回升
                    if self.params.min_price < self.sale_break and self.dataclose[0] > self.sale_open:
                        self.log('SALE CLOSE HIT, %.2f' % self.dataclose[0])
                        self.order = self.buy()
                        self.params.min_price = None
                        self.sale_open = None
                        self.sale_break = None
                        trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

                    # 移动平仓
                    elif self.dataclose[0] - self.data.atr[-1]/2  > self.dataclose[-1]:
                        self.log('SALE CLOSE MOV, %.2f' % self.dataclose[0])
                        self.order = self.buy()
                        self.params.min_price = None
                        self.sale_open = None
                        self.sale_break = None
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
    # moves = []
    # Create a cerebro entity
    cerebro = bt.Cerebro()
    # Add a strategy
    cerebro.addstrategy(MyStrategy)
    # 本地数据，笔者用Wind获取的东风汽车数据以csv形式存储在本地。
    # parase_dates = True是为了读取csv为dataframe的时候能够自动识别datetime格式的字符串，big作为index
    # 注意，这里最后的pandas要符合backtrader的要求的格式
    #dataframe = pd.read_csv('./data/hsi202003.csv', index_col=0, parse_dates=True)
    dataframe = pd.read_csv(csv_path, index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])
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
            fromdate=from_date,  # 起始日2002, 4, 1
            todate=to_date,  # 结束日 2015, 12, 31
            plot=False
        )
    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    # Set our desired cash start
    cerebro.broker.setcash(700000.0)
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
    # with open(json_path + ".move.json", 'w') as f:
    #     json.dump(moves, f)
    # cerebro.plot()
