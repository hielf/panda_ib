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

from tqdm import tqdm

starttime = time.time()

reg_buy_open = joblib.load('reg_buy_open4.pkl')
reg_buy_break = joblib.load('reg_buy_break4.pkl')
reg_sale_open = joblib.load('reg_sale_open4.pkl')
reg_sale_break = joblib.load('reg_buy_break4.pkl')



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

    def DUAL(self, df2, period):
        
        period_data = df2.resample(period).last()

        period_data['open'] = df2['open'].resample(period).first()
        # 处理最高价和最低价
        period_data['high'] = df2['high'].resample(period).max()
        # 最低价
        period_data['low'] = df2['low'].resample(period).min()

        period_data['hh'] = period_data['high'].rolling(1).max()
        period_data['ll'] = period_data['low'].rolling(1).min()

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
        pred_data = self.DUAL(dt, self.params.dual_period)

        # lines 0 是当前, 1是未来, -1 是上一个 和pandas 不一样, pd 是 -1 为当前时间
        self.lines.close_resample[0] = pred_data.close[-2] # 当前价格和上一个close 价格比较

        prenext_num = -2
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
        ('dual_window',24),
        ('dual_period', '04T'),
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
 
        self.dual_lines = dual_trust(dual_window=self.params.dual_window, dual_period= self.params.dual_period, subplot=False)
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

            if self.dataclose[0] > self.dual_lines.dual_buy_open[0]:
                 self.log('BUY CREATE, %.2f' % self.dataclose[0])
                 self.order = self.buy()
                 
            elif self.dataclose[0] < self.dual_lines.dual_sale_open[0]:
                 self.log('SELL CREATE, %.2f' % self.dataclose[0])
                 self.order = self.sell()
 
        else:
            '''
            > 0 is long (you have taken)
            == 0 is no position
            < 0 is short (you have given)
            '''
            if self. position.size > 0 and len(self) >= (self.bar_executed + 4):
                if self.params.max_price < self.dataclose[0]:
                    self.params.max_price = self.dataclose[0]
                # 冲高回落
                if self.params.max_price > self.dual_lines.dual_buy_break[0] and self.dataclose[0] < self.dual_lines.dual_buy_open[0]:
                    self.log('BUY CLOSE HIT, %.2f' % self.dataclose[0])
                    self.order = self.sell()
                    self.params.max_price = 0 
                # # 移动平仓
                elif self.dataclose[0] < self.dual_lines.close_resample[0]:
                    self.log('BUY CLOSE MOV, %.2f' % self.dataclose[0])
                    self.order = self.sell()
                    self.params.max_price = 0

            if self. position.size < 0 and len(self) >= (self.bar_executed + 4):
                if self.params.min_price > -self.dataclose[0]:
                    self.params.min_price = -self.dataclose[0]
                # 冲低回升
                if abs(self.params.min_price) < self.dual_lines.dual_sale_break[0] and self.dataclose[0] > self.dual_lines.dual_sale_open[0]:
                    self.log('SALE CLOSE HIT, %.2f' % self.dataclose[0])
                    self.order = self.buy()
                    self.params.min_price = 0
                # 移动平仓
                elif self.dataclose[0] > self.dual_lines.close_resample[0]:
                    self.log('SALE CLOSE MOV, %.2f' % self.dataclose[0])
                    self.order = self.buy()
                    self.params.min_price = 0

            
    def stop(self):
        print("death")
 
if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()
    # Add a strategy
    cerebro.addstrategy(MyStrategy)
    # 本地数据，笔者用Wind获取的东风汽车数据以csv形式存储在本地。
    # parase_dates = True是为了读取csv为dataframe的时候能够自动识别datetime格式的字符串，big作为index
    # 注意，这里最后的pandas要符合backtrader的要求的格式
    #dataframe = pd.read_csv('./data/hsi202003.csv', index_col=0, parse_dates=True)
    dataframe = pd.read_csv('./data/hsi202003.csv', index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])
    dataframe['openinterest'] = 0
    data = bt.feeds.PandasData(dataname=dataframe,
                            fromdate = datetime.datetime(2020, 3, 17),
                            todate = datetime.datetime(2020, 3, 26)
                            )
    # Add the Data Feed to Cerebro
    cerebro.adddata(data)
    # Set our desired cash start
    cerebro.broker.setcash(700000.0)
    # 设置每笔交易交易的股票数量
    cerebro.addsizer(bt.sizers.FixedSize, stake=4)
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
    print('from 2018,1,1 to 2018,3,1', '+4')
    print ('time:', (endtime - starttime), 'seconds')
    print('='*5, 'program running time', '='*5)

    strat = results[0]
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    print('SR:', strat.analyzers.SharpeRatio.get_analysis())
    print('DW:', strat.analyzers.DW.get_analysis())

    cerebro.plot()
