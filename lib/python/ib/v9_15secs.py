# ? bt009.py
import backtrader as bt
from backtrader.feeds import PandasData # 用于扩展DataFeed
import datetime
import uuid

#先引入后面可能用到的包（package）
import pandas as pd
# import matplotlib.pyplot as plt
import os
import sys
import yaml
import json
# % matplotlib inline
from tqdm import tqdm
import sys
import talib as ta

import pytz
pytz.common_timezones[-8:]

tz = pytz.timezone('Asia/Shanghai')

#正常显示画图时出现的中文和负号
# from pylab import mpl
# mpl.rcParams['font.sans-serif']=['SimHei']

# 初始化, 加载配置文件

# def get_yaml_data(yaml_file):
#
#     # 打开yaml文件
#     print("***获取yaml文件数据***")
#     file = open(yaml_file, 'r', encoding="utf-8")
#     file_data = file.read()
#     file.close()
#
#     print(file_data)
#     print("类型：", type(file_data))
#
#     # 将字符串转化为字典或列表
#     print('\n' * 3)
#     print("***转化yaml数据为字典或列表***")
#     data = yaml.safe_load(file_data)
#     print(data)
#     print("类型：", type(data))
#     print('\n' * 3)
#     return data
#
# config_file = sys.argv[1]
# current_path = os.path.abspath(".")
# yaml_path = os.path.join(current_path, config_file)
# my_config = get_yaml_data(yaml_path)


#回测期间
# start_time = datetime.datetime.strptime(my_config['from_date'],
#                                        '%Y-%m-%d %H:%M:%S')
# end_time = datetime.datetime.strptime(my_config['end_date'],
#                                      '%Y-%m-%d %H:%M:%S')


class PandasDataExtend(PandasData):
    # 增加线
    lines = ('hb', 'hh', 'lb', 'll', 'hb_1', 'hh_1', 'lb_1', 'll_1', 'high_1',
             'low_1', 'close_1', 'atr_1')

    # 第几列, 或者直接给列名
    params = (
        ('hb', 'hb'),
        ('hh', 'hh'),
        ('lb', 'lb'),
        ('ll', 'll'),
        ('hb_1', 'hb_1'),
        ('hh_1', 'hh_1'),
        ('lb_1', 'lb_1'),
        ('ll_1', 'll_1'),
        ('high_1', 'high_1'),
        ('low_1', 'low_1'),
        ('close_1', 'close_1'),
        ('atr_1', 'atr_1'),
    )  # 上市天数


# Create a Stratey
class MyStrategy(bt.Strategy):
    params = (
        ('maperiod', 4 * 5 * 12),
        ('printlog', True),
        ('max_price', 0),
        ('min_price', 0),
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
        self.dataatr = self.datas[0].atr_1

        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None
        self.sellprice = None
        self.sellcomm = None

        self.overcross = False
        self.overcross2 = False
        self.overcross_price = None
        self.win_loss = 0
        self.tr = self.datas[0].high - self.datas[0].low_1
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
                self.log('BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price, order.executed.value,
                          order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm

            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price, order.executed.value,
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
        if self.data.datetime.time() > datetime.time(
                16, 20) or self.data.datetime.time() < datetime.time(9, 15):
            self.win_loss = 0
            if self.position.size > 0:
                self.order = self.sell()
                self.log('BUY Close by Day end, %.4f' % self.dataclose[0])
                trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            if self.position.size < 0:
                self.order = self.buy()
                self.log('SELL Close by Day end, %.4f' % self.dataclose[0])
                trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            return

        if self.order:
            return

        # Check if we are in the market

        if not self.position:

            c1 = self.datas[0].atr_1[0] > self.datas[0].atr_1[-25]
            c2 = self.datas[0].atr_1[0] > self.datas[0].atr_1[-45]

            if self.datahigh[0] > self.datas[0].hb_1[0] and (c1 and c2):
                self.log('BUY CREATE, %.4f, %.4f' %
                         (self.dataclose[0], self.datas[0].hb_1[0]))
                self.order = self.buy()
                trades.append({'order': 'buy', 'time': self.data.datetime.time().strftime('%H:%M:%S')})
                self.max_price = self.dataclose[0]
                self.overcross = False
                self.overcross_price = None

            elif self.datalow[0] < self.datas[0].lb_1[0] and (c1 and c2):
                self.log('SELL CREATE, %.4f, %.4f' %
                         (self.dataclose[0], self.datas[0].lb_1[0]))
                self.order = self.sell()
                trades.append({'order': 'sell', 'time': self.data.datetime.time().strftime('%H:%M:%S')})
                self.min_price = self.dataclose[0]
                self.overcross = False
                self.overcross_price = None

            # # 波动收敛策略
            if not c1 and not c2:
                if self.datahigh[
                        0] > self.datas[0].hh_1[0] + self.datas[0].atr_1[0]:
                    self.log('SELL CREATE, %.4f, %.4f' %
                             (self.dataclose[0], self.datas[0].lb_1[0]))
                    self.order = self.sell()
                    trades.append({'order': 'sell', 'time': self.data.datetime.time().strftime('%H:%M:%S')})
                    self.min_price = self.dataclose[0]
                    self.overcross = False
                    self.overcross_price = None

                if self.datalow[
                        0] < self.datas[0].ll_1[0] - self.datas[0].atr_1[0]:
                    self.log('BUY CREATE, %.4f, %.4f' %
                             (self.dataclose[0], self.datas[0].hb_1[0]))
                    self.order = self.buy()
                    trades.append({'order': 'buy', 'time': self.data.datetime.time().strftime('%H:%M:%S')})
                    self.max_price = self.dataclose[0]
                    self.overcross = False
                    self.overcross_price = None

        else:
            if self.position.size > 0:
                # 冲高回落

                close_sig = False
                self.max_price = max(self.max_price, self.datahigh[0])

                # # 冲高20点开始计算回落
                if self.max_price > self.datas[0].hh_1[0] + self.dataatr[0] / 2:
                    self.overcross = True

                if self.datahigh[0] < self.max_price - self.dataatr[0]:
                    self.log('BUY CLOSE A: -20, %4f' % (self.dataclose[0]))
                    close_sig = True

                if self.overcross and self.datalow[0] < self.datas[0].hh_1[0]:
                    self.log('BUY CLOSE A: hl, %4f' % (self.dataclose[0]))
                    close_sig = True

                if self.datas[0].hh_1[0] != self.datas[0].hh_1[-2]:
                    self.overcross = False

                # 移动平仓

                if self.dataclose[
                        0] < self.datas[0].close_1[0] - self.dataatr[0] / 2:
                    self.log('BUY CLOSE B: move , %4f' % (self.dataclose[0]))
                    close_sig = True

                if close_sig:
                    self.order = self.sell()
                    trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})
                    self.max_price = None
                    self.overcross = False

            if self.position.size < 0:
                close_sig = False
                self.min_price = min(self.min_price, self.datalow[0])

                # 冲高20点开始计算回落
                if self.min_price < self.datas[0].ll_1[0] - self.dataatr[0] / 2:
                    self.overcross = True

                if self.datalow[0] > self.min_price + self.dataatr[0]:
                    self.log('SELL CLOSE A: -20, %4f' % (self.dataclose[0]))
                    close_sig = True

                if self.overcross and self.datahigh[0] > self.datas[0].ll_1[0]:
                    self.log('SELL CLOSE A: ll, %4f' % (self.dataclose[0]))
                    close_sig = True

                if self.datas[0].ll_1[0] != self.datas[0].ll_1[-2]:
                    self.overcross = False

                # 移动平仓

                if self.dataclose[
                        0] > self.datas[0].close_1[0] + self.dataatr[0] / 2:
                    self.log('SELL CLOSE B: move , %4f' % (self.dataclose[0]))
                    close_sig = True

                if close_sig:
                    self.order = self.buy()
                    trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})
                    self.min_price = None
                    self.overcross = False

    def stop(self):
        print("death")


if __name__ == '__main__':
    csv_path = sys.argv[1]
    json_path = sys.argv[2]
    begin_time = sys.argv[3]
    end_time = sys.argv[4]
    yaml_path = sys.argv[5]
    h5_path = sys.argv[6]

    begin_time = datetime.datetime.strptime(begin_time, '%Y-%m-%d %H:%M:%S +0800')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S +0800')

    trades = []
    profits = []

    cerebro = bt.Cerebro()

    # 本地数据
    dataframe = pd.read_hdf(h5_path, key='df2')
    dataframe.reset_index(inplace=True)

    data = PandasDataExtend(
        dataname=dataframe,
        datetime=-1,  # 日期列
        open=-1,  # 开盘价所在列
        high=-1,  # 最高价所在列
        low=-1,  # 最低价所在列
        close=-1,  # 收盘价价所在列
        volume=-1,  # 成交量所在列
        openinterest=-1,
        fromdate=begin_time,  # 起始日2002, 4, 1
        todate=end_time,  # 结束日 2015, 12, 31
        plot=False)

    cerebro.broker.setcash(250000.0)
    # 设置每笔交易交易的股票数量
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)
    # Set the commission
    cerebro.broker.setcommission(
        commission=30,
        commtype=bt.CommInfoBase.COMM_FIXED,  # 固定手续费
        automargin=5,  # 保证金10% , 这里5是因为hsi 指数 一个点50元, 10%保证金, 交易一次30元
        mult=50  # 利润乘数, hsi 是1个点50
    )


    cerebro.addstrategy(strategy_kam)
    cerebro.adddata(data)

    # uuid_str = uuid.uuid4().hex
    # cerebro.addwriter(bt.WriterFile,
    #                   csv=True,
    #                   out="./output/{}.csv".format(uuid_str))

    # 策略执行前的资金
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # cerebro.addanalyzer(bt.analyzers.SharpeRatio,
    #                     _name='SharpeRatio',
    #                     timeframe=bt.TimeFrame.Months)

    # cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')

    # cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='myannual')

    # cerebro.addanalyzer(bt.analyzers.TimeReturn,
    #                     _name='TR',
    #                     timeframe=bt.TimeFrame.Months)


    results = cerebro.run()

    # 策略执行后的资金
    # print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    #
    # strat = results[0]
    # print('SR:', strat.analyzers.SharpeRatio.get_analysis())
    # print('DW:', strat.analyzers.DW.get_analysis())
    # print('AN:', strat.analyzers.myannual.get_analysis())
    # print('TimeReturn')
    # for date, value in results[0].analyzers.TR.get_analysis().items():
    #     print(date, value)
    #
    # cerebro.plot()
    print(trades)
    with open(json_path, 'w') as f:
        json.dump(trades, f)
