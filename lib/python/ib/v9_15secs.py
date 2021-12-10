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


# 构建基本策略
class strategy_kam(bt.Strategy):
    #全局设定交易策略的参数
    # 15s, 需要构建5分钟判断, 所以4*5=20 个bar, 考虑其他情况翻倍
    params=(
        ('period', 20),
        ('maperiod',20*3),
        ('printlog', True),
           )
    def log(self, txt, dt=None, doprint=False):
        ''' Logging function fot this strategy'''
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.datetime(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        #指定价格序列
        self.dataclose=self.data.close
        self.datahigh=self.data.high
        self.datalow=self.data.low
        # 初始化交易指令、买卖价格和手续费
        self.order = None
        self.buyprice = None
        self.buycomm = None

        #添加指标，内置了talib模块
        self.atr = bt.talib.ATR(self.data.high, self.data.low, self.data.close, timeperiod=20*3, subplot=False)
        self.dch = bt.ind.Highest(self.data.high, period=self.p.period, subplot=False)
        self.dcl = bt.ind.Lowest(self.data.low, period=self.p.period, subplot=False)
        self.tr = self.dch - self.dcl


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
            # self.log('Order Canceled/Margin/Rejected')

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, COMM %.2f, GROSS %.2f, NET %.2f \n\n' %
                 (trade.commission, trade.pnl, trade.pnlcomm))

        if trade.pnlcomm < 0:
            pass

    def next(self):

        # ? 交易时段判断
        if self.data.datetime.time() > datetime.time(
                16, 20) or self.data.datetime.time() < datetime.time(9, 15):
            if self.position.size > 0:
                self.order = self.sell()
                self.log('BUY Close by Day end, %.4f' % self.dataclose[0])
                trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            if self.position.size < 0:
                self.order = self.buy()
                self.log('SELL Close by Day end, %.4f' % self.dataclose[0])
                trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            return

        if self.order: # 检查是否有指令等待执行,
            return

        # 检查是否持仓
        if not self.position: # 没有持仓
            #执行买入条件判断：收盘价格上涨突破20日均线
            if self.datahigh[0] > self.dch[-4] and self.datahigh[0] > self.dch[-self.p.period] + self.tr[-self.p.period]/2 and self.tr[-self.p.period] > 20:
                #执行买入
                self.order = self.buy()
                trades.append({'order': 'sell', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            if self.datalow[0] < self.dcl[-4] and self.datalow[0] < self.dcl[-self.p.period] - self.tr[-self.p.period]/2 and self.tr[-self.p.period] > 20:
                #执行买入
                self.order = self.sell()
                trades.append({'order': 'buy', 'time': self.data.datetime.time().strftime('%H:%M:%S')})
        else:
            #执行卖出条件判断：收盘价格跌破20日均线
            if self.position.size > 0:
                if (
                    self.dataclose[0] < self.dcl[-1] + self.tr[-1]/3
                        or self.dch[-1] > self.dch[-self.p.period] + self.tr[-self.p.period]*1.3
                        and self.dataclose[0] < self.dch[-self.p.period] + self.tr[-self.p.period]/3
                        and self.tr[-1] > 10
                    ):
                    self.order = self.sell()
                    trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            if self.position.size < 0:
                if (
                    self.dataclose[0] > self.dch[-1] - self.tr[-1]/3
                        or self.dcl[-1] < self.dcl[-self.p.period] - self.tr[-self.p.period]*1.3
                        and self.dataclose[0] > self.dcl[-self.p.period] - self.tr[-self.p.period]/3
                        and self.tr[-1] > 10
                    ):
                    self.order = self.buy()
                    trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})



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
    df = pd.read_csv(csv_path, index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])
    df.index = pd.to_datetime(df.index)

    data = PandasData(
            dataname=df,
            fromdate=begin_time,
            todate=end_time,
        )

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
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # strat = results[0]
    # print('SR:', strat.analyzers.SharpeRatio.get_analysis())
    # print('DW:', strat.analyzers.DW.get_analysis())
    # print('AN:', strat.analyzers.myannual.get_analysis())
    # print('TimeReturn')
    # for date, value in results[0].analyzers.TR.get_analysis().items():
    #     print(date, value)

    # cerebro.plot()
    print(trades)
    with open(json_path, 'w') as f:
        json.dump(trades, f)
