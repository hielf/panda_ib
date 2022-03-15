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
# % matplotlib inline

#正常显示画图时出现的中文和负号
# from pylab import mpl
# mpl.rcParams['font.sans-serif']=['SimHei']

# 初始化, 加载配置文件

def get_yaml_data(yaml_file):

    # 打开yaml文件
    print("***获取yaml文件数据***")
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()

    print(file_data)
    print("类型：", type(file_data))

    # 将字符串转化为字典或列表
    print('\n' * 3)
    print("***转化yaml数据为字典或列表***")
    data = yaml.safe_load(file_data)
    print(data)
    print("类型：", type(data))
    print('\n' * 3)
    return data

config_file = sys.argv[1]
current_path = os.path.abspath(".")
yaml_path = os.path.join(current_path, config_file)
my_config = get_yaml_data(yaml_path)


#回测期间
start_time = datetime.datetime.strptime(my_config['from_date'],
                                       '%Y-%m-%d %H:%M:%S')
end_time = datetime.datetime.strptime(my_config['end_date'],
                                     '%Y-%m-%d %H:%M:%S')


# 构建基本策略
class strategy_kam(bt.Strategy):
    #全局设定交易策略的参数
    # 15s, 需要构建5分钟判断, 所以4*5=20 个bar, 考虑其他情况翻倍
    params=(
        ('period', 4*5*2),
        ('BBandsperiod', 4*5*6),

        ('maperiod',4*5*6),
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
        self.minprice= None
        self.maxprice=None
        self.bar_executed = 0
        self.close_bar_executed = 0

        #添加指标，内置了talib模块

        self.dch = bt.ind.Highest(self.data.high, period=self.p.period, subplot=False)
        self.dcl = bt.ind.Lowest(self.data.low, period=self.p.period, subplot=False)

        self.atr = bt.talib.ATR(self.dch, self.dcl, self.data.close, self.p.period*3, subplot=False)
        self.bband = bt.indicators.BBands(self.datas[0], period=self.params.BBandsperiod, devfactor=2)

        self.tr = self.dch - self.dcl
        self.ema = bt.ind.SMA(self.data.close, period=self.p.period*6, subplot=False)



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

            if self.position.size == 0:
                self.close_bar_executed = len(self)

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
                16, 15) or self.data.datetime.time() < datetime.time(9, 15):
            if self.position.size > 0:
                self.order = self.sell()
                self.log('BUY Close by Day end, %.4f' % self.dataclose[0])

            if self.position.size < 0:
                self.order = self.buy()
                self.log('SELL Close by Day end, %.4f' % self.dataclose[0])

            return

        start_k = (len(self) - self.close_bar_executed ) % self.params.period + 1


        if self.order: # 检查是否有指令等待执行,
            return

        # 检查是否持仓
        if not self.position: # 没有持仓

            if self.dataclose[0] > (self.dch[-start_k] + self.atr[-start_k]) and (self.ema[-1] > self.ema[-self.params.period]+ self.atr[-start_k]/2):
                #执行买入
                # pass
                self.order = self.sell()

            if self.dataclose[0] < (self.dcl[-start_k] - self.atr[-start_k]) and (self.ema[-1] < self.ema[-self.params.period]- self.atr[-start_k]/2):
                #执行买入
                # pass
                self.order = self.buy()
        else:
            if self.maxprice is None:
                self.maxprice = self.dataclose[-start_k]

            self.maxprice = max(self.dataclose[-start_k], self.maxprice)

            if self.minprice is None:
                self.minprice = self.dataclose[-start_k]

            self.minprice = min(self.dataclose[-start_k], self.minprice)

            if self.position.size < 0 and start_k > self.p.period -2:
                if (
                    self.dataclose[0] < self.maxprice  - self.atr[-start_k]*2
                    or self.maxprice - self.atr[-start_k]*2 > self.sellprice
                    # or self.dataclose[0] > self.minprice + self.atr[-start_k]*4
                        # or (self.dch[0] > self.dch[-start_k] + self.tr[-start_k]
                        # and self.dataclose[0] < self.dch[-start_k] + self.tr[-start_k]/2 )
                    ):
                    self.order = self.buy()
                    self.maxprice = None
                    self.minprice = None

            if self.position.size > 0 and start_k > self.p.period -2:

                if (
                    self.dataclose[0] > self.minprice + self.atr[-start_k]*2
                    or self.minprice + self.atr[-start_k]*2 < self.buyprice
                    # or self.dataclose[0] < self.maxprice  - self.atr[-start_k]*4
                        # or (self.dcl[0] < self.dcl[-start_k] - self.tr[-start_k]
                        # and self.dataclose[0] > self.dcl[-start_k] - self.tr[-start_k]/2 )
                    ):
                    self.order = self.sell()
                    self.maxprice = None
                    self.minprice = None



if __name__ == '__main__':

    cerebro = bt.Cerebro()

    # 本地数据
    df = pd.read_csv(my_config['data_source'], index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])
    df.index = pd.to_datetime(df.index)

    data = PandasData(
            dataname=df,
            fromdate=start_time,
            todate=end_time,
        )

    cerebro.broker.setcash(my_config['cash'])
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

    uuid_str = uuid.uuid4().hex
    cerebro.addwriter(bt.WriterFile,
                      csv=True,
                      out="./output/{}.csv".format(uuid_str))

    # 策略执行前的资金
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    cerebro.addanalyzer(bt.analyzers.SharpeRatio,
                        _name='SharpeRatio',
                        timeframe=bt.TimeFrame.Months)

    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')

    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='myannual')

    cerebro.addanalyzer(bt.analyzers.TimeReturn,
                        _name='TR',
                        timeframe=bt.TimeFrame.Months)


    results = cerebro.run()

    # 策略执行后的资金
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())

    strat = results[0]
    print('SR:', strat.analyzers.SharpeRatio.get_analysis())
    print('DW:', strat.analyzers.DW.get_analysis())
    print('AN:', strat.analyzers.myannual.get_analysis())
    print('TimeReturn')
    for date, value in results[0].analyzers.TR.get_analysis().items():
        print(date, value)

    cerebro.plot()
