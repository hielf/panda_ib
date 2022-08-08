import backtrader as bt
from backtrader.order import Order
import datetime
import json

class kam002(bt.Strategy):
    # 反向回归
    # 15s, 需要构建5分钟判断, 所以4*5=20 个bar, 考虑其他情况翻倍
    params=(
        ('atr_timeperiod', 4*6),
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
        self.datahh = self.datas[0].hh_5T
        self.datall = self.data.ll_5T
        self.datatr = self.data.tr_5T
        # 初始化交易指令、买卖价格和手续费
        self.minprice= None
        self.maxprice=None
        self.stop_range = None
        self.old_close5T = self.data.close_5T
        self.bar_executed = 0
        self.close_bar_executed = 0
        self.order = None
        self.trades = []

        print(self)

        #添加指标，内置了talib模块
        # self.atr = bt.talib.ATR(self.datahigh, self.datalow, self.dataclose, self.p.atr_timeperiod, subplot=False)


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

                self.trades.append({'order': 'buy', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price, order.executed.value,
                          order.executed.comm))
                self.sellprice = order.executed.price
                self.sellcomm = order.executed.comm

                self.trades.append({'order': 'sell', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

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
                16, 15) or self.data.datetime.time() < datetime.time(9, 25):
            if self.position.size > 0:
                self.order = self.close()
                self.log('BUY Close by Day end, %.4f' % self.dataclose[0])
                self.trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            if self.position.size < 0:
                self.order = self.close()
                self.log('SELL Close by Day end, %.4f' % self.dataclose[0])
                self.trades.append({'order': 'close', 'time': self.data.datetime.time().strftime('%H:%M:%S')})

            return

        if self.order: # 检查是否有指令等待执行,
            return



        # 检查是否持仓
        if not self.position: # 没有持仓
            tr_condition = True
            if self.config['condition']:
                tr_condition = eval(self.config['condition'])

            if self.dataclose[0] > self.datahh[0] and tr_condition:
                #执行买入
                self.order = self.buy()

            if self.dataclose[0] < self.datall[0] and tr_condition:
                #执行买入
                self.order = self.sell()
        else:
            if self.maxprice is None:
                self.maxprice = self.data.high[0]

            if self.minprice is None:
                self.minprice = self.data.low[0]

            if self.stop_range is None:
                self.stop_range = self.datatr[0]

            self.minprice = min(self.data.close[0], self.minprice)
            self.maxprice = max(self.data.close[0], self.maxprice)
            is_close = False
            if self.position.size > 0:
                # 不能小于开仓价格
                if self.dataclose[0] > self.buyprice + self.stop_range*4:
                    self.log('long1:小于开仓价')
                    is_close = True

                # 跟踪平仓, 当前价格和最高价的差
                if self.dataclose[0] < self.maxprice - self.stop_range*2:
                    self.log('long4:移动跟踪平仓')
                    is_close = True


                if is_close:
                    self.order = self.close()
                    self.maxprice = None
                    self.minprice = None
                    self.stop_range = None
                    is_close = False

            if self.position.size < 0:

                # 不能>开仓价格
                if self.dataclose[0] < self.sellprice - self.stop_range*4:
                    self.log('short1:>开仓价')
                    is_close = True


                # 跟踪平仓, 当前价格和最高价的差
                if self.dataclose[0] > self.minprice + self.stop_range*2:
                    self.log('short4:移动跟踪平仓')
                    is_close = True


                if is_close:
                    self.order = self.close()
                    self.maxprice = None
                    self.minprice = None
                    self.stop_range = None
                    is_close = False

        if self.old_close5T != self.data.close_5T[0]:
            self.old_close5T = self.data.close_5T[0]

        with open('/Users/hielf/workspace/projects/panda_ib/tmp/hsi_15secs_trades.json', 'w') as f:
            json.dump(self.trades, f)
