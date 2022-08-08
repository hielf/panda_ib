# ? bt009.py
import backtrader as bt
from backtrader.feeds import PandasData  # 用于扩展DataFeed

import uuid
# from matplotlib.pyplot import subplot
import yaml
import json
#先引入后面可能用到的包（package）
import pandas as pd
# import matplotlib.pyplot as plt
import os
import sys
import datetime
import time
from common import utils
from strategies import kam, online
# % matplotlib inline

#正常显示画图时出现的中文和负号
# from pylab import mpl
# mpl.rcParams['font.sans-serif']=['SimHei']

# 初始化, 加载配置文件

# config_file = sys.argv[1]

# my_config = utils.get_config(config_file)

#回测期间
# start_time = datetime.datetime.strptime(my_config['from_date'],
#                                         '%Y-%m-%d %H:%M:%S')
# end_time = datetime.datetime.strptime(my_config['end_date'],
#                                       '%Y-%m-%d %H:%M:%S')


class PandasData(bt.feeds.PandasData):
    period = '_5T'
    period2 = '_10T'
    lines = (f'open{period}', f'high{period}', f'low{period}',
             f'close{period}', f'volume{period}', f'tr{period}',
             f'hh{period}',f'll{period}',
             f'open{period2}', f'high{period2}',
             f'low{period2}', f'close{period2}', f'volume{period2}',
             f'tr{period2}', f'hh{period2}', f'll{period2}')
    params = (
        ('datetime', None),
        ('open', -1),
        ('high', -1),
        ('low', -1),
        ('close', -1),
        ('volume', -1),
        ('openinterest', -1),
        (f'open{period}', -1),
        (f'high{period}', -1),
        (f'low{period}', -1),
        (f'close{period}', -1),
        (f'volume{period}', -1),
        (f'tr{period}', -1),
        (f'hh{period}', -1),
        (f'll{period}', -1),
        (f'open{period2}', -1),
        (f'high{period2}', -1),
        (f'low{period2}', -1),
        (f'close{period2}', -1),
        (f'volume{period2}', -1),
        (f'tr{period2}', -1),
        (f'hh{period2}', -1),
        (f'll{period2}', -1),
    )


# 构建基本策略
def read_data(source):
    start_time = datetime.datetime.strptime(my_config['from_date'],
                                            '%Y-%m-%d %H:%M:%S')
    end_time = datetime.datetime.strptime(my_config['end_date'],
                                          '%Y-%m-%d %H:%M:%S')
    df = pd.read_csv(source, parse_dates=True)
    df['date'] = pd.to_datetime(df['date'])
    df.set_index('date', inplace=True)
    print(list(df.columns))
    print(df.head())
    data = PandasData(
        dataname=df,
        fromdate=start_time,
        todate=end_time,
    )
    return data


if __name__ == '__main__':
    csv_path = sys.argv[1]
    json_path = sys.argv[2]
    begin_time = sys.argv[3]
    end_time = sys.argv[4]
    yaml_path = sys.argv[5]
    middle_path = sys.argv[6]
    config_file = sys.argv[7]

    my_config = utils.get_config(config_file)

    begin_time = datetime.datetime.strptime(begin_time, '%Y-%m-%d %H:%M:%S +0800')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S +0800')

    # 创建引擎
    cerebro = bt.Cerebro()

    # 设定现金
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

    # 添加策略
    # s01 = online.kam999
    s01 = kam.kam002
    s01.config = my_config
    cerebro.addstrategy(s01)

    # 读取数据
    data = read_data(middle_path)
    cerebro.adddata(data)

    # uuid_str = uuid.uuid4().hex
    # now = time.strftime("%Y-%m-%d-%H_%M_%S", time.localtime(time.time()))
    # cerebro.addwriter(bt.WriterFile,
    #                   csv=True,
    #                   out="./output/{}-{}.csv".format(now, uuid_str))
    #
    # # 策略执行前的资金
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())
    #
    # cerebro.addanalyzer(bt.analyzers.SharpeRatio,
    #                     _name='SharpeRatio',
    #                     timeframe=bt.TimeFrame.Months)
    #
    # cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')
    #
    # cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='myannual')
    #
    # cerebro.addanalyzer(bt.analyzers.TimeReturn,
    #                     _name='TR',
    #                     timeframe=bt.TimeFrame.Months)
    #
    #
    # cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')

    results = cerebro.run()


    # 策略执行后的资金
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
    #
    # strat = results[0]
    # print('SR:', strat.analyzers.SharpeRatio.get_analysis())
    # print('DW:', strat.analyzers.DW.get_analysis())
    # print('TA:', strat.analyzers.myannual.get_analysis())
    # print('TimeReturn')
    # for date, value in results[0].analyzers.TR.get_analysis().items():
    #     print(date, value)
    #
    # result = pd.DataFrame([])
    # if os.path.exists('./output/result.csv'):
    #     result = pd.read_csv(
    #         './output/result.csv',
    #         usecols=['SR', 'no', 'config', 'TR-date', 'TR-value'])
    #
    # result = result.append(
    #     {
    #         'SR': strat.analyzers.SharpeRatio.get_analysis()['sharperatio'],
    #         'TA': cerebro.broker.getvalue(),
    #         'TR-date': '--',
    #         'TR-value': '--',
    #         'no': "./output/{}-{}.csv".format(now, uuid_str),
    #         'config': sys.argv[1]
    #     },
    #     ignore_index=True)
    # for date, value in results[0].analyzers.TR.get_analysis().items():
    #     result = result.append({
    #         'TR-date': date,
    #         'TR-value': value
    #     },
    #                            ignore_index=True)
    # result.to_csv('./output/result.csv')
    #
    #
    # portfolio_stats = strat.analyzers.getbyname('pyfolio')
    # returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
    # returns.index = returns.index.tz_convert(None)
    #
    #
    #
    # import quantstats
    # quantstats.reports.html(returns, output='stats.html', title='HSI')
    # print(trades)
    # with open(json_path, 'w') as f:
    #     json.dump(trades, f)

    # cerebro.plot()
