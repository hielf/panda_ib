#%%
'''
读取格式为['date', 'open', 'high', 'low', 'close', 'volume']的原始数据

然后转换为可以被bt策略执行的h5格式结构(hdf5格式读写速度比csv快)

input: read data
output: write hdf5

'''

import json
import pandas as pd

import os
import csv
import yaml
from tqdm import tqdm
import sys
import talib as ta


#%% 自定义函数


def get_yaml_data(yaml_file):

    # 打开yaml文件
    print("***获取yaml文件数据***")
    file = open(yaml_file, 'r', encoding="utf-8")
    file_data = file.read()
    file.close()

    print(file_data)
    print("类型：", type(file_data))

    # 将字符串转化为字典或列表
    print("***转化yaml数据为字典或列表***")
    data = yaml.load(file_data)
    print(data)
    print("类型：", type(data))
    return data

def format_data(dataframe, period='5T', localize_zone='Asia/Shanghai', convert_zone= 'US/Eastern'):
    '''
        目前最小单位15s, 默认组合一行数据为15s+5T
    '''

    # 设置日期索引后才能做resample
    dataframe.reset_index(inplace=True)
    dataframe.rename(columns={'date':'datetime'}, inplace=True)
    dataframe.set_index('datetime', inplace=True)

    # spx需要做时区转换
    # ts_utc = dataframe.index.tz_localize(localize_zone)
    # dataframe.index = ts_utc.tz_convert(convert_zone)

    df1 = dataframe
    df1.sort_index(inplace=True)

    # 如果df1 小于15s, 那么需要处理为15s
    # df1= df1.resample('15S').agg({'open': 'first',
    #                             'high': 'max',
    #                             'low': 'min',
    #                             'close': 'last', 'volume': 'sum'})

    # df1 = df1.dropna(axis=0)

    df1['openinterest'] = 0

    # 生成低频周期
    df2= df1.resample(period, closed='right',label='right').agg({'open': 'first',
                                'high': 'max',
                                'low': 'min',
                                'close': 'last', 'volume': 'sum'})

    df3 = df2.dropna(axis=0) # 缺失值处理

    df3['atr'] = ta.ATR(df3['high'] , df3['low'], df3['close'], timeperiod=26)
    # 当前bar的hh,ll 是上一个bar决定的, shift(1)
    df3['tr'] = df3['high'] - df3['low']
    df3['tr_1'] = df3['atr'].shift(1)
    df3['tr_1'].fillna(0, inplace=True)

    df3['hb'] = df3['high'] + df3['tr_1'] * config_params['base_line']
    df3['hh'] = df3['hb'] + df3['tr_1'] * config_params['break_line']

    df3['lb'] =  df3['low'] - df3['tr_1'] * config_params['base_line']
    df3['ll'] =  df3['lb'] - df3['tr_1'] * config_params['break_line']



    # df3['slowk'], df3['slowd'] = ta.STOCH(
	# 		            df3['high'] , df3['low'], df3['close'],
    #                     fastk_period=9,
    #                     slowk_period=3,
    #                     slowk_matype=0,
    #                     slowd_period=3,slowd_matype=0)



    for item in [ 'hb', 'hh', 'lb', 'll', 'high', 'low', 'close','atr']:
        df3[item+'_1'] = df3[item].shift(1)



    df3 = df3[[ 'hb', 'hh', 'lb', 'll', 'hb_1', 'hh_1', 'lb_1', 'll_1', 'high_1', 'low_1', 'close_1', 'atr_1']]
    print('-'*30)
    print(df3.head(2))
    df3.dropna(inplace=True)
    df3.to_csv('./output/df3.csv')

    df3 = df3.asfreq(freq='15S', method='bfill')


    df3.reset_index(inplace=True)
    df1.reset_index(inplace=True)

    df4 = pd.merge(df1, df3, how='left')
    df4.fillna(method='ffill', inplace=True)#用前面的值来填充
    df4.dropna(inplace=True)
    print('df count:', df4.shape[0])

    return df4

#%% 初始化, 读取数据
configfile = sys.argv[1]
current_path = os.path.abspath(".")
yaml_path = os.path.join(current_path, configfile)
config_params = get_yaml_data(yaml_path)

df = pd.read_csv(config_params['data_source'], index_col=0, parse_dates=True, usecols=['date', 'open', 'high', 'low', 'close', 'volume'])


# %% 处理数据
df2 = format_data(df, period=config_params['period'])

# %% write hdf5
df2.reset_index(inplace=True)
df2.drop(columns='index', inplace=True)
df2.set_index('datetime', inplace=True)
df2.to_hdf('./hsi.h5', key='df2')
df2.to_csv('./output/hsi002.csv')
