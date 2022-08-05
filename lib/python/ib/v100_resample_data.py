# from turtle import end_fill
import pandas as pd
import sys
import os
import click
import talib as ta

# 用pprint 替代 print, 改善格式化输出
import pprint

from time import time
start = time()
print = pprint.pprint


'''
参数: 指标, 周期
'''


@click.command()
@click.option('--source', default=None, help='source file')
@click.option('--target', default=None, help='target file')
@click.option('--indicators',
              default=[],
              help='{"5T": {"KDJ": [9,3,3], "KAM": [0.5, 0.5], "ATR": [14]}}')


def resample_data(source, target, indicators):

    try:
        filename = source
        _indicators = eval(indicators)
        periods = list(_indicators.keys())

        print('source file: %s' % os.path.basename(filename))

        df = pd.read_csv(filename,
                         usecols=[
                             'date', 'open', 'high', 'low', 'close', 'volume',
                             'average'
                         ],
                         index_col='date',
                         parse_dates=True)

        df_result = merge_all_df(df, _indicators)

        export_df(df_result, target)

    except IOError:
        print("Error: 没有找到文件或读取文件失败")


def DualThrust(_df, ind_params):
    up_range = ind_params[0]
    down_range = ind_params[1]
    _df['tr'] = _df['high'] - _df['low']
    _df['hh'] = _df['high'] + _df['tr'] * up_range
    _df['ll'] = _df['low'] - _df['tr'] * up_range

    return _df


def merge_all_df(df, _indicators):
    for period in _indicators:

        _df = df.resample(period, closed='left', label='right').agg({
            'open':
            'first',
            'high':
            'max',
            'low':
            'min',
            'close':
            'last',
            'volume':
            'sum'
        })

        _df = _df.dropna()

        for ind in _indicators[period]:
            ind_params = _indicators[period][ind]
            if ind == 'KAM':
                _df = DualThrust(_df, ind_params)
            if ind == 'ATR':
                _df['atr'] = ta.ATR(_df['high'],
                                    _df['low'],
                                    _df['close'],
                                    timeperiod=ind_params[0])

        columns = list(_df.columns)
        rename_list = {}
        for col in columns:
            rename_list[col] = '%s_%s' % (col, period)
        _df.rename(columns=rename_list, inplace=True)

        print(period)
        print(rename_list)

        merge_df = pd.merge(df, _df, how='left', on='date')

        merge_df.ffill(inplace=True)
        #merge_df.dropna(inplace=True)

        df = merge_df

    return df


def export_df(df, export_filename):
    print('==' * 10)
    print(df.tail())
    df.to_csv('{}'.format(export_filename))
    end = time()
    print(f'execute time: {end - start}s')
    # print('tmp/{}_{}'.format(period, export_filename))
    # print('tmp/merge_{}_{}'.format(period, export_filename))

if __name__ == '__main__':


    resample_data()
