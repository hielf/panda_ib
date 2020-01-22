# 搭建训练环境
# 环境准备
import pandas as pd
import numpy as np
import sys

import pandaAI.robotV2 as robot
import sklearn
import sklearn.metrics as me
import importlib as imp
imp.reload(robot)
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
import joblib
import matplotlib.pyplot as plt #可视化模块
from matplotlib.pylab import date2num
import datetime
from sklearn.metrics import log_loss, f1_score, mean_absolute_error,mean_squared_error,r2_score,accuracy_score,roc_auc_score, balanced_accuracy_score

print('==准备数据集合==')

# 基础设置
dual_params={
    'resample': '6T',
    'barNum': 10, # 看几根k线
    'file_path': '/data/hsi2019.csv',
    # 'file_path2': '/data/hsi2018.csv',
    'rolling_window': 6
}


frequency = [0,28]

feature_params = {
    #'ema': frequency,
    'atr': frequency,
    'rsi': frequency,
    # 'wma': frequency,
    'cci': frequency,
    # 'aroon': frequency,
    # 'willr': frequency,
    # 'adx': frequency,
    # 'adxr': frequency,
#     'roc': frequency
}


# 加载数据
print(sys.path[0])
df = pd.read_csv(sys.path[0] + dual_params['file_path'],  skiprows=1, header=None, sep=',', names=['dates', 'open', 'high', 'low', 'close','volume', 'barcount','avg'])
# 只训练到6月

# 整理数据
import matplotlib.dates as mdates

# 整理数据
def flag1(row):
    if row['future_close2'] > row['buy_base_price']:
        return 1
    else:
        return 0

def set_buyY(row):
    if  row['future_hh'] > row['buy_base_price'] and row['future_hh'] / row['close'] - 1> 0.003:
        return 1
    else:
        return 0

def set_saleY(row):
    if  row['future_ll'] < row['sale_base_price'] and row['future_ll'] / row['close'] -1 < -0.003:
        return 1
    else:
        return 0

df2 = df.copy()
cname = 'dates'
df2.loc[:, cname] = pd.to_datetime(df2[cname])
df2.set_index(cname, inplace=True)
df2.sort_index()
df2['dates2'] = df2.index
df2['dates2'] = df2['dates2'].apply(mdates.date2num)

df2['hour'] = pd.to_datetime(df2['dates2']).dt.hour
df2['minute'] = pd.to_datetime(df2['dates2']).dt.minute


mm = robot.MM()
flist =  mm.input_features(feature_params)
# resample k线
mm.format_data(df2, dual_params)

# 创建时间特征
mm.data_set = mm.generate_datetime_feature(mm.data_set)
#创建指标特征
mm.generate_features(flist)
df4 = mm.generate_exfeature_rolling(int(dual_params['rolling_window']), mm.features)
# 结果y, 现在价格突破buy_base
df4['buy_base_price']= df4['buy_base_price'].shift()
df4['sale_base_price']= df4['sale_base_price'].shift()
df4['buy_high_price']= df4['buy_high_price'].shift()
df4['sale_low_price']= df4['sale_low_price'].shift()


df4['atr_rsi'] = df4['RSI_0'] / (1+df4['ATR_0'])
df4['cci_atr'] = df4['ATR_0'] / (1+df4['CCI_0'])
df4['atr_rsi28'] = df4['RSI_28'] / (1+df4['ATR_0'])
df4['cci28_atr28'] = df4['ATR_28'] / (1+df4['CCI_28'])
df4['atr_rsi1'] = df4['atr_rsi'].shift(1)
df4['atr_rsi2'] = df4['atr_rsi'].shift(2)
df4['atr_rsi3'] = df4['atr_rsi'].shift(3)

df4['hh_close'] = df4['hh']/df4['close']
df4['hh_close2'] = df4['hh']/df4['close'].shift(2)
df4['hh_close3'] = df4['hh']/df4['close'].shift(4)
df4['hh_1'] = df4['hh'].shift(1)
df4['hh_1_close'] = df4['hh_1']/df4['close']
df4['hh_1_close_1'] = df4['hh_1']/df4['close'].shift()
df4['hh_2'] = df4['hh'].shift(2)
df4['hh2_close'] = df4['hh_2']/df4['close']
df4['hh2_close_1'] = df4['hh_2']/df4['close'].shift()

df4['ll_close'] = df4['ll']/df4['close']
df4['ll_close2'] = df4['ll']/df4['close'].shift(2)
df4['ll_close3'] = df4['ll']/df4['close'].shift(4)
df4['ll_1'] = df4['ll'].shift(1)
df4['ll_1_close'] = df4['ll_1']/df4['close']
df4['ll_1_close_1'] = df4['ll_1']/df4['close'].shift()
df4['ll_2'] = df4['ll'].shift(2)
df4['ll-2_close'] = df4['ll_2']/df4['close']
df4['ll_2_close_1'] = df4['ll_2']/df4['close'].shift()

df4['future_close'] = df4['high'].shift(-1)
df4['future_atr'] = df4['ATR_0'].shift(-1)
df4['future_atr28'] = df4['ATR_28'].shift(-1)
df4['future_close2'] = df4['close'].shift(-2)
df4['future_hh'] = df4['hh'].shift(-1)
df4['future_ll'] = df4['ll'].shift(-1)
df4.dropna(inplace=True)

mm.data_set.head()

# 设置标签
df4['flag1']=df4.apply(flag1, axis=1)

df4['label_buy']=df4.apply(set_buyY,axis=1)
df4['label_sale']=df4.apply(set_saleY, axis=1)
print(df4['label_buy'].sum(), df4['label_sale'].sum())

# 去除未来信息
df4.drop(['datetime'],axis=1, inplace=True)
df4.drop(['future_close'],axis=1, inplace=True)
df4.drop(['future_close2'],axis=1, inplace=True)
df4.drop(['future_atr'],axis=1, inplace=True)
df4.drop(['future_atr28'],axis=1, inplace=True)
df4.drop(['future_hh'],axis=1, inplace=True)
df4.drop(['future_ll'],axis=1, inplace=True)
df4.head()


print('==实例化trader==')

def split_dataX(data_x, split_rate=0.8, set_Y = 'label_buy'):
    train_rate = split_rate
    # #参数n_components为降维后的维数
    # LDA(n_components=2).fit_transform(iris.data, iris.target)
    data_len = round(data_x.shape[0]*train_rate)
    test_len = round(data_len * 0.2)
    train_len = data_len - test_len
    train_X = data_x[:train_len].loc[:,'open':"flag1"]
    test_X= data_x[train_len:data_len].loc[:,'open':"flag1"]
    ver_X = data_x[data_len:].loc[:,'open':"flag1"]
    train_Y = data_x[:train_len][set_Y]
    test_Y = data_x[train_len:data_len][set_Y]
    ver_Y = data_x[data_len:][set_Y]
    train_Y2 = data_x[:train_len]['label_sale']
    test_Y2 = data_x[train_len:data_len]['label_sale']
    ver_Y2 = data_x[data_len:]['label_sale']
    train_X.drop(['flag1'],axis=1, inplace=True)
    test_X.drop(['flag1'],axis=1, inplace=True)
    ver_X.drop(['flag1'],axis=1, inplace=True)
    return data_len, train_X, test_X,  ver_X, train_Y, test_Y, ver_Y, train_Y2, test_Y2, ver_Y2

jn = 1
profit_list = []

# 逐步递推训练, 确认数据量增加模型预测能力增加
for i in range(1, len(df4['close']), round(len(df4['close'])/2)-10):
    data_set = df4.iloc[1 : i+round(len(df4['close'])/2) -1]
    print(data_set.shape)
    # 划分数据集合
    data_len, train_X, test_X,  ver_X, train_Y, test_Y, ver_Y, train_Y2, test_Y2, ver_Y2 = split_dataX(data_set, 1/(2+jn))
    jn +=1
    rf = RandomForestClassifier(n_estimators=200, random_state=99)
    rf.fit(train_X, train_Y)
    joblib.dump(rf, 'rf.pkl')
    pred_y = rf.predict_proba(ver_X)[:,1]
    pred_y = rf.predict(ver_X)
    clf2 = RandomForestClassifier(n_estimators=200, random_state=99)
    clf2.fit(train_X, train_Y2)
    joblib.dump(clf2, 'clf2.pkl')
