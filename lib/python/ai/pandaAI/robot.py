import talib as ta
import pandas as pd
import numpy as np
# 机器学习
import sklearn
import sklearn.metrics as me
from sklearn import preprocessing 
from sklearn.model_selection import train_test_split # 数据集划分
from sklearn.ensemble import RandomForestClassifier as RM#随机森林分类模型
import xgboost as xgb
import seaborn as sns
import matplotlib.pyplot as plt #可视化模块

from sklearn.metrics import log_loss, f1_score, mean_absolute_error,mean_squared_error,r2_score,accuracy_score,roc_auc_score, balanced_accuracy_score

def num_config(x):
    #[-40,+40] 分布差
    classification='binary'
    if(classification == 'binary'):
        
        if x>0:
            return 1
        else:
            return 0
    else:
    
        if x >= -2 and x <= 2:
            return 0

        if x >2 and x <= 5:
            return 1

        if x < - 2 and x >= -5:
            return -1

        if x>5 and x <=10:
            return 2

        if x<-5 and x >=-10:
            return -2

        if x > 10 :
            return 3

        if x < -10 :
            return -3

class KBar:
    Version = '1.0'
    count = 0
    period = 0
    
    def __init__(self, period):
        self.date = ''
        self.open_price = 0
        self.high_price = 0
        self.low_price = 0
        self.close_price = 0
        self.volume = 0
        self.period = period
    
    def insert_row(self, date, open_price, high_price,  low_price, close_price, volume=0):
        
        if (self.count % self.period) == 0:
            
            self.date = date
            self.open_price = open_price
            self.high_price = high_price
            self.low_price = low_price
            self.close_price = close_price
            self.volume = volume

            self.count = 0	# 到period的倍数就重新计数
            
        else:
            if high_price > self.high_price:
                self.high_price = high_price
            
            if low_price < self.low_price:
                self.low_price = low_price
            
            self.close_price = close_price


        self.count += 1
    
    
class MM(object):
    
    from sklearn.externals import joblib # 模型处理
        
    def set_index(self, mmdf, cname = 'dates'):
        df = mmdf.copy()
        df[cname] = pd.to_datetime(df[cname])
        df.set_index(cname, inplace=True)
        print(df.head())
        return df
    
    def generate_datetime_feature(self, mmdf):
        df =  mmdf
        df['datetime'] = df.index
        df['minute'] = df['datetime'].dt.minute
        df['hour'] = df['datetime'].dt.hour
        df['day'] = df['datetime'].dt.day
        df['week_day'] = df['datetime'].map(lambda x: x.weekday() + 1)
        df['month'] = df['datetime'].dt.month
        
        return df


    def input_features(self, params={}):
        # art_1, rsi_1, ma_5, wma_5, cci_1, macd_1, aroon_1, willr_1, adx_1, adxr_1, roc_1
        feature_list = []
        for item in params.keys():
            for v in params[item]:
                feature_list.append( "{0}_{1:.0f}".format(item,v))
               
        self.features = feature_list
        
        print(list(self.features))
    
    def generate_features(self, mmdf):
        self.df = mmdf.copy()
        print(mmdf.head())
        np_close = np.array(self.df['CLOSE'])
        np_high = np.array(self.df['HIGH'])
        np_low = np.array(self.df['LOW'])
        
        for item in list(self.features):
            #feature name + period

            ff = item.split('_')
            fname = ff[0]
            period = int(ff[1])
            t_period = 0
         
            if fname == 'art':
                t_period = period
                if period == 1:
                    period = 14
                    
                art = ta.ATR(np_high, np_low, np_close, timeperiod=period)
                self.df[ fname.upper() + '_' +str(t_period)] = art
            
            if fname == 'rsi':
                t_period = period
                if period == 1:
                    period = 10
                rsi = ta.RSI(np_close, timeperiod=period)
                self.df['RSI_'+str(t_period)] = rsi
                
            if fname == 'ma':
                t_period = period
                if period == 1:
                    period = 25
                values = ta.MA(np_close, timeperiod=period)
                self.df[ fname.upper() + '_' +str(t_period)] = values
            
            if fname == 'ema':
                t_period = period
                if period == 1:
                    period = 25
                values = ta.EMA(np_close, timeperiod=period)
                self.df[ fname.upper() + '_' +str(t_period)] = values   
                
            if fname == 'wma':
                t_period = period
                if period == 1:
                    period = 25
                values = ta.WMA(np_close, timeperiod=period)
                self.df[ fname.upper() + '_' +str(t_period)] = values
            
            if fname == 'cci':
                t_period = period
                if period == 1:
                    period = 14
                values = ta.CCI(np_high, np_low, np_close, timeperiod=period)
                self.df[ fname.upper() + '_' +str(t_period)] = values
            
            if fname == 'macd':
                t_period = period
                dif, dea, macd = ta.MACD(np_close, fastperiod=(12*period), slowperiod=(26*period), signalperiod=(9*period))
                self.df['DIF_'+str(t_period)] = dif
                self.df['DEA_'+str(t_period)] = dea
                self.df['MACD_'+str(t_period)] = macd
                
            if fname == 'aroon':
                t_period = period
                if period == 1:
                    period = 14
                aroondown, aroonup = ta.AROON(np_high, np_low, timeperiod=period)
                self.df['AROON_'+str(t_period)] = aroonup - aroondown
            
            if fname == 'willr':
                t_period = period
                if period == 1:
                    period = 14
                real = ta.WILLR(np_high, np_low, np_close, timeperiod=period)
                self.df[ fname.upper() + '_' +str(t_period)] = real
             
            if fname == 'adx':
                t_period = period
                if period == 1:
                    period = 14
                real = ta.ADX(np_high, np_low, np_close, timeperiod=period)
                self.df[ fname.upper() + '_' +str(t_period)] = real
            
            if fname == 'adxr':
                t_period = period
                if period == 1:
                    period = 14
                real = ta.ADXR(np_high, np_low, np_close, timeperiod=period)
                self.df[ fname.upper() + '_' +str(t_period)] = real
            
            if fname == 'roc':
                t_period = period
                if period == 1:
                    period = 10
                real = ta.ROC(np_close, timeperiod=period)
                self.df[ fname.upper() + '_' +str(t_period)] = real
                
        return self.df
            
    def generate_exfeature_diff(self, mmdf, f_list):
        # 相减
        df = mmdf.copy()
        cols = []
        for item in f_list:
            fname1= item[0]
            fname2= item[1]
            df[fname1.upper()+'-'+fname2.upper()] = (df[fname1.upper()] - df[fname2.upper()])/df[fname1.upper()]
            cols.append(fname1.upper()+'-'+fname2.upper())
            
        print(cols)
        
        return df 
        
        
    def generate_exfeature_rolling(self, mmdf, period, feature_list, step=1):
    
        for item in list(feature_list):
            for i in range(1, period):
                mmdf[item.upper() + '_' + str(i)] = mmdf[item.upper()].shift(i*step)
                print(item.upper() + '_' + str(i))
        return mmdf
        
    def drop_na(self, mmdf):
        df = mmdf
        df.dropna() #剔除缺失值
        
        return df
        
    def set_label(self, df, window=15, point=20, target='close', pip_grid=1000, predict_type='close'):
        # point 是预期获得的盈利
        # predict_type = [close|max|min]
        
        df2 = df.copy()
        df2['label_close'] =df2['CLOSE']
        print('before set label:', df2.shape)
        
        config_col = "(t+1)-(t)"
        
        if predict_type == 'close'.upper():
            df2["(t+1)-(t)"] = (df2[target.upper()].shift(-1*window) - df2[target.upper()]) * pip_grid - point 
        else:
            config_col = f'(t+1)-label_{predict_type}'
            label_command= f'df2["{target.upper()}"].rolling({window}).{predict_type}()'
            print(label_command)
            df2[f"label_{predict_type}"] = eval(label_command) 
            df2[f"(t+1)-label_{predict_type}"] = (df2[f'label_{predict_type}'].shift(-1*window) - df2[target.upper()]) * pip_grid - point
        
        df2['label_close'] =df2['CLOSE']
        print(df2.tail())

        df2 = df2.dropna()  
        
#         def function(a, b, c):
#             max_point =  max(a,b) - c
            
#             return max_point
        
#         df2["(t+1)-label_std"] = df2.apply(lambda x: function(x["(t+1)-label_max"], x["(t+1)-label_min"], point), 
#                                                  axis = 1)

        
        df2['label'] = df2[config_col].map(num_config)
        print('after set label:', df2.shape)
 
              

        import seaborn as sns
        plt.figure(figsize=(12, 6))
        sns.distplot(df2[config_col])
        plt.show()
        
    
        return df2
    
    def resample(self, mmdf, period):
        # https://pandas-docs.github.io/pandas-docs-travis/timeseries.html#offset-aliases
        # 周 W、月 M、季度 Q、10天 10D、2周 2W
        #period = 'W'
        df = mmdf.copy()
        weekly_df = df.resample(period, how='last')
        weekly_df['OPEN'] = df['OPEN'].resample(period, how='first')
        weekly_df['HIGH'] = df['HIGH'].resample(period, how='max')
        weekly_df['LOW'] = df['LOW'].resample(period, how='min')
        weekly_df['CLOSE'] = df['CLOSE'].resample(period, how='last')
        weekly_df['VOLUME'] = df['VOLUME'].resample(period, how='sum')
        #weekly_df['amount'] = df['amount'].resample(period, how='sum')
        # 去除空的数据（没有交易的周）
        #weekly_df = weekly_df[weekly_df.instrument.notnull()]
        weekly_df.reset_index(inplace=True)
        return weekly_df
    
    def model_fit(self, clf, test_X, test_Y, is_scale=True, kind='xgb', is_best=True):
        if(is_scale == True):
            print('数据归一化处理')
            X = preprocessing.scale(test_X)
        else:
            print('数据未处理')
            X = test_X
            
        if kind == 'xgb':
            if is_best:
                pred_y = clf.predict(X,ntree_limit=clf.best_ntree_limit)
            else:
                pred_y = clf.predict(X)
        if kind == 'rf':
            pred_y = clf.predict(X)

        print('neg_log_loss:', log_loss(test_Y,pred_y))
        print('mean_absolute_error:', mean_absolute_error(test_Y,pred_y))
        print('roc_auc_score:',roc_auc_score(test_Y,pred_y))
        print("Accuracy: %.2f%%" % (accuracy_score(test_Y, pred_y) * 100.0))
        
        print("F1: %.2f%%" % (f1_score(test_Y, pred_y, average='micro')* 100.0))
        
        # 分类报告
        print(me.classification_report(test_Y, pred_y))
        
        # 预测值和真实值的关系
        results = me.confusion_matrix(test_Y, pred_y) 
        print('Confusion Matrix :')
        print(results)
        plt.clf()
        plt.imshow(results, interpolation='nearest', cmap=plt.cm.Wistia)
        classNames = ['Negative','Positive']
        plt.title('Versicolor or Not Versicolor Confusion Matrix - Test Data')
        plt.ylabel('True label')
        plt.xlabel('Predicted label')
        tick_marks = np.arange(len(classNames))
        plt.xticks(tick_marks, classNames, rotation=45)
        plt.yticks(tick_marks, classNames)
        s = [['TN','FP'], ['FN', 'TP']]
        for i in range(2):
            for j in range(2):
                plt.text(j,i, str(s[i][j])+" = "+str(results[i][j]))
        plt.show()
        
        return results

    def model_save(self, clf, model_name="train_model.m"):
        from sklearn.externals import joblib
        # # 模型保存
        notice = joblib.dump(clf, model_name)
        print(notice)
        
    def model_load(self,  model_name="train_model.m"):
        train_model = joblib.load(model_name)
        return train_model

    def plot_confusion_matrix(self, 
                              cm,
                              target_names,
                              title='Confusion matrix',
                              cmap=None,
                              normalize=True):
        """
        given a sklearn confusion matrix (cm), make a nice plot

        Arguments
        ---------
        cm:           confusion matrix from sklearn.metrics.confusion_matrix

        target_names: given classification classes such as [0, 1, 2]
                      the class names, for example: ['high', 'medium', 'low']

        title:        the text to display at the top of the matrix

        cmap:         the gradient of the values displayed from matplotlib.pyplot.cm
                      see http://matplotlib.org/examples/color/colormaps_reference.html
                      plt.get_cmap('jet') or plt.cm.Blues

        normalize:    If False, plot the raw numbers
                      If True, plot the proportions

        Usage
        -----
        plot_confusion_matrix(cm           = cm,                  # confusion matrix created by
                                                                  # sklearn.metrics.confusion_matrix
                              normalize    = True,                # show proportions
                              target_names = y_labels_vals,       # list of names of the classes
                              title        = best_estimator_name) # title of graph

        Citiation
        ---------
        http://scikit-learn.org/stable/auto_examples/model_selection/plot_confusion_matrix.html

        """

        import numpy as np
        import itertools

        accuracy = np.trace(cm) / float(np.sum(cm))
        misclass = 1 - accuracy

        if cmap is None:
            cmap = plt.get_cmap('Blues')

        plt.figure(figsize=(8, 6))
        plt.imshow(cm, interpolation='nearest', cmap=cmap)
        plt.title(title)
        plt.colorbar()

        if target_names is not None:
            tick_marks = np.arange(len(target_names))
            plt.xticks(tick_marks, target_names, rotation=45)
            plt.yticks(tick_marks, target_names)

        if normalize:
            cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]


        thresh = cm.max() / 1.5 if normalize else cm.max() / 2
        for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
            if normalize:
                plt.text(j, i, "{:0.4f}".format(cm[i][j]),
                         horizontalalignment="center",
                         color="white" if cm[i][j] > thresh else "black")
            else:
                plt.text(j, i, "{:,}".format(cm[i][j]),
                         horizontalalignment="center",
                         color="white" if cm[i][j] > thresh else "black")


        plt.tight_layout()
        plt.ylabel('True label')
        plt.xlabel('Predicted label\naccuracy={:0.4f}; misclass={:0.4f}'.format(accuracy, misclass))
        plt.show()