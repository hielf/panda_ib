#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
# require 'net/ping'
# require 'pycall/import'
# include PyCall::Import

module ContractsHelper

  def index_to_csv(contract)
    # contract = "hsi_5mins"
    # 1200 bars
    begin_time = EventLog.maximum(:created_at).nil? ? Time.zone.now - 4.days : (EventLog.maximum(:created_at) - 4.days).beginning_of_day
    end_time = Time.zone.now
    # url = "http://#{ENV["market_db"]}:3000/#{contract}?and=(date.gte.#{begin_time.strftime('%Y-%m-%dT%H:%M:%S')},date.lte.#{end_time.strftime('%Y-%m-%dT%H:%M:%S')})"
    url = "http://#{ENV["market_db"]}:3000/#{contract}_last_1200"
    res = HTTParty.get url
    json = JSON.parse res.body
    begin
      latest_time = strftime_time(json.last["date"].to_datetime + 2.minutes)
      if latest_time >= strftime_time(end_time)
        csv = CSV.generate(headers: false) { |csv| json.map(&:to_a).each { |row| csv << row } }

        file = Rails.root.to_s + "/tmp/csv/#{contract}.csv"
        CSV.open( file, 'w' ) do |writer|
          json.each do |c|
            writer << [c["date"], c["open"], c["high"], c["low"], c["close"], c["volume"], c["barCount"], c["average"]]
          end
        end
        return file
      else
        return false
      end
    rescue Exception => e
      Rails.logger.warn "index_to_csv failed: #{e}"
    end
  end


  def online_data(file, version)
    Rails.logger.warn "online_data start: #{Time.zone.now}"
    # PyCall.exec("")
    data = {}
    # file_name = "data.json"
    # destination = Rails.root.to_s + "/tmp/" + file_name
    #
    # begin
    #   File.open(destination, 'r') do |f|
    #     # do something with file
    #     File.delete(f)
    #   end
    # rescue Errno::ENOENT
    # end

    case version.upcase
    when "V4"
      hash = predict_v4(file)
    when "V5"
      hash = predict_v5(file)
    end

    # PyCall.exec("import json")
    # # PyCall.exec("print (hash)")
    # PyCall.exec("destination='#{destination}'")
    # PyCall.exec("saveFile = open(destination, 'w')")
    # PyCall.exec("saveFile.write(json.dumps(#{hash}))")
    # PyCall.exec("saveFile.close()")
    #
    # begin
    #   f = File.open(destination, "r")
    #   data = JSON.load f
    #   f.close
    #   Rails.logger.warn "online_data success: #{Time.zone.now}"
    # rescue Exception => ex
    #   # Rails.logger.warn "#{ex.message}"
    #   Rails.logger.warn "online_data no file error: #{Time.zone.now}"
    # end

    data = hash.to_h

    return data
  end

  def predict_v4(file)
    pandaAI = Rails.root.to_s + '/lib/python/ai/pandaAI'
    today = Date.today.strftime('%Y-%m-%d')
    begin
      PyCall.exec("dual_params={'resample': '5T', 'step_n': 5, 'barNum': 1, 'file_path': '#{file}',  'check_date': '#{today}'}")
      PyCall.exec("import sys")
      PyCall.exec("sys.path.append('#{pandaAI}')")
      PyCall.exec("import robot")
      PyCall.exec("import importlib as imp")
      PyCall.exec("imp.reload(robot)")
      PyCall.exec("import pandas as pd")
      PyCall.exec("import numpy as np")
      PyCall.exec("import copy")
      PyCall.exec("import datetime,pickle")

      PyCall.exec("from datetime import datetime")
      PyCall.exec("from datetime import timedelta")
      # 机器学习
      PyCall.exec("import sklearn")
      PyCall.exec("import sklearn.metrics as me")
      # 目标函数, error 越小越好, score 越大约好
      PyCall.exec("from sklearn.metrics import log_loss, f1_score, mean_absolute_error,mean_squared_error,r2_score,accuracy_score,roc_auc_score, balanced_accuracy_score")
      PyCall.exec("from sklearn.model_selection import train_test_split")# 数据集划分

      PyCall.exec("from sklearn import preprocessing")
      PyCall.exec("import seaborn as sns")
      # import matplotlib.pyplot as plt #可视化模块
      PyCall.exec("from sklearn.preprocessing import StandardScaler")
      PyCall.exec("import talib as ta")#金融数据计算

      PyCall.exec("df1 = pd.read_csv(dual_params['file_path'],  skiprows=1, header=None, sep=',', names=['dates', 'open', 'high', 'low', 'close','volume','barCount','average'])")
      # PyCall.exec("print(df1.info())")

      # 整理数据
      PyCall.exec("import matplotlib.dates as mdates")
      PyCall.exec("mm = robot.MM()")

      #设置索引
      PyCall.exec("df2 = mm.set_index(df1,cname='dates')")
      PyCall.exec("df2.dropna(inplace=True)")
      PyCall.exec("df2.sort_index(inplace=True)")
      # PyCall.exec("n = len(df2)")
      # PyCall.exec("period_data = df2.resample(dual_params['resample']).last()")
      # # 分别对于开盘、收盘、最高价、最低价进行处理
      # PyCall.exec("period_data['open'] = df2['open'].resample(dual_params['resample']).first()")
      # # 处理最高价和最低价
      # PyCall.exec("period_data['high'] = df2['high'].resample(dual_params['resample']).max()")
      # # 最低价
      # PyCall.exec("period_data['low'] = df2['low'].resample(dual_params['resample']).min()")
      # # 成交量 这一周的每天成交量的和
      # PyCall.exec("period_data['volume'] = df2['volume'].resample(dual_params['resample']).sum()")
      # PyCall.exec("online_data = period_data.dropna(axis=0)") # 缺失值处理
      # PyCall.exec("online_data['dates2'] = online_data.index")
      # PyCall.exec("online_data['dates2'] = online_data['dates2'].apply(mdates.date2num)")
      #
      # PyCall.exec("online_data = online_data.dropna(axis=0)") # 缺失值处理
      # PyCall.exec("print (online_data.head())")
      # PyCall.exec("print (online_data.tail(1))")

      PyCall.exec("old_row = []")
      PyCall.exec("current_row = []")
      PyCall.exec("profit_list = []")
      PyCall.exec("step_n = 5")
      PyCall.exec("i = step_n * 2")
      PyCall.exec("n = len(df2)")
      # PyCall.exec("print(n)")
      PyCall.exec("start_n = n - step_n - n % step_n")
      PyCall.exec("end_n = n")
      # PyCall.exec("print(start_n, n)")
      # PyCall.exec("print(df1)")

      start_n = PyCall.eval("start_n")
      if start_n >= 0
        PyCall.exec("period_data = df2.iloc[start_n:end_n].resample(dual_params['resample']).last()")
        # 分别对于开盘、收盘、最高价、最低价进行处理
        PyCall.exec("period_data['open'] = df2.iloc[start_n:end_n]['open'].resample(dual_params['resample']).first()")
        # 处理最高价和最低价
        PyCall.exec("period_data['high'] = df2.iloc[start_n:end_n]['high'].resample(dual_params['resample']).max()")
        # 最低价
        PyCall.exec("period_data['low'] = df2.iloc[start_n:end_n]['low'].resample(dual_params['resample']).min()")
        # 成交量 这一周的每天成交量的和
        PyCall.exec("period_data['volume'] = df2.iloc[start_n:end_n]['volume'].resample(dual_params['resample']).sum()")
        PyCall.exec("period_data.dropna(inplace=True)")

        if PyCall.eval("len(period_data) > 1")
          PyCall.exec("old_row = period_data.iloc[0]['open':'close']")
          PyCall.exec("current_row = period_data.iloc[1]['open':'close']")
          PyCall.exec("pre_data = pd.concat([old_row,current_row], axis=1)")
          PyCall.exec("pre_data = pd.DataFrame(pre_data.values.T, index=pre_data.columns, columns=pre_data.index)")
          PyCall.exec("old_price = old_row['close']")
          PyCall.exec("current_price = current_row['close']")

          # 基于线性回归的模型加载
          PyCall.exec("from sklearn.linear_model import LinearRegression")
          PyCall.exec("from sklearn.externals import joblib")
          PyCall.exec("reg_buy_open = joblib.load('#{Rails.root.to_s}' + '/lib/python/ai/reg_buy_open.pkl')")
          PyCall.exec("reg_buy_break = joblib.load('#{Rails.root.to_s}' + '/lib/python/ai/reg_buy_break.pkl')")
          PyCall.exec("reg_sale_open = joblib.load('#{Rails.root.to_s}' + '/lib/python/ai/reg_sale_open.pkl')")
          PyCall.exec("reg_sale_break = joblib.load('#{Rails.root.to_s}' + '/lib/python/ai/reg_sale_break.pkl')")
          PyCall.exec("scale = joblib.load('#{Rails.root.to_s}' + '/lib/python/ai/scale.pkl')")

          # PyCall.exec("pre_data = online_data.iloc[-2:-1][['open','high','low','close']]") # 倒数第二个bar作为数据预测基准, 因为当前bar还没有走完
          # PyCall.exec("current_price = online_data.tail(1)['close']")
          # PyCall.exec("open_price = pre_data['open']")
          # PyCall.exec("high_price = pre_data['high']")
          # PyCall.exec("low_price = pre_data['low']")
          # PyCall.exec("close_price = pre_data['close']"
          # PyCall.exec("print (current_row)")

          PyCall.exec("hash = {}")
          PyCall.exec("hash['current_open'] = current_row['open']")
          PyCall.exec("hash['current_close'] = current_row['close']")
          PyCall.exec("hash['current_high'] = current_row['high']")
          PyCall.exec("hash['current_low'] = current_row['low']")
          PyCall.exec("hash['prev_open'] = old_row['open']")
          PyCall.exec("hash['prev_close'] = old_row['close']")
          PyCall.exec("hash['prev_high'] = old_row['high']")
          PyCall.exec("hash['prev_low'] = old_row['low']")
          PyCall.exec("hash['reg_buy_open'] = reg_buy_open.predict(pre_data)[0]")
          PyCall.exec("hash['reg_buy_break'] = reg_buy_break.predict(pre_data)[0]")
          PyCall.exec("hash['reg_sale_open'] = reg_sale_open.predict(pre_data)[0]")
          PyCall.exec("hash['reg_sale_break'] = reg_sale_break.predict(pre_data)[0]")
        end
      end
    rescue Exception => ex
      Rails.logger.warn "#{ex.message}"
    end
    return PyCall.eval("hash")
  end

  def predict_v5(file)
    pandaAI = Rails.root.to_s + '/lib/python/ai/pandaAI'
    begin
      PyCall.exec("dual_params={'resample': '6T', 'barNum': 10, 'file_path': '#{file}',  'rolling_window': 6}")
      PyCall.exec("import pandas as pd")
      PyCall.exec("import numpy as np")
      PyCall.exec("import sys")
      PyCall.exec("sys.path.append('#{pandaAI}')")
      PyCall.exec("import robotV2 as robot")
      PyCall.exec("import importlib as imp")
      PyCall.exec("imp.reload(robot)")
      PyCall.exec("import sklearn")
      PyCall.exec("import sklearn.metrics as me")
      PyCall.exec("from sklearn.ensemble import RandomForestClassifier")
      PyCall.exec("from sklearn.ensemble import RandomForestRegressor")
      PyCall.exec("from sklearn.externals import joblib")
      PyCall.exec("from matplotlib.pylab import date2num")
      PyCall.exec("import datetime ")
      PyCall.exec("from sklearn.metrics import log_loss, f1_score, mean_absolute_error,mean_squared_error,r2_score,accuracy_score,roc_auc_score, balanced_accuracy_score")

      PyCall.exec("frequency = [0,28]")
      PyCall.exec("feature_params = {'atr': frequency, 'rsi': frequency, 'cci': frequency}")
      PyCall.exec("df0 = pd.read_csv(dual_params['file_path'],  skiprows=1, header=None, sep=',', names=['dates', 'open', 'high', 'low', 'close','volume', 'barcount','avg'])")

      PyCall.exec("import matplotlib.dates as mdates")
      PyCall.exec("df6 = df0")
      PyCall.exec("n = len(df6)")
      PyCall.exec("cname = 'dates'")
      PyCall.exec("df6.loc[:, cname] = pd.to_datetime(df6[cname])")
      PyCall.exec("df6.set_index(cname, inplace=True)")
      PyCall.exec("df6.sort_index(inplace=True)")
      PyCall.exec("df6.loc[:, 'dates2'] = df6.index")
      PyCall.exec("df6.loc[:, 'dates2'] = df6['dates2'].apply(mdates.date2num)")
      PyCall.exec("df6['hour'] = pd.to_datetime(df6['dates2']).dt.hour")
      PyCall.exec("df6['minute'] = pd.to_datetime(df6['dates2']).dt.minute")
      # PyCall.exec("step_n = 5") # bar 的 period 间隔
      # PyCall.exec("i = step_n") # 第一个old bar
      PyCall.exec("cols= ['open',	'high',	'low',	'close',	'volume',	'barcount',	'avg',	'dates2' ]")
      PyCall.exec("df6 = df6.loc[:, cols]")
      # PyCall.exec("df2_index = list(df6.index)")
      PyCall.exec("mm2 = robot.MM()")
      PyCall.exec("flist =  mm2.input_features(feature_params)")
      PyCall.exec("mm2.format_data(df6, dual_params)") # resample k线
      PyCall.exec("mm2.data_set = mm2.generate_datetime_feature(mm2.data_set)") # 创建时间特征
      PyCall.exec("mm2.generate_features(flist)")
      PyCall.exec("df7 = mm2.generate_exfeature_rolling(int(dual_params['rolling_window']), mm2.features)")
      PyCall.exec("df7['buy_base_price']= df7['buy_base_price'].shift()")
      PyCall.exec("df7['sale_base_price']= df7['sale_base_price'].shift()")
      PyCall.exec("df7['buy_high_price']= df7['buy_high_price'].shift()")
      PyCall.exec("df7['sale_low_price']= df7['sale_low_price'].shift()")
      PyCall.exec("df7['atr_rsi'] = df7['RSI_0'] / (1+df7['ATR_0'])")
      PyCall.exec("df7['cci_atr'] = df7['ATR_0'] / (1+df7['CCI_0'])")
      PyCall.exec("df7['atr_rsi28'] = df7['RSI_28'] / (1+df7['ATR_0'])")
      PyCall.exec("df7['cci28_atr28'] = df7['ATR_28'] / (1+df7['CCI_28'])")
      PyCall.exec("df7['atr_rsi1'] = df7['atr_rsi'].shift(1)")
      PyCall.exec("df7['atr_rsi2'] = df7['atr_rsi'].shift(2)")
      PyCall.exec("df7['atr_rsi3'] = df7['atr_rsi'].shift(3)")
      PyCall.exec("df7['hh_close'] = df7['hh']/df7['close']")
      PyCall.exec("df7['hh_close2'] = df7['hh']/df7['close'].shift(2)")
      PyCall.exec("df7['hh_close3'] = df7['hh']/df7['close'].shift(4)")
      PyCall.exec("df7['hh_1'] = df7['hh'].shift(1)")
      PyCall.exec("df7['hh_1_close'] = df7['hh_1']/df7['close']")
      PyCall.exec("df7['hh_1_close_1'] = df7['hh_1']/df7['close'].shift()")
      PyCall.exec("df7['hh_2'] = df7['hh'].shift(2)")
      PyCall.exec("df7['hh2_close'] = df7['hh_2']/df7['close']")
      PyCall.exec("df7['hh2_close_1'] = df7['hh_2']/df7['close'].shift()")
      PyCall.exec("df7['ll_close'] = df7['ll']/df7['close']")
      PyCall.exec("df7['ll_close2'] = df7['ll']/df7['close'].shift(2)")
      PyCall.exec("df7['ll_close3'] = df7['ll']/df7['close'].shift(4)")
      PyCall.exec("df7['ll_1'] = df7['ll'].shift(1)")
      PyCall.exec("df7['ll_1_close'] = df7['ll_1']/df7['close']")
      PyCall.exec("df7['ll_1_close_1'] = df7['ll_1']/df7['close'].shift()")
      PyCall.exec("df7['ll_2'] = df7['ll'].shift(2)")
      PyCall.exec("df7['ll-2_close'] = df7['ll_2']/df7['close']")
      PyCall.exec("df7['ll_2_close_1'] = df7['ll_2']/df7['close'].shift()")
      PyCall.exec("df7.drop(['datetime'],axis=1, inplace=True)")
      PyCall.exec("df7.dropna(inplace=True)")
      # PyCall.exec("df7_index = list(df7.index)")

      PyCall.exec("rf = joblib.load('#{Rails.root.to_s}' + '/lib/python/ai/rf.pkl')")
      PyCall.exec("clf2 = joblib.load('#{Rails.root.to_s}' + '/lib/python/ai/clf2.pkl')")

      PyCall.exec("ver_X = df7.tail(6)")
      PyCall.exec("pred_y = rf.predict_proba(ver_X)[:,1]")
      PyCall.exec("pred_y2 = clf2.predict_proba(ver_X)[:,1]")
      PyCall.exec("dict_action = pred_y[-2]")
      PyCall.exec("dict_action2 = pred_y2[-2]")

      PyCall.exec("hash = {}")
      PyCall.exec("hash['dict_action'] = dict_action")
      PyCall.exec("hash['dict_action2'] = dict_action2")
      PyCall.exec("hash['condition1'] = ver_X.iloc[-2]['close'] + ver_X.iloc[-2]['ATR_0_1']")
      PyCall.exec("hash['condition2'] = ver_X.iloc[-2]['close'] - ver_X.iloc[-2]['ATR_0_1']/2")
      PyCall.exec("hash['condition3'] = ver_X.iloc[-2]['close'] - ver_X.iloc[-2]['ATR_0_1']")
      PyCall.exec("hash['open_1'] = ver_X.iloc[-1]['open']")
      PyCall.exec("hash['open_2'] = ver_X.iloc[-2]['open']")
      PyCall.exec("hash['close_1'] = ver_X.iloc[-1]['close']")
      PyCall.exec("hash['close_2'] = ver_X.iloc[-2]['close']")
      PyCall.exec("hash['high_1'] = ver_X.iloc[-1]['high']")
      PyCall.exec("hash['high_2'] = ver_X.iloc[-2]['high']")
      PyCall.exec("hash['low_1'] = ver_X.iloc[-1]['low']")
      PyCall.exec("hash['low_2'] = ver_X.iloc[-2]['low']")
      PyCall.exec("hash['buy_high_price_2'] = ver_X.iloc[-2]['buy_high_price']")
      PyCall.exec("hash['buy_base_price_2'] = ver_X.iloc[-2]['buy_base_price']")
      # PyCall.exec("print(pred_y, pred_y2, dict_action, dict_action2)")

    rescue Exception => ex
      Rails.logger.warn "#{ex.message}"
    end
    return PyCall.eval("hash")
  end

  def check_position(data, version)
    position = ib_positions
    Rails.logger.warn "ib position: #{position}"
    if position
      case version.upcase
      when "V4"
        order, amount = check_v4(data, position)
      when "V5"
        order, amount = check_v5(data, position)
      end

      Rails.logger.warn "ib data #{version}: #{data}"
      Rails.logger.warn "ib order #{version}: #{order == "" ? "NO" : order} #{amount.to_s}"

      if order != "" && amount != 0
        ib_order(order, amount, 0)
        EventLog.create(content: "#{order} #{version} #{amount.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}")
      end
    end

    return {"order": order, "amount": amount}
  end

  def check_v4(data, position)
    # {"current_open"=>26520.14, "current_close"=>26515.06, "current_high"=>26525.11, "current_low"=>26508.94,
    # "prev_open"=>26530.69, "prev_close"=>26519.39, "prev_high"=>26536.75, "prev_low"=>26514.3,
    # "reg_buy_open"=>26547.974999999995, "reg_buy_break"=>26559.200000000008, "reg_sale_open"=>26503.075,
    # "reg_sale_break"=>26491.85}
    amount = 4
    order = ""
    if position == {}
      if data["current_close"].to_f > data["reg_buy_open"].to_f
        order = "BUY"
      elsif data["current_close"].to_f < data["reg_sale_open"].to_f
        order = "SELL"
      end
    elsif !position["position"].nil? && position["position"].to_f > 0 # buy
      # 冲高回落, 平仓
      if data['current_high'].to_f > data["reg_buy_break"].to_f && data["current_close"].to_f < data["reg_buy_open"].to_f
        order = "SELL"
        amount = position["position"].abs
      # 移动平仓
      elsif data["current_close"].to_f <  data['prev_close'].to_f
        order = "SELL"
        amount = position["position"].abs
      end
    elsif !position["position"].nil? && position["position"].to_f < 0 # sell
      if data['current_low'].to_f < data["reg_sale_break"].to_f && data["current_close"].to_f > data["reg_sale_open"].to_f
        order = "BUY"
        amount = position["position"].abs
      elsif data["current_close"].to_f > data['prev_close'].to_f
        order = "BUY"
        amount = position["position"].abs
      end
    end
    return order, amount
  end

  def check_v5(data, position)
    # {"condition1"=>28766.18518420852, "condition2"=>28731.092407895743, "condition3"=>28719.394815791482,
    # "open_1"=>28739.61, "open_2"=>28726.08, "close_1"=>28753.79, "close_2"=>28742.79, "high_1"=>28764.47,
    # "high_2"=>28747.62, "low_1"=>28738.35, "low_2"=>28724.34, "buy_high_price_2"=>28787.559999999998,
    # "buy_base_price_2"=>28761.175}
    amount = 4
    order = ""

    if position == {}
      # 从低到高冲击
      if data['close_1'].to_f > data["condition1"].to_f && data['dict_action'].to_f > 0.5
        order = "BUY"
      # 移动平仓
      elsif data['close_1'].to_f < data["condition3"].to_f &&  data['dict_action2'].to_f > 0.5
        order = "SELL"
      end
    elsif !position["position"].nil? && position["position"].to_f > 0 && data['dict_action'].to_f < 0.5 # buy
      if data['high_2'].to_f > data['condition1'].to_f && data['close_1'].to_f < data['condition1'].to_f #冲高回落
        order = "SELL"
        amount = position["position"].abs
      elsif data['close_1'].to_f < data['condition3'].to_f && data['open_1'].to_f > data['condition3'].to_f #移动平仓
        order = "SELL"
        amount = position["position"].abs
      end
    elsif !position["position"].nil? && position["position"].to_f < 0 && data['dict_action2'].to_f < 0.5 # sell
      if data['low_2'].to_f < data['condition3'].to_f && data['close_1'].to_f > data['condition3'].to_f #冲高回落
        order = "BUY"
        amount = position["position"].abs
      elsif data['close_1'].to_f > data['condition1'].to_f && data['open_1'].to_f  < data['condition1'].to_f # 移动平仓
        order = "BUY"
        amount = position["position"].abs
      end
    end
    return order, amount
  end

  def close_position
    order = ""
    position = ib_positions
    amount = 0

    if !position["position"].nil? && position["position"] > 0 # buy
      order = "SELL"
      amount = position["position"].abs
    elsif !position["position"].nil? && position["position"] < 0 # sell
      order = "BUY"
      amount = position["position"].abs
    end

    Rails.logger.warn "ib close_position: #{order} #{amount.to_s}"

    if order != "" && amount != 0
      ib_order(order, amount, 0)
    end

    return {"order": order, "amount": amount}
  end

  def trades_to_csv(contract)
    # contract = "hsi_5mins"

    json = Trade.all.as_json
    # csv = CSV.generate(headers: false) { |csv| json.map(&:to_a).each { |row| csv << row } }

    file = Rails.root.to_s + "/tmp/csv/trades_#{contract}.csv"
    CSV.open( file, 'w' ) do |writer|
      writer << ["TIME", "ACTION", "SYMBOL", "CURRENCY", "SHARES", "PRICE", "COMMISSION", "PNL"]
      json.each do |c|
        writer << [(c["time"]).strftime("%Y%m%d %H:%M:%S"), c["action"], c["symbol"] + "-" + c["last_trade_date_or_contract_month"], c["currency"], c["shares"], c["price"], c["commission"], c["realized_pnl"]]
      end
    end

    return file
  end
end
