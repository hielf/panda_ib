#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
require 'net/ping'
require 'pycall/import'
include PyCall::Import

module ContractsHelper

  def index_to_csv(index)
    # index = "hsi_5mins"
    begin_time = Time.now - 1.month
    end_time = Time.now
    url = "http://#{ENV["quant_db"]}:3000/#{index}?and=(date.gte.#{begin_time.strftime('%Y-%m-%dT%H:%M:%S')},date.lte.#{end_time.strftime('%Y-%m-%dT%H:%M:%S')})"
    res = HTTParty.get url
    json = JSON.parse res.body
    csv = CSV.generate(headers: false) { |csv| json.map(&:to_a).each { |row| csv << row } }

    file = Rails.root.to_s + "/tmp/csv/#{index}.csv"
    CSV.open( file, 'w' ) do |writer|
      json.each do |c|
        writer << [c["date"], c["open"], c["high"], c["low"], c["close"], c["volume"]]
      end
    end

    return file
  end


  def online_data(file)
    # PyCall.exec("")
    pandaAI = Rails.root.to_s + '/lib/python/ai/pandaAI'
    data = {}
    PyCall.exec("dual_params={'resample': '05T', 'barNum': 1, 'file_path': '#{file}',  'check_date': '2019-09-26'}")
    PyCall.exec("import sys")
    PyCall.exec("sys.path.append('#{pandaAI}')")
    PyCall.exec("import robot")
    PyCall.exec("import imp")
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

    PyCall.exec("df1 = pd.read_csv(dual_params['file_path'],  skiprows=1, header=None, sep=',', names=['dates', 'open', 'high', 'low', 'close','volume'])")
    PyCall.exec("print(df1.info())")

    # 整理数据
    PyCall.exec("import matplotlib.dates as mdates")
    PyCall.exec("mm = robot.MM()")

    #设置索引
    PyCall.exec("df2 = mm.set_index(df1,cname='dates')")
    PyCall.exec("period_data = df2.resample(dual_params['resample']).last()")
    # 分别对于开盘、收盘、最高价、最低价进行处理
    PyCall.exec("period_data['open'] = df2['open'].resample(dual_params['resample']).first()")
    # 处理最高价和最低价
    PyCall.exec("period_data['high'] = df2['high'].resample(dual_params['resample']).max()")
    # 最低价
    PyCall.exec("period_data['low'] = df2['low'].resample(dual_params['resample']).min()")
    # 成交量 这一周的每天成交量的和
    PyCall.exec("period_data['volume'] = df2['volume'].resample(dual_params['resample']).sum()")
    PyCall.exec("online_data = period_data.dropna(axis=0)") # 缺失值处理
    PyCall.exec("online_data['dates2'] = online_data.index")
    PyCall.exec("online_data['dates2'] = online_data['dates2'].apply(mdates.date2num)")

    PyCall.exec("online_data = online_data.dropna(axis=0)") # 缺失值处理
    # PyCall.exec("print (online_data.head())")
    # PyCall.exec("print (online_data.tail(1))")

    # 基于线性回归的模型加载
    PyCall.exec("from sklearn.linear_model import LinearRegression")
    PyCall.exec("from sklearn.externals import joblib")
    PyCall.exec("reg_buy_open = joblib.load('#{Rails.root.to_s}' + '/lib/python/ib/reg_buy_open.pkl')")
    PyCall.exec("reg_buy_break = joblib.load('#{Rails.root.to_s}' + '/lib/python/ib/reg_buy_break.pkl')")
    PyCall.exec("reg_sale_open = joblib.load('#{Rails.root.to_s}' + '/lib/python/ib/reg_sale_open.pkl')")
    PyCall.exec("reg_sale_break = joblib.load('#{Rails.root.to_s}' + '/lib/python/ib/reg_sale_break.pkl')")
    PyCall.exec("scale = joblib.load('#{Rails.root.to_s}' + '/lib/python/ib/scale.pkl')")

    PyCall.exec("pre_data = online_data.iloc[-2:-1][['open','high','low','close']]") # 倒数第二个bar作为数据预测基准, 因为当前bar还没有走完
    PyCall.exec("current_price = online_data.tail(1)['close']")
    PyCall.exec("open_price = pre_data['open']")
    PyCall.exec("high_price = pre_data['high']")
    PyCall.exec("low_price = pre_data['low']")
    PyCall.exec("close_price = pre_data['close']")
    PyCall.exec("hash = {}")
    PyCall.exec("hash['current_price'] = current_price[-1]")
    PyCall.exec("hash['open'] = open_price[-1]")
    PyCall.exec("hash['high'] = high_price[-1]")
    PyCall.exec("hash['low'] = low_price[-1]")
    PyCall.exec("hash['close'] = close_price[-1]")
    PyCall.exec("hash['reg_buy_open'] = reg_buy_open.predict(pre_data)[-1]")
    PyCall.exec("hash['reg_buy_break'] = reg_buy_break.predict(pre_data)[-1]")
    PyCall.exec("hash['reg_sale_open'] = reg_sale_open.predict(pre_data)[-1]")
    PyCall.exec("hash['reg_sale_break'] = reg_sale_break.predict(pre_data)[-1]")

    PyCall.exec("import json")
    PyCall.exec("print (hash)")
    PyCall.exec("destination='#{Rails.root.to_s}' + '/tmp/data.json'")
    PyCall.exec("saveFile = open(destination, 'w')")
    PyCall.exec("saveFile.write(json.dumps(hash))")
    PyCall.exec("saveFile.close()")

    file_name = "data.json"
    location = Rails.root.to_s + "/tmp/" + file_name
    begin
      f = File.open(location, "r")
      data = JSON.load f
      f.close
    rescue Exception => ex
      Rails.logger.warn "#{ex.message}"
    end

    return data
  end

  def check_position(data)
    position = ib_positions
    amount = 4
    order = ""

    if position == {}
      if data["current_price"] > data["reg_buy_open"]
        order = "BUY"
      elsif data["current_price"] < data["reg_buy_open"]
        order = "SELL"
      end
    elsif !position["position"].nil? && position["position"] > 0 # buy
      # 冲高回落, 平仓
      if data['high'] > data["reg_buy_break"] && data["current_price"] < data["reg_buy_open"]
        order = "SELL"
        amount = position["position"].abs
      # 移动平仓
      elsif data["current_price"] <  data['close']
        order = "SELL"
        amount = position["position"].abs
      end
    elsif !position["position"].nil? && position["position"] < 0 # sell
      if data['low'] > data["reg_buy_break"] && data["current_price"] > data["reg_buy_open"]
        order = "BUY"
        amount = position["position"].abs
      elsif data["current_price"] > data['close']
        order = "BUY"
        amount = position["position"].abs
      end
    end

    Rails.logger.warn "ib hsi_5mins order: #{order} #{amount.to_s}"

    if order != "" && amount != 0
      ib_order(order, amount, 0)
    end

    return {"order": order, "amount": amount}
  end

end
