#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
# require 'net/ping'
# require 'pycall/import'
# include PyCall::Import

module ContractsHelper

  def document_files(contract, version, file)
    dir = Rails.root.to_s + "/tmp/csv/#{Time.zone.now.strftime("%Y%m%d")}"
    unless File.directory?(dir)
      FileUtils.mkdir_p(dir)
    end
    dest = "#{dir}/#{contract}_#{version}_#{Time.zone.now.strftime("%Y%m%d%H%M")}.csv"
    File.open(dest, 'w') { |f| f.write(File.read(file)) }
  end

  def index_to_csv(contract, version, market_data, with_index, db_collect=ENV['db_collect'])

    file = Rails.root.to_s + "/tmp/csv/#{contract}_#{version}.csv"
    if db_collect == 'false' && market_data
      market_data.to_csv(file, sep: ',', encoding: 'utf-8', index: false)
    else
      begin_time = EventLog.maximum(:created_at).nil? ? Time.zone.now - 1.day : EventLog.maximum(:created_at).beginning_of_day
      end_time = Time.zone.now
      # url = "http://#{ENV["market_db"]}:3000/#{contract}?and=(date.gte.#{begin_time.strftime('%Y-%m-%dT%H:%M:%S')},date.lte.#{end_time.strftime('%Y-%m-%dT%H:%M:%S')})"
      url = "http://#{ENV["market_db"]}:3000/#{contract}_last_1200"
      res = HTTParty.get url
      json = JSON.parse res.body
      begin
        csv = CSV.generate(headers: false) { |csv| json.map(&:to_a).each { |row| csv << row } }
        CSV.open( file, 'w' ) do |writer|
          writer << ["date", "open", "high", "low", "close", "volume", "barCount", "average"] if with_index
          json.each do |c|
            writer << [c["date"], c["open"], c["high"], c["low"], c["close"], c["volume"], c["barCount"], c["average"]]
          end
        end
      rescue Exception => e
        Rails.logger.warn "index_to_csv failed: #{e}"
      end
    end
    return file
  end

  def join_realtime_csv(file, bars)
    csv = CSV.parse(File.read(file), headers: true)
    last_time = csv[-1]["date"].to_time
    new_bars = []
    bars.each_with_index do |bar, index|
      if last_time < bar[:date].to_time
        new_bars << bar
      end
    end

    CSV.open(file, "a+") do |writer|
      new_bars.each do |bar|
        writer << [bar[:date], bar[:open], bar[:high], bar[:low], bar[:close], bar[:volume], bar[:average], bar[:barCount]]
      end
    end
    return file
  end

  def online_data(file)
    Rails.logger.warn "online_data start: #{Time.zone.now}"
    # PyCall.exec("")
    pandaAI = Rails.root.to_s + '/lib/python/ai/pandaAI'
    data = {}
    today = Date.today.strftime('%Y-%m-%d')
    file_name = "data.json"
    destination = Rails.root.to_s + "/tmp/" + file_name

    begin
      File.open(destination, 'r') do |f|
        # do something with file
        File.delete(f)
      end
    rescue Errno::ENOENT
    end

    begin
      PyCall.exec("dual_params={'resample': '5T', 'step_n': 5, 'barNum': 1, 'file_path': '#{file}',  'check_date': '#{today}'}")
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
          # PyCall.exec("from sklearn.linear_model import LinearRegression")
          PyCall.exec("import joblib")
          PyCall.exec("reg_buy_open = joblib.load('#{Rails.root.to_s}' + '/lib/python/ib/reg_buy_open.pkl')")
          PyCall.exec("reg_buy_break = joblib.load('#{Rails.root.to_s}' + '/lib/python/ib/reg_buy_break.pkl')")
          PyCall.exec("reg_sale_open = joblib.load('#{Rails.root.to_s}' + '/lib/python/ib/reg_sale_open.pkl')")
          PyCall.exec("reg_sale_break = joblib.load('#{Rails.root.to_s}' + '/lib/python/ib/reg_sale_break.pkl')")
          PyCall.exec("scale = joblib.load('#{Rails.root.to_s}' + '/lib/python/ib/scale.pkl')")

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

          PyCall.exec("import json")
          # PyCall.exec("print (hash)")
          PyCall.exec("destination='#{destination}'")
          PyCall.exec("saveFile = open(destination, 'w')")
          PyCall.exec("saveFile.write(json.dumps(hash))")
          PyCall.exec("saveFile.close()")
        end
      end
    rescue Exception => ex
      Rails.logger.warn "#{ex.message}"
    end

    begin
      f = File.open(destination, "r")
      data = JSON.load f
      f.close
      Rails.logger.warn "online_data success: #{Time.zone.now}"
    rescue Exception => ex
      # Rails.logger.warn "#{ex.message}"
      Rails.logger.warn "online_data no file error: #{Time.zone.now}"
    end

    return data
  end

  def py_check_position(contract, version, amount = ENV["amount"])
    order = ""
    position = TraderPosition.init("#{contract}").position
    csv = Rails.root.to_s + "/tmp/csv/#{contract}_#{version}.csv"
    h5 = Rails.root.to_s + "/tmp/csv/#{contract}_#{version}.h5"
    json = Rails.root.to_s + "/tmp/#{contract}_#{version}_trades.json"
    yaml_path = Rails.root.to_s + "/lib/python/ib/#{ENV["ib_version"]}_#{ENV["backtrader_version"]}.yml"
    # begin_date = Time.zone.now < (Time.parse "11:30 am") ? 1.business_day.ago.to_date : Date.today
    # end_date = 1.business_day.from_now.to_date
    skip_minute = Time.zone.now < (Time.parse "10:15 am") ? 75 : 0
    #回溯1小时，舍去9:15-9:44 & 15:45-16:29交易时间
    # begin_time = CSV.read(csv)[CSV.read(csv).count-skip_minute-90][0].to_time
    # end_time = (Time.zone.now - 1.minute).beginning_of_minute.to_time
    end_time = Time.zone.now.beginning_of_minute.to_time
    # end_time = ("2021-01-11 11:23:05".to_time - 1.minute).beginning_of_minute.to_time
    begin_time = 10.days.before(end_time)
    data = Array.new()
    action = Action.today.last
    # first_trade = Trade.today.where("realized_pnl != 0").first
    reverse_flag = false
    n = 0
    pnls_flag = Trade.today.where("realized_pnl != 0").map{|t| t.realized_pnl > 0 ? 1 : 0}
    pnls_flag.each do |t|
      n = n + 1 if t == 0
      n = 0 if t == 1
      reverse_flag = true if n == 3
    end
    # reverse_flag = (first_trade && first_trade.realized_pnl < 0) ? true : false
    3.times do
      if reverse_flag
        Rails.logger.warn "ib py_check_position reversed!"
        system( "cd #{Rails.root.to_s + '/lib/python/ib'} && python3 #{ENV["ib_version"]}_#{ENV["backtrader_version"]}_reverse.py '#{csv}' '#{json}' '#{begin_time}' '#{end_time}' '#{yaml_path}' '#{h5}'" )
      else
        system( "cd #{Rails.root.to_s + '/lib/python/ib'} && python3 #{ENV["ib_version"]}_#{ENV["backtrader_version"]}.py '#{csv}' '#{json}' '#{begin_time}' '#{end_time}' '#{yaml_path}' '#{h5}'" )
      end

      begin
        data = JSON.parse(File.read(json))
      rescue Exception => e
        error_message = e.message
        break
      end
      break if !data.empty?
      Rails.logger.warn "ib check no csv file"
      sleep 2
    end
    if data.last
      Rails.logger.warn("ib check last data: " + Rainbow("#{data.last}").blue)
      time_diff = 1.minute.ago(Time.zone.now.beginning_of_minute) - data.last["time"].to_time
      Rails.logger.warn "ib check time_diff: #{time_diff}"
      ot = case ENV['backtrader_version']
      when '1min'
        60
      when '5min'
        300
      when '4min'
        480
      when '3min'
        360
      when '15sec'
        60
      else
        120
      end
      if time_diff.abs <= ot || data.last["order"].upcase == "CLOSE"
        Rails.logger.warn "ib position: #{position} last_order: #{data.last["order"].upcase} prev_action: #{action ? action.order : ""}@#{action ? action.action_time : ""}"
        if position && position != 0 && data.last["order"].upcase == "CLOSE"
          amount = position.abs
          order = "CLOSE"
        end
        if position && position < 0 && data.last["order"].upcase == "BUY"
          amount = position.abs
          begin
            job = OrdersJob.perform_later("CLOSE", amount, "", 0)
            100.times do
              status = ActiveJob::Status.get(job)
              break if status.completed?
              sleep 0.2
            end
          rescue Exception => e
            error_message = e.message
          end
          order = data.last["order"].upcase
        end
        if position && position > 0 && data.last["order"].upcase == "SELL"
          amount = position.abs
          begin
            job = OrdersJob.perform_later("CLOSE", amount, "", 0)
            100.times do
              status = ActiveJob::Status.get(job)
              break if status.completed?
              sleep 0.2
            end
          rescue Exception => e
            error_message = e.message
          end
          order = data.last["order"].upcase
        end
        if position && position > 0 && data.last["order"].upcase == "BUY"
          order = ""
        end
        if position && position < 0 && data.last["order"].upcase == "SELL"
          order = ""
        end
        if position == 0 && data.last["order"].upcase != "CLOSE"
          order = data.last["order"].upcase
        end
      end
    else
      if position && position != 0
        order = "CLOSE"
      end
    end

    # if last action is the same
    last_action = Action.today.last
    if last_action && last_action.order != "CLOSE" && last_action.order == order
      order = ""
    end

    begin
      moves = JSON.parse(File.read(json + ".move.json"))
    rescue Exception => e
      p e
    end

    move_order = ""
    move_price = 0
    move_time = ""

    if moves && moves.last
      time_diff = Time.zone.now.beginning_of_minute - moves.last["time"].to_time
      Rails.logger.warn "ib check move time_diff: #{time_diff}"
      if time_diff.abs <= 30
        move_order = moves.last["order"].upcase
        move_price = moves.last["price"]
        move_time = moves.last["time"].to_time
        begin
          Action.act(move_order, amount, move_price, move_time) if Action.find_by(action_time: move_time).nil?
          Rails.logger.warn "ib move order: #{move_order} #{move_price} #{move_time}"
        rescue Exception => e
          Rails.logger.warn "Action create failed: #{e}"
        end
      end
    end

    Rails.logger.warn "ib order: #{order == "" ? "NO" : order} #{amount.to_s}"
    begin
      Action.act(order, amount, 0, data.last["time"].to_time) if order != ""
    rescue Exception => e
      Rails.logger.warn "Action create failed: #{e}"
    end

    return order, amount, move_order, move_price
  end

  def check_position(data)
    # {"current_open"=>26520.14, "current_close"=>26515.06, "current_high"=>26525.11, "current_low"=>26508.94,
    # "prev_open"=>26530.69, "prev_close"=>26519.39, "prev_high"=>26536.75, "prev_low"=>26514.3,
    # "reg_buy_open"=>26547.974999999995, "reg_buy_break"=>26559.200000000008, "reg_sale_open"=>26503.075,
    # "reg_sale_break"=>26491.85}

    amount = ENV["amount"].to_i
    order = ""
    position = ib_positions

    Rails.logger.warn "ib position: #{position}"

    # 3.times do
    #   position = ib_positions
    #   if position == false
    #     Rails.logger.warn "ib position WRONG"
    #     sleep 1
    #     position = ib_positions
    #   else
    #     next
    #   end
    # end
    if position
      if position == {}
        if data["current_close"] > data["reg_buy_open"]
          order = "BUY"
        elsif data["current_close"] < data["reg_sale_open"]
          order = "SELL"
        end
      elsif !position["position"].nil? && position["position"] > 0 # buy
        # 冲高回落, 平仓
        if data['current_high'] > data["reg_buy_break"] && data["current_close"] < data["reg_buy_open"]
          order = "SELL"
          amount = position["position"].abs
        # 移动平仓
        elsif data["current_close"] <  data['prev_close']
          order = "SELL"
          amount = position["position"].abs
        end
      elsif !position["position"].nil? && position["position"] < 0 # sell
        if data['current_low'] < data["reg_sale_break"] && data["current_close"] > data["reg_sale_open"]
          order = "BUY"
          amount = position["position"].abs
        elsif data["current_close"] > data['prev_close']
          order = "BUY"
          amount = position["position"].abs
        end
      end

      Rails.logger.warn "ib data: #{data}"
      Rails.logger.warn "ib order: #{order == "" ? "NO" : order} #{amount.to_s}"

      if order != "" && amount != 0
        ib_order(order, amount, 0)
        EventLog.create(content: "#{order} #{amount.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}")
      end
    end

    return {"order": order, "amount": amount}
  end

  def close_position
    contract = case ENV['backtrader_version']
    when '15sec'
      'hsi_15secs'
    else
      'hsi'
    end
    order = ""
    amount = 0
    position = TraderPosition.init(contract).position

    if position > 0 # buy
      order = "SELL"
      amount = position.abs
    elsif position < 0 # sell
      order = "BUY"
      amount = position.abs
    end

    Rails.logger.warn "ib close_position: #{order} #{amount.to_s}"

    if order != "" && amount != 0
      ib_order(order, amount, 0)
    end

    return order, amount
  end

  def trades_to_csv(contract, duration)
    # contract = "hsi_5mins"
    json = []
    case duration
    when "day"
      json = Trade.where("time >= ?", DateTime.now.beginning_of_day).as_json
    when "week"
      json = Trade.where("time >= ?", DateTime.now.beginning_of_week).as_json
    when "month"
      json = Trade.where("time >= ?", DateTime.now.beginning_of_month).as_json
    else
      json = Trade.all.as_json
    end
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

  def test_trade
    contract = "hsi"
    1000.times do
      current_time = Time.zone.now.strftime('%H:%M')
      TradersJob.perform_now contract
      sleep 56
    end
  end

  def last_n_csv_dirs(n)
    a = Array.new
    Dir.entries(dir).sort.each do |d|
      next if d == '.' or d == '..'
      path = dir + d
      next if File.file?(path)
      next if path.include? Date.today.strftime("%Y%m%d")
      a << path
    end

    retrun a.last(n)
  end

  def daily_last_csv(date)
    dir = Rails.root.to_s + "/tmp/csv/#{date}/"
    path = dir + Dir.entries(dir).sort.last

    return path
  end

  def merge_csv(contract, version, file_1, file_2, merged_file)
    merged_file = Rails.root.to_s + "/tmp/csv/#{contract}_#{version}_combined.csv"
    file_2 = Rails.root.to_s + "/tmp/csv/#{contract}_#{version}.csv"
    file_1 = Rails.root.to_s + "/tmp/csv/20220316/#{contract}_#{version}_202203161550.csv"
    file_1 = Rails.root.to_s + "/tmp/csv/20220315/#{contract}_#{version}_202203151550.csv"
    table = CSV.parse(File.read(file_2), headers: true)
    tmp_table = CSV.parse(File.read(file_1), headers: true)
    start_date = table[0]["date"]
    tmp_table.delete_if do |row|
      row["date"].to_time >= start_date.to_time
    end
    table.each do |row|
      tmp_table.push(row)
    end
    File.open(merged_file, 'w') do |f|
      f.write(tmp_table.to_csv)
    end
    return file
  end

  # def merge(*csvs)
  #   headers = csvs.map {|csv| csv.headers }.flatten.compact.uniq.sort
  #   csvs.flat_map(&method(:csv_to_hash_array))
  # end
  #
  # def csv_to_hash_array(csv)
  #   csv.to_a[1..-1].map do |row|
  #     Hash[csv.headers.zip(row)]
  #   end
  # end

  def csv_to_db
    dir = Rails.root.to_s + "/tmp/csv/"
    dump_file = dir + "/" + "dump_sql.sql"

    postgres = PG.connect :host => ENV['quant_db_host'], :port => ENV['quant_db_port'], :dbname => ENV['quant_db_name'], :user => ENV['quant_db_user'], :password => ENV['quant_db_pwd']

    sql = "select max(date) as date from hsi_fut;"
    res = postgres.exec(sql)
    begin_date = res.first["date"].to_date.strftime('%Y%m%d')

    Dir.entries(dir).sort.each do |d|
      next if d == '.' or d == '..'
      path = dir + d
      next if File.file?(path)
      next if begin_date && d.to_i < begin_date.to_i
      # csv_file = Dir.glob(File.join(path, '*.*')).max { |a,b| File.ctime(a) <=> File.ctime(b) }
      csv_file = Dir.glob(File.join(path, '*.*')).min { |a,b| File.ctime(a) <=> File.ctime(b) }
      # table = CSV.parse(File.read(csv_file), headers: true)
      table = []
      total_count = (("12:00".to_time - "09:15".to_time) / 60 * 4) + (("15:51".to_time - "13:00".to_time) / 60 * 4)
      Dir.entries(path).sort.each do |tmp_file|
        next if tmp_file == '.' or tmp_file == '..'
        p "before table has: #{table.count}"
        file_path = path + "/" + tmp_file
        File.file?(file_path)
        p file_path
        # tmp_table = CSV.parse(File.read(file_path), headers: true)
        # table.each do |tr|
        #   tmp_table.delete_if do |row|
        #     row["date"].to_time == tr["date"].to_time
        #   end
        # end
        # p "add tmp_table has: #{tmp_table.count}"
        # # next if tmp_table.count == 0
        # tmp_table.each do |row|
        #   table.push(row)
        # end
        # p "now table has: #{table.count}"
        tmp_table = CSV.open(file_path, headers: :first_row).map(&:to_h)
        tmp_table.each do |r|
          table << r
        end
      end
      # p csv_file
      # csv_text = File.read(csv_file)
      # csv = CSV.parse(csv_text, :headers => true)
      count = 0
      table = table.uniq

      final = []
      table.each do |r|
        n = table.select {|e| e["date"] == r["date"]}
        if n.count == 1
          final << r
        elsif n.count > 1
          n2 = final.select {|e| e["date"] == r["date"]}
          if n2.empty? || n2[0]["volume"] < r["volume"]
            final.delete_if {|a| a["date"] == r["date"]}
            final << r
          end
        end
      end

      p "now table has: #{final.count}"
      final.each do |row|
        h = row
        sql = "insert into hsi_fut_tmp (select 0 as index, '#{h["date"]}' as date, #{h["open"]} as open,#{h["high"]} as high,#{h["low"]} as low,#{h["close"]} as close,#{h["volume"]} as volume,#{h["barCount"]} as barCount,#{h["average"]} as average);"
        postgres.exec(sql)
        count = count + 1
        p count
      end
      # table.each do |row|
      #   # Moulding.create!(row.to_hash)
      #   h = row.to_hash
      #   sql = "insert into hsi_fut_tmp (select 0 as index, '#{h["date"]}' as date, #{h["open"]} as open,#{h["high"]} as high,#{h["low"]} as low,#{h["close"]} as close,#{h["volume"]} as volume,#{h["barCount"]} as barCount,#{h["average"]} as average);"
      #   postgres.exec(sql)
      #   count = count + 1
      #   p count
      # end
      sql = "insert into hsi_fut select * from hsi_fut_tmp b where not exists (select 1 from hsi_fut a where a.date = b.date);"
      postgres.exec(sql)
      sql = "truncate table hsi_fut_tmp;"
      postgres.exec(sql)
      # sleep 0.2
    end
  end
end
