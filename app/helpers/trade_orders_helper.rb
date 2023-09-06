#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
require 'net/ping'
require 'pycall'
# require 'pycall/import'
# require 'pycall/init'
# require 'pycall/libpython'
# include PyCall::Import

# pyimport 'ib_insync'
# pyimport 'sys'
# pyimport 'json'
# pyimport 'math'
# pyfrom 'ib_insync', import: :IB
# pyfrom 'datetime', import: :timezone

module TradeOrdersHelper

  def ib_connect
    ip = ENV['tws_ip'] #PyCall.eval("str('127.0.0.1')")
    port = ENV['tws_port'].to_i
    clientId = ENV['tws_clientid'].to_i #master client id
    clientId = rand(11..19999)

    begin
      PyCall.exec("import sys, pytz")
      PyCall.exec("modulename = 'ib_insync'")
      check = PyCall.eval("modulename not in sys.modules")
      if check
        sleep rand(0.1..0.7)
        PyCall.exec("from ib_insync import *")
      end
      PyCall.exec("ib_status = ''")
      PyCall.exec("ib = IB()")
      # PyCall.exec("ib.TimezoneTWS = pytz.timezone('Asia/Hong_Kong')")
      # PyCall.exec("ib.connect('#{ip}', #{port}, clientId=#{clientId}), 5")
      PyCall.exec("try: ib.connect(host='#{ip}', port=#{port}, clientId=#{clientId}, timeout=10, readonly=False)\nexcept Exception as e: ib_status = str(e)")
    rescue Exception => e
      error_message = e
      Rails.logger.warn "ib_connect failed: #{error_message}"
    ensure
      ib_status = PyCall.eval("ib_status")
      Rails.logger.warn "ib_connect failed: #{ib_status}" if !ib_status.empty?
      ib = PyCall.eval("ib")
    end

    return ib
  end

  def ib_disconnect(ib)
    status = false
    begin
      ib.disconnect()
    rescue Exception => e
      error_message = e
      Rails.logger.warn "ib_disconnect failed: #{error_message}"
    ensure
      Rails.logger.warn "ib disconnection: #{ib.to_s}"
      # system (`pkill -9 python`) if Rails.env == "production"
      status = true if ib && ib.isConnected() == false
      # sleep(2)
    end

    return status
  end

  def ib_main_contract(contract)
    main_contract = false
    case contract.upcase
    when "HSI"
      exchange = 'HKFE'
      currency = 'HKD'
    when "SPX"
      exchange = 'GLOBEX'
      currency = 'USD'
    end
    begin
      PyCall.exec("c = Future('#{contract.upcase}')")
      PyCall.exec("cds = ib.reqContractDetails(c)")
      PyCall.exec("contracts = [cd.contract for cd in cds]")

      PyCall.exec("months = []")
      PyCall.exec("for co in contracts: months.append(co.lastTradeDateOrContractMonth)")
      PyCall.exec("months.sort()")

      PyCall.exec("import datetime")
      PyCall.exec("dt = datetime.datetime.now()")
      PyCall.exec("today = dt.strftime( '%Y%m%d' )")
      PyCall.exec("last_day = (datetime.datetime.strptime(months[0], '%Y%m%d').date() - datetime.timedelta(days=7)).strftime( '%Y%m%d' )")

      if PyCall.eval("today >= last_day")
        PyCall.exec("month = months[#{ENV['contract_num'].to_i + 1}]")
      else
        PyCall.exec("month = months[#{ENV['contract_num'].to_i}]")
      end
      PyCall.exec("fut = Future(symbol='#{contract.upcase}', lastTradeDateOrContractMonth=month, exchange='#{exchange}', currency='#{currency}')")
      PyCall.exec("contract = ib.reqContractDetails(fut)[0].contract")

      main_contract = PyCall.eval("contract")
    rescue Exception => e
      error_message = e
    end

    return main_contract
  end

  def ib_order(order_type, amount, price)

    # order_type = 'SELL'
    # amount = 2
    # price = 0
    contract = "hsi"

    begin
      order_status = false
      main_contract = ApplicationController.helpers.ib_main_contract(contract)
      PyCall.exec("contract = #{main_contract}")
      case price
      when 0
        PyCall.exec("order = MarketOrder('#{order_type}', #{amount})")
      else
        PyCall.exec("order = LimitOrder('#{order_type}', #{amount}, #{price})")
      end
      PyCall.exec("trade = ib.placeOrder(contract, order)")
      PyCall.exec("trade")
      log = PyCall.eval("trade.log")
      if log.first.status && log.first.status != ''
        order_status = true
        PyCall.exec("ib.sleep(0)")
      end
    rescue Exception => e
      error_message = e
    ensure
      # ib_disconnect(ib)
      contract = case ENV['backtrader_version']
      when '15sec'
        'hsi_15secs'
      else
        'hsi'
      end
    end

    return order_status
  end

  def position_update(contract)
    # position = case order_type
    # when 'SELL'
    #   amount.to_i * -1
    # when 'BUY'
    #   amount.to_i
    # else
    #   0
    # end
    position = ApplicationController.helpers.ib_positions(contract)
    tp = TraderPosition.init(contract)
    # previous_position = 0
    # previous_position = tp.position if (tp && !tp.position.nil?)
    # tp.position = position + previous_position if price > 0
    tp.position = position["position"].nil? ? 0 : position["position"]
    tp.save!

    return tp.position
  end

  def ib_positions(contract)
    main_contract = ApplicationController.helpers.ib_main_contract(contract)
    begin
      # ib = ib_connect
      PyCall.exec("lastTradeDateOrContractMonth = '#{main_contract.lastTradeDateOrContractMonth}'")
      PyCall.exec("pos = ib.positions()")
      PyCall.exec("list = {}")
      PyCall.exec("for po in pos:
          if po.contract.symbol == 'HSI' and po.contract.lastTradeDateOrContractMonth == lastTradeDateOrContractMonth:
            list.update({'position': po.position, 'currency': po.contract.currency, 'contract_date': po.contract.lastTradeDateOrContractMonth, 'symbol': po.contract.symbol})")

      data = PyCall.eval("list").to_h
    rescue Exception => e
      data = false
      error_message = e
    # ensure
    #   ib_disconnect(ib)
    end

    return data
  end

  def ib_cancelorder(order_type, amount, price)
    begin
      PyCall.eval("ib.reqGlobalCancel()")
      # PyCall.exec("import asyncio")
      # PyCall.exec("future = ib.reqCurrentTimeAsync()")
      # PyCall.exec("loop = asyncio.get_event_loop()")
      # PyCall.exec("loop.run_until_complete(future)")
      # PyCall.exec("print(future.result())")
      # orders = PyCall.eval("ib.reqGlobalCancel()")
      # orders.each do |order|
      #   if order.orderType == "LMT" #&& order.action.upcase == order_type && order.totalQuantity == amount && order.lmtPrice == price
      #     p order
      #     PyCall.exec("ib.cancelOrder(#{order})")
      #     PyCall.eval("#{order}")
      #
      #     Rails.logger.warn "ib_cancelorder: #{order_type}@#{price}"
      #     sleep 0.1
      #   end
      # end
    rescue Exception => e
      data = false
      error_message = e
    # ensure
    #   ib_disconnect(ib)
    end

    return true
  end

  def ib_portfolio
    begin
      # ib = ib_connect
      PyCall.exec("pnls = ib.portfolio()")
      PyCall.exec("list = {}")
      PyCall.exec("for pnl in pnls:
          if pnl.contract.symbol == 'HSI':
            list.update({'position': pnl.position, 'marketPrice': pnl.marketPrice, 'marketValue': pnl.marketValue, 'unrealizedPNL': pnl.unrealizedPNL,  'realizedPNL': pnl.realizedPNL})")

      data = PyCall.eval("list").to_h
    rescue Exception => e
      data = false
      error_message = e
    # ensure
    #   ib_disconnect(ib)
    end

    return data
  end

  def ib_trades(contract)
    data = []
    main_contract = ApplicationController.helpers.ib_main_contract(contract)
    begin
      # ib = ib_connect
      PyCall.exec("trades = ib.trades()")
      # PyCall.exec("print(trades)")
      PyCall.exec("array = []")
      PyCall.exec("list = {}")
      PyCall.exec("for t in trades: array.append(dict({'permId': t.order.permId, 'action': t.order.action, 'symbol': t.contract.symbol, 'lastTradeDateOrContractMonth': t.contract.lastTradeDateOrContractMonth, 'currency': t.contract.currency, 'fills': t.fills}))")
      a = PyCall.eval("array")

      a.each do |d|
        d['fills'].each do |f|
          if main_contract.lastTradeDateOrContractMonth == d['lastTradeDateOrContractMonth']
            data << {"perm_id": d['permId'], "action": d['action'], "symbol": d['symbol'],
              "last_trade_date_or_contract_month": d['lastTradeDateOrContractMonth'],
              "currency": d['currency'], "shares": f.execution.shares, "price": f.execution.price,
              "time": f.execution.time.timestamp(), "commission": f.commissionReport.commission,
              "realized_pnl": f.commissionReport.realizedPNL, "exec_id": f.commissionReport.execId}
          end
        end
      end

    rescue Exception => e
      data = false
      error_message = e
    # ensure
    #   ib_disconnect(ib)
    end

    return data
  end

  def ib_account_values
    # [v for v in ib.accountValues() if v.tag == 'NetLiquidationByCurrency' and v.currency == 'BASE']
    begin
      # ib = ib_connect
      PyCall.exec("list = [v for v in ib.accountValues()]")

      list = PyCall.eval("list").to_a
    rescue Exception => e
      error_message = e
    # ensure
    #   ib_disconnect(ib)
    end

    data = {}

    if list
      list.each_with_index do |l, index|
        case l.tag
        when "NetLiquidationByCurrency"
          data["account_" + l.tag + "_" + l.currency] = {}
          data["account_" + l.tag + "_" + l.currency]["value"] = l.value
          data["account_" + l.tag + "_" + l.currency]["currency"] = l.currency
        when "AvailableFunds"
          data["account_" + l.tag + "_" + l.currency] = {}
          data["account_" + l.tag + "_" + l.currency]["value"] = l.value
          data["account_" + l.tag + "_" + l.currency]["currency"] = l.currency
        end
      end
    end

    return data
  end

  def bar_second_covert(second)
    version = ENV['backtrader_version']
    end_second = false
    case version
    when "15secs"
      if second.to_i >= 0 && second.to_i < 15
        end_second = 15
      elsif second.to_i >= 15 && second.to_i < 30
        end_second = 30
      elsif second.to_i >= 30 && second.to_i < 45
        end_second = 45
      elsif second.to_i >= 45
        end_second = 0
      end
    when "1min"
      end_second = 0
    when "5mins"
      end_second = 0
    end
    return end_second
  end

  def realtime_market_data(contract, version, force_collect=false, db_collect=ENV['db_collect'])
    # contract = ENV['contract']
    # version = ENV['backtrader_version']
    result = true
    list = nil
    # file = Rails.root.to_s + "/tmp/#{contract}_#{version}_bars.json"
    file = Rails.root.to_s + "/tmp/#{contract}_#{version}_bars.csv"
    begin
      main_contract = ApplicationController.helpers.ib_main_contract(contract)
      # PyCall.exec("import json")
      PyCall.exec("def onBarUpdate(bars, hasNewBar):
          df = util.df(bars[-100:])
          df.to_csv('#{file}', sep=',', encoding='utf-8', index=False)")
          # with open('#{file}', 'w') as f:
          #     json.dump(df.to_json(orient='records', lines=True), f)")
      PyCall.exec("contract = #{main_contract}")
      PyCall.exec("bars = ib.reqRealTimeBars(contract, 5, 'TRADES', False)")
      PyCall.exec("bars.updateEvent += onBarUpdate")
      PyCall.exec("ib.sleep(10800)")
      PyCall.exec("ib.cancelRealTimeBars(bars)")
      # Rails.logger.warn "market_data_latest: #{PyCall.eval("bars[-1].date").to_s} force_collect: #{force_collect.to_s}"
    rescue Exception => e
      error_message = e
      result = false
      Rails.logger.warn "realtime_market_data error: #{error_message}, #{Time.zone.now}"
    end
    return result
  end

  def realtime_bar_resample(contract, version)
    # contract = ENV['contract']
    # version = ENV['backtrader_version']
    result = false
    file = Rails.root.to_s + "/tmp/#{contract}_#{version}_bars.csv"

    table = nil
    3.times do
      begin
        table = CSV.parse(File.read(file), headers: true)
      rescue Exception => e
        error_message = e
        result = false
        Rails.logger.warn "realtime_bar_resample error: #{error_message}, #{Time.zone.now}"
      end
      break if table && table.count > 0
      sleep 0.3
    end

    if table && table.count > 0
      array = []
      table.each do |row|
        time = row["time"].in_time_zone
        open = row["open_"]
        high = row["high"]
        low = row["low"]
        close = row["close"]
        volume = row["volume"].to_i.to_s
        average = ((high.to_f + low.to_f) / 2).to_s
        count  = row["count"].to_i.to_s

        if ["00", "15", "30", "45"].include? time.strftime('%S')
          # time = time.change({sec: bar_second_covert(time.strftime('%S'))})
          time = time.strftime('%Y-%m-%d %H:%M:%S')
          bar = {"date": time, "open": open, "high": high, "low": low, "close": close, "volume": volume, "average": average, "barCount": count}
          array << bar
        else
          # time = time.change({sec: bar_second_covert(time.strftime('%S'))})
          last_bar = array[-1]
          if last_bar
            open = last_bar[:open]
            high = (last_bar[:high].to_f > high.to_f ? last_bar[:high].to_f : high).to_s
            low = (last_bar[:low].to_f < low.to_f ? last_bar[:low].to_f : low).to_s
            volume = (last_bar[:volume].to_f + volume.to_f).to_i.to_s
            average = ((high.to_f + low.to_f) / 2).to_s
            count  = (last_bar[:barCount].to_f + count.to_f).to_i.to_s

            # bar = {"time": time, "open": open, "high": high, "low": low, "close": close, "volume": volume, "average": average, "count": count}
            last_bar[:open] = open
            last_bar[:high] = high
            last_bar[:low] = low
            last_bar[:close] = close
            last_bar[:volume] = volume
            last_bar[:average] = average
            last_bar[:barCount] = count
          end
        end
      end
      result = array
    end
    return result
  end

  def market_data(contract, version, force_collect=false, db_collect=ENV['db_collect'])
    # contract = "hsi_5mins"
    bar_size = case version
    when "1min"
      "1 min"
    when "5min"
      "5 mins"
    when "30min"
      "30 mins"
    when "15secs"
      "15 secs"
    end
    today_start = Time.zone.now.change({hour: 9, min: 15})
    duration = case version
    when "15secs"
      (db_collect == "true" ? "14400" : 20000.to_s)
    else
      (db_collect == "true" ? "72000" : (Time.zone.now - today_start + 86400).to_i.to_s)
    end
    if duration.to_i > 86400
      duration = "86400"
    end
    result = true
    list = nil
    # Rails.logger.warn "market_data start: #{Time.zone.now}"
    return false if duration.to_i < 0
    begin
      # ib = ib_connect
      # PyCall.exec("from sqlalchemy import create_engine")
      # PyCall.exec("import os,sys")
      # PyCall.exec("import psycopg2")
      # PyCall.exec("import sched, time")
      # if ib.isConnected()
      main_contract = ApplicationController.helpers.ib_main_contract(contract)
      PyCall.exec("contract = #{main_contract}")
      PyCall.exec("t_z=pytz.timezone('Asia/Hong_Kong')")
      # PyCall.exec("bars = ib.reqHistoricalData(contract, endDateTime='#{end_date}', durationStr='#{duration} S', barSizeSetting='#{bar_size}', whatToShow='TRADES', useRTH=False, keepUpToDate=False)")
      PyCall.exec("bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='#{duration} S', barSizeSetting='#{bar_size}', whatToShow='TRADES', useRTH=True, keepUpToDate=True)")
      # PyCall.exec("for i in range(len(bars)): bars[i].date = bars[i].date.astimezone(t_z).replace(tzinfo=None)")
      # PyCall.exec("tmp_table = '#{contract}' + '_tmp'")
      # PyCall.exec("table = '#{contract}'")
      result = PyCall.eval("bars[-1].date == datetime.datetime.now().replace(second=0, microsecond=0)") unless contract == "hsi"

      result = true if force_collect
      # Rails.logger.warn "market_data_latest: #{PyCall.eval("bars[-1].date").to_s} force_collect: #{force_collect.to_s}"

      PyCall.exec("df = util.df(bars)")
      list = PyCall.eval("df")

      #
      # PyCall.exec("engine = create_engine('postgresql+psycopg2://chesp:Chesp2021@postgres.ripple-tech.com:5432/panda_quant',echo=True,client_encoding='utf8')")
      # PyCall.exec("df.tail(2000).to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');")
      # PyCall.exec("conn = psycopg2.connect(host='#{ENV['quant_db_host']}', dbname='#{ENV['quant_db_name']}', user='#{ENV['quant_db_user']}', password='#{ENV['quant_db_pwd']}', port='#{ENV['quant_db_port']}')")
      # PyCall.exec("cur = conn.cursor()")
      # PyCall.exec("sql = 'insert into ' + table + ' select * from ' + tmp_table +  ' b where not exists (select 1 from ' + table + ' a where a.date = b.date);'")
      # PyCall.exec("cur.execute(sql, (10, 1000000, False, False))")
      # PyCall.exec("conn.commit()")
      # PyCall.exec("conn.close()")

      # data = PyCall.eval("list").to_h
      # end
    rescue Exception => e
      error_message = e
      result = false
      Rails.logger.warn "market_data error: #{error_message}, #{Time.zone.now}"
      # ApplicationController.helpers.ib_connect #test
    # ensure
      # ib_disconnect(ib)
    end

    if list.nil? || result == false
      result = false
    else
      return list if db_collect == 'false'

      begin
        tmp_table = contract + '_' + version + '_tmp'
        table = 'hsi_fut'
        json = list.to_dict(orient='records')
        conn = PG.connect(host: ENV['quant_db_host'], dbname: ENV['quant_db_name'], user: ENV['quant_db_user'], password: ENV['quant_db_pwd'], port: ENV['quant_db_port'])
        conn.exec("truncate table #{tmp_table}")

        json.each do |row|
          h = row
          if (h["date"].to_s).to_time.strftime("%H:%M") >= "09:15" && (h["date"].to_s).to_time.strftime("%H:%M") <= "16:30"
            sql = "insert into #{tmp_table} (select 0 as index, '#{h["date"]}' as date, #{h["open"]} as open,#{h["high"]} as high,#{h["low"]} as low,#{h["close"]} as close,#{h["volume"]} as volume,#{h["barCount"]} as barCount,#{h["average"]} as average);"
            conn.exec(sql)
          end
        end

        sql = "delete from " + table + " a where a.date = " + "'#{PyCall.eval("bars[-2].date").to_s}'" + ";"
        res  = conn.exec(sql)

        sql2 = 'insert into ' + table + ' select * from ' + tmp_table +  ' b where not exists (select 1 from ' + table + ' a where a.date = b.date);'
        res  = conn.exec(sql2)

        p "market_data success: #{Time.zone.now}"
      rescue Exception => e
        error_message = e
        result = false
      ensure
        conn.close()
      end
    end

    return result
  end

  def trades_logger(data)
    flag = false
    if data && !data.empty?
      # Rails.logger.warn "ib trades data: #{data}"
      Rails.logger.warn "ib trades got: #{Time.zone.now}"
      data.sort_by { |h| -h[:time] }.reverse.each do |d|
        trade = Trade.find_or_initialize_by(exec_id: d[:exec_id])
        trade.update(perm_id: d[:perm_id], action: d[:action], symbol: d[:symbol],
          last_trade_date_or_contract_month: d[:last_trade_date_or_contract_month],
          currency: d[:currency], shares: d[:shares], price: d[:price], time: Time.at(d[:time]),
          commission: d[:commission], realized_pnl: d[:realized_pnl])
      end
      flag = true
    end
    return flag
  end

end
