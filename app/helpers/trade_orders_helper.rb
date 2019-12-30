#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
require 'net/ping'
require 'pycall'
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
    clientId = ENV['tws_clientid'].to_i

    begin
      PyCall.exec("from ib_insync import *")
      PyCall.exec("ib = IB()")
      # PyCall.exec("ib.connect('#{ip}', #{port}, clientId=#{clientId}), 5")
      PyCall.exec("ib.connect(host='#{ip}', port=#{port}, clientId=#{clientId}, timeout=5, readonly=False)")
    rescue Exception => e
      error_message = e
      Rails.logger.warn "ib_connect failed: #{error_message}"
    ensure
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
      Rails.logger.warn "ib: #{ib.to_s}"
      # system (`pkill -9 python`) if Rails.env == "production"
      status = true if ib && ib.isConnected() == false
      sleep(0.5)
    end

    return status
  end

  def ib_order(order_type, amount, price)

    # order_type = 'SELL'
    # amount = 2
    # price = 0

    begin
      ib = ib_connect
      order_status = false
      PyCall.exec("hsi = Future('HSI')")
      PyCall.exec("cds = ib.reqContractDetails(hsi)")
      PyCall.exec("contracts = [cd.contract for cd in cds]")

      PyCall.exec("months = []")
      PyCall.exec("for co in contracts: months.append(co.lastTradeDateOrContractMonth)")

      PyCall.exec("months.sort()")
      PyCall.exec("month = months[0]")
      PyCall.exec("hsi = Future(symbol='HSI', lastTradeDateOrContractMonth=month, exchange='HKFE', currency='HKD')")
      PyCall.exec("contract = ib.reqContractDetails(hsi)[0].contract")

      PyCall.exec("order = MarketOrder('#{order_type}', #{amount})")
      PyCall.exec("trade = ib.placeOrder(contract, order)")
      PyCall.exec("trade")
      log = PyCall.eval("trade.log")
      if log.first.status && log.first.status != ''
        order_status = true
      end
    rescue Exception => e
      error_message = e
    ensure
      ib_disconnect(ib)
    end

    return order_status
  end

  def ib_positions
    begin
      ib = ib_connect
      PyCall.exec("pos = ib.positions()")
      PyCall.exec("list = {}")
      PyCall.exec("for po in pos: list.update({'position': po.position, 'currency': po.contract.currency, 'contract_date': po.contract.lastTradeDateOrContractMonth, 'symbol': po.contract.symbol})")
      # PyCall.exec("for po in pos: list.update(symbol: po.symbol, contract_date: po.lastTradeDateOrContractMonth, currency: po.currency, position: po.position)")

      data = PyCall.eval("list").to_h
    rescue Exception => e
      data = false
      error_message = e
    ensure
      ib_disconnect(ib)
    end

    return data
  end

  def ib_trades
    data = []
    begin
      ib = ib_connect
      PyCall.exec("trades = ib.trades()")
      PyCall.exec("print(trades)")
      PyCall.exec("array = []")
      PyCall.exec("list = {}")
      PyCall.exec("for t in trades: array.append(dict({'permId': t.order.permId, 'action': t.order.action, 'symbol': t.contract.symbol, 'lastTradeDateOrContractMonth': t.contract.lastTradeDateOrContractMonth, 'currency': t.contract.currency, 'fills': t.fills}))")
      a = PyCall.eval("array")

      a.each do |d|
        d['fills'].each do |f|
          data << {"perm_id": d['permId'], "action": d['action'], "symbol": d['symbol'],
            "last_trade_date_or_contract_month": d['lastTradeDateOrContractMonth'],
            "currency": d['currency'], "shares": f.execution.shares, "price": f.execution.price,
            "time": f.execution.time.timestamp(), "commission": f.commissionReport.commission,
            "realized_pnl": f.commissionReport.realizedPNL, "exec_id": f.commissionReport.execId}
        end
      end

    rescue Exception => e
      data = false
      error_message = e
    ensure
      ib_disconnect(ib)
    end

    return data
  end

  def ib_account_values
    # [v for v in ib.accountValues() if v.tag == 'NetLiquidationByCurrency' and v.currency == 'BASE']
    begin
      ib = ib_connect
      PyCall.exec("list = [v for v in ib.accountValues()]")

      list = PyCall.eval("list").to_a
    rescue Exception => e
      error_message = e
    ensure
      ib_disconnect(ib)
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

  def market_data(contract)
    # contract = "hsi_5mins"
    bar_size = case contract
    when "hsi"
      "1 min"
    when "hsi_5mins"
      "5 mins"
    when "hsi_30mins"
      "30 mins"
    end
    result = true
    list = nil
    Rails.logger.warn "market_data start: #{Time.zone.now}"
    begin
      ib = ib_connect
      # PyCall.exec("from sqlalchemy import create_engine")
      # PyCall.exec("import os,sys")
      # PyCall.exec("import psycopg2")
      # PyCall.exec("import sched, time")
      if ib.isConnected()
        PyCall.exec("contracts = [Index(symbol = 'HSI', exchange = 'HKFE')]")
        PyCall.exec("contract = contracts[0]")
        PyCall.exec("bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='7200 S', barSizeSetting='#{bar_size}', whatToShow='TRADES', useRTH=True)")
        # PyCall.exec("tmp_table = '#{contract}' + '_tmp'")
        # PyCall.exec("table = '#{contract}'")
        PyCall.exec("df = util.df(bars)")

        list = PyCall.eval("df")
        #
        # PyCall.exec("engine = create_engine('postgresql+psycopg2://chesp:Chesp92J5@rm-2zelv192ymyi9680vo.pg.rds.aliyuncs.com:3432/panda_quant',echo=True,client_encoding='utf8')")
        # PyCall.exec("df.tail(2000).to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');")
        # PyCall.exec("conn = psycopg2.connect(host='rm-2zelv192ymyi9680vo.pg.rds.aliyuncs.com', dbname='panda_quant', user='chesp', password='Chesp92J5', port='3432')")
        # PyCall.exec("cur = conn.cursor()")
        # PyCall.exec("sql = 'insert into ' + table + ' select * from ' + tmp_table +  ' b where not exists (select 1 from ' + table + ' a where a.date = b.date);'")
        # PyCall.exec("cur.execute(sql, (10, 1000000, False, False))")
        # PyCall.exec("conn.commit()")
        # PyCall.exec("conn.close()")

        # data = PyCall.eval("list").to_h
      end
    rescue Exception => e
      error_message = e
      result = false
      Rails.logger.warn "market_data error: #{error_message}, #{Time.zone.now}"
    ensure
      ib_disconnect(ib)
    end

    if list.nil? || result == false
      result = false
    else
      begin
        tmp_table = contract + '_tmp'
        table = contract
        json = list.to_dict(orient='records')
        conn = PG.connect(host: ENV['quant_db_host'], dbname: ENV['quant_db_name'], user: ENV['quant_db_user'], password: ENV['quant_db_pwd'], port: ENV['quant_db_port'])
        conn.exec("truncate table #{tmp_table}")
        conn.prepare('statement2', "insert into #{tmp_table} values ($1, $2, $3, $4, $5, $6, $7, $8, $9)")
        json.each_with_index do |row, index|
          conn.exec_prepared('statement2', [index, row['date'].strftime('%Y-%m-%d %H:%M:%S'), row['open'], row['high'], row['low'], row['close'], row['volume'], row['average'], row['barCount']])
        end;0

        sql = 'insert into ' + table + ' select * from ' + tmp_table +  ' b where not exists (select 1 from ' + table + ' a where a.date = b.date);'
        res  = conn.exec(sql)

        Rails.logger.warn "market_data success: #{Time.zone.now}"
      rescue Exception => e
        error_message = e
        result = false
      ensure
        conn.close()
      end
    end

    return result
  end

end
