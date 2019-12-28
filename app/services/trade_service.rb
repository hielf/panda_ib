require 'net/ping'
require 'pycall/import'
include PyCall::Import

class TradeService
  def initialize(params)
    @contract = params[:contract]
  end

  def check
    Rails.logger.warn "ib service start: #{@contract}, #{Time.zone.now}"

    # contract = "hsi_5mins"
    bar_size = case @contract
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
      error_message = e.value.to_s
      result = false
      Rails.logger.warn "market_data error: #{Time.zone.now}"
    ensure
      ib_disconnect(ib)
    end

    if list.nil? || result == false
      result = false
    else
      begin
        tmp_table = @contract + '_tmp'
        table = @contract
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
        error_message = e.value.to_s
        result = false
      ensure
        conn.close()
      end
    end

    # TradersJob.perform_now @contract

    # market_data = ApplicationController.helpers.market_data(@contract)
    # if market_data
    #   file = ApplicationController.helpers.index_to_csv(@contract)
    #   data = ApplicationController.helpers.online_data(file)
    #   if data && !data.empty?
    #     current_time = Time.zone.now.strftime('%H:%M')
    #     if (current_time > "09:15" && current_time < "12:00") || (current_time > "13:00" && current_time < "15:30")
    #       ApplicationController.helpers.check_position(data)
    #     else
    #       ApplicationController.helpers.close_position
    #     end
    #   end
    # end
    Rails.logger.warn "ib service end: #{@contract}, #{Time.zone.now}"
  end

private
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
      error_message = e.value.to_s
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
      error_message = e.value.to_s
    ensure
      # system (`pkill -9 python`) if Rails.env == "production"
      status = true if ib && ib.isConnected() == false
      sleep(0.5)
    end

    return status
  end

end

# TradeService.new(params).check
