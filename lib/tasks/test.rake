namespace :ib do
  task :test => :environment do

    require 'net/ping'
    require 'pycall/import'
    include PyCall::Import

    Rails.logger.warn "ib test: start"
    # url = "http://127.0.0.1/api/trade_orders/test"
    # res = HTTParty.get(url)

    # contract = "hsi"
    # TradersJob.perform_now contract


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
        error_message = e.value.to_s
      ensure
        ib_disconnect(ib)
      end

      return data
    end

    data = ib_trades
    Rails.logger.warn "ib test data: #{data}"

    Rails.logger.warn "ib test: end"

  end
end
