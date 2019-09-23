#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
require 'net/ping'
require 'pycall/import'
include PyCall::Import

# pyimport 'ib_insync'
# pyimport 'sys'
# pyimport 'json'
# pyimport 'math'
# pyfrom 'ib_insync', import: :IB

module TradeOrdersHelper

  def ib_connect
    ip = ENV['tws_ip'] #PyCall.eval("str('127.0.0.1')")
    port = ENV['tws_port'].to_i
    clientId = ENV['tws_clientid'].to_i

    begin
      PyCall.exec("from ib_insync import *")
      PyCall.exec("ib = IB()")
      PyCall.exec("ib.connect('#{ip}', #{port}, clientId=#{clientId})")
    rescue Exception => e
      error_message = e.value.to_s
    ensure
      ib = PyCall.eval("ib")
    end

    return ib
  end

  def ib_disconnect(ib)
    PyCall.exec("ib.disconnect()")

    return true
  end

  def ib_order(order_type, amount, price)

    # order_type = 'SELL'
    # amount = 2
    # price = 0

    begin
      ib = ib_connect
      order_status = false
      PyCall.exec("from ib_insync import *")
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
      error_message = e.value.to_s
    ensure
      ib_disconnect(ib)
    end

    return order_status
  end

  def ib_positions
    begin
      ib = ib_connect
      PyCall.exec("from ib_insync import *")
      PyCall.exec("pos = ib.positions()")
      PyCall.exec("list = {}")
      PyCall.exec("for po in pos: list.update({'position': po.position, 'currency': po.contract.currency, 'contract_date': po.contract.lastTradeDateOrContractMonth, 'symbol': po.contract.symbol})")
      # PyCall.exec("for po in pos: list.update(symbol: po.symbol, contract_date: po.lastTradeDateOrContractMonth, currency: po.currency, position: po.position)")

      data = PyCall.eval("list").to_h
    rescue Exception => e
      error_message = e.value.to_s
    ensure
      ib_disconnect(ib)
    end

    return data
  end

  def ib_account_values
    # [v for v in ib.accountValues() if v.tag == 'NetLiquidationByCurrency' and v.currency == 'BASE']
    begin
      ib = ib_connect
      PyCall.exec("from ib_insync import *")
      PyCall.exec("list = [v for v in ib.accountValues() if v.tag == 'NetLiquidationByCurrency']")

      list = PyCall.eval("list").to_a
    rescue Exception => e
      error_message = e.value.to_s
    ensure
      ib_disconnect(ib)
    end

    data = {}

    if list
      list.each_with_index do |value, index|
        data["account_" + index.to_s] = {}
        data["account_" + index.to_s]["value"] = value.value
        data["account_" + index.to_s]["currency"] = value.currency
      end
    end

    return data
  end

end
