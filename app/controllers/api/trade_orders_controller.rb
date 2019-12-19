class Api::TradeOrdersController < Api::ApplicationController
  skip_before_action :authenticate_user!
  include TradeOrdersHelper
  include ContractsHelper

  def test
    contract = "hsi"
    result = [0, '成功']
    TradersJob.perform_now contract
    render_json(result)
  end

  def order
    m_requires! [:order_type, :amount, :price, :rand_code]
    result = [0, '下单成功']

    order_type = params[:order_type].upcase
    amount = params[:amount].to_i
    price = params[:price].to_f
    rand_code = params[:rand_code]

    begin
      p [order_type, amount, price]
      helpers.ib_order(order_type, amount, price)
    rescue Exception => e
      result = [1, '下单失败']
    end
    render_json(result)
  end

  def positions
    begin
      data = helpers.ib_positions
      result = [0, "success", data]
    rescue Exception => e
      result = [1, e.value.to_s]
    end
    render_json(result)
  end

  def account_values
    begin
      data = helpers.ib_account_values
      result = [0, "success", data]
    rescue Exception => e
      result = [1, e.value.to_s]
    end
    render_json(result)
  end

  def contract_data
    contract = params[:contract]
    result = [1, 'failed']
    if contract
      data = helpers.market_data(contract)
      result = [0, 'success'] if data
    end
    render_json(result)
  end

  def trades_data
    if request.format.csv?
      contract = "hsi"
      trades_to_csv(contract)
      file = Rails.root.to_s + "/tmp/csv/trades_#{contract}.csv"
      send_data(File.read(file), type: "application/csv", disposition:  "attachment", filename: "trades_#{contract}.csv")
    else
      result = [1, 'failed']
      data = ib_trades
      if !data.empty?
        data.sort_by { |h| -h[:time] }.reverse.each do |d|
          trade = Trade.find_or_initialize_by(exec_id: d[:exec_id])
          trade.update(perm_id: d[:perm_id], action: d[:action], symbol: d[:symbol],
            last_trade_date_or_contract_month: d[:last_trade_date_or_contract_month],
            currency: d[:currency], shares: d[:shares], price: d[:price], time: Time.at(d[:time]),
            commission: d[:commission], realized_pnl: d[:realized_pnl])
        end

        result = [0, 'success', data]
      end
      render_json(result)
    end
  end

  def position_check
    type = params[:type]
    contract = params[:contract]
    result = [0, 'success']

    Rails.logger.warn "contract & type: #{contract}, #{type}"
    if contract
      file = index_to_csv(contract)
      data = online_data(file)
    end

    if !type.nil? && type == "check" && data
      check_position(data)
    else
      close_position
    end

    render_json(result)
  end

  private

  def trade_order_params
    params.permit(:order_type, :amount, :price, :rand_code)
  end
end
