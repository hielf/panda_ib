class Api::TradeOrdersController < Api::ApplicationController
  skip_before_action :authenticate_user!
  include TradeOrdersHelper
  include ContractsHelper

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

  def check_position
    result = [0, 'success']
    Rails.logger.warn "ib hsi_5mins: market_data start"
    contract = "hsi_5mins"
    market_data(contract)
    Rails.logger.warn "ib hsi_5mins: market_data done"
    file = index_to_csv(contract)
    Rails.logger.warn "ib hsi_5mins: file done"
    data = online_data(file)
    Rails.logger.warn "ib hsi_5mins: data #{data.to_s}"
    check_position(data)
    Rails.logger.warn "ib hsi_5mins: check_position done"

    render_json(result)
  end

  private

  def trade_order_params
    params.permit(:order_type, :amount, :price, :rand_code)
  end
end
