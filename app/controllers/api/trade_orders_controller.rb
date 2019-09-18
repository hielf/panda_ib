include PyCall::Import
class Api::TradeOrdersController < Api::ApplicationController
  skip_before_action :authenticate_user!

  private

  def trade_order_params
    params.permit(:capital_account, :stock_code, :order_type, :amount, :price)
  end
end
