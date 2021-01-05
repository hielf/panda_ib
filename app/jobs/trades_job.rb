class TradesJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  def perform(*args)
    contract = args[0]
    @ib_trade = ApplicationController.helpers.ib_connect
    if @ib_trade.isConnected()
      data = ApplicationController.helpers.ib_trades
      ApplicationController.helpers.trades_logger(data)
    end
  end

  private
  def around_check
    ApplicationController.helpers.ib_disconnect(@ib_trade) if @ib_trade.isConnected()
  end

end
