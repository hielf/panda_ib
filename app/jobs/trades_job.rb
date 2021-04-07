class TradesJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  def perform(*args)
    contract = args[0]
    version = args[1]

    @ib = ApplicationController.helpers.ib_connect
    if @ib.isConnected()
      data = ApplicationController.helpers.ib_trades(contract)
      ApplicationController.helpers.trades_logger(data)
    end
  end

  private
  def around_check
    ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()
  end

end
