class RisksJob < ApplicationJob
  queue_as :low_priority

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]
    # sleep 15
    Rails.logger.warn "ib risk start: #{contract}, #{Time.zone.now}"
    @ib = ApplicationController.helpers.ib_connect
    if @ib.isConnected()
      loss_limit = ENV["total_asset"].to_f * 0.001 * -1
      market_data = ApplicationController.helpers.market_data(contract, true)
      trades = ApplicationController.helpers.ib_trades
      last_trade = trades.sort_by { |h| -h[:time] }.reverse.last
      close = market_data.iloc[-1].close
      unrealizedPNL = 0
      if last_trade[:realized_pnl] == 0
        case last_trade[:action]
        when "BUY"
          unrealizedPNL = (close - last_trade[:price]) * ENV['amount'].to_i * 50
        when "SELL"
          unrealizedPNL = -1 * (close - last_trade[:price]) * ENV['amount'].to_i * 50
        end
      end
      Rails.logger.warn "ib risk position: #{last_trade[:action]}, open: #{last_trade[:price]}, close: #{close}, current unrealizedPNL: #{unrealizedPNL}, #{Time.zone.now}" if unrealizedPNL != 0

      if unrealizedPNL < 0 && unrealizedPNL < loss_limit
        Rails.logger.warn "ib risk loss_limit: #{loss_limit} alert: #{unrealizedPNL}, #{Time.zone.now}"
        ApplicationController.helpers.close_position
      end
    end
  end

  private
  def around_check
    ApplicationController.helpers.ib_disconnect(@ib)
  end

end
