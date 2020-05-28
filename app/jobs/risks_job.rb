class RisksJob < ApplicationJob
  queue_as :low_priority

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]

    current_time = Time.zone.now.strftime('%H:%M')
    if (current_time > "09:45" && current_time < "12:00") || (current_time > "13:00" && current_time < "15:45")
      @ib = ApplicationController.helpers.ib_connect
      if @ib.isConnected()
        loss_limit = ENV["total_asset"].to_f * 0.001 * -1
        market_data = ApplicationController.helpers.market_data(contract, true)
        trades = ApplicationController.helpers.ib_trades
        last_trade = trades.sort_by { |h| -h[:time] }.reverse.last
        if last_trade && market_data
          close = market_data.iloc[-1].close
          unrealized_pnl = 0
          if last_trade[:realized_pnl] == 0
            case last_trade[:action]
            when "BUY"
              unrealized_pnl = (close - last_trade[:price]) * ENV['amount'].to_i * 50
            when "SELL"
              unrealized_pnl = -1 * (close - last_trade[:price]) * ENV['amount'].to_i * 50
            end
          end
          Rails.logger.warn "ib risk loss_limit: #{loss_limit}, position: #{last_trade[:action]}, open: #{last_trade[:price]}, close: #{close}, unrealized_pnl: #{unrealized_pnl}, #{Time.zone.now}" if unrealized_pnl != 0

          if unrealized_pnl.to_f < loss_limit.to_f
            ApplicationController.helpers.close_position
          end
        end
      end
    end
  end

  private
  def around_check
    ApplicationController.helpers.ib_disconnect(@ib)
  end

end
