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
        @profit_losses = ProfitLoss.latest(4)
        @order = last_trade[:action]

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

          begin
            ProfitLoss.create(open: last_trade[:price], close: close, unrealized_pnl: unrealized_pnl)
          rescue Exception => e
            Rails.logger.warn "profit_losses create error: #{e}"
          end

          Rails.logger.warn "ib risk loss_limit: #{loss_limit}, position: #{last_trade[:action]}, open: #{last_trade[:price]}, close: #{close}, unrealized_pnl: #{unrealized_pnl}, #{Time.zone.now}" if unrealized_pnl != 0

          pnls = @profit_losses.to_a.map{|pr| pr.unrealized_pnl}

          if pnls.length >= 3
            if pnls[0] < pnls[1] && pnls[1] < pnls[2]
              ApplicationController.helpers.close_position
              begin
                EventLog.create(content: "RISK CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}")
              rescue Exception => e
                Rails.logger.warn "EventLog create error: #{e}"
              end
            end
          end

          if unrealized_pnl.to_f < loss_limit.to_f
            ApplicationController.helpers.close_position
            begin
              EventLog.create(content: "RISK CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}")
            rescue Exception => e
              Rails.logger.warn "EventLog create error: #{e}"
            end
          end

        end
      end
    end
  end

  private
  def around_check
    ApplicationController.helpers.ib_disconnect(@ib)

    begin
      if @profit_losses.count == 4
        @profit_losses.last.update(current: false)
      end
    rescue Exception => e
      Rails.logger.warn "profit_losses error: #{e}"
    end
  end

end
