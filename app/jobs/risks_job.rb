class RisksJob < ApplicationJob
  queue_as :low_priority

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    @ib = args[0]
    @contract = args[1]

    current_time = Time.zone.now.strftime('%H:%M')
    @order = ""
    if (current_time > "09:45" && current_time < "12:00") || (current_time > "13:00" && current_time < "15:45")
      # @ib = ApplicationController.helpers.ib_connect
      # Rails.logger.warn "ib risk start: #{@ib}"
      if @ib.isConnected()
        # Rails.logger.warn "#{@ib}"
        loss_limit = ENV["total_asset"].to_f * 0.001 * -1
        @market_data = ApplicationController.helpers.market_data(@contract, true) unless ENV["remote_index"] == "true"
        trades = ApplicationController.helpers.ib_trades
        last_trade = trades.sort_by { |h| -h[:time] }.reverse.last

        if last_trade.nil?
          ProfitLoss.where(current: true).update_all(current: false)
        end

        if last_trade && @market_data
          @order = last_trade[:action]
          close = @market_data.iloc[-1].close
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
            ProfitLoss.create(open: last_trade[:price], close: close, unrealized_pnl: unrealized_pnl) if unrealized_pnl != 0
          rescue Exception => e
            Rails.logger.warn "profit_losses create error: #{e}"
          end

          Rails.logger.warn "ib risk loss_limit: #{loss_limit}, position: #{last_trade[:action]}, open: #{last_trade[:price]}, close: #{close}, unrealized_pnl: #{unrealized_pnl}, #{Time.zone.now}" if unrealized_pnl != 0

          @profit_losses = ProfitLoss.latest(4)
          pnls = @profit_losses.to_a.map{|pr| pr.unrealized_pnl}

          if unrealized_pnl.to_f < loss_limit.to_f
            order, amount = ApplicationController.helpers.close_position
            begin
              EventLog.create(log_type: "RISK", order_type: @order, content: "RISK unrealized_pnl: #{unrealized_pnl} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if order != "" && amount != 0
            rescue Exception => e
              Rails.logger.warn "EventLog create error: #{e}"
            end
          end

          if pnls && pnls.length >= 3 && ENV["backtrader_version"] != "5min"
            if pnls[0] > 0 && pnls[0] < pnls[1] && pnls[1] < pnls[2]
              order, amount = ApplicationController.helpers.close_position
              begin
                EventLog.create(log_type: "RISK", order_type: @order, content: "RISK profit: #{pnls[0]} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if order != "" && amount != 0
              rescue Exception => e
                Rails.logger.warn "EventLog create error: #{e}"
              end
            end
          end

        end
      end
    end
  end

  private
  def around_check
    # ApplicationController.helpers.ib_disconnect(@ib) if @ib
    file = ApplicationController.helpers.index_to_csv(@contract, @market_data, true) if @market_data
    begin
      if @profit_losses && @profit_losses.count == 4
        @profit_losses.last.update(current: false)
      end
    rescue Exception => e
      Rails.logger.warn "profit_losses error: #{e}"
    end
  end

end
