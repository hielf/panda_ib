class RisksJob < ApplicationJob
  queue_as :risks

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    @contract = args[0]

    current_time = Time.zone.now.strftime('%H:%M')
    @order = ""
    if (current_time >= "09:15" && current_time <= "12:00") || (current_time >= "13:00" && current_time <= "16:30")
      loss_limit = ENV["total_asset"].to_f * 0.006 * -1
      last_trade = Trade.last
      position = TraderPosition.find_or_initialize_by(contract: @contract).position
      csv = Rails.root.to_s + "/tmp/csv/#{@contract}.csv"
      @market_data = []
      CSV.foreach(csv, headers: true) do |row|
        @market_data << row.to_hash
      end


      if position == 0
        ProfitLoss.where(current: true).update_all(current: false)
      end

      if last_trade && last_trade.realized_pnl == 0 && position != 0 && @market_data
        return if @market_data.last["date"].to_time != Time.zone.now.beginning_of_minute

        @order = last_trade.action
        close = @market_data.last["close"].to_f
        unrealized_pnl = 0
        case last_trade.action
        when "BUY"
          unrealized_pnl = (close - last_trade.price) * ENV['amount'].to_i * 50
        when "SELL"
          unrealized_pnl = -1 * (close - last_trade.price) * ENV['amount'].to_i * 50
        end

        begin
          ProfitLoss.create(open: last_trade.price, close: close, unrealized_pnl: unrealized_pnl) if unrealized_pnl != 0
        rescue Exception => e
          Rails.logger.warn "profit_losses create error: #{e}"
        end

        # Rails.logger.warn "ib risk loss_limit: #{loss_limit}, position: #{last_trade.action}, open: #{last_trade.price}, close: #{close}, unrealized_pnl: #{unrealized_pnl}, #{Time.zone.now}" if unrealized_pnl != 0

        if ENV['backtrader_version'] != "15sec"
          @profit_losses = ProfitLoss.latest(4)
          pnls = @profit_losses.to_a.map{|pr| pr.unrealized_pnl}
          # realized_pnl = Trade.where("time >= ? AND perm_id = ?", Date.today, last_trade.perm_id).sum(:realized_pnl)
          if unrealized_pnl < loss_limit
            amount = position
            order = "CLOSE"
            OrdersJob.set(wait: 2.seconds).perform_later("CLOSE", amount, "", 0)
            begin
              EventLog.create(log_type: "RISK", order_type: @order, content: "RISK unrealized_pnl: #{unrealized_pnl} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if order != "" && amount != 0
            rescue Exception => e
              Rails.logger.warn "EventLog create error: #{e}"
            end
          end

          # if unrealized_pnl.to_f < loss_limit.to_f
          #   amount = position
          #   order = "CLOSE"
          #   OrdersJob.perform_later("CLOSE", amount, "", 0)
          #   begin
          #     EventLog.create(log_type: "RISK", order_type: @order, content: "RISK unrealized_pnl: #{unrealized_pnl} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if order != "" && amount != 0
          #   rescue Exception => e
          #     Rails.logger.warn "EventLog create error: #{e}"
          #   end
          # end

          # if pnls && pnls.length >= 3 && ENV["backtrader_version"] != "5min"
          #   if pnls[0] > 0 && pnls[0] < pnls[1] && pnls[1] < pnls[2]
          #     order, amount = ApplicationController.helpers.close_position
          #     begin
          #       EventLog.create(log_type: "RISK", order_type: @order, content: "RISK profit: #{pnls[0]} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if order != "" && amount != 0
          #     rescue Exception => e
          #       Rails.logger.warn "EventLog create error: #{e}"
          #     end
          #   end
          # end
        end
      end
    end
  end

  private
  def around_check
    begin
      if @profit_losses && @profit_losses.count == 4
        @profit_losses.last.update(current: false)
      end
    rescue Exception => e
      Rails.logger.warn "profit_losses error: #{e}"
    end
  end

end
