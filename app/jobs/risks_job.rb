class RisksJob < ApplicationJob
  queue_as :risks

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    @contract = args[0]
    @version = args[1]

    current_time = Time.zone.now.strftime('%H:%M')
    @order = ""
    if (current_time >= "09:15" && current_time <= "12:00") || (current_time >= "13:00" && current_time <= "16:30")
      loss_limit = ENV["total_asset"].to_f * 0.006 * -1
      loss_limit_total = ENV["total_asset"].to_f * 0.01 * -1
      last_trade = Trade.last
      position = TraderPosition.init(@contract).position
      csv = Rails.root.to_s + "/tmp/csv/#{@contract}_#{@version}.csv"
      @market_data = []
      CSV.foreach(csv, headers: true) do |row|
        @market_data << row.to_hash
      end

      if position == 0
        ProfitLoss.where(current: true).update_all(current: false)
      end

      if last_trade && last_trade.realized_pnl == 0 && position != 0 && !@market_data.empty?
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

        @profit_losses = ProfitLoss.latest(4)
        pnls = @profit_losses.to_a.map{|pr| pr.unrealized_pnl}
        # realized_pnl = Trade.where("time >= ? AND perm_id = ?", Date.today, last_trade.perm_id).sum(:realized_pnl)
        if unrealized_pnl < loss_limit
          amount = position
          order = "CLOSE"
          begin
            Action.act(order, amount, 0, Time.zone.now) if order != ""
          rescue Exception => e
            Rails.logger.warn "Action create error: #{e}"
          end

          OrdersJob.set(wait: 2.seconds).perform_later("CLOSE", amount, "", 0)
          begin
            EventLog.create(log_type: "RISK", order_type: @order, content: "RISK unrealized_pnl: #{unrealized_pnl} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if order != "" && amount != 0
          rescue Exception => e
            Rails.logger.warn "EventLog create error: #{e}"
          end
        end

        today_pnl = Trade.today_pnl
        if today_pnl < loss_limit_total
          amount = position
          order = "CLOSE"
          begin
            Action.act(order, amount, 0, Time.zone.now) if order != ""
          rescue Exception => e
            Rails.logger.warn "Action create error: #{e}"
          end

          OrdersJob.set(wait: 2.seconds).perform_later("CLOSE", amount, "", 0)
          begin
            EventLog.create(log_type: "STOP", order_type: @order, content: "STOP today_pnl: #{today_pnl} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if order != "" && amount != 0
          rescue Exception => e
            Rails.logger.warn "EventLog create error: #{e}"
          end
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
