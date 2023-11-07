class RisksJob < ApplicationJob
  queue_as :risks

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    @contract = args[0]
    @version = args[1]

    loss_limit = ENV["total_asset"].to_f * 0.0025 * -1
    loss_limit_total = ENV["total_asset"].to_f * 0.005 * -1
    last_trade = Trade.last
    position = TraderPosition.init(@contract).position
    last_action = Action.last
    if (last_action.order == "CLOSE" && position != 0) || (last_action.order != "CLOSE" && last_trade.action != last_action.order)
      Rails.logger.warn "find: last_action #{last_action.order}, position #{position}, last_trade #{last_trade}"
      job1 = PositionsJob.perform_now(@contract, @version)
      job2 = TradesJob.perform_now(@contract, @version)

      last_trade = Trade.last
      position = TraderPosition.init(@contract).position
    end

    current_time = Time.zone.now.strftime('%H:%M')
    @order = ""
    if ((current_time >= "09:15" && current_time <= "12:00") || (current_time >= "13:00" && current_time <= "16:30") || (current_time >= "17:15" && current_time <= "23:59") || (current_time >= "00:00" && current_time <= "03:00"))
      csv = Rails.root.to_s + "/tmp/csv/#{@contract}_#{@version}.csv"
      @market_data = []
      CSV.foreach(csv, headers: true) do |row|
        @market_data << row.to_hash
      end
      @close_flag = false

      if position == 0
        ProfitLoss.where(current: true).update_all(current: false)
      end

      if last_trade && last_trade.realized_pnl == 0 && position != 0 && !@market_data.empty?
        return if @market_data.last["date"].to_time.beginning_of_minute != Time.zone.now.beginning_of_minute

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
          ProfitLoss.create(open: last_trade.price, close: close, unrealized_pnl: unrealized_pnl) if unrealized_pnl
        rescue Exception => e
          Rails.logger.warn "profit_losses create error: #{e}"
        end

        # Rails.logger.warn "ib risk loss_limit: #{loss_limit}, position: #{last_trade.action}, open: #{last_trade.price}, close: #{close}, unrealized_pnl: #{unrealized_pnl}, #{Time.zone.now}" if unrealized_pnl != 0

        @profit_losses = ProfitLoss.latest(1000)
        pnls = @profit_losses.to_a.map{|pr| pr.unrealized_pnl}
        # pnls_detail = @profit_losses.to_a.map.with_index(1) {|pr,index| [pr.unrealized_pnl, index]}
        # realized_pnl = Trade.where("time >= ? AND perm_id = ?", Date.today, last_trade.perm_id).sum(:realized_pnl)

        # profit getting back
        if !pnls.empty? && pnls.max > 0 && (pnls.first < pnls.max && pnls.first >= pnls.max * 0.8)
          if pnls.find_index(pnls.max) >= 3
            @amount = position
            @order = "CLOSE"
            begin
              Action.act(@order, @amount, 0, Time.zone.now) if @order != ""
            rescue Exception => e
              Rails.logger.warn "Action create error: #{e}"
            ensure
              OrdersJob.set(wait: 2.seconds).perform_later("CLOSE", @amount, "", 0)
              @close_flag = true
            end

            begin
              EventLog.find_or_create_by(log_type: "BENEFIT", order_type: @order, content: "BENEFIT unrealized_pnl: #{unrealized_pnl} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if @order != "" && @amount != 0
            rescue Exception => e
              Rails.logger.warn "EventLog create error: #{e}"
            end
          end
        end

        # profit limit
        if !pnls.empty? && pnls.max >= ENV["total_asset"].to_f * 0.05
          @amount = position
          @order = "CLOSE"
          begin
            Action.act(@order, @amount, 0, Time.zone.now) if @order != ""
          rescue Exception => e
            Rails.logger.warn "Action create error: #{e}"
          ensure
            OrdersJob.set(wait: 2.seconds).perform_later("CLOSE", @amount, "", 0)
            @close_flag = true
          end

          begin
            EventLog.find_or_create_by(log_type: "BENEFIT", order_type: @order, content: "BENEFIT unrealized_pnl: #{unrealized_pnl} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if @order != "" && @amount != 0
          rescue Exception => e
            Rails.logger.warn "EventLog create error: #{e}"
          end
        end

        # start with under 0
        if !pnls.empty? && pnls.last < 0
          @amount = position
          @order = "CLOSE"
          begin
            Action.act(@order, @amount, 0, Time.zone.now) if @order != ""
          rescue Exception => e
            Rails.logger.warn "Action create error: #{e}"
          ensure
            OrdersJob.set(wait: 2.seconds).perform_later("CLOSE", @amount, "", 0)
            @close_flag = true
          end

          begin
            EventLog.find_or_create_by(log_type: "BENEFIT", order_type: @order, content: "BENEFIT unrealized_pnl: #{unrealized_pnl} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if @order != "" && @amount != 0
          rescue Exception => e
            Rails.logger.warn "EventLog create error: #{e}"
          end
        end

        # achieve loss limit
        if unrealized_pnl < loss_limit
          @amount = position
          @order = "CLOSE"
          begin
            Action.act(@order, @amount, 0, Time.zone.now) if @order != ""
          rescue Exception => e
            Rails.logger.warn "Action create error: #{e}"
          ensure
            OrdersJob.set(wait: 2.seconds).perform_later("CLOSE", @amount, "", 0)
            @close_flag = true
          end

          begin
            EventLog.find_or_create_by(log_type: "RISK", order_type: @order, content: "RISK unrealized_pnl: #{unrealized_pnl} CLOSE #{@order} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if @order != "" && @amount != 0
          rescue Exception => e
            Rails.logger.warn "EventLog create error: #{e}"
          end
        end
      end

      today_pnl = Trade.today_pnl
      if today_pnl < loss_limit_total
        @amount = position
        @order = "CLOSE"
        begin
          Action.act(@order, @amount, 0, Time.zone.now) if @order != ""
        rescue Exception => e
          Rails.logger.warn "Action create error: #{e}"
        ensure
          OrdersJob.set(wait: 2.seconds).perform_later("CLOSE", @amount, "", 0)
          @close_flag = true
        end

        begin
          last_stop = EventLog.where(log_type: "STOP").last
          if last_stop && last_stop.created_at.to_date != Date.today
            EventLog.find_or_create_by(log_type: "STOP", order_type: @order, content: "STOP today_pnl: #{today_pnl} CLOSE at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}")
          end
        rescue Exception => e
          Rails.logger.warn "EventLog create error: #{e}"
        end
      end
    end
  end

  private
  def around_check
    begin
      # if @profit_losses && @profit_losses.count == 4
      #   @profit_losses.last.update(current: false)
      # end
      if @close_flag && @profit_losses && !@profit_losses.empty?
        @profit_losses.update(current: false)
      end
    rescue Exception => e
      Rails.logger.warn "profit_losses error: #{e}"
    end
  end

end
