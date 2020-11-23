class OrdersJob < ApplicationJob
  queue_as :default

  after_perform :event_log

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    @order = args[0]
    @amount = args[1]
    @move_order = args[2]
    @move_price = args[3]
    # @ib = args[2]

    # @ib = ApplicationController.helpers.ib_connect if @ib.nil?

    @ib = ApplicationController.helpers.ib_connect
    if @ib.isConnected()
      Rails.logger.warn "ib placing order: #{@order}, amount:#{@amount.to_s}, move_order:#{@move_order}, move_price:#{@move_price}"
      # today_pnl = Trade.where("created_at >= ?", Date.today).sum(:realized_pnl)
      # last_pnl = Trade.where("created_at >= ?", Date.today).last
      # if today_pnl >= (ENV["total_asset"].to_i * 0.012) && !last_pnl.nil?
      #   @order, @amount = ApplicationController.helpers.close_position
      # else
        if @order != "" && @amount != 0
          ApplicationController.helpers.ib_order(@order, @amount, 0)
          if @order == "CLOSE"
            privious_move = Action.where.not(price: 0).last(2).first
            ApplicationController.helpers.ib_cancelorder(privious_move.order, privious_move.amount, privious_move.price)
            @order, @amount = ApplicationController.helpers.close_position
          end
        end

        if @move_order != "" && @move_price != 0
          privious_move = Action.where.not(price: 0).last(2).first if Action.where.not(price: 0).last(2).count == 2
          ApplicationController.helpers.ib_cancelorder(privious_move.order, privious_move.amount, privious_move.price) if privious_move
          ApplicationController.helpers.ib_order(@move_order, @amount, @move_price)
        end
      # end
    else
      SmsJob.perform_later ENV["admin_phone"], ENV["superme_user"] + " " + ENV["backtrader_version"], "无法连接"
    end
  end

  private
  # ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()

  def event_log
    EventLog.create(log_type: "ORDER", order_type: @order, content: "#{@order} #{@amount.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if @order != "" && @amount != 0
    EventLog.create(log_type: "MOVE", order_type: @move_order, content: "#{@move_order} #{@move_price.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if @move_order != "" && @move_price != 0

    data = ApplicationController.helpers.ib_trades
    ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()
    if data && !data.empty?
      # Rails.logger.warn "ib trades data: #{data}"
      Rails.logger.warn "ib trades got: #{Time.zone.now}"
      data.sort_by { |h| -h[:time] }.reverse.each do |d|
        trade = Trade.find_or_initialize_by(exec_id: d[:exec_id])
        trade.update(perm_id: d[:perm_id], action: d[:action], symbol: d[:symbol],
          last_trade_date_or_contract_month: d[:last_trade_date_or_contract_month],
          currency: d[:currency], shares: d[:shares], price: d[:price], time: Time.at(d[:time]),
          commission: d[:commission], realized_pnl: d[:realized_pnl])
      end
    end
  end

end
