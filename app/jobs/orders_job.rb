class OrdersJob < ApplicationJob
  queue_as :default

  after_perform :event_log

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    @order = args[0]
    @amount = args[1]
    # @ib = args[2]

    # @ib = ApplicationController.helpers.ib_connect if @ib.nil?

    @ib = ApplicationController.helpers.ib_connect
    if @ib.isConnected()
      today_pnl = Trade.where("created_at >= ?", Date.today).sum(:realized_pnl)
      last_pnl = Trade.where("created_at >= ?", Date.today).last.realized_pnl
      if today_pnl >= (ENV["total_asset"].to_i * 0.12) && last_pnl == 0
        @order, @amount = ApplicationController.helpers.close_position
        break
      end
      if @order != "" && @amount != 0
        ApplicationController.helpers.ib_order(@order, @amount, 0)
        @order, @amount = ApplicationController.helpers.close_position if @order == "CLOSE"
      end
    else
      SmsJob.perform_later ENV["admin_phone"], ENV["superme_user"] + " " + ENV["backtrader_version"], "无法连接"
    end
  end

  private
  # ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()

  def event_log
    EventLog.create(log_type: "ORDER", order_type: @order, content: "#{@order} #{@amount.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if @order != "" && @amount != 0

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
