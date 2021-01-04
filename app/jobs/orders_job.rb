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

    @contract = case ENV['backtrader_version']
    when '15sec'
      'hsi_15secs'
    else
      'hsi'
    end
    # @ib = args[2]

    # @ib = ApplicationController.helpers.ib_connect if @ib.nil?

    @ib = ApplicationController.helpers.ib_connect
    if @ib.isConnected()
      Rails.logger.warn "ib placing order: #{@order}, amount:#{@amount.to_s}, move_order:#{@move_order}, move_price:#{@move_price}"

      if @order != "" && @amount != 0 && @order != "CLOSE"
        ApplicationController.helpers.ib_order(@order, @amount, 0)
      elsif @order == "CLOSE"
        @order, @amount = ApplicationController.helpers.close_position
      end
    else
      SmsJob.perform_later ENV["admin_phone"], ENV["superme_user"] + " " + ENV["backtrader_version"], "无法连接"
    end
    ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()
  end

  private
  def event_log
    EventLog.create(log_type: "ORDER", order_type: @order, content: "#{@order} #{@amount.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if @order != "" && @amount != 0
    EventLog.create(log_type: "MOVE", order_type: @move_order, content: "#{@move_order} #{@move_price.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if @move_order != "" && @move_price != 0

    PositionsJob.set(wait: 6.seconds).perform_later(@contract)
    TradesJob.set(wait: 15.seconds).perform_later(@contract)
  end

end
