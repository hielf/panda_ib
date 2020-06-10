class OrdersJob < ApplicationJob
  queue_as :default

  after_perform :event_log

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    @order = args[0]
    @amount = args[1]

    if @order != "" && @amount != 0
      ApplicationController.helpers.ib_order(@order, @amount, 0)
      ApplicationController.helpers.close_position if @order == "CLOSE"
    end
  end

  private
  def event_log
    EventLog.create(log_type: "ORDER", order_type: @order, content: "#{@order} #{@amount.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}") if @order != "" && @amount != 0
  end

end
