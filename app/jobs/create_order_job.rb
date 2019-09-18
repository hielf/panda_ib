class CreateOrderJob < ApplicationJob
  queue_as :default

  around_perform :around_order

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    port = args[0]
    # p "port: #{port}"
    order_params = args[1]
    # p "order_params: #{order_params}"
    Rails.logger.warn "job start: #{Time.now}"
    @data = ApplicationController.helpers.client(port, order_params)
  end

  private
    def around_order
      args = self.arguments.second
      order_type = args[0]
      stock_code = args[1][:stock_code]
      amount = args[1][:amount]
      price = args[1][:price]
      trade_order = TradeOrder.new(:stock_code => stock_code, :order_type => order_type, :amount => amount, :price => price, :trade_date => Time.now)
      yield
      begin
        json = eval @data[0]
        trade_order.entrust_no = json[:entrust_no] if !json[:entrust_no].nil? && !json[:entrust_no].empty?
        trade_order.deal
        Rails.logger.warn "job success: #{@data} #{Time.now}"
      rescue Exception => ex
        Rails.logger.warn "job error: #{@data} #{Time.now}"
        message = case @data.class.name
        when "Array"
          @data[0].to_s
        when "String"
          @data
        end
        trade_order.cancel
        trade_order.message = message
      ensure
        trade_order.save!
      end
    end
end

# CreateOrderJob.perform_later perform
