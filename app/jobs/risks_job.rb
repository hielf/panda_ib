class RisksJob < ApplicationJob
  queue_as :low_priority

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]
    # sleep 15
    Rails.logger.warn "ib risk start: #{contract}, #{Time.zone.now}"
    @ib = ApplicationController.helpers.ib_connect
    if @ib.isConnected()
      market_data = ApplicationController.helpers.market_data(contract, true)
      close = market_data.iloc[-1].close
      last_trade = Trade.last
      unrealizedPNL = 0
      if last_trade.realized_pnl == 0
        case last_trade.action
        when "BUY"
          unrealizedPNL = (close - last_trade.price) * ENV['amount'].to_i * 50
        when "SELL"
          unrealizedPNL = -1 * (close - last_trade.price) * ENV['amount'].to_i * 50
        end
      end
      Rails.logger.warn "ib risk position: #{last_trade.action}, open: #{last_trade.price}, close: #{close}, current unrealizedPNL: #{unrealizedPNL}, #{Time.zone.now}" if unrealizedPNL != 0
      loss_limit = ENV["total_asset"].to_f * 0.001 * -1
      if unrealizedPNL < loss_limit
        Rails.logger.warn "ib risk loss_limit alert: #{unrealizedPNL}, #{Time.zone.now}"
        ApplicationController.helpers.close_position
      end
      # 
      #
      #
      # data = ApplicationController.helpers.ib_portfolio
      # Rails.logger.warn "ib risk close: #{close}, current unrealizedPNL: #{data["unrealizedPNL"]}, #{Time.zone.now}" if !data["unrealizedPNL"].nil?
      # loss_limit = ENV["total_asset"].to_f * 0.001 * -1
      #
      # if !data["unrealizedPNL"].nil? && data["unrealizedPNL"] < loss_limit
      #   Rails.logger.warn "ib risk loss_limit alert: #{data["unrealizedPNL"]}, #{Time.zone.now}"
      #   ApplicationController.helpers.close_position
      # end
    end
  end

  private
  def around_check
    ApplicationController.helpers.ib_disconnect(@ib)
  end

end
