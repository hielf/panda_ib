class MarketDataJob < ApplicationJob
  queue_as :low_priority

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    @ib = args[0]
    @contract = args[1]

    current_time = Time.zone.now.strftime('%H:%M')
    if (current_time >= "09:15" && current_time <= "12:00") || (current_time >= "13:00" && current_time <= "16:30")
      if @ib.isConnected()
        @market_data = ApplicationController.helpers.market_data(@contract, true) unless ENV["remote_index"] == "true"
      end
    end
  end

  private
  def around_check
    file = ApplicationController.helpers.index_to_csv(@contract, @market_data, true) if @market_data
  end

end
