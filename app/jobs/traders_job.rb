class TradersJob < ApplicationJob
  queue_as :default

  # around_perform :around_reset

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]
    Rails.logger.warn "ib test: start #{contract}, #{Time.zone.now}"
    market_data = ApplicationController.helpers.market_data(contract)
    if market_data
      file = ApplicationController.helpers.index_to_csv(contract)
      data = ApplicationController.helpers.online_data(file)
      if data
        current_time = Time.zone.now.strftime('%H:%M')
        if (current_time > "09:15" && current_time < "12:00") || (current_time > "13:00" && current_time < "15:30")
          ApplicationController.helpers.check_position(data)
        else
          ApplicationController.helpers.close_position
        end
      end
    end

    Rails.logger.warn "ib data test: #{data}"
  end

  private
  def around_reset
    # logger
  end

end
