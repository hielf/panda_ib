class TradeService
  def initialize(params)
    @contract = params[:contract]
  end

  def check
    Rails.logger.warn "ib service start: #{@contract}, #{Time.zone.now}"
    market_data = ApplicationController.helpers.market_data(@contract)
    if market_data
      file = ApplicationController.helpers.index_to_csv(@contract)
      data = ApplicationController.helpers.online_data(file)
      if data && !data.empty?
        current_time = Time.zone.now.strftime('%H:%M')
        if (current_time > "09:15" && current_time < "12:00") || (current_time > "13:00" && current_time < "15:30")
          ApplicationController.helpers.check_position(data)
        else
          ApplicationController.helpers.close_position
        end
      end
    end
    Rails.logger.warn "ib service end: #{@contract}, #{Time.zone.now}"
  end

end

# TradeService.new(params).check
