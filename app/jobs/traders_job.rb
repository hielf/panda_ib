class TradersJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]
    Rails.logger.warn "ib start: #{contract}, #{Time.zone.now}"
    # market_data = ApplicationController.helpers.market_data(contract)
    # if market_data
    #   file = ApplicationController.helpers.index_to_csv(contract)
    #   data = ApplicationController.helpers.online_data(file)
    #   if data
    #     current_time = Time.zone.now.strftime('%H:%M')
    #     if (current_time > "09:15" && current_time < "12:00") || (current_time > "13:00" && current_time < "15:30")
    #       ApplicationController.helpers.check_position(data)
    #     else
    #       ApplicationController.helpers.close_position
    #     end
    #   end
    # end
  end

  private
  def around_check
    data = ApplicationController.helpers.ib_trades
    if !data.empty?
      Rails.logger.warn "ib trades data: #{data}"
      data.sort_by { |h| -h[:time] }.reverse.each do |d|
        trade = Trade.find_or_initialize_by(exec_id: d[:exec_id])
        trade.update(perm_id: d[:perm_id], action: d[:action], symbol: d[:symbol],
          last_trade_date_or_contract_month: d[:last_trade_date_or_contract_month],
          currency: d[:currency], shares: d[:shares], price: d[:price], time: Time.at(d[:time]),
          commission: d[:commission], realized_pnl: d[:realized_pnl])
      end
    end
    Rails.logger.warn "ib trades got: #{Time.zone.now}"
  end

end
