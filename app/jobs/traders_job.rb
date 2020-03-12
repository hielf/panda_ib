class TradersJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]
    Rails.logger.warn "ib job start: #{contract}, #{Time.zone.now}"
    @ib = ApplicationController.helpers.ib_connect
    market_on = false
    if @ib.isConnected()
      5.times do
        market_data = ApplicationController.helpers.market_data(contract)
        if market_data
          market_on = true
          break
        else
          Rails.logger.warn "await for 3 seconds.."
          sleep 3
        end
      end

      if market_data
        file = ApplicationController.helpers.index_to_csv(contract, true)

        current_time = Time.zone.now.strftime('%H:%M')
        if (current_time > "09:35" && current_time < "15:30")
          @order, @amount = ApplicationController.helpers.py_check_position(contract) if file
          Rails.logger.warn "ib py_check_position: #{@order} #{@amount.to_s}, #{Time.zone.now}"
        else
          ApplicationController.helpers.close_position
        end

        if @order != "" && @amount != 0
          ApplicationController.helpers.ib_order(@order, @amount, 0)
          EventLog.create(content: "#{@order} #{@amount.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}")
        end

        # file = ApplicationController.helpers.index_to_csv(contract, false)
        # data = ApplicationController.helpers.online_data(file) if file
        # if data && !data.empty?
        #   if (current_time > "09:35" && current_time < "12:00") || (current_time > "13:00" && current_time < "15:30")
        #     ApplicationController.helpers.check_position(data)
        #   else
        #     ApplicationController.helpers.close_position
        #   end
        # end

      end
    end
  end

  private
  def around_check
    data = ApplicationController.helpers.ib_trades
    ApplicationController.helpers.ib_disconnect(@ib)
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
