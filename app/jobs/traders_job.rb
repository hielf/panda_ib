class TradersJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]
    # sleep 15
    Rails.logger.warn "ib job start: #{contract}, #{Time.zone.now}"
    @ib = ApplicationController.helpers.ib_connect
    if @ib.isConnected()
      attempt = 0
      market_data = nil

      if ENV["remote_index"] == "true"
        10.times do
          attempt += 1
          url = "http://#{ENV['remote_index_url']}/csv/hsi.csv"
          download = open(url)
          IO.copy_stream(download, Rails.root.to_s + '/tmp/csv/hsi.csv')
          file = Rails.root.to_s + "/tmp/csv/#{contract}.csv"
          if (Time.zone.now - CSV.read(file).last[0].to_time < 60)
            break
          else
            sleep 2
          end
        end
      else
        5.times do
          attempt += 1
          market_data = ApplicationController.helpers.market_data(contract)
          if market_data
            break
          else
            Rails.logger.warn "await for 3 seconds.."
            sleep 3
            market_data = ApplicationController.helpers.market_data(contract, true) if attempt == 5
          end
        end
        file = ApplicationController.helpers.index_to_csv(contract, market_data, true)
      end

      current_time = Time.zone.now.strftime('%H:%M')
      if (current_time >= "09:45" && current_time <= "15:45")
        @order, @amount = ApplicationController.helpers.py_check_position(contract) if file
        Rails.logger.warn "ib py_check_position: #{@order} #{@amount.to_s}, #{Time.zone.now}"
      else
        ApplicationController.helpers.close_position
      end

      OrdersJob.perform_now @order, @amount

      # if @order != "" && @amount != 0
      #   ApplicationController.helpers.ib_order(@order, @amount, 0)
      #   ApplicationController.helpers.close_position if @order == "CLOSE"
      #   EventLog.create(content: "#{@order} #{@amount.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}")
      # end
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
