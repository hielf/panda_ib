class TradersJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]
    run_time = Time.zone.now
    Rails.logger.warn "ib job start: #{contract}, #{Time.zone.now}"

    attempt = 0
    market_data = nil
    file = Rails.root.to_s + "/tmp/csv/#{contract}.csv"

    if ENV["remote_index"] == "true"
      10.times do
        attempt += 1
        url = "http://#{ENV['remote_index_url']}/csv/#{contract}.csv"
        download = open(url)
        IO.copy_stream(download, file)
        if (run_time - CSV.read(file).last[0].to_time < 60)
          break
        else
          Rails.logger.warn "await for 2 seconds.."
          sleep 2
        end
      end
    else
      5.times do
        attempt += 1
        case ENV['backtrader_version']
        when '15sec'
          market_data = true if (run_time - CSV.read(file).last[0].to_time < 30)
        else
          market_data = true if (run_time - CSV.read(file).last[0].to_time < 60)
        end
        break if market_data
        Rails.logger.warn "await for 1 second.."
        sleep 1
      end
      if market_data.nil?
        Rails.logger.warn "ib job return: no market_data, #{Time.zone.now}"
        return
      end
      # 5.times do
      #   attempt += 1
      #   market_data = ApplicationController.helpers.market_data(contract)
      #   if market_data
      #     break
      #   else
      #     Rails.logger.warn "await for 3 seconds.."
      #     sleep 3
      #     market_data = ApplicationController.helpers.market_data(contract, true) if attempt == 5
      #   end
      # end
      # file = ApplicationController.helpers.index_to_csv(contract, market_data, true)
    end

    current_time = run_time.strftime('%H:%M')
    if (current_time >= "09:45" && current_time <= "15:45")
      @order, @amount = ApplicationController.helpers.py_check_position(contract) if file
      ApplicationController.helpers.document_files(contract, file) if file
      Rails.logger.warn "ib py_check_position: #{@order} #{@amount.to_s}, #{Time.zone.now}"

      elr = EventLog.where("log_type = ? ", "RISK").last
      if elr
        ot = case ENV['backtrader_version']
        when '5min'
          60
        when '4min'
          480
        when '3min'
          360
        when '15sec'
          30
        else
          120
        end
        elo = EventLog.where("log_type = ? AND created_at > ?", "ORDER", ot.seconds.ago).last
        if elo && elo.order_type == @order && elr.id > elo.id
          Rails.logger.warn "ib return for last RISK CLOSE: #{@order}, #{Time.zone.now}"
          return
        end
      end

      OrdersJob.perform_now @order, @amount

    elsif (current_time > "09:15" && current_time < "09:45") || (current_time > "15:45" && current_time < "16:30")
      ApplicationController.helpers.close_position
    else
      return
    end

    # if @order != "" && @amount != 0
    #   ApplicationController.helpers.ib_order(@order, @amount, 0)
    #   ApplicationController.helpers.close_position if @order == "CLOSE"
    #   EventLog.create(content: "#{@order} #{@amount.to_s} at #{Time.zone.now.strftime('%Y-%m-%d %H:%M')}")
    # end
  end

  private
  def around_check
    
  end

end
