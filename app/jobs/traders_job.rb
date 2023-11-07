class TradersJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]
    version = args[1]
    run_time = Time.zone.now
    Rails.logger.warn "ib job start: #{contract} #{version}, #{Time.zone.now}"

    attempt = 0
    market_data = nil
    file = Rails.root.to_s + "/tmp/csv/#{contract}_#{version}.csv"

    if ENV["remote_index"] == "true"
      10.times do
        attempt += 1
        url = "http://#{ENV['remote_index_url']}/csv/#{contract}_#{version}.csv"
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
        case version
        when '15secs'
          market_data = true if (run_time - File.mtime(file) < 15)
        else
          market_data = true if (run_time - File.mtime(file) < 60)
        end
        break if market_data
        Rails.logger.warn "await for 1 second.."
        sleep 1
      end
      if market_data.nil?
        Rails.logger.warn "ib job return: no market_data, #{Time.zone.now}"
        return
      end
    end

    current_time = run_time.strftime('%H:%M')
    # close
    if (current_time > "09:00" && current_time < "09:50") || (current_time > "15:50" && current_time < "16:30")
      position = TraderPosition.init(contract).position
      begin
        job = OrdersJob.perform_later("CLOSE", position.abs, "", 0)
        100.times do
          status = ActiveJob::Status.get(job)
          break if status.completed?
          sleep 0.2
        end
      rescue Exception => e
        error_message = e.message
      ensure
        return
      end
    end

    if ENV["superme_user"] != "test"
      # risk
      last_risk = EventLog.where(log_type: "RISK").last.nil? ? Time.now-10.days : EventLog.where(log_type: "RISK").last.created_at
      risk_diff_time = (run_time.beginning_of_minute - last_risk.to_time).abs / 60

      if risk_diff_time <= 60
        Rails.logger.warn "ib returned for last RISK at: #{last_risk.to_time.to_s}, #{Time.zone.now}"
        return
      end

      # benefit
      last_benefit = EventLog.where(log_type: "BENEFIT").last.nil? ? Time.now-10.days : EventLog.where(log_type: "BENEFIT").last.created_at
      benefit_diff_time = (run_time.beginning_of_minute - last_benefit.to_time).abs / 60
      if benefit_diff_time <= 1
        Rails.logger.warn "ib returned for last BENEFIT at: #{last_benefit.to_time.to_s}, #{Time.zone.now}"
        return
      end

      # stop
      last_stop = EventLog.where(log_type: "STOP").last.nil? ? Time.now-10.days : EventLog.where(log_type: "STOP").last.created_at
      if last_stop.to_date == Date.today
        Rails.logger.warn "ib returned for last STOP at: #{last_stop.to_time.to_s}, #{Time.zone.now}"
        return
      end
    end

    # trade
    if ((current_time >= "09:15" && current_time <= "12:00") || (current_time >= "13:00" && current_time <= "16:30") || (current_time >= "17:15" && current_time <= "23:59") || (current_time >= "00:00" && current_time <= "03:00"))
      begin
        @order, @amount, @move_order, @move_price = ApplicationController.helpers.py_check_position(contract, version) if file
      rescue Exception => e
        error_message = e.message
      ensure
        ApplicationController.helpers.document_files(contract, version, file) if file
      end
      Rails.logger.warn "ib py_check_position: #{@order} #{@amount.to_s}, #{Time.zone.now}" if @order != ""
      OrdersJob.perform_later(@order, @amount, @move_order, @move_price) if ((@order != "" && @amount != 0) || @move_order != "" && @move_price != 0)
    end

  end

  private
  def around_check

  end

end
