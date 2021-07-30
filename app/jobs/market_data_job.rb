class MarketDataJob < ApplicationJob
  queue_as :low_priority

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    @ib = args[0]
    @contract = args[1]
    @version = args[2]

    current_time = Time.zone.now.strftime('%H:%M')
    if (current_time >= "09:15" && current_time <= "12:00") || (current_time >= "13:00" && current_time <= "16:30")
      # if @ib.isConnected()
      @market_data = ApplicationController.helpers.market_data(@contract, @version, true) unless ENV["remote_index"] == "true"
      # end

      file = Rails.root.to_s + "/tmp/csv/#{@contract}_#{@version}.csv"
      begin
        table = CSV.parse(File.read(file), headers: true)
      rescue Exception => e
        Rails.logger.warn "IB.realtime_bar_get failed: #{e}"
      end
      if table && table.count > 0
        current_time = Time.zone.now
        if current_time - table[-1]["date"].in_time_zone > 120
          if (current_time >= "09:18" && current_time <= "12:00") || (current_time >= "13:03" && current_time <= "16:30")
            SmsJob.perform_later ENV["admin_phone"], ENV["superme_user"] + " " + ENV["backtrader_version"], "行情数据中断"
          end
        end
      end
    end
  end

  private
  def around_check
    file = ApplicationController.helpers.index_to_csv(@contract, @version, @market_data, true) if @market_data
  end

end
