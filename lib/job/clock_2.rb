require 'clockwork'
# require 'clockwork/database_events'
require '../../config/boot'
require '../../config/environment'
require 'active_support/time' # Allow numeric durations (eg: 1.minutes)

module Clockwork
  # configure do |config|
  #   config[:sleep_timeout] = 5
  #   config[:logger] = Logger.new(log_file_path)
  #   config[:tz] = 'EST'
  #   config[:max_threads] = 15
  #   config[:thread] = true
  # end

  # handler receives the time when job is prepared to run in the 2nd argument
  handler do |job, time|
    if job == 'IB.market_data'
      contract = ENV['contract']
      version = ENV['backtrader_version']

      case version
      when '15secs'
        await = 6
      when "1min"
        await = 6
      when "2min"
        await = 8
      when "3min"
        await = 12
      when "4min"
        await = 16
      when "5min"
        await = 20
      end
      stop_time = Time.zone.now + 2.minutes - await.seconds
      req_times = 0

      # file = Rails.root.to_s + "/tmp/csv/#{contract}_#{version}.csv"
      #
      # 3.times do
      #   begin
      #     table = CSV.parse(File.read(file), headers: true)
      #   rescue Exception => e
      #     Rails.logger.warn "IB.realtime_bar_get failed: #{e}"
      #     sleep 0.3
      #   end
      #
      #   if table && table.count > 0
      #     current_time = Time.zone.now
      #     if current_time - table[-1]["date"].in_time_zone > 60
      #       if (current_time >= "09:15" && current_time <= "12:00") || (current_time >= "13:00" && current_time <= "16:30")
      #         system( "god restart panda_ib-clock_2" )
      #         break
      #       end
      #     end
      #   end
      # end

      loop do
        if Time.zone.now > stop_time
          Rails.logger.warn "IB.market_data disconnecting #{@ib}"
          ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()
          break
        end
        @ib = ApplicationController.helpers.ib_connect if @ib.nil?
        @ib = ApplicationController.helpers.ib_connect if !@ib.nil? && !@ib.isConnected()

        begin
          job = MarketDataJob.perform_later('@ib', contract, version)
          100.times do
            status = ActiveJob::Status.get(job)
            break if status.completed?
            sleep 0.2
          end
        rescue Exception => e
          error_message = e.message
        end
        req_times = req_times + 1
        if req_times >= 20
          ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()
          system( "god restart panda_ib-clock_2" )
          break
        end
        sleep await
      end
    end

    if job == 'IB.trades_data'
      @contract = ENV['contract']
      @version = ENV['backtrader_version']
      run_time = Time.zone.now
      current_time = run_time.strftime('%H:%M')
      if ((current_time >= "09:15" && current_time <= "12:00") || (current_time >= "13:00" && current_time <= "15:55"))
        job = PositionsJob.set(wait: 2.seconds).perform_later(@contract, @version)
      end
    end
  end

  every(1.minute, 'IB.market_data', :thread => true)
  # every(5.minute, 'IB.trades_data', :thread => true)

  # every(1.minute, 'timing', :skip_first_run => true, :thread => true)
  # every(1.hour, 'hourly.job')
  #
  # every(1.day, 'midnight.job', :at => '00:00')
end

# cd /var/www/panda_ib/current/lib/job && clockworkd -c clock.rb start --log -d /var/www/panda_ib/current/lib/job
# clockworkd -c clock.rb start --log -d /Users/hielf/workspace/projects/panda_ib/lib/job
# clockworkd -c clock.rb start --log -d /var/www/panda_ib/current/lib/job
# clockworkd -c clock.rb stop
