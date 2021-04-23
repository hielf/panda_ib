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
    if job == 'IB.realtime_bar_get'
      contract = ENV['contract']
      version = ENV['backtrader_version']

      file = Rails.root.to_s + "/tmp/#{contract}_#{version}_bars.csv"

      3.times do
        table = CSV.parse(File.read(file), headers: true)
        if table && table.count > 0
          current_time = Time.zone.now
          if current_time - table[-1]["time"].in_time_zone > 15
            @ib = ApplicationController.helpers.ib_connect
            ApplicationController.helpers.realtime_market_data(contract, version) if @ib
          else
            break
          end
        end
      end
    end

    if job == 'IB.realtime_bar_csv'
      contract = ENV['contract']
      version = ENV['backtrader_version']
      RealtimeBarJob.perform_later(contract, version)
    end

    if job == 'IB.market_data'
      contract = ENV['contract']
      version = ENV['backtrader_version']

      case version
      when '15secs'
        await = 7
      when "1min"
        await = 4
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
      loop do
        if Time.zone.now > stop_time
          ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()
          break
        end
        @ib = ApplicationController.helpers.ib_connect if @ib.nil?
        @ib = ApplicationController.helpers.ib_connect if !@ib.nil? && !@ib.isConnected()
        MarketDataJob.perform_now @ib, contract, version if @ib
        req_times = req_times + 1
        if req_times >= 20
          ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()
          system( "god restart panda_ib-clock_2" )
          break
        end
        sleep await
      end
    end

    if job == 'IB.history'
      if ENV['his_collect'] == "true"
        Rails.logger.warn "IB.history started.."
        system( "cd #{Rails.root.to_s + '/lib/python/ib'} && python3 ibmarket_idx_his.py" )
        Rails.logger.warn "IB.history idx ended.."
        ApplicationController.helpers.csv_to_db
        Rails.logger.warn "IB.history fut ended.."
        Rails.logger.warn "IB.history ended.."
      end
    end
  end

  every(1.minute, 'IB.realtime_bar_get', :thread => true)
  every(2.second, 'IB.realtime_bar_csv', :thread => true)
  every(2.minute, 'IB.market_data', :thread => true)
  every(1.day, 'IB.history', :at => '18:00', :thread => true)

  # every(1.minute, 'timing', :skip_first_run => true, :thread => true)
  # every(1.hour, 'hourly.job')
  #
  # every(1.day, 'midnight.job', :at => '00:00')
end

# cd /var/www/panda_ib/current/lib/job && clockworkd -c clock.rb start --log -d /var/www/panda_ib/current/lib/job
# clockworkd -c clock.rb start --log -d /Users/hielf/workspace/projects/panda_ib/lib/job
# clockworkd -c clock.rb start --log -d /var/www/panda_ib/current/lib/job
# clockworkd -c clock.rb stop
