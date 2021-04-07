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
      contract = ''
      case ENV['backtrader_version']
      when '15sec'
        contract = 'hsi_15secs'
      else
        contract = 'hsi'
      end
      case ENV['backtrader_version']
      when '15sec'
        await = 4
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
      stop_time = Time.zone.now + 3.minutes - 5.seconds
      60.times do
        if Time.zone.now > stop_time
          ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()
          break
        end
        @ib = ApplicationController.helpers.ib_connect if @ib.nil?
        @ib = ApplicationController.helpers.ib_connect if !@ib.nil? && !@ib.isConnected()
        MarketDataJob.perform_now @ib, contract if @ib
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

  every(3.minute, 'IB.market_data', :thread => true)
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
