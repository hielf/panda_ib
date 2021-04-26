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
    if job == 'IB.realtime_bar_get' && ENV['backtrader_version'] == "15secs"
      contract = ENV['contract']
      version = ENV['backtrader_version']

      file = Rails.root.to_s + "/tmp/csv/#{contract}_#{version}.csv"

      3.times do
        begin
          table = CSV.parse(File.read(file), headers: true)
        rescue Exception => e
          Rails.logger.warn "IB.realtime_bar_get failed: #{e}"
        end

        if table && table.count > 0
          current_time = Time.zone.now
          if current_time - table[-1]["date"].in_time_zone > 15
            Rails.logger.warn "IB.realtime_bar_get start"
            @ib = ApplicationController.helpers.ib_connect
            ApplicationController.helpers.realtime_market_data(contract, version) if @ib
          else
            break
          end
        end
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
