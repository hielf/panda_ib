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
    if job == 'IB.history'
      if ENV['his_collect'] == "true"

        Rails.logger.warn "IB.history started.."
        begin
          ApplicationController.helpers.csv_to_db
        rescue Exception => e
          error_message = e.message
          Rails.logger.warn "IB.history csv_to_db error: #{error_message}"
        end
        Rails.logger.warn "IB.history fut ended.."

        unless system( "cd #{Rails.root.to_s + '/lib/python/ib'} && python3 ibmarket_idx_his.py" )
          Rails.logger.warn "IB.history idx_his error #{$?}"
        end

        Rails.logger.warn "IB.history idx ended.."
        Rails.logger.warn "IB.history ended.."
      end
    end
  end

  every(1.day, 'IB.history', :at => '4:00', :thread => true)

  # every(1.minute, 'timing', :skip_first_run => true, :thread => true)
  # every(1.hour, 'hourly.job')
  #
  # every(1.day, 'midnight.job', :at => '00:00')
end

# cd /var/www/panda_ib/current/lib/job && clockworkd -c clock.rb start --log -d /var/www/panda_ib/current/lib/job
# clockworkd -c clock.rb start --log -d /Users/hielf/workspace/projects/panda_ib/lib/job
# clockworkd -c clock.rb start --log -d /var/www/panda_ib/current/lib/job
# clockworkd -c clock.rb stop
