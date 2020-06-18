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
  def connect
    @ib = ApplicationController.helpers.ib_connect
    return @ib
  end
  # handler receives the time when job is prepared to run in the 2nd argument
  handler do |job, time|
    if job == 'IB risk'
      @ib = ApplicationController.helpers.ib_connect
      p @ib

      180.times do
        RisksJob.perform_now @ib, 'hsi'
        sleep 5
      end
    end
  end

  every(15.minute, 'IB risk', :thread => false)
  # every(1.minute, 'timing', :skip_first_run => true, :thread => true)
  # every(1.hour, 'hourly.job')
  #
  # every(1.day, 'midnight.job', :at => '00:00')
end

# cd /var/www/panda_ib/current/lib/job && clockworkd -c clock.rb start --log -d /var/www/panda_ib/current/lib/job
# clockworkd -c clock.rb start --log -d /Users/hielf/workspace/projects/panda_ib/lib/job
# clockworkd -c clock.rb start --log -d /var/www/panda_ib/current/lib/job
# clockworkd -c clock.rb stop
