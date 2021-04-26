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
    contract = ENV['contract']
    version = ENV['backtrader_version']
    # puts "ib.trader started at #{Time.zone.now}"
    begin
      j = TradersJob.perform_later contract, version
      100.times do
        status = ActiveJob::Status.get(j)
        break if status.completed?
        sleep 0.2
      end
    rescue Exception => e
      error_message = e.message
    end if job == 'ib.trader'
  end

  # -----------------------------------temp -----------------------------------
  # every(1.second, 'ib.trader', :if => lambda { |t| [11,26,41,56].include? t.sec }, :thread => true) if ENV["backtrader_version"] == "15sec"
  # every(1.second, 'ib.trader', :if => lambda { |t| (([6,11,16,21,26,31,36,41,46,51,56,1].include? t.min) && t.sec == 54) }, :thread => true) if ENV["backtrader_version"] == "15sec"
  every(1.second, 'ib.trader', :if => lambda { |t| ([10,25,40,55].include? t.sec) }, :thread => true) if ENV["backtrader_version"] == "15secs"
  # -----------------------------------temp -----------------------------------
  every(1.second, 'ib.trader', :if => lambda { |t| t.sec == 5 }, :thread => true) if ENV["backtrader_version"] == "1min"
  every(1.second, 'ib.trader', :if => lambda { |t| (([0,3,6,9,12,15,18,21,24,27,30,33,36,39,42,45,48,51,54,57].include? t.min) && t.sec == 56) }, :thread => true) if ENV["backtrader_version"] == "3min"
  # every(1.second, 'ib.trader', :if => lambda {  |t| [0,15,30,45].include? t.sec }, :thread => true) if ENV["backtrader_version"] == "3min"
  every(1.second, 'ib.trader', :if => lambda { |t| t.sec == 56 }, :thread => true) if ENV["backtrader_version"] == "4min"
  # every(1.second, 'ib.trader', :if => lambda { |t| (([4,9,14,19,24,29,34,39,44,49,54,59].include? t.min) && t.sec == 54) }, :thread => true) if ENV["backtrader_version"] == "5min"
  every(1.second, 'ib.trader', :if => lambda { |t| t.sec == 54 }, :thread => true) if ENV["backtrader_version"] == "5min"

  # # trades
  # every(5.minutes, 'ib.trades', :thread => true)
  # every(1.minute, 'timing', :skip_first_run => true, :thread => true)
  # every(1.hour, 'hourly.job')
  #
  # every(1.day, 'midnight.job', :at => '00:00')
end

# cd /var/www/panda_ib/current/lib/job && clockworkd -c clock.rb start --log -d /var/www/panda_ib/current/lib/job
# clockworkd -c clock.rb start --log -d /Users/hielf/workspace/projects/panda_ib/lib/job
# clockworkd -c clock.rb start --log -d /var/www/panda_ib/current/lib/job
# clockworkd -c clock.rb stop
