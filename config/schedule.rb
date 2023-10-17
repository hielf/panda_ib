# Use this file to easily define all of your cron jobs.
#
# It's helpful, but not entirely necessary to understand cron before proceeding.
# http://en.wikipedia.org/wiki/Cron

# Example:
#
# set :output, "/path/to/my/cron_log.log"
#
# every 2.hours do
#   command "/usr/bin/some_great_command"
#   runner "MyModel.some_method"
#   rake "some:great:rake:task"
# end
#
# every 4.days do
#   runner "AnotherModel.prune_old_records"
# end

# Learn more: http://github.com/javan/whenever

# every :reboot do
#   command "service ssh start"
#   command "service nginx start"
#   command "cd /var/www/panda_ib/current && /usr/local/rvm/bin/rvm 2.4.0@panda_ib do bundle exec puma -C /var/www/panda_ib/shared/puma.rb --daemon"
#   command "cd /var/www/panda_ib/current && /usr/local/rvm/bin/rvm 2.4.0@panda_ib do bundle exec pumactl -S /var/www/panda_ib/shared/tmp/pids/puma.state -F /var/www/panda_ib/shared/puma.rb restart"
#   command "cd /var/www/panda_ib-frontend/ && pm2 start server/app.js"
# end

# every 20.minutes do
#   rake "scan:onus"
# end

every :reboot do
 command "cd /var/www/panda_ib/current && god -c config.god"
end

# every 1.minute do
#   # rake "ib:hsi"
#   rake "ib:test"
#   # runner 'TradersJob.perform_later'
# end

every 1.day, at: '6:00 am' do
  command "cat /dev/null > /var/www/panda_ib/current/log/puma.access.log"
  command "cat /dev/null > /var/www/panda_ib/current/log/puma.error.log"
end

every 1.day, at: ['9:00 am', '12:50 pm', '17:00 pm'] do
  command "god start panda_ib-clock_1" #trader
  command "god start panda_ib-clock_2" #market_data
  # command "god start panda_ib-clock_3" #risk
  command "god start panda_ib-clock_4" #realtime_market_data
end

every 1.day, at: ['3:30 am', '12:10 pm', '16:30 pm'] do
  command "god stop panda_ib-clock_1"
  command "god stop panda_ib-clock_2"
  command "god stop panda_ib-clock_4"
end

every 1.day, at: ['3:00 am'] do
  command "god start panda_ib-clock_5" #history
end

every 1.day, at: ['7:00 am'] do
  command "god stop panda_ib-clock_5" #history
end
#
