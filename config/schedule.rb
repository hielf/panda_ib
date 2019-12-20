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
#
every 1.minute do
  # rake "ib:hsi"
  # rake "ib:test"
  # runner 'TradersJob.perform_later'
end
#
