CONFIG_ROOT = File.dirname(__FILE__)

God::Contacts::Email.defaults do |d|
  d.from_email = 'zishi.mou@ripple-tech.com'
  d.from_name = 'Process monitoring'
  d.delivery_method = :smtp
  d.server_host = 'smtp.ym.163.com'
  d.server_port = 25
  d.server_auth = true
  d.server_domain = 'smtp.ym.163.com'
  d.server_user = 'zishi.mou@ripple-tech.com'
  d.server_password = 'myPassword'
end

God.contact(:email) do |c|
  c.name = 'me'
  c.group = 'developers'
  c.to_email = 'hielf@qq.com'
end

["panda_ib"].each do |app_name|

  app_root = "/var/www/#{app_name}"

  def generic_monitoring(w, options = {})

    w.start_grace = 20.seconds
    w.restart_grace = 20.seconds
    w.interval = 60.seconds

    w.start_if do |start|
      start.condition(:process_running) do |c|
        c.interval = 10.seconds
        c.running = false
        c.notify = {:contacts => ['me'], :priority => 1, :category => 'staging'}
      end
    end

    w.restart_if do |restart|
      restart.condition(:memory_usage) do |c|
        c.above = options[:memory_limit]
        c.times = [3, 5] # 3 out of 5 intervals
      end

      restart.condition(:cpu_usage) do |c|
        c.above = options[:cpu_limit]
        c.times = 5
      end
    end

    w.lifecycle do |on|
      on.condition(:flapping) do |c|
        c.to_state = [:start, :restart]
        c.times = 5
        c.within = 5.minute
        c.transition = :unmonitored
        c.retry_in = 10.minutes
        c.retry_times = 5
        c.retry_within = 2.hours
      end
    end
  end

  env_1 = "production"
  God.watch do |w|
    w.name = app_name + "-" + env_1
    w.group = app_name
    # assets = (env_1 == "production") ? "rake assets:precompile --trace RAILS_ENV=production && " : ""
    # cmd = "/usr/local/rvm/bin/rvm default do bundle exec puma -C /var/www/#{app_name}/shared/puma.rb --daemon"
    # w.start = "cd #{app_root} && #{assets}puma -e #{env_1}"
    w.start = "cd #{app_root}/current && RAILS_ENV=production bundle exec pumactl -S #{app_root}/shared/tmp/pids/puma.state -F #{app_root}/shared/puma.rb start"
    w.restart = "cd #{app_root}/current && RAILS_ENV=production bundle exec pumactl -S #{app_root}/shared/tmp/pids/puma.state -F #{app_root}/shared/puma.rb restart"
    w.stop = "cd #{app_root}/current && RAILS_ENV=production bundle exec pumactl -S #{app_root}/shared/tmp/pids/puma.state -F #{app_root}/shared/puma.rb stop"
    w.pid_file = "#{app_root}/shared/tmp/pids/puma.pid"

    w.log = "#{app_root}/shared/log/rails_app.log"

    w.behavior(:clean_pid_file)

    generic_monitoring(w, :cpu_limit => 100.percent, :memory_limit => 1000.megabytes)
  end

  env_2 = "clock_1"
  God.watch do |w|
    w.name = app_name + "-" + env_2
    w.group = app_name
    w.start = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_1.rb start --log -d #{app_root}/current/lib/job"
    w.restart = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_1.rb restart --log -d #{app_root}/current/lib/job"
    w.stop = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_1.rb stop"
    w.pid_file = "#{app_root}/current/lib/job/tmp/clockworkd.clock_1.pid"

    w.log = "#{app_root}/shared/log/clock_1.log"

    w.behavior(:clean_pid_file)

    generic_monitoring(w, :cpu_limit => 100.percent, :memory_limit => 1000.megabytes)
  end

  env_3 = "clock_2"
  God.watch do |w|
    w.name = app_name + "-" + env_3
    w.group = app_name
    w.start = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_2.rb start --log -d #{app_root}/current/lib/job"
    w.restart = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_2.rb restart --log -d #{app_root}/current/lib/job"
    w.stop = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_2.rb stop"
    w.pid_file = "#{app_root}/current/lib/job/tmp/clockworkd.clock_2.pid"

    w.log = "#{app_root}/shared/log/clock_2.log"

    w.behavior(:clean_pid_file)

    generic_monitoring(w, :cpu_limit => 100.percent, :memory_limit => 1500.megabytes)
  end

  env_4 = "clock_3"
  God.watch do |w|
    w.name = app_name + "-" + env_4
    w.group = app_name
    w.start = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_3.rb start --log -d #{app_root}/current/lib/job"
    w.restart = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_3.rb restart --log -d #{app_root}/current/lib/job"
    w.stop = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_3.rb stop"
    w.pid_file = "#{app_root}/current/lib/job/tmp/clockworkd.clock_3.pid"

    w.log = "#{app_root}/shared/log/clock_3.log"

    w.behavior(:clean_pid_file)

    generic_monitoring(w, :cpu_limit => 100.percent, :memory_limit => 1000.megabytes)
  end

  env_5 = "clock_4"
  God.watch do |w|
    w.name = app_name + "-" + env_5
    w.group = app_name
    w.start = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_4.rb start --log -d #{app_root}/current/lib/job"
    w.restart = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_4.rb restart --log -d #{app_root}/current/lib/job"
    w.stop = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_4.rb stop"
    w.pid_file = "#{app_root}/current/lib/job/tmp/clockworkd.clock_4.pid"

    w.log = "#{app_root}/shared/log/clock_4.log"

    w.behavior(:clean_pid_file)
    w.keepalive(:memory_max => 1000.megabytes, :cpu_max => 100.percent)
    # generic_monitoring(w, :cpu_limit => 100.percent, :memory_limit => 1000.megabytes)
  end

  env_6 = "clock_5"
  God.watch do |w|
    w.name = app_name + "-" + env_6
    w.group = app_name
    w.start = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_5.rb start --log -d #{app_root}/current/lib/job"
    w.restart = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_5.rb restart --log -d #{app_root}/current/lib/job"
    w.stop = "cd #{app_root}/current/lib/job && RAILS_ENV=production bundle exec clockworkd -c clock_5.rb stop"
    w.pid_file = "#{app_root}/current/lib/job/tmp/clockworkd.clock_5.pid"

    w.log = "#{app_root}/shared/log/clock_5.log"

    w.behavior(:clean_pid_file)
    w.keepalive(:memory_max => 1000.megabytes, :cpu_max => 80.percent)
    # generic_monitoring(w, :cpu_limit => 100.percent, :memory_limit => 1000.megabytes)
  end

end
