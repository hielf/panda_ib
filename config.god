CONFIG_ROOT = File.dirname(__FILE__)

["panda_ib"].each do |app_name|

  app_root = "#{CONFIG_ROOT}"

  def generic_monitoring(w, options = {})

    w.start_grace = 20.seconds
    w.restart_grace = 20.seconds
    w.interval = 60.seconds

    w.start_if do |start|
      start.condition(:process_running) do |c|
        c.interval = 10.seconds
        c.running = false
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

  ["staging", "production"].each do |env|
    God.watch do |w|
      w.name = app_name + "-" + env
      w.group = app_name
      assets = (env == "production") ? "rake assets:precompile --trace RAILS_ENV=production && " : ""
      cmd = "/usr/local/rvm/bin/rvm default do bundle exec puma -C /var/www/#{app_name}/shared/puma.rb --daemon"
      # w.start = "cd #{app_root} && #{assets}puma -e #{env}"
      w.start = "cd #{app_root} && #{assets}pumactl -P shared/tmp/pids/puma.pid start"
      w.restart = "cd #{app_root} && #{assets}pumactl -P shared/tmp/pids/puma.pid restart"
      w.stop = "cd #{app_root} && pumactl -P shared/tmp/pids/puma.pid stop"
      w.pid_file = "#{app_root}/shared/tmp/pids/puma.pid"

      w.log = "#{app_root}/shared/log/god.log"

      w.behavior(:clean_pid_file)

      generic_monitoring(w, :cpu_limit => 80.percent, :memory_limit => 200.megabytes)
    end
  end
end
