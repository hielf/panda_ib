# config valid for current version and patch releases of Capistrano
lock "~> 3.11.0"

set :repo_url, "git@github.com:hielf/panda_ib.git"
set :application, "panda_ib"
# set :user, "deploy"
set :puma_threads, [4, 16]
set :puma_workers, 0
set :rails_env, 'production'
# set :rvm_ruby_version, '2.4.0@panda_ib'

# Default branch is :master
# ask :branch, `git rev-parse --abbrev-ref HEAD`.chomp

# Default deploy_to directory is /var/www/my_app_name
# set :deploy_to, "/var/www/my_app_name"
set :deploy_to, "/var/www/#{fetch(:application)}"

# Default value for :format is :airbrussh.
# set :format, :airbrussh

# You can configure the Airbrussh format using :format_options.
# These are the defaults.
# set :format_options, command_output: true, log_file: "log/capistrano.log", color: :auto, truncate: :auto

# Default value for :pty is false
# set :pty, true

# Default value for :linked_files is []
append :linked_files, "config/database.yml", "config/secrets.yml", "config/application.yml", "tmp/hsi_trades.json"

# Default value for linked_dirs is []
append :linked_dirs, "log", "tmp/pids", "tmp/cache", "tmp/sockets", 'tmp/image', "tmp/csv"

# Default value for default_env is {}
# set :default_env, { path: "/opt/ruby/bin:$PATH" }
set :default_env, {rvm_bin_path: '~/.rvm/bin'}

# Default value for local_user is ENV['USER']
# set :local_user, -> { `git config user.name`.chomp }

# Default value for keep_releases is 5
# set :keep_releases, 5

# Uncomment the following to require manually verifying the host key before first deploy.
# set :ssh_options, verify_host_key: :secure
set :pty, true
set :use_sudo, false
set :stage, :production
set :deploy_via, :remote_cache
set :puma_bind, "unix://#{shared_path}/tmp/sockets/#{fetch(:application)}-puma.sock"
set :puma_state, "#{shared_path}/tmp/pids/puma.state"
set :puma_pid, "#{shared_path}/tmp/pids/puma.pid"
set :puma_access_log, "#{release_path}/log/puma.error.log"
set :puma_error_log, "#{release_path}/log/puma.access.log"
set :ssh_options, {forward_agent: true, user: fetch(:user), keys: %w(~/.ssh/id_rsa.pub)}
set :puma_preload_app, true
set :puma_worker_timeout, nil
set :puma_init_active_record, true # Change to true if using ActiveRecord

set :whenever_identifier, ->{ "#{fetch(:application)}_#{fetch(:stage)}" }

# set :god_roles, [:app, :web]

namespace :puma do
  desc 'Create Directories for Puma Pids and Socket'
  task :make_dirs do
    on roles(:app) do
      execute "mkdir #{shared_path}/tmp/sockets -p"
      execute "mkdir #{shared_path}/tmp/pids -p"
    end
  end

  before :start, :make_dirs
end

namespace :deploy do
  desc "Make sure local git is in sync with remote."
  task :check_revision do
    on roles(:app) do
      unless `git rev-parse HEAD` == `git rev-parse #{fetch(:branch)}`
        puts "WARNING: HEAD is not the same as #{fetch(:branch)}"
        puts "Run `git push` to sync changes."
        exit
      end
    end
  end

  desc 'Initial Deploy'
  task :initial do
    on roles(:app) do
      before 'deploy:restart', 'puma:start'
      invoke 'deploy'
    end
  end

  desc 'Restart application'
  task :restart do
    on roles(:app), in: :sequence, wait: 5 do
      invoke 'puma:restart'
      # invoke 'god:deploy'
    end
  end

  before :starting, :check_revision
  after :finishing, :compile_assets
  after :finishing, :cleanup
  # after :finishing, :restart
end

namespace :god do
  def god_is_running
    capture(:bundle, "exec god status > /dev/null 2>&1 || echo 'god not running'") != 'god not running'
  end

  # Must be executed within SSHKit context
  def config_file
    "#{release_path}/config.god"
  end

  # Must be executed within SSHKit context
  def start_god
    execute :bundle, "exec god -c #{config_file}"
  end

  desc "Start god and his processes"
  task :start do
    on roles(:web) do
      within release_path do
        with RAILS_ENV: fetch(:rails_env) do
          start_god
        end
      end
    end
  end

  desc "Terminate god and his processes"
  task :stop do
    on roles(:web) do
      within release_path do
        if god_is_running
          execute :bundle, "exec god terminate"
        end
      end
    end
  end

  desc "Restart god's child processes"
  task :restart do
    on roles(:web) do
      within release_path do
        with RAILS_ENV: fetch(:rails_env) do
          if god_is_running
            execute :bundle, "exec god load #{config_file}"
            execute :bundle, "exec god restart"
          else
            start_god
          end
        end
      end
    end
  end
end

after "deploy:updated", "god:restart"
