require_relative 'boot'

require 'rails/all'

# Require the gems listed in Gemfile, including any gems
# you've limited to :test, :development, or :production.
Bundler.require(*Rails.groups)

class Application < Rails::Application
  # Initialize configuration defaults for originally generated Rails version.
  config.load_defaults 5.1

  # config.autoload_paths << Rails.root.join('lib')
  config.time_zone = 'Beijing'
  config.i18n.default_locale = 'zh-CN'
  config.encoding = "utf-8"

  # config.active_job.queue_adapter = :sidekiq

  config.middleware.insert_before 0, Rack::Cors do
    allow do
      origins '*'
      resource '*', headers: :any, methods: [:get, :post, :options]
    end
  end

  config.generators do |g|
    g.test_framework :rspec
    g.fixture_replacement :factory_bot, dir: 'spec/factories'
  end
  # Settings in config/environments/* take precedence over those specified here.
  # Application configuration should go into files in config/initializers
  # -- all .rb files in that directory are automatically loaded.
end
