namespace :ib do
  task :test => :environment do

    Rails.logger.warn "ib test: start"
    contract = "hsi"
    TradersJob.perform_now contract
  end
end
