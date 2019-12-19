namespace :ib do
  task :test => :environment do

    Rails.logger.warn "ib test: start"
    url = "http://127.0.0.1/api/trade_orders/test"
    res = HTTParty.get(url)
    Rails.logger.warn "ib test: end"

  end
end
