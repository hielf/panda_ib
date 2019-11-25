namespace :ib do
  task :hsi_5mins => :environment do
    Rails.logger.warn "ib position_check: start"
    url = "http://127.0.0.1/api/trade_orders/position_check"
    res = HTTParty.post(url,
      headers: {"Content-Type" => "application/json"},
      body: {})
  end
end
