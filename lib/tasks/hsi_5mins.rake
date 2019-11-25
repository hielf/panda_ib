namespace :ib do
  task :hsi_5mins => :environment do
    Rails.logger.warn "ib check_position: start"
    url = "http://127.0.0.1/api/trade_orders/check_position"
    res = HTTParty.post(url,
      headers: {"Content-Type" => "application/json"},
      body: {})
  end
end
