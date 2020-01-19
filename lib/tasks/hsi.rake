namespace :ib do
  task :hsi => :environment do
    Rails.logger.warn "ib hsi: start"
    contract = "hsi"
    url = "http://127.0.0.1/api/trade_orders/position_check"
    res = HTTParty.post(url,
      headers: {"Content-Type" => "application/json"},
      body: {:type => "check", :contract=> contract}.to_json)
    Rails.logger.warn "ib hsi: end"
  end
end
