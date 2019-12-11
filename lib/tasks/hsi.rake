namespace :ib do
  task :hsi => :environment do
    current_time = Time.zone.now.strftime('%H:%M')

    contract = "hsi"
    url = "http://127.0.0.1/api/trade_orders/contract_data?contract=#{contract}"
    res = HTTParty.get(url)
    json = JSON.parse(res.body)
    status = json["status"]
    Rails.logger.warn "ib contract_data: #{json["message"]}"

    if status == 0
      if (current_time > "09:15" && current_time < "12:00") || (current_time > "13:00" && current_time < "15:30")
        Rails.logger.warn "ib position_check: start check"
        url = "http://127.0.0.1/api/trade_orders/position_check"
        res = HTTParty.post(url,
          headers: {"Content-Type" => "application/json"},
          body: {:type => "check", :contract=> contract}.to_json)
        p [res.code, res.body]
      else
        Rails.logger.warn "ib position_check: start close"
        url = "http://127.0.0.1/api/trade_orders/position_check"
        res = HTTParty.post(url,
          headers: {"Content-Type" => "application/json"},
          body: {:type => "close", :contract=> contract}.to_json)
        p [res.code, res.body]
      end
    end

  end
end
