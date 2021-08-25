#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
# require 'net/ping'
# require 'pycall/import'
# include PyCall::Import
# require 'faye/websocket'
# require 'eventmachine'
# ApplicationController.helpers.huobi_data_insert
module HuobisHelper
# ["ethusdt", "btcusdt", "dogeusdt", "xrpusdt", "lunausdt", "adausdt", "bttusdt", "nftusdt", "dotusdt", "trxusdt", "icpusdt", "abtusdt", "skmusdt", "bhdusdt", "aacusdt", "canusdt", "fisusdt", "nhbtcusdt", "letusdt", "massusdt", "achusdt", "ringusdt", "stnusdt", "mtausdt", "itcusdt", "atpusdt", "gofusdt", "pvtusdt", "auctionus", "ocnusdt"]

  def currencys_list
    url = "https://api.huobi.pro"
    api = "/v1/common/symbols"
    res = HTTParty.get url + api
    json = JSON.parse res.body
    list = []
    json["data"].each do |d|
      list << d["symbol"]
    end

    usdt = []
    list.each do |l|
      usdt << l if l.end_with?("usdt")
    end

    # begin
    #   csv = CSV.generate(headers: false) { |csv| json.map(&:to_a).each { |row| csv << row } }
    #   CSV.open( file, 'w' ) do |writer|
    #     writer << ["date", "open", "high", "low", "close", "volume", "barCount", "average"] if with_index
    #     json.each do |c|
    #       writer << [c["date"], c["open"], c["high"], c["low"], c["close"], c["volume"], c["barCount"], c["average"]]
    #     end
    #   end
    # rescue Exception => e
    #   Rails.logger.warn "index_to_csv failed: #{e}"
    # end
    return list
  end

  def huobi_data_insert
    table_name = "huobi_start_1min"
    dir = Rails.root.to_s + "/lib/python/cryptocurrency/"
    his_dir = Rails.root.to_s + "/lib/python/cryptocurrency/his/"
    total = 0

    Dir.entries(dir).sort.each do |d|
      next if d == '.' or d == '..'
      fpath = dir + d
      # p path if File.file?(path)
      if File.extname(fpath) == ".csv"
        file_name = File.basename(fpath, File.extname(fpath))
        currency = file_name.split('_')[0]
        date = file_name.split('_')[1]
        csv_text = File.read(fpath)
        csv = CSV.parse(csv_text, :headers => true)
        count = 0

        p file_name

        begin
          postgres = PG.connect :host => ENV['quant_db_host'], :port => ENV['quant_db_port'], :dbname => ENV['quant_db_name'], :user => ENV['quant_db_user'], :password => ENV['quant_db_pwd']
          csv.each do |row|
            sql = "insert into #{table_name} select '#{currency}', '#{row[0]}', #{row[1]}, #{row[2]}, #{row[3]}, #{row[4]}, #{row[5]}, #{row[6]} WHERE NOT EXISTS (select '#{currency}', '#{row[0]}' from #{table_name} where contract = '#{currency}' and date = '#{row[0]}');"
            postgres.exec(sql)
            p sql
            count = count + 1
          end
        rescue PG::Error => e
          Rails.logger.warn e.message
        ensure
          p "#{count} inserted"
          postgres.close if postgres

          FileUtils.mv(fpath, his_dir + d)
          total = count + total
        end
      end
    end

    return total
  end

  def huobi_tickers
    contracts = []
    filepath = Rails.root.to_s + "/tmp/contracts.json"
    url = "https://api.huobi.pro/market/tickers"

    loop do
      current_time = Time.zone.now.strftime('%H:%M')
      p current_time
      if (current_time >= "00:00" && current_time < "00:01")
        res = HTTParty.get url
        json = JSON.parse res.body
        ticker_time = Time.at(json["ts"]/1000)
        if !contracts.any?{|d| d["time"] == ticker_time}
          hash = {}
          data = []
          json["data"].each do |d|
            if d["symbol"].end_with?("usdt")
              data << d
            end
          end
          hash["time"] = ticker_time
          hash["data"] = data
          contracts << hash
        end
      elsif current_time == "00:01"
        p "end"
        break
      end
      sleep 0.1
    end

    File.write(filepath, JSON.dump(contracts))

    return filepath
  end

  def huobi_ticker_insert(filepath)
    f = File.open filepath
    json_data = JSON.load f
    table_name = "huobi_tickers"

    begin
      postgres = PG.connect :host => ENV['quant_db_host'], :port => ENV['quant_db_port'], :dbname => ENV['quant_db_name'], :user => ENV['quant_db_user'], :password => ENV['quant_db_pwd']
      json_data.each do |time|
        time["data"].each do |d|
          sql = "insert into #{table_name} select '#{d["symbol"]}', '#{time["time"]}', '#{d["open"]}', #{d["high"]}, #{d["low"]}, #{d["close"]}, #{d["vol"]}, #{d["amount"]}, #{d["count"]}, #{d["bid"]}, #{d["bidSize"]}, #{d["ask"]}, #{d["askSize"]} WHERE NOT EXISTS (select '#{d["symbol"]}', '#{time["time"]}' from #{table_name} where symbol = '#{d["symbol"]}' and time = '#{time["time"]}');"
          postgres.exec(sql)
          p sql
        end
      end
    rescue PG::Error => e
      Rails.logger.warn e.message
    ensure
      postgres.close if postgres
    end
  end

  # sql = "CREATE TABLE public.#{table_name} (symbol varchar, time timestamp, open float8, high float8, low float8, close float8, vol float8, amount float8, count int8, bid float8, bidSize float8, ask float8, askSize float8);"
  # res = postgres.exec(sql)
  # sql = "CREATE UNIQUE INDEX #{table_name}_idx ON public.#{table_name} (symbol, time);"
  # res = postgres.exec(sql)

  # def ws_client
  #   WebSocket::Client::Simple.connect 'wss://api.huobi.pro/ws' do |ws|
  #     ws.on :open do
  #       puts "connect!"
  #     end
  #
  #     ws.on :message do |msg|
  #       puts msg.data
  #     end
  #   end
  #
  #
  #   EM.run {
  #     ws = Faye::WebSocket::Client.new('wss://api.huobi.pro/ws', [], :tls => {
  #       :verify_peer => false
  #     })
  #
  #     ws.on :open do |event|
  #       p [:open]
  #       ws.send({
  #               "req": "market.btcusdt.kline.1min",
  #               "id": "id1",
  #               "from": 1629129608,
  #               "to": 1629131336,
  #           }.to_json)
  #     end
  #
  #     ws.on :message do |event|
  #       # data = {"pong": event.data['ping']}
  #       # ws.send(data)
  #       # p [:message, event.data]
  #       # buf = ActiveSupport::Gzip.decompress(event.data)
  #       p [:message, event.data]
  #     end
  #
  #     ws.on :close do |event|
  #       p [:close, event.code, event.reason]
  #       ws = nil
  #     end
  #   }
  #
  #   App = lambda do |env|
  #     if Faye::WebSocket.websocket?(env)
  #       # ws = Faye::WebSocket.new(env)
  #       ws = Faye::WebSocket::Client.new('wss://api.huobi.pro/ws', [], :tls => {
  #         :verify_peer => false
  #       })
  #
  #       ws.on :message do |event|
  #         p event
  #         ws.send(event.data)
  #       end
  #
  #       ws.on :close do |event|
  #         p [:close, event.code, event.reason]
  #         ws = nil
  #       end
  #
  #       # Return async Rack response
  #       ws.rack_response
  #
  #     else
  #       # Normal HTTP request
  #       [200, { 'Content-Type' => 'text/plain' }, ['Hello']]
  #     end
  #   end
  # end

end
