module BinanceHelper

  # <base_url>/data/spot/monthly/klines/<symbol_in_uppercase>/<interval>/<symbol_in_uppercase>-<interval>-<year>-<month>.zip
  # https://data.binance.vision/data/futures/um/daily/trades/BTCBUSD/BTCBUSD-trades-2021-06-27.zip

  # ApplicationController.helpers.binance_data
  def binance_data
    (4.months.ago.to_date..Date.today).each do |dt|
      p [dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"), "1m"]
      get_data(dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d"), "1m")
    end
  end

  def get_data(year, month, day, interval)
    # interval = "1m"
    # year = "2021"
    # month = "06"
    # day = "27"
    res = HTTParty.get('https://api.binance.com/api/v1/exchangeInfo')
    json = JSON.parse res.body
    key_symbols = ["ETH", "BTC", "LTC", "EOS", "XMR", "DODO", "ONT", "VET", "AAVE", "SUSHI", "YFI", "FTM", "CRV"]
    json["symbols"].each do |sym|
      symbol_in_uppercase = sym["symbol"]
      # p symbol_in_uppercase
      # p key_symbols.any? { |word| symbol_in_uppercase.include?(word) }
      if key_symbols.any? { |word| symbol_in_uppercase.include?(word) }
        p symbol_in_uppercase
        base_url = "https://data.binance.vision"
        trades_url = "#{base_url}/data/futures/um/daily/trades/#{symbol_in_uppercase}/#{symbol_in_uppercase}-trades-#{year}-#{month}-#{day}.zip"
        kilnes_url = "#{base_url}/data/spot/daily/klines/#{symbol_in_uppercase}/#{interval}/#{symbol_in_uppercase}-#{interval}-#{year}-#{month}-#{day}.zip"
        begin
          download = open(kilnes_url)
          dir = "#{Rails.root.to_s}/tmp/csv/binance/#{Time.zone.now.strftime("%Y%m%d")}"
          file_name = "#{symbol_in_uppercase}-#{interval}-#{year}-#{month}-#{day}.zip"
          unless File.directory?(dir)
            FileUtils.mkdir_p(dir)
          end
          IO.copy_stream(download, "#{dir}/#{file_name}")
          zip_file = "#{dir}/#{file_name}"

          table_name = binance_table(symbol_in_uppercase, interval)
          count = binance_data_insert(zip_file, table_name)
          Rails.logger.warn "Binance get_data #{symbol_in_uppercase} #{count} rows"
        rescue Exception => e
          Rails.logger.warn "Binance get_data failed: #{e}"
        end
      end
      sleep 0.5
    end
  end

  # table_name = binance_table(symbol, interval)
  def binance_table(symbol, interval)
    begin
      postgres = PG.connect :host => ENV['quant_db_host'], :port => ENV['quant_db_port'], :dbname => ENV['quant_db_name'], :user => ENV['quant_db_user'], :password => ENV['quant_db_pwd']
      table_name = symbol.downcase + "_" + interval.downcase
      sql = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE  table_schema = 'public' AND table_name = '#{table_name}');"
      res = postgres.exec(sql)

      if res.first["exists"] != "t"
        sql = "CREATE TABLE public.#{table_name} (open_time timestamp, open float8, high float8, low float8, close float8, volume decimal, close_time timestamp, quote_asset_volume decimal, number_of_trades int8, taker_buy_base_asset_volume decimal, taker_buy_quote_asset_volume decimal, ignore decimal);"
        res = postgres.exec(sql)
        sql = "CREATE UNIQUE INDEX #{table_name}_idx ON public.#{table_name} (open_time);"
        res = postgres.exec(sql)
      end
    rescue PG::Error => e
      Rails.logger.warn e.message
    ensure
      postgres.close if postgres
    end
    return table_name
  end

  def binance_data_insert(file, table_name)
    # file = "/Users/hielf/workspace/projects/panda_ib/tmp/csv/binance/20210629/ETHBTC-1m-2021-06-27.zip"
    require 'zip'
    fpath = ""
    begin
      Zip::File.open(file, create: true) do |zip_file|
        zip_file.each do |f|
          fpath = File.join(File.dirname(file), f.name)
          zip_file.extract(f, fpath) unless File.exist?(fpath)
        end
      end
    rescue Exception => e
      Rails.logger.warn "binance_data_insert failed: #{e}"
    ensure
      csv_text = File.read(fpath)
      csv = CSV.parse(csv_text, :headers => false)
    end

    count = 0
    begin
      postgres = PG.connect :host => ENV['quant_db_host'], :port => ENV['quant_db_port'], :dbname => ENV['quant_db_name'], :user => ENV['quant_db_user'], :password => ENV['quant_db_pwd']
      csv.each do |row|
        sql = "insert into #{table_name} select '#{Time.at row[0].to_i / 1000}', #{row[1]}, #{row[2]}, #{row[3]}, #{row[4]}, #{row[5]}, '#{Time.at row[6].to_i / 1000}', #{row[7]}, #{row[8]}, #{row[9]}, #{row[10]}, #{row[11]} WHERE NOT EXISTS (select '#{Time.at row[0].to_i / 1000}' from #{table_name} where open_time = '#{Time.at row[0].to_i / 1000}');"
        postgres.exec(sql)
        count = count + 1
        p count
      end
    rescue PG::Error => e
      Rails.logger.warn e.message
    ensure
      postgres.close if postgres
    end

    begin
      File.open(fpath, 'r') do |f|
        File.delete(f)
      end
    rescue Exception => e
      Rails.logger.warn "binance_data_insert failed: #{e}"
    end
    return count
  end

end
