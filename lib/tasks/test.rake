namespace :ib do
  task :test => :environment do

    Rails.logger.warn "ib test: start"
    require 'pycall'
    exec 'PYCALL_DEBUG_FIND_LIBPYTHON=1 ruby -rpycall -ePyCall.builtins'
    # url = "http://127.0.0.1/api/trade_orders/test"
    # res = HTTParty.get(url)

    contract = "hsi"
    TradersJob.perform_now contract

    # params = {:contract => "hsi"}
    # TradeService.new(params).check

    Rails.logger.warn "ib test: end"

  end
end
