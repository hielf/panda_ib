namespace :ib do
  task :hsi_5mins => :environment do
    Rails.logger.warn "ib hsi_5mins: requires"
    require 'net/ping'
    require 'pycall/import'
    Rails.logger.warn "ib hsi_5mins: Import"
    include PyCall::Import
    Rails.logger.warn "ib hsi_5mins: include"
    include TradeOrdersHelper
    include ContractsHelper
    Rails.logger.warn "ib hsi_5mins: start"

    contract = "hsi_5mins"
    Rails.logger.warn "ib hsi_5mins: test #{test(contract)}"
    market_data(contract)
    Rails.logger.warn "ib hsi_5mins: market_data done"
    file = index_to_csv(contract)
    Rails.logger.warn "ib hsi_5mins: file done"
    data = online_data(file)
    Rails.logger.warn "ib hsi_5mins: data #{data.to_s}"
    check_position(data)
    Rails.logger.warn "ib hsi_5mins: check_position done"

  end
end
