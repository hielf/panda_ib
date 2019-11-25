namespace :ib do
  task :hsi_5mins => :environment do
    Rails.logger.warn "ib hsi_5mins: start"
    include TradeOrdersHelper
    include ContractsHelper

    contract = "hsi_5mins"
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
