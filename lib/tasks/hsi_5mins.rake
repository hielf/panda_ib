namespace :ib do
  task :hsi_5mins => :environment do
    include TradeOrdersHelper
    include ContractsHelper

    index = "hsi_5mins"
    file = index_to_csv(index)
    data = online_data(file)
    check_position(data)

  end
end
