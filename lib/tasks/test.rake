namespace :ib do
  task :test => :environment do
    include ContractsHelper
      Rails.logger.warn "ib test: start"
      index = "hsi"
      file = Rails.root.to_s + "/tmp/csv/#{index}.csv"
      data = online_data(file)
      Rails.logger.warn "ib data test: #{data}"
  end
end
