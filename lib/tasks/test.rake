namespace :ib do
  task :test => :environment do
    include ContractsHelper

    require 'pycall/import'
    include PyCall::Import

    Rails.logger.warn "ib test: start"
    index = "hsi"
    file = Rails.root.to_s + "/tmp/csv/#{index}.csv"

    pandaAI = Rails.root.to_s + '/lib/python/ai/pandaAI'
    data = {}
    today = Date.today.strftime('%Y-%m-%d')
    PyCall.exec("dual_params={'resample': '5T', 'step_n': 5, 'barNum': 1, 'file_path': '#{file}',  'check_date': '#{today}'}")
    dual_params = PyCall.eval("dual_params")
    Rails.logger.warn "ib data dual_params: #{dual_params}"

    data = online_data(file)
    Rails.logger.warn "ib data test: #{data}"
  end
end
