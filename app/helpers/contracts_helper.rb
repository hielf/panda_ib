#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
require 'net/ping'
require 'pycall/import'
include PyCall::Import

# pyimport 'ib_insync'
# pyimport 'sys'
# pyimport 'json'
# pyimport 'math'
# pyfrom 'ib_insync', import: :IB

module ContractsHelper

  def index_to_csv(index)
    url = "http://212.64.71.254:3000/#{index}?and=(date.gte.2019-10-27T05:15:00,date.lte.2019-10-28T18:18:00)"
    res = HTTParty.get url
    json = JSON.parse res.body
    csv = CSV.generate(headers: false) { |csv| json.map(&:to_a).each { |row| csv << row } }

    file = "my_file.csv"
    CSV.open( file, 'w' ) do |writer|
      json.each do |c|
        writer << [c["date"], c["open"], c["high"], c["low"], c["close"], c["volume"]]
      end
    end
  end

end
