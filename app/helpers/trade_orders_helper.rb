#export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
require 'net/ping'
require 'pycall/import'
include PyCall::Import

pyimport 'os'
pyimport 'sys'
pyimport 'json'
pyimport 'math'
pyfrom 'easytrader', import: :remoteclient
pyimport 'pathlib'
# path = PyCall.import_module('pathlib.Path')
pyimport 'easyquotation'

module TradeOrdersHelper

  

end
