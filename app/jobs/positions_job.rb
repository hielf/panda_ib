class PositionsJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  def perform(*args)
    contract = args[0]
    version = args[1]

    @ib = ApplicationController.helpers.ib_connect if @ib.nil? || !@ib.isConnected()
  end

  private
  def around_check
    position = ApplicationController.helpers.position_update(contract)

    ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()
  end

end
