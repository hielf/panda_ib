class PositionsJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  def perform(*args)
    @ib = args[0]
    @contract = args[1]
    if !@ib.isConnected()
      @ib = ApplicationController.helpers.ib_connect
    end
  end

  private
  def around_check
    position = ApplicationController.helpers.position_update(@contract)
  end

end
