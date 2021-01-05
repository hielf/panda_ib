class PositionsJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  def perform(*args)
    @contract = args[0]

    @ib_position = ApplicationController.helpers.ib_connect if @ib_position.nil?
  end

  private
  def around_check
    position = ApplicationController.helpers.position_update(@contract)

    ApplicationController.helpers.ib_disconnect(@ib_position) if @ib_position.isConnected()
  end

end
