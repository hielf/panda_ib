class PositionsJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  def perform(*args)
    @contract = args[0]
    position = ApplicationController.helpers.position_update(@contract)
  end

  private
  def around_check
  end

end
