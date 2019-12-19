class TradersJob < ApplicationJob
  queue_as :default

  # around_perform :around_reset

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]
    Rails.logger.warn "ib test: start #{contract}, #{Time.zone.now}"
    file = Rails.root.to_s + "/tmp/csv/#{contract}.csv"
    data = ApplicationController.helpers.online_data(file)
    Rails.logger.warn "ib data test: #{data}"
  end

  private
  def around_reset
    # logger
  end

end
