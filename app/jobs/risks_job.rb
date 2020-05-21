class RisksJob < ApplicationJob
  queue_as :low_priority

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    contract = args[0]
    # sleep 15
    Rails.logger.warn "ib risk start: #{contract}, #{Time.zone.now}"
    @ib = ApplicationController.helpers.ib_connect
    if @ib.isConnected()
      data = ApplicationController.helpers.ib_portfolio
      loss_limit = ENV["total_asset"].to_f * 0.001 * -1

      if !data["unrealizedPNL"].nil? && data["unrealizedPNL"] < loss_limit
        Rails.logger.warn "ib risk loss_limit alert: #{data["unrealizedPNL"]}, #{Time.zone.now}"
        ApplicationController.helpers.close_position
      end
    end
  end

  private
  def around_check
    ApplicationController.helpers.ib_disconnect(@ib)
  end

end
