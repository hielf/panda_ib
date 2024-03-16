class PositionsJob < ApplicationJob
  queue_as :default

  after_perform :around_check

  def perform(*args)
    @contract = args[0]
    @version = args[1]

    @ib = ApplicationController.helpers.ib_connect if @ib.nil? || !@ib.isConnected()

    if @ib.nil? || !@ib.isConnected()
      SmsJob.perform_later ENV["admin_phone"], ENV["superme_user"] + " " + ENV["backtrader_version"], "无法连接"
    end
  end

  private
  def around_check
    position = ApplicationController.helpers.position_update(@contract)

    Rails.logger.warn "ib position job amount unnormal: #{position}, #{Time.zone.now}" if (position.abs != ENV["amount"].to_i && position != 0)

    ApplicationController.helpers.ib_disconnect(@ib) if @ib.isConnected()

    if position.abs > ENV["amount"].to_i
      SmsJob.perform_later ENV["admin_phone"], ENV["superme_user"] + " " + ENV["backtrader_version"], "持仓#{position.to_s}大于预设"
    end
  end

end
