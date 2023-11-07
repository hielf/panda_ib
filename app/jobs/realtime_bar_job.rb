class RealtimeBarJob < ApplicationJob
  queue_as :low_priority

  after_perform :around_check

  rescue_from(ActiveRecord::RecordNotFound) do |exception|
     Rails.logger.warn "#{exception.message.to_s}"
  end

  def perform(*args)
    @contract = args[0]
    @version = args[1]

    current_time = Time.zone.now.strftime('%H:%M')
    file = Rails.root.to_s + "/tmp/csv/#{@contract}_#{@version}.csv"
    if ((current_time >= "09:15" && current_time <= "12:00") || (current_time >= "13:00" && current_time <= "16:30") || (current_time >= "17:15" && current_time <= "23:59") || (current_time >= "00:00" && current_time <= "03:00"))
      @bars = ApplicationController.helpers.realtime_bar_resample(@contract, @version) #unless ENV["remote_index"] == "true"
      ApplicationController.helpers.join_realtime_csv(file, @bars) if @bars
    end
  end

  private
  def around_check

  end

end
