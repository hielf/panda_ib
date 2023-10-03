class SmsJob < ApplicationJob
  queue_as :low_priority

  after_perform :around_check

  def perform(*args)
    cell        = args[0]
    @backtrader_version = args[1]
    @message = args[2]

    last_sm = Sm.where("created_at > ?", 60.minutes.ago).last

    @var        = {}
    @var["backtrader_version"] = @backtrader_version
    @var["message"] = @message
    @status = nil

    if last_sm.nil?
      run_time = Time.zone.now
      current_time = run_time.strftime('%H:%M')
      if current_time >= "09:15" && current_time <= "16:45"
        begin
          uri             = URI.parse("https://api.mysubmail.com/message/xsend.json")
          sms_appid       = ENV['sms_appid']
          sms_signature   = ENV['sms_signature']
          sms_project     = ENV['sms_project']
          res             = Net::HTTP.post_form(uri, appid: sms_appid, to: cell, project: sms_project, signature: sms_signature, vars: @var.to_json)

          # res = HTTParty.post("https://api.mysubmail.com/message/xsend.json",
          #   headers: {"Content-Type" => "application/json"},
          #   body: {:appid => sms_appid, :to => cell, :project => sms_project, :signature => sms_signature, :vars => @var.to_json}.to_json)

          @status      = JSON.parse(res.body)["status"]
        rescue Exception => e
          error_message = e.message
        end
      end unless run_time.saturday? || run_time.sunday?
    end
  end
# SmsJob.perform_later ENV["admin_phone"], ENV["superme_user"] + " " + ENV["backtrader_version"], "无法连接"

  private
  def around_check
    Sm.create!(message: @backtrader_version + @message, message_type: "alert")# if @status
  end
end
