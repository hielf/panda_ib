module ApplicationHelper

  def strftime_time(time_obj)
    time_obj.strftime('%Y-%m-%d %H:%M:%S')
  end

end
