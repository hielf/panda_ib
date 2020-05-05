class Action < ApplicationRecord

  def today
    Action.where("action_time >= ?", Date.today)
  end
end
