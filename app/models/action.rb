class Action < ApplicationRecord

  def self.today
    where("action_time >= ?", Date.today)
  end
end
