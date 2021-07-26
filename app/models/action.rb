class Action < ApplicationRecord

  def self.today
    where("action_time >= ?", Date.today)
  end

  def self.act(order, amount, price, time)
    create(order: order, amount: amount, price: price, action_time: time) if amount != 0
  end
end
