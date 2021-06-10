class Trade < ApplicationRecord

  def self.today
    where("time >= ?", Date.today)
  end

  def self.today_pnl
    where("time >= ?", Date.today).sum(:realized_pnl)
  end

end
