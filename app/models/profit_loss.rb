class ProfitLoss < ApplicationRecord

  def self.latest(n)
    where(current: true).limit(n).order('id desc')
  end
end

# ProfitLoss.create(open: 25983, close: 25984, unrealized_pnl: 700)
