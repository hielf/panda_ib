FactoryBot.define do
  factory :profit_loss do
    current { false }
    open { 1.5 }
    close { 1.5 }
    unrealized_pnl { 1.5 }
  end
end
