FactoryBot.define do
  factory :trade do
    perm_id { 1 }
    action { "MyString" }
    symbol { "MyString" }
    last_trade_date_or_contract_month { "MyString" }
    currency { "MyString" }
    shares { 1.5 }
    price { 1.5 }
    time { "2019-12-16 22:34:20" }
    commission { 1.5 }
    realized_pnl { 1.5 }
  end
end
