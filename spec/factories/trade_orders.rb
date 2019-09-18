FactoryBot.define do
  factory :trade_order do
    capital_account { "MyString" }
    stock_code { "MyString" }
    sec_name { "MyString" }
    trade_date { "2019-05-17 22:13:33" }
    volume { "9.99" }
    price { "9.99" }
  end
end
