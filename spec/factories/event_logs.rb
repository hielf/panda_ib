FactoryBot.define do
  factory :event_log do
    eventable { nil }
    content { "MyString" }
  end
end
