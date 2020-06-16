class CreateSms < ActiveRecord::Migration[5.2]
  def change
    create_table :sms do |t|
      t.string :message
      t.string :message_type

      t.timestamps
    end
  end
end
