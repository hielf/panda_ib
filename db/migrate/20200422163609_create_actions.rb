class CreateActions < ActiveRecord::Migration[5.2]
  def change
    create_table :actions do |t|
      t.string :order
      t.integer :amount
      t.datetime :action_time

      t.timestamps
    end
  end
end
