class CreateTraderPositions < ActiveRecord::Migration[5.2]
  def change
    create_table :trader_positions do |t|
      t.string :contract
      t.integer :position

      t.timestamps
    end
  end
end
