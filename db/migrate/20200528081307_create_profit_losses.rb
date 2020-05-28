class CreateProfitLosses < ActiveRecord::Migration[5.2]
  def change
    create_table :profit_losses do |t|
      t.boolean :current, :default => true
      t.float :open
      t.float :close
      t.float :unrealized_pnl

      t.timestamps
    end
  end
end
