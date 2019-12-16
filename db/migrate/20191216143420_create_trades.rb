class CreateTrades < ActiveRecord::Migration[5.2]
  def change
    create_table :trades do |t|
      t.string :exec_id
      t.integer :perm_id
      t.string :action
      t.string :symbol
      t.string :last_trade_date_or_contract_month
      t.string :currency
      t.float :shares
      t.float :price
      t.datetime :time
      t.float :commission
      t.float :realized_pnl

      t.timestamps
    end
    add_index :trades, :perm_id
    add_index :trades, :exec_id, unique: true
  end
end
