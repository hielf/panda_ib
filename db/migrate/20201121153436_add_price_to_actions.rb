class AddPriceToActions < ActiveRecord::Migration[5.2]
  def change
    add_column :actions, :price, :float, :default => 0
  end
end
