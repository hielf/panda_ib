class CreateEventLogs < ActiveRecord::Migration[5.2]
  def change
    create_table :event_logs do |t|
      # t.references :eventable_id, foreign_key: true
      t.string :content

      t.timestamps
    end
  end
end
