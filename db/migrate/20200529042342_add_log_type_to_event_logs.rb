class AddLogTypeToEventLogs < ActiveRecord::Migration[5.2]
  def change
    add_column :event_logs, :log_type, :string
    add_column :event_logs, :order_type, :string
  end
end
