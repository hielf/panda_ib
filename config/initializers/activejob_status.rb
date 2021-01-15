# By default
# ActiveJob::Status.store = Rails.cache

# # Set another storage
ActiveJob::Status.store = ActiveSupport::Cache::MemoryStore.new
#
# # Use the ActiveSupport#lookup_store syntax
# ActiveJob::Status.store = :redis_store

ActiveJob::Status.options = { expires_in: 20.seconds.to_i }
