class TraderPosition < ApplicationRecord
# TraderPosition.init(contract)
  def self.init(contract)
    tp = TraderPosition.find_or_initialize_by(contract: contract)
    if tp.position.nil?
      tp.position = 0
      tp.save!
    end
    return tp
  end
end
