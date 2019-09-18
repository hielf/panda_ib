module APISupport
  extend ActiveSupport::Concern

  included do

    def user_login(role)
      user = create(:user, role: role, access_token: SecureRandom.base64(64))
      header = {
          Authorization: ActionController::HttpAuthentication::Token.encode_credentials("#{user.access_token}, username=#{user.username}")
      }
      return [user, header]
    end

  end
end