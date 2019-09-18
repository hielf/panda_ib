module Api
  class RootController < Api::ApplicationController
    skip_before_action :authenticate_user!

    def route_not_found
      # raise ActiveRecord::RecordNotFound
      render_json([404, 'Request page not found'])
    end

    def home
      render_json([0, 'ok'])
    end

  end
end
