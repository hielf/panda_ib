class HomeController < ApplicationController
  def index

  end

  def route_not_found
    render json: {code: 404, message: t('messages.c_404')}
  end
end