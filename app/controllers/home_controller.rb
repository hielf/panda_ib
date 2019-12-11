class HomeController < ApplicationController
  def index
    render json: {code: 200, message: 'success'}
  end

  def route_not_found
    render json: {code: 404, message: t('messages.c_404')}
  end
end
