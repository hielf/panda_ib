Rails.application.routes.draw do

  root 'home#index'
  namespace :api, defaults: {format: :json} do
    root 'root#home'
    resources :trade_orders do
      collection do
        post :order
        get :positions
        get :account_values
        get :contract_data
        post :position_check
        get :trades_data
        get :test
      end
    end
    match '*path', via: :all, to: 'root#route_not_found'
  end
  match '*path', via: :all, to: 'home#route_not_found'

end
