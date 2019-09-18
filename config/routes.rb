Rails.application.routes.draw do

  root 'home#index'
  namespace :api, defaults: {format: :json} do
    root 'root#home'
    resources :traders do
      collection do
        post :init_trader
        post :terminate_trader
        post :restart_server
        post :reset_trader
        get :accounts
      end
    end
    resources :trade_orders do
      collection do
        post :order
        post :queue_order
        # post :init_server
        post :user_ipo
        get :capital_accounts_balance
        get :capital_accounts_position
        get :today_trades
        get :today_ipos
        get :today_entrusts
        get :stock_list
        get :stock_price
      end
    end
    match '*path', via: :all, to: 'root#route_not_found'
  end
  match '*path', via: :all, to: 'home#route_not_found'

end
