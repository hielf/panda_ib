# 熊猫股票交易套件API文档

### API说明
* root_url: http://:8090/api
* Need Auth: no
* HEADERS Content-Type: application/x-www-form-urlencoded

## 交易类 trade_orders
### 下单
* POST root_url/trade_orders/queue_order
* params {capital_account: ['1', '2'], stock_code: '600000', order_type: 'sell', amount: 500, price: 11.58, rand_code: 1111, quota: 0}
