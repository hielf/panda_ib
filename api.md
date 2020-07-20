# 熊猫IB API文档

### API说明
* host: http://129.226.126.93/
* root_url: host:81/api --grace
* root_url: host:82/api --test
* root_url: host/api --kam
* Need Auth: no
* HEADERS Content-Type: application/x-www-form-urlencoded

## 交易类 trade_orders
### 下单
* POST root_url/trade_orders/order
* params {order_type: 'BUY', amount: 4, price: 0, rand_code: 1111}     
* params {order_type: 'SELL', amount: 4, price: 0, rand_code: 1111}    
* {
    "status": 0,
    "message": "下单成功"
}

### 查询持仓
* GET root_url/trade_orders/positions
* params {}
* {
    "status": 0,
    "message": "success",
    "data": {
        "position": -1.0,
        "currency": "HKD",
        "contract_date": "20190927",
        "symbol": "HSI"
    }
}

### 查询资金
* GET root_url/trade_orders/account_values
* params {}
* {
    "status": 0,
    "message": "success",
    "data": {
        "account_0": {
            "value": "7959204.6464",
            "currency": "BASE"
        },
        "account_1": {
            "value": "10258.1055",
            "currency": "HKD"
        },
        "account_2": {
            "value": "1014032.03",
            "currency": "USD"
        }
    }
}
