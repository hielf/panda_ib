#!/bin/env python
#-*- encoding=utf8 -*-
import os,sys,json,math
from easytrader import remoteclient
import easyquotation
from pathlib import Path
sys.stdout = open(os.devnull, 'w')
sys.stdout = sys.__stdout__

def rounddown(x):
    return int(math.floor(x / 100.0)) * 100

def get_price(stock_code, request):
    quotation = easyquotation.use('sina')
    data = quotation.real(stock_code)
    return data[stock_code][request], data[stock_code]['name']

def get_balance(user):
    data = user.balance
    # user.sell('600001', price=0.55, amount=100)
    # user.today_trades
    # print(data)
    return data['总资产'], data['股票市值'], data['资金余额']

def get_position(user):
    data = user.position
    return json.dump(data, sys.stdout)

def buy(user, stock_code, amount, price, quota):
    # if price == 0:
    price = get_price(stock_code, 'buy')[0]
    total_asset = get_balance(user)[0]
    if float(quota) > 0:
        amount = rounddown(total_asset * quota / price)
    try:
        data = user.buy(stock_code, price=price, amount=amount)
    except Exception as e:
        error = e.args
        error = error[0].replace("<class 'easytrader.exceptions.TradeError'>: 提交失败：", "")
        data = {"message": error}
    return json.dump(data, sys.stdout)

def sell(user, stock_code, amount, price):
    # if price == 0:
    price = get_price(stock_code, 'sell')[0]
    try:
        data = user.sell(stock_code, price=price, amount=amount)
    except Exception as e:
        error = e.args
        error = error[0].replace("<class 'easytrader.exceptions.TradeError'>: 提交失败：", "")
        data = {"message": error}
    return json.dump(data, sys.stdout)

def cancel_entrust(user, entrust_no):
    data = user.cancel_entrust(entrust_no)
    return json.dump(data, sys.stdout)

if __name__ == "__main__":
    capital_account = sys.argv[1]
    request = sys.argv[2]
    stock_code = sys.argv[3]
    amount = sys.argv[4]
    price = sys.argv[5]
    entrust_no = sys.argv[6]
    quota = sys.argv[7]

    str = os.path.dirname(os.path.abspath(__file__))
    # print(str)
    dir = Path(str)
    filename = "ht_client_" + capital_account + ".json"
    client = dir / filename
    # print(client.name)
    fh = open(dir / "hosts.json")
    hosts = json.load(fh)
    ip = hosts[capital_account]

    user = remoteclient.use('ht_client', host=ip, port='1430')
    with open(client) as f:
        data = json.load(f)
    user.prepare(user=data["user"], password=data["password"], comm_password=data["comm_password"])

    if request == "get_balance":
        get_balance(user)
    elif request == "get_position":
        get_position(user)
    elif request == "buy":
        buy(user, stock_code, amount, price, quota)
    elif request == "sell":
        sell(user, stock_code, amount, price)
    elif request == "cancel_entrust":
        cancel_entrust(user, entrust_no)
    else:
        "unknown request"
