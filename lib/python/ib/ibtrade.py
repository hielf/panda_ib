from ib_insync import *
from sqlalchemy import create_engine
import os
import psycopg2
import sched, time
# util.startLoop()  # uncomment this line when in a notebook

ib = IB()
ib.connect('127.0.0.1', 7496, clientId=100)

hsi = Future('HSI')
cds = ib.reqContractDetails(hsi)
print(len(cds))
contracts = [cd.contract for cd in cds]

months = []
for co in contracts:
    months.append(co.lastTradeDateOrContractMonth)

months.sort()
month = months[0]
hsi = Future(symbol='HSI', lastTradeDateOrContractMonth=month, exchange='HKFE', currency='HKD')
contract = ib.reqContractDetails(hsi)[0].contract

# contract = Future(symbol='HSI', lastTradeDateOrContractMonth='20190927', exchange='HKFE', currency='HKD')
order = MarketOrder('SELL', 2)
trade = ib.placeOrder(contract, order)
trade
trade.log

pos = ib.positions()
for po in pos:
    print(po)

[v for v in ib.accountValues() if v.tag == 'NetLiquidationByCurrency' and v.currency == 'BASE']

trades = ib.trades()
for t in trades:
    [t.contract.symbol, t.contract.lastTradeDateOrContractMonth, t.contract.currency]
    for f in t.fills:
        [f.execution.shares, f.execution.price, f.execution.time, f.commissionReport.commission, f.commissionReport.realizedPNL]

ib.disconnect()
