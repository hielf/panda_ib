# -*- coding: UTF-8 -*-
from ib_insync import *
from sqlalchemy import create_engine
import os
import psycopg2
import sched, time
# util.startLoop()  # uncomment this line when in a notebook

ib = IB()
# ib.connect('127.0.0.1', 7496, clientId=100)
# ib.connect('129.226.51.237', 7497, clientId=101)
ib.connect(host='129.226.51.237', port=7497, clientId=101, timeout=10, readonly=False)

# contracts = [Forex('USDJPY'), Forex('EURUSD'), Index(symbol = "HSI", exchange = "HKFE")]
contracts = [Forex('USDJPY'), Forex('EURUSD'), Forex('EURJPY'), Forex('EURGBP'), Forex('GBPJPY')]
# bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='1 D',
#         barSizeSetting='1 min', whatToShow='TRADES', useRTH=True)

s = sched.scheduler(time.time, time.sleep)
def get_index_1min(date_time):
    for contract in contracts:
        if contract.secType == 'CASH':
            bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='6 D', barSizeSetting='1 min', whatToShow='MIDPOINT', useRTH=True)
            if contract == Forex('USDJPY'):
                tmp_table = 'usd_jpy_tmp'
                table = 'usd_jpy'
            elif contract == Forex('EURUSD'):
                tmp_table = 'eur_usd_tmp'
                table = 'eur_usd'
            elif contract == Forex('EURJPY'):
                tmp_table = 'eur_jpy_tmp'
                table = 'eur_jpy'
            elif contract == Forex('EURGBP'):
                tmp_table = 'eur_gbp_tmp'
                table = 'eur_gbp'
            elif contract == Forex('GBPJPY'):
                tmp_table = 'gbp_jpy_tmp'
                table = 'gbp_jpy'

        # convert to pandas dataframe:
        df = util.df(bars)
        print("got contract %s" % str(contract))
        # print(df[['date', 'open', 'high', 'low', 'close']])

        engine = create_engine('postgresql+psycopg2://chesp:Chesp2021@postgres.ripple-tech.com:5432/panda_quant',echo=True,client_encoding='utf8')


        print("waiting for collect %s" % table)
        # get last 2000 bars
        df.to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');

        #不再清空表
        # sql = "delete from %s;" % table

        conn = psycopg2.connect("host='postgres.ripple-tech.com' dbname='panda_quant' user='chesp' password='Chesp2021' port='5432'")
        cur = conn.cursor()
        # cur.execute(sql, (10, 1000000, False, False))
        # conn.commit()

        sql = "insert into " + table + " select * from " + tmp_table +  " b where not exists (select 1 from " + table + " a where a.date = b.date);"

        cur.execute(sql, (10, 1000000, False, False))
        conn.commit()
        conn.close()

        s.enter(60, 1, get_index_1min, (date_time,))

s.enter(10, 1, get_index_1min, (s,))
s.run()
