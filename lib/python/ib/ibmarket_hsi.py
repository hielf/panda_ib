from ib_insync import *
from sqlalchemy import create_engine
import os
import psycopg2
import sched, time
# util.startLoop()  # uncomment this line when in a notebook

ib = IB()
# ib.connect('127.0.0.1', 7496, clientId=100)
ib.connect('129.226.51.237', 7497, clientId=101)

# contracts = [Forex('USDJPY'), Forex('EURUSD'), Index(symbol = "HSI", exchange = "HKFE")]
contracts = [Index(symbol = "HSI", exchange = "HKFE")]
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
        elif contract.secType == 'IND':
            bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='10 D', barSizeSetting='1 min', whatToShow='TRADES', useRTH=True)
            if contract.symbol == 'HSI':
                tmp_table = 'hsi_tmp'
                table = 'hsi'

        # convert to pandas dataframe:
        df = util.df(bars)
        print("got contract %s" % str(contract))
        # print(df[['date', 'open', 'high', 'low', 'close']])

        engine = create_engine('postgresql+psycopg2://chesp:Chesp92J5@rm-2zelv192ymyi9680vo.pg.rds.aliyuncs.com:3432/panda_quant',echo=True,client_encoding='utf8')


        print("waiting for collect %s" % table)
        # get last 2000 bars
        df.to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');

        #不再清空表
        # sql = "delete from %s;" % table

        conn = psycopg2.connect("host='rm-2zelv192ymyi9680vo.pg.rds.aliyuncs.com' dbname='panda_quant' user='chesp' password='Chesp92J5' port='3432'")
        cur = conn.cursor()
        # cur.execute(sql, (10, 1000000, False, False))
        # conn.commit()

        sql = "insert into " + table + " select * from " + tmp_table +  " b where not exists (select 1 from " + table + " a where a.date = b.date);"

        cur.execute(sql, (10, 1000000, False, False))
        conn.commit()
        conn.close()

        s.enter(60, 1, get_index_5min, (date_time,))

def get_index_5min(date_time):
    for contract in contracts:
        if contract.secType == 'CASH':
            bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='15 D', barSizeSetting='5 mins', whatToShow='MIDPOINT', useRTH=True)
            if contract == Forex('USDJPY'):
                tmp_table = 'usd_jpy_30mins_tmp'
                table = 'usd_jpy_30mins'
            elif contract == Forex('EURUSD'):
                tmp_table = 'eur_usd_30mins_tmp'
                table = 'eur_usd_30mins'
        elif contract.secType == 'IND':
            bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='50 D', barSizeSetting='5 mins', whatToShow='TRADES', useRTH=True)
            if contract.symbol == 'HSI':
                tmp_table = 'hsi_5mins_tmp'
                table = 'hsi_5mins'

        # convert to pandas dataframe:
        df = util.df(bars)
        # print(df[['date', 'open', 'high', 'low', 'close']])

        engine = create_engine('postgresql+psycopg2://chesp:Chesp92J5@rm-2zelv192ymyi9680vo.pg.rds.aliyuncs.com:3432/panda_quant',echo=True,client_encoding='utf8')

        print("collecting %s" % table)
        # get last 2000 bars
        df.tail(2000).to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');

        #不再清空表
        # sql = "delete from %s;" % table

        conn = psycopg2.connect("host='rm-2zelv192ymyi9680vo.pg.rds.aliyuncs.com' dbname='panda_quant' user='chesp' password='Chesp92J5' port='3432'")
        cur = conn.cursor()

        sql = "insert into " + table + " select * from " + tmp_table +  " b where not exists (select 1 from " + table + " a where a.date = b.date);"

        cur.execute(sql, (10, 1000000, False, False))
        conn.commit()
        conn.close()

        s.enter(60, 1, get_index_30min, (date_time,))

def get_index_30min(date_time):
    for contract in contracts:
        if contract.secType == 'CASH':
            bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='45 D', barSizeSetting='30 mins', whatToShow='MIDPOINT', useRTH=True)
            if contract == Forex('USDJPY'):
                tmp_table = 'usd_jpy_30mins_tmp'
                table = 'usd_jpy_30mins'
            elif contract == Forex('EURUSD'):
                tmp_table = 'eur_usd_30mins_tmp'
                table = 'eur_usd_30mins'
        elif contract.secType == 'IND':
            bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='200 D', barSizeSetting='30 mins', whatToShow='TRADES', useRTH=True)
            if contract.symbol == 'HSI':
                tmp_table = 'hsi_30mins_tmp'
                table = 'hsi_30mins'

        # convert to pandas dataframe:
        df = util.df(bars)
        # print(df[['date', 'open', 'high', 'low', 'close']])

        engine = create_engine('postgresql+psycopg2://chesp:Chesp92J5@rm-2zelv192ymyi9680vo.pg.rds.aliyuncs.com:3432/panda_quant',echo=True,client_encoding='utf8')

        print("collecting %s" % table)
        # get last 2000 bars
        df.tail(2000).to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');

        #不再清空表
        # sql = "delete from %s;" % table

        conn = psycopg2.connect("host='rm-2zelv192ymyi9680vo.pg.rds.aliyuncs.com' dbname='panda_quant' user='chesp' password='Chesp92J5' port='3432'")
        cur = conn.cursor()

        sql = "insert into " + table + " select * from " + tmp_table +  " b where not exists (select 1 from " + table + " a where a.date = b.date);"

        cur.execute(sql, (10, 1000000, False, False))
        conn.commit()
        conn.close()

        s.enter(60, 1, get_index_1min, (date_time,))

s.enter(10, 1, get_index_1min, (s,))
s.run()
