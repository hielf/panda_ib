from ib_insync import *
from sqlalchemy import create_engine
import os, sys
import psycopg2
import sched, time
import datetime
import random
import yaml

def production_config(yaml_file):
    with open(yaml_file) as f:
        config = yaml.safe_load(f)

        production_config = config['production']
    return production_config

# yaml_file = "/Users/hielf/workspace/projects/panda_ib/config/application.yml"
config_file = "config/application.yml"
current_path = os.path.abspath("../../../")
yaml_path = os.path.join(current_path, config_file)
conf = production_config(yaml_path)

ib = IB()
# ib.connect('127.0.0.1', 7496, clientId=100)
# ib.connect('129.226.51.237', 7497, clientId=101)
ib.connect(host=conf["tws_ip"], port=int(conf["tws_port"]), clientId=random.randint(1,50), timeout=10, readonly=False)

# contracts = [Index(symbol = "HSI", exchange = "HKFE"), Index(symbol = "SPX", exchange = "CBOE"), Forex('USDJPY'), Forex('EURUSD'), Contract(exchange = "ECBOT", secType = "CONTFUT", symbol = "YM")]
# contracts = [Contract(exchange = "ECBOT", secType = "CONTFUT", symbol = "YM")]
contracts = [Index(symbol = "HSI", exchange = "HKFE")]
# bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='1 D',
#         barSizeSetting='1 min', whatToShow='TRADES', useRTH=True)

def get_index_5sec(end_datetime):
    print("start 5sec collect %s" % str(end_datetime))
    for contract in contracts:
        print(str(contract))
        conn_str = "host='{}' dbname='{}' user='{}' password='{}' port='{}'"
        conn_str = conn_str.format(conf['quant_db_host'], conf['quant_db_name'], conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_port'])
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        if contract.secType == 'IND':
            if contract.symbol == 'HSI':
                tmp_table = 'hsi_5secs_tmp'
                table = 'hsi_5secs'
            bars = ib.reqHistoricalData(contract, endDateTime=end_datetime, durationStr='14400 S', barSizeSetting='5 secs', whatToShow='TRADES', useRTH=True)
        df = util.df(bars)
        print("got bars %s" % str(bars))
        print("got contract %s" % str(contract))
        engine_str = "postgresql+psycopg2://{}:{}@{}:{}/{}"
        engine_str = engine_str.format(conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_host'], conf['quant_db_port'], conf['quant_db_name'])
        engine = create_engine(engine_str,echo=True,client_encoding='utf8')
        print("waiting for collect %s" % table)
        try:
            df.to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');
            sql = "insert into " + table + " select * from " + tmp_table +  " b where not exists (select 1 from " + table + " a where a.date = b.date);"
            cur.execute(sql, (10, 1000000, False, False))
            conn.commit()
            conn.close()
        except:
            print ("Unexpected error:", sys.exc_info()[0])
            continue

def get_index_15sec(end_datetime):
    print("start 15sec collect %s" % str(end_datetime))
    for contract in contracts:
        print(str(contract))
        conn_str = "host='{}' dbname='{}' user='{}' password='{}' port='{}'"
        conn_str = conn_str.format(conf['quant_db_host'], conf['quant_db_name'], conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_port'])
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        if contract.secType == 'CASH':
            if contract.symbol == 'USD' and contract.currency == 'JPY':
                tmp_table = 'usd_jpy_15secs_tmp'
                table = 'usd_jpy_15secs'
            elif contract.symbol == 'EUR' and contract.currency == 'USD':
                tmp_table = 'eur_usd_15secs_tmp'
                table = 'eur_usd_15secs'
            bars = ib.reqHistoricalData(contract, endDateTime=end_datetime, durationStr='14400 S', barSizeSetting='15 secs', whatToShow='MIDPOINT', useRTH=True)
        elif contract.secType == 'IND':
            if contract.symbol == 'HSI':
                tmp_table = 'hsi_15secs_tmp'
                table = 'hsi_15secs'
            elif contract.symbol == 'SPX':
                tmp_table = 'spx_15secs_tmp'
                table = 'spx_15secs'
            bars = ib.reqHistoricalData(contract, endDateTime=end_datetime, durationStr='14400 S', barSizeSetting='15 secs', whatToShow='TRADES', useRTH=True)
        elif contract.secType == 'CONTFUT':
            if contract.symbol == 'YM':
                tmp_table = 'ym_15secs_tmp'
                table = 'ym_15secs'
            bars = ib.reqHistoricalData(contract, endDateTime=end_datetime, durationStr='14400 S', barSizeSetting='15 secs', whatToShow='TRADES', useRTH=True)
        df = util.df(bars)
        print("got bars %s" % str(bars))
        print("got contract %s" % str(contract))
        engine_str = "postgresql+psycopg2://{}:{}@{}:{}/{}"
        engine_str = engine_str.format(conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_host'], conf['quant_db_port'], conf['quant_db_name'])
        engine = create_engine(engine_str,echo=True,client_encoding='utf8')
        print("waiting for collect %s" % table)
        try:
            df.to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');
            sql = "insert into " + table + " select * from " + tmp_table +  " b where not exists (select 1 from " + table + " a where a.date = b.date);"
            cur.execute(sql, (10, 1000000, False, False))
            conn.commit()
            conn.close()
        except:
            print ("Unexpected error:", sys.exc_info()[0])
            continue

def get_index_30sec(end_date):
    print("start 30sec collect %s" % str(end_date))
    for contract in contracts:
        print(str(contract))
        conn_str = "host='{}' dbname='{}' user='{}' password='{}' port='{}'"
        conn_str = conn_str.format(conf['quant_db_host'], conf['quant_db_name'], conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_port'])
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        if contract.secType == 'CASH':
            if contract.symbol == 'USD' and contract.currency == 'JPY':
                tmp_table = 'usd_jpy_30secs_tmp'
                table = 'usd_jpy_30secs'
            elif contract.symbol == 'EUR' and contract.currency == 'USD':
                tmp_table = 'eur_usd_30secs_tmp'
                table = 'eur_usd_30secs'
            bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='30 secs', whatToShow='MIDPOINT', useRTH=True)
        elif contract.secType == 'IND':
            if contract.symbol == 'HSI':
                tmp_table = 'hsi_30secs_tmp'
                table = 'hsi_30secs'
            elif contract.symbol == 'SPX':
                tmp_table = 'spx_30secs_tmp'
                table = 'spx_30secs'
            bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='30 secs', whatToShow='TRADES', useRTH=True)
        elif contract.secType == 'CONTFUT':
            if contract.symbol == 'YM':
                tmp_table = 'ym_30secs_tmp'
                table = 'ym_30secs'
            bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='30 secs', whatToShow='TRADES', useRTH=True)
        df = util.df(bars)
        print("got bars %s" % str(bars))
        print("got contract %s" % str(contract))
        engine_str = "postgresql+psycopg2://{}:{}@{}:{}/{}"
        engine_str = engine_str.format(conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_host'], conf['quant_db_port'], conf['quant_db_name'])
        engine = create_engine(engine_str,echo=True,client_encoding='utf8')
        print("waiting for collect %s" % table)
        try:
            df.to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');
            sql = "insert into " + table + " select * from " + tmp_table +  " b where not exists (select 1 from " + table + " a where a.date = b.date);"
            cur.execute(sql, (10, 1000000, False, False))
            conn.commit()
            conn.close()
        except:
            print ("Unexpected error:", sys.exc_info()[0])
            continue

def get_index_1min(end_date):
    print("start 1min collect %s" % str(end_date))
    for contract in contracts:
        print(str(contract))
        conn_str = "host='{}' dbname='{}' user='{}' password='{}' port='{}'"
        conn_str = conn_str.format(conf['quant_db_host'], conf['quant_db_name'], conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_port'])
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        if contract.secType == 'CASH':
            if contract.symbol == 'USD' and contract.currency == 'JPY':
                tmp_table = 'usd_jpy_tmp'
                table = 'usd_jpy'
            elif contract.symbol == 'EUR' and contract.currency == 'USD':
                tmp_table = 'eur_usd_tmp'
                table = 'eur_usd'
            bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='1 min', whatToShow='MIDPOINT', useRTH=True)
        elif contract.secType == 'IND':
            if contract.symbol == 'HSI':
                tmp_table = 'hsi_tmp'
                table = 'hsi'
            elif contract.symbol == 'SPX':
                tmp_table = 'spx_tmp'
                table = 'spx'
            bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='1 min', whatToShow='TRADES', useRTH=True)
        elif contract.secType == 'CONTFUT':
            if contract.symbol == 'YM':
                tmp_table = 'ym_tmp'
                table = 'ym'
            bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='1 min', whatToShow='TRADES', useRTH=True)
        df = util.df(bars)
        print("got bars %s" % str(bars))
        print("got contract %s" % str(contract))
        engine_str = "postgresql+psycopg2://{}:{}@{}:{}/{}"
        engine_str = engine_str.format(conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_host'], conf['quant_db_port'], conf['quant_db_name'])
        engine = create_engine(engine_str,echo=True,client_encoding='utf8')
        print("waiting for collect %s" % table)
        try:
            df.to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');
            sql = "insert into " + table + " select * from " + tmp_table +  " b where not exists (select 1 from " + table + " a where a.date = b.date);"
            cur.execute(sql, (10, 1000000, False, False))
            conn.commit()
            conn.close()
        except:
            print ("Unexpected error:", sys.exc_info()[0])
            continue


def get_index_5min(end_date):
    print("start 5min collect %s" % str(end_date))
    for contract in contracts:
        print(str(contract))
        conn_str = "host='{}' dbname='{}' user='{}' password='{}' port='{}'"
        conn_str = conn_str.format(conf['quant_db_host'], conf['quant_db_name'], conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_port'])
        conn = psycopg2.connect(conn_str)
        cur = conn.cursor()
        if contract.secType == 'CASH':
            if contract.symbol == 'USD' and contract.currency == 'JPY':
                tmp_table = 'usd_jpy_5mins_tmp'
                table = 'usd_jpy_5mins'
            elif contract.symbol == 'EUR' and contract.currency == 'USD':
                tmp_table = 'eur_usd_5mins_tmp'
                table = 'eur_usd_5mins'
            bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='5 mins', whatToShow='MIDPOINT', useRTH=True)
        elif contract.secType == 'IND':
            if contract.symbol == 'HSI':
                tmp_table = 'hsi_5mins_tmp'
                table = 'hsi_5mins'
            elif contract.symbol == 'SPX':
                tmp_table = 'spx_5mins_tmp'
                table = 'spx_5mins'
            bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='5 mins', whatToShow='TRADES', useRTH=True)
        elif contract.secType == 'CONTFUT':
            if contract.symbol == 'YM':
                tmp_table = 'ym_5mins_tmp'
                table = 'ym_5mins'
            bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='5 mins', whatToShow='TRADES', useRTH=True)
        df = util.df(bars)
        print("got bars %s" % str(bars))
        print("got contract %s" % str(contract))
        engine_str = "postgresql+psycopg2://{}:{}@{}:{}/{}"
        engine_str = engine_str.format(conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_host'], conf['quant_db_port'], conf['quant_db_name'])
        engine = create_engine(engine_str,echo=True,client_encoding='utf8')
        print("waiting for collect %s" % table)
        try:
            df.to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');
            sql = "insert into " + table + " select * from " + tmp_table +  " b where not exists (select 1 from " + table + " a where a.date = b.date);"
            cur.execute(sql, (10, 1000000, False, False))
            conn.commit()
            conn.close()
        except:
            print ("Unexpected error:", sys.exc_info()[0])
            continue


# def get_index_30min(date_time):
#     for contract in contracts:
#         conn_str = "host='{}' dbname='{}' user='{}' password='{}' port='{}'"
        # conn_str = conn_str.format(conf['quant_db_host'], conf['quant_db_name'], conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_port'])
        # conn = psycopg2.connect(conn_str)
#         cur = conn.cursor()
#         if contract.secType == 'CASH':
#             bars = ib.reqHistoricalData(contract, endDateTime='', durationStr='45 D', barSizeSetting='30 mins', whatToShow='MIDPOINT', useRTH=True)
#             if contract == Forex('USDJPY'):
#                 tmp_table = 'usd_jpy_30mins_tmp'
#                 table = 'usd_jpy_30mins'
#             elif contract == Forex('EURUSD'):
#                 tmp_table = 'eur_usd_30mins_tmp'
#                 table = 'eur_usd_30mins'
#         elif contract.secType == 'IND':
#             if contract.symbol == 'HSI':
#                 tmp_table = 'hsi_30mins_tmp'
#                 table = 'hsi_30mins'
#             sql = "select min(date) from %s;" % table
#             cur.execute(sql, (10, 1000000, False, False))
#             rows = cur.fetchall()
#             end_date = rows[0][0]
#             bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='200 D', barSizeSetting='30 mins', whatToShow='TRADES', useRTH=True)
#
#         # convert to pandas dataframe:
#         df = util.df(bars)
#         # print(df[['date', 'open', 'high', 'low', 'close']])
#
#         engine_str = "postgresql+psycopg2://{}:{}@{}:{}/{}"
        # engine_str = engine_str.format(conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_host'], conf['quant_db_port'], conf['quant_db_name'])
        # engine = create_engine(engine_str,echo=True,client_encoding='utf8')
#
#         print("collecting %s" % table)
#         # get last 2000 bars
#         df.tail(2000).to_sql(tmp_table,engine,chunksize=1000,if_exists='replace');
#
#         #不再清空表
#         # sql = "delete from %s;" % table
#
#         sql = "insert into " + table + " select * from " + tmp_table +  " b where not exists (select 1 from " + table + " a where a.date = b.date);"
#
#         cur.execute(sql, (10, 1000000, False, False))
#         conn.commit()
#         conn.close()
#
#         s.enter(60, 1, get_index_1min, (date_time,))

if __name__ == '__main__':
    now = datetime.datetime.now()
    # d1 = datetime.datetime(2020,12,16,0,0)
    conn_str = "host='{}' dbname='{}' user='{}' password='{}' port='{}'"
    conn_str = conn_str.format(conf['quant_db_host'], conf['quant_db_name'], conf['quant_db_user'], conf['quant_db_pwd'], conf['quant_db_port'])
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    sql = "select max(date) from hsi;"
    cur.execute(sql)
    result = cur.fetchone()
    print (result)
    conn.commit()
    conn.close()

    d1 = result[0]
    if d1:
        pass
    else:
        d1 = (datetime.datetime.now() - datetime.timedelta(days=180)).replace(hour=23, minute=59, second=59, microsecond=000000)

    d2 = now.replace(hour=23, minute=59, second=59, microsecond=000000)
    diff = d2 - d1
    for i in range(diff.days + 1):
        end_date = (d1 + datetime.timedelta(i))
        print (end_date)
        print ("=========================")
        get_index_1min(end_date)
        # get_index_5min(end_date)
        # get_index_30sec(end_date)
        for j in range(6):
            end_datetime = (end_date + datetime.timedelta(j/6))
            print (end_datetime)
            # get_index_5sec(end_datetime)
            get_index_15sec(end_datetime)
