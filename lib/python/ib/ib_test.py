from ib_insync import *
from sqlalchemy import create_engine
import os, sys
import psycopg2
import sched, time
import datetime, pytz
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

t_z=pytz.timezone('Asia/Hong_Kong')
now = datetime.datetime.now(tz=t_z)
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

d1 = t_z.localize(result[0])
d2 = now.replace(hour=23, minute=59, second=59, microsecond=000000)
print(d2, d1)

diff = d2 - d1

print(diff)

end_date = (d1 + datetime.timedelta(0))

ib = IB()
ib.connect(host=conf["tws_ip"], port=int(conf["tws_port"]), clientId=random.randint(1,50), timeout=10, readonly=False)
contracts = [Index(symbol = "HSI", exchange = "HKFE")]
contract = contracts[0]
tmp_table = 'hsi_tmp'
table = 'hsi'
bars = ib.reqHistoricalData(contract, endDateTime=end_date, durationStr='1 D', barSizeSetting='1 min', whatToShow='TRADES', useRTH=True)
bars[-1].date.astimezone(t_z).replace(tzinfo=None)

for i in range(len(bars)):
    bars[i].date = bars[i].date.astimezone(t_z).replace(tzinfo=None)
df = util.df(bars)
