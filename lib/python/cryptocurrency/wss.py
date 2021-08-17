import gzip
import json
import pprint
import threading
import csv
import time
import websocket
import pandas as pd
import datetime
import time
from functools import partial

def send_message(ws, message_dict):
    data = json.dumps(message_dict).encode()
    print("Sending Message:")
    pprint.pprint(message_dict)
    ws.send(data)


def on_message(ws, message):
    unzipped_data = gzip.decompress(message).decode()
    msg_dict = json.loads(unzipped_data)
    print("Recieved Message: ")
    # pprint.pprint(msg_dict["data"])

    data = msg_dict["data"]
    id = msg_dict["id"]
    file_date = datetime.datetime.fromtimestamp(int(data[0]["id"])).strftime('%Y%m%d')
    file_name = id + "_" + file_date + ".csv"
    f = csv.writer(open(file_name, "w", newline=''))
    f.writerow(["date", "open", "high", "low", "close", "amount", "vol"])
    for d in data:
        dt = datetime.datetime.fromtimestamp(int(d["id"])).strftime('%Y-%m-%d %H:%M:%S')
        print ([dt,d["open"],d["high"],d["low"],d["close"],d["amount"],d["vol"]])
        f.writerow([dt,d["open"],d["high"],d["low"],d["close"],d["amount"],d["vol"]])

    if 'ping' in msg_dict:
        data = {
            "pong": msg_dict['ping']
        }
        send_message(ws, data)


def on_error(ws, error):
    print("Error: " + str(error))
    error = gzip.decompress(error).decode()
    print(error)


def on_close(ws):
    print("### closed ###")


def on_open(ws, c):
    def run(*args):
        # for c in usdts:
        req = "market." + c + ".kline.1min"
        begin_date = datetime.datetime(2021,6,16,0,0)
        end_date = datetime.datetime(2021,8,16,0,0)

        diff = end_date - begin_date
        for i in range(diff.days):
            begin_time = (begin_date + datetime.timedelta(i))
            print ("start")
            print (int(time.mktime(begin_time.timetuple())))
            for end_time in (begin_time + datetime.timedelta(minutes=1*it) for it in range(16)):
                end_time
            print ("end")
            print (int(time.mktime(end_time.timetuple())))

            # # 每2秒请求一次K线图，请求5次
            for i in range(2):
                time.sleep(2)
                data = {
                    "req": req,
                    "id": c,
                    "from": int(time.mktime(begin_time.timetuple())),
                    "to": int(time.mktime(end_time.timetuple())),
                }
                print (data)
                send_message(ws, data)
        ws.close()
        print("thread terminating...")

    t = threading.Thread(target=run, args=())
    t.start()


if __name__ == "__main__":
    usdts = ["fildausdt", "ektusdt", "bsvusdt", "mtausdt", "ethusdt", "utkusdt", "ankrusdt", "wnxmusdt", "xrp3lusdt", "cnnsusdt", "aacusdt", "lendusdt", "dacusdt", "bchausdt", "mirusdt", "creusdt", "phausdt", "nasusdt", "flowusdt", "skmusdt", "achusdt", "nbsusdt", "gxcusdt", "steemusdt", "neousdt", "mxcusdt", "zilusdt", "ltc3susdt", "lunausdt", "eosusdt", "eos3lusdt", "btc3susdt", "boringusdt", "gtusdt", "topusdt", "oneusdt", "dashusdt", "hptusdt", "fil3susdt", "grtusdt", "aeusdt", "abtusdt", "vsysusdt", "zec3lusdt", "xecusdt", "api3usdt", "newusdt", "seeleusdt", "kncusdt", "borusdt", "apnusdt", "akrousdt", "venusdt", "nknusdt", "yamv2usdt", "pondusdt", "uniusdt", "maticusdt", "etcusdt", "linausdt", "ctsiusdt", "smtusdt", "xmxusdt", "bixusdt", "xlmusdt", "xemusdt", "kanusdt", "lrcusdt", "antusdt", "adausdt", "swftcusdt", "dotusdt", "btc1susdt", "reefusdt", "iostusdt", "o3usdt", "dkausdt", "uuuusdt", "zec3susdt", "yfiusdt", "bch3lusdt", "fisusdt", "sntusdt", "ltc3lusdt", "hitusdt", "bntusdt", "scusdt", "1inchusdt", "xrtusdt", "iotxusdt", "lhbusdt", "jstusdt", "raiusdt", "rlcusdt", "linkusdt", "ksmusdt", "storjusdt", "dfusdt", "sunusdt", "gnxusdt", "aaveusdt", "btmusdt", "blzusdt", "axsusdt", "forthusdt", "icpusdt", "woousdt", "dogeusdt", "ogousdt", "nsureusdt", "waxpusdt", "valueusdt", "manausdt", "dot2susdt", "elausdt", "ltcusdt", "hbcusdt", "mxusdt", "firousdt", "cruusdt", "kcashusdt", "renusdt", "pvtusdt", "sandusdt", "qtumusdt", "lbausdt", "massusdt", "nftusdt", "polsusdt", "eth1susdt", "trxusdt", "dfausdt", "ckbusdt", "batusdt", "forusdt", "icxusdt", "bethusdt", "elfusdt", "bhdusdt", "mlnusdt", "ognusdt", "lambusdt", "atomusdt", "bttusdt", "irisusdt", "yeeusdt", "thetausdt", "csprusdt", "cmtusdt", "arusdt", "uni2susdt", "egtusdt", "uipusdt", "mdxusdt", "shibusdt", "bagsusdt", "crvusdt", "xchusdt", "fttusdt", "latusdt", "itcusdt", "trbusdt", "rsrusdt", "kavausdt", "injusdt", "socusdt", "dhtusdt", "sushiusdt", "wbtcusdt", "balusdt", "paiusdt", "omgusdt", "zenusdt", "auctionusdt", "dtausdt", "xtzusdt", "enjusdt", "yamusdt", "zksusdt", "umausdt", "link3lusdt", "actusdt", "canusdt", "fil3lusdt", "glmusdt", "cvcusdt", "nodeusdt", "vidyusdt", "bsv3susdt", "nuusdt", "crousdt", "sklusdt", "xrpusdt", "uni2lusdt", "chzusdt", "xrp3susdt", "stakeusdt", "iotausdt", "algousdt", "filusdt", "ruffusdt", "btc3lusdt", "rndrusdt", "bsv3lusdt", "dockusdt", "nearusdt", "clvusdt", "cvpusdt", "tnbusdt", "arpausdt", "dcrusdt", "emusdt", "nanousdt", "wtcusdt", "zecusdt", "letusdt", "mdsusdt", "ftiusdt", "ontusdt", "fsnusdt", "solusdt", "link3susdt", "yfiiusdt", "hotusdt", "rvnusdt", "btsusdt", "hbarusdt", "compusdt", "btcusdt", "htusdt", "lxtusdt", "avaxusdt", "paxusdt", "ttusdt", "atpusdt", "loomusdt", "vetusdt", "titanusdt", "stnusdt", "maskusdt", "botusdt", "nestusdt", "eth3lusdt", "wavesusdt", "lolusdt", "bch3susdt", "ocnusdt", "ctxcusdt", "chrusdt", "swrvusdt", "gofusdt", "usdcusdt", "zrxusdt", "dot2lusdt", "wiccusdt", "hcusdt", "nexousdt", "hiveusdt", "snxusdt", "insurusdt", "ringusdt", "nhbtcusdt", "pearlusdt", "mcousdt", "bandusdt", "wxtusdt", "xmrusdt", "daiusdt", "badgerusdt", "oxtusdt", "mkrusdt", "astusdt", "frontusdt", "stptusdt", "nulsusdt", "eos3susdt", "bchusdt", "eth3susdt"]
    # begin_date = datetime.datetime(2021,6,16,0,0)
    # end_date = datetime.datetime(2021,8,16,0,0)
    #
    # diff = end_date - begin_date
    # for i in range(diff.days):
    #     begin_time = (begin_date + datetime.timedelta(i))
    #     print ("start")
    #     print (int(time.mktime(begin_time.timetuple())))
    #     for end_time in (begin_time + datetime.timedelta(minutes=1*it) for it in range(16)):
    #         end_time
    #     print ("end")
    #     print (int(time.mktime(end_time.timetuple())))
    # websocket.enableTrace(True)
    for c in usdts:
        ws = websocket.WebSocketApp(
            "wss://api.huobi.pro/ws",
            # on_open=on_open,
            on_message=on_message,
            on_open=lambda ws: on_open(ws, c),
            # on_message=lambda ws: on_message(ws, message),
            on_error=on_error,
            on_close=on_close
        )
        ws.run_forever()
