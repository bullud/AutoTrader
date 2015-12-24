#coding=utf-8
import http.client
import threading
import time
import datetime
import urllib
from common.bid import *
from peewee import *

class SinaLevel1(threading.Thread):
    def __init__(self, monitor_list, interval):
        threading.Thread.__init__(self)
        self.base_url = 'hq.sinajs.cn'
        self.query = '/list='

        for code in monitor_list:
            self.query += code + ','

    def run(self):
        self.get_data()

    def stop(self):
        self.thread_stop = True

    def parse_data(self, data):
        rows = data.split('\n')
        for row in rows:
            row = row[11:]
            bd = bid()

            bd.code = row[2:8]
            bd.market = row[0:2]

            subrow = row[10:len(row) - 2]
            print(subrow)
            items = subrow.split(',')
            if len(items) == 33:
                bd.name = items[0]

                bd.to_open_price  = float(items[1])
                bd.ye_close_price = float(items[2])
                bd.cur_price      = float(items[3])
                bd.to_high_price  = float(items[4])
                bd.to_low_price   = float(items[5])
                #bd.buy_1_price   = items[6]
                #bd.sell_1_price  = items[7]
                bd.traded_share   = int(items[8])
                bd.traded_money   = float(items[9])

                bd.buy1_count     = int(items[10])
                bd.buy1_price     = float(items[11])
                bd.sell1_count    = int(items[20])
                bd.sell1_price    = float(items[21])

                bd.buy2_count     = int(items[12])
                bd.buy2_price     = float(items[13])
                bd.sell2_count    = int(items[22])
                bd.sell2_price    = float(items[23])

                bd.buy3_count     = int(items[14])
                bd.buy3_price     = float(items[15])
                bd.sell3_count    = int(items[24])
                bd.sell3_price    = float(items[25])

                bd.buy4_count     = int(items[16])
                bd.buy4_price     = float(items[17])
                bd.sell4_count    = int(items[26])
                bd.sell4_price    = float(items[27])

                bd.buy5_count     = int(items[18])
                bd.buy5_price     = float(items[19])
                bd.sell5_count    = int(items[28])
                bd.sell5_price    = float(items[29])

                #print(items[30] + ' ' + items[31])
                bd.date_time = datetime.datetime.strptime(items[30] + ' ' + items[31], '%Y-%m-%d %H:%M:%S')
                print(bd.date_time)
                bd.save()


    def get_data(self):
        httpClient = None
        try:
            httpClient = http.client.HTTPConnection(self.base_url)
            httpClient.request("GET", self.query)
            res = httpClient.getresponse()
            #print(res.status)
            if res.status == 200:
                data = res.read().decode('gbk')
                self.parse_data(data)
            else:
                print(res.status)
                return
        except Exception as e:
            print(e)
        finally:
            if httpClient:
                httpClient.close()

def main():
    db.connect()
    db.create_table(bid)

    monitor_list = ['sz002466']
    #, 'sz002460', 'sz300073', 'sz000558', 'sz300151', 'sz000952', 'sz000004', 'sz002421']
    sinaL1 = SinaLevel1(monitor_list, 2)
    sinaL1.start()
    time.sleep(3)
    sinaL1.stop()

if __name__ == '__main__':
    main()