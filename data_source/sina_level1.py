#coding=utf-8
import http.client
import threading
import time
import datetime
import urllib

#from data_process.manager import *
from common.bid import *
from peewee import *

class sinaLevel1(threading.Thread):
    def __init__(self, monitor_list, interval, manager):
        threading.Thread.__init__(self)
        self.base_url = 'hq.sinajs.cn'
        self.query = '/list='
        self.interval = interval
        self.thread_stop = True
        self.manager = manager

        for code in monitor_list:
            self.query += code + ','

    def run(self):
        while self.thread_stop == False:
            time.sleep(self.interval)
            self.get_data()

    def start(self):
        self.thread_stop = False
        super().start()

    def stop(self):
        self.thread_stop = True

    def parse_data(self, data):
        bids = []
        print('begin parse')
        rows = data.split('\n')
        for row in rows:
            row = row[11:]
            bd = bid()

            print(row[2:8])
            bd.code = row[2:8]
            #print(bd.code)
            #print(bd.code)
            bd.market = row[0:2]

            subrow = row[10:len(row) - 2]
            #print(subrow)
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
                #print(bd.date_time)
                #count = bd.save()
                #if count != 1:
                #    print('save failed:' + subrow)

            bids.append(bd)

        self.manager.update('sinaL1', bids)

        #print(len(bids))
        for bi in bids:
            c = bi.save()
            if c != 1:
                print('save failed')


        print('end parse')

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
    db = SqliteDatabase('test_bids.db')

    bid._meta.database = db
    #db.connect()
    #db.create_table(bid)

    monitor_list = ['sz002466', 'sz002460', 'sz300073', 'sz000558', 'sz300151', 'sz000952', 'sz000004', 'sz002421']
    sinaL1 = sinaLevel1(monitor_list, 2, None)
    sinaL1.start()
    time.sleep(2)
    sinaL1.stop()

if __name__ == '__main__':
    main()