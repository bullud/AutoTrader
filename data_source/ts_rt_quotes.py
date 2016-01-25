#coding=utf-8
import threading
import time
import datetime
import tushare as ts

#from data_process.manager import *
from common.bid import *
from peewee import *


class tsQuotes(threading.Thread):
    def __init__(self, monitor_list, interval, manager):
        threading.Thread.__init__(self)
        self.interval = interval
        self.thread_stop = True
        self.manager = manager
        self.query = '['

        self.shIndexCode = 'sh000001'
        self.szIndexCode = 'sz399001'
        self.cyIndexCode = 'sz399006'

        self.monitor = dict()
        for code in monitor_list:
            if code[0:2] != 'sh' and code[0:2] != 'sz':
                continue

            if code == 'sh000001':
                self.query += "'sh'" + ','
            elif code == 'sz399001':
                self.query += "'sz'" + ','
            elif code == 'sz399006':
                self.query += "'cyb'" + ','
            else:
                self.query += "'" + code[2:] + "'" + ','

        self.query += ']'

        for code in monitor_list:
            result = bid.select().where(bid.code == code).order_by(bid.date_time.desc()).limit(1)
            if len(result) == 0:
                self.monitor[code] = ''
            else:
                #print(result[0].date_time)
                self.monitor[code] = result[0].date_time.strftime('%Y-%m-%d %H:%M:%S')

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
        #print(data.head())
        return

    def get_data(self):
        try:
            print(self.query)
            query = "'sh','sz'"
            print(query)
            df = ts.get_realtime_quotes([query])
            self.parse_data(df)

        except Exception as e:
            print(e)

def main():
    db = SqliteDatabase('../bid.db')

    bid._meta.database = db

    monitor_list = ['sh000001', 'sz399001', 'sz399006', 'sh600526', 'sh600000', 'sz002466', 'sz002460', 'sz300073', 'sz000558', 'sz300151', 'sz000952', 'sz000004', 'sz002421']
    ts_quotes = tsQuotes(monitor_list, 2, None)
    ts_quotes.start()
    time.sleep(20)
    ts_quotes.stop()

if __name__ == '__main__':
    main()