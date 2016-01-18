from common.oneMinute import *
from peewee import *
import datetime

class realtimeProc:
    def __init__(self, monitor_list, manager):
        self._manager = manager
        self._m1s = dict()
        self._bids = dict()
        yesterday = datetime.date.today() - datetime.timedelta(1)
        #datetime.datetime.strptime(str(yesterday), '%Y-%m-%d')
        print(yesterday)

        for code in monitor_list:
            result = oneMinute.select().where((oneMinute.code == code) & (oneMinute.date_time > yesterday))\
                .order_by(oneMinute.date_time.desc())

            if len(result) == 0:
                self._m1s[code] = None
            else:
                #print(result[0].date_time)
                self._m1s[code] = result


    def proc(self, bds):

        print(len(bds))