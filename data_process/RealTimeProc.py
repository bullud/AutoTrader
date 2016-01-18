from common.oneMinute import *
from peewee import *

class realtimeProc:
    def __init__(self, monitor_list, manager):
        self._manager = manager

        for code in monitor_list:
            result = oneMinute.select().where(oneMinute.code == code).order_by(oneMinute.date_time.desc()).limit(1)
            if len(result) == 0:
                self.monitor[code] = ''
            else:
                #print(result[0].date_time)
                self.monitor[code] = result[0].date_time.strftime('%Y-%m-%d %H:%M:%S')

    def proc(self, bds):

        print(len(bds))