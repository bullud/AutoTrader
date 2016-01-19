from common.oneMinute import *
from common.bid import *
from peewee import *
import datetime

class tmpBDData:
    def __init__(self, bids):
        self._bids = bids
        self._m1 = oneMinute()

class realtimeProc:
    def __init__(self, monitor_list, manager):
        self._manager = manager
        self._m1s = dict()
        self._tpDatas = dict()

        yesterday = datetime.date.today() - datetime.timedelta(1)
        #datetime.datetime.strptime(str(yesterday), '%Y-%m-%d')
        print(yesterday)

        for code in monitor_list:
            m1Entrys = oneMinute.select().where((oneMinute.code == code) & (oneMinute.minuteT > yesterday))\
                .order_by(oneMinute.minuteT.desc())

            if len(m1Entrys) == 0:
                self._m1s[code] = []
                lastBDT = yesterday

            else:
                self._m1s[code] = m1Entrys
                #load bids havn't been calc for m1

                lastBDT = m1Entrys[0].endBDT

            bids = bid.select().where((bid.code == code) & (bid.date_time > lastBDT))\
                .order_by(bid.date_time.asc())

            print(bids)
            if bids == None:
                bids = []

            self._tpDatas[code] = tmpBDData(bids)

    #def
    def proc(self, bds):
        print('proc:' + str(len(bds)))
        for bd in bds:
            tpd = self._tpDatas[bd.code]
            if tpd._m1.code == '000000':
                tpd._m1.code = bd.code
                tpd._m1.open_price   = bd.cur_price
                tpd._m1.close_price  = bd.cur_price
                tpd._m1.high_price   = bd.cur_price
                tpd._m1.low_price    = bd.cur_price
                tpd._m1.traded_share = bd.traded_share
                tpd._m1.traded_money = bd.traded_money
                tpd._m1.minuteT      = bd.date_time.minute
                tpd._m1.begBDT       =
                tpd._m1.endBDT       =

            #if bd.date_time - bdL[0].date_time > datetime.timedelta(seconds = 60):

            #else:
            #    bdL = self._bids[bd.code]
            #    bdL.append(bd)

