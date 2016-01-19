from common.oneMinute import *
from common.bid import *
from peewee import *
import datetime
import copy

class tmpBDData:
    def __init__(self, bids):
        self._bids = bids
        self._m1 = oneMinute()
        self._last_traded_share = 0
        self._last_traded_money = 0
        self._newM1 = False

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
            print(m1Entrys)

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
            if tpd._m1.code == '000000' or (bd.date_time - tpd._m1.minuteT) > datetime.timedelta(seconds = 60):
                if tpd._m1.code != '000000':
                    m1 = copy.copy(tpd._m1)
                    self._m1s[bd.code].append(m1)
                    tpd._newM1 = True

                tpd._m1.code = bd.code
                tpd._m1.open_price   = bd.cur_price
                tpd._m1.close_price  = bd.cur_price
                tpd._m1.high_price   = bd.cur_price
                tpd._m1.low_price    = bd.cur_price
                tpd._m1.traded_share = 0
                tpd._m1.traded_money = 0
                tpd._last_traded_share = bd.traded_share
                tpd._last_traded_money = bd.traded_money
                tpd._m1.minuteT      = datetime.datetime(bd.date_time.year, \
                                                         bd.date_time.month,\
                                                         bd.date_time.day,  \
                                                         bd.date_time.hour, \
                                                         bd.date_time.minute)
                tpd._m1.begBDT       = bd.date_time
                tpd._m1.endBDT       = bd.date_time

            else:
                tpd._m1.close_price  = bd.cur_price
                if bd.cur_price > tpd._m1.high_price:
                    tpd._m1.high_price   = bd.cur_price
                if bd.cur_price < tpd._m1.low_price:
                    tpd._m1.low_price    = bd.cur_price
                tpd._m1.traded_share = bd.traded_share - tpd._last_traded_share
                tpd._m1.traded_money = bd.traded_money - tpd._last_traded_money
                tpd._m1.endBDT       = bd.date_time


