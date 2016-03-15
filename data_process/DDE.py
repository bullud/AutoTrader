import sys
import getopt
import os
import os.path
import pandas as pd
from pandas import DataFrame
#from peewee import *
import sqlite3
import datetime
import math
import queue
import threadpool
import threading
import time

from utils import _const
from utils import config_test


diskIOlock = threading.Lock()

#paramWrapper for thread
class pWrapper:
    def __init__(self, queue = None, index = None):
        self._queue = queue
        self._index = index

class DDE:
    def __init__(self, DDEPath, timeModes = None):
        self._table='DDEs'
        self._DDEPath = DDEPath
        #self._timeModes = ['M1', 'M5', 'M15', 'M30', 'M60', 'M120', 'D1', 'W1', 'mm1', 'mm3', 'mm6', 'mm12']
        self._timeModes = ['M1', 'M5', 'M15', 'M30', 'M60', 'M120']

        if timeModes != None:
            self._timeModes = timeModes

        for tMode in self._timeModes:
            DDE_M_dir = os.path.join(self._DDEPath, tMode)
            if os.path.exists(DDE_M_dir) == False:
                try:
                    os.makedirs(DDE_M_dir)
                except:
                    return

    def getDBPath(self, code, tMode):
        DBPath = os.path.join(self._DDEPath, tMode, code + '_DDE_' + tMode + '.db')

        return DBPath

    def getLastDay(self, code, tMode):
        complete = False
        lastDay = _const.minDate

        DBPath = self.getDBPath(code, tMode)

        if os.path.exists(DBPath) == False:
            return (tMode, lastDay, complete)

        sql = "SELECT date from " + self._table +  " ORDER by date DESC LIMIT 1"

        con = sqlite3.connect(DBPath)

        lastTime = None
        try:
            lastTime = pd.read_sql(sql, con)
        except Exception as e:
            print(e)
        con.close()
        if lastTime is not None and len(lastTime) != 0:
            #read_sql默认读出的date数据类型是str，需要转换为datetime类型
            lt = datetime.datetime.strptime(lastTime['date'][0], "%Y-%m-%d %H:%M:%S")
            #don't make a completion check
            #complete = True
            #print('hour = ' + str(lt.hour))
            #print(str(lt))
            endT = lt + datetime.timedelta(minutes = int(tMode[1:]))
            #print(str(endT))
            if endT.hour == 15:
                #暂不考虑熔断提前收盘的情况,默认15点收盘
                complete = True
                lastDay = lt.date()
            else:
                complete = False
                self.cleanEntry(code, tMode, lt.date(), datetime.timedelta(days = 1))
                lastDay = lt.date() - datetime.timedelta(days=1)


        return (tMode, lastDay)

    def getLastDays(self, code):
        lastDays = []
        for tMode in self._timeModes:
            lastDay = self.getLastDay(code, tMode)
            lastDays.append(lastDay)

        return lastDays

    def cleanEntry(self, code, tMode, tpoint, tspan):
        DBPath = self.getDBPath(code, tMode)
        con = sqlite3.connect(DBPath)
        cursor = con.cursor()
        print(DBPath)

        if tspan is not None:
            sql = 'DELETE from ' + self._table + ' where date >= "' + str(tpoint) + '" and date < "' + str(tpoint + tspan) + '"'
        else:
            sql = 'DELETE from ' + self._table + ' where date >= "' + str(tpoint) + '"'

        print(sql)
        cursor.execute(sql)
        con.commit()

        print(cursor.rowcount)
        cursor.close()

        con.close()

    def createTable(dbpath, schema):
        con = sqlite3.connect(dbpath)
        cur = con.cursor()
        cur.execute('create table if not exists ' + schema)
        con.commit()

    def time2Str(x):
        #0 days 13:22:00.000000000
        return x[7:15]

    def getTime(x):
        x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        print(x, type(x))
        dt = datetime.timedelta(hours = x.hour, minutes = x.minute, seconds = x.second)
        #print(dt.item())
        d = x.date()
        return d, dt

    def storeToDB(self, code, data, tMode):
        DBPath = self.getDBPath(code, tMode)
        #print("DBPath = " + DBPath)
        con = sqlite3.connect(DBPath)
        data.to_sql(self._table, con, if_exists = 'append', index = False)
        con.close()

    def preProcess(self, data):
        f = lambda x: x=='B' and 1 or -1
        data['bs'] = data['bs'].apply(f)

        f2= lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        data['date'] = data['date'].apply(f2)

        data['amount']  = data['volumn'] * data['price']
        data['volumnN'] = data['volumn'] * data['bs']
        data['amountN'] = data['amount'] * data['bs']

        def setSize(x):
            return abs(x) <= _const.small and 1 or (abs(x) <= _const.middle and 2 or (abs(x) <= _const.large and 3 or 4))

        data['size'] = data['amount'].apply(setSize)

        #print(data.head(5))
        datacopy = data
        for i in range(1, 5, 1):
            ti = datacopy.query('size == ' + str(i))
            del ti['date']
            del ti['price']
            del ti['bs']
            del ti['size']

            ti.insert(0, 'amountN'  + str(i), ti.pop('amountN'))
            ti.insert(0, 'volumnN'  + str(i), ti.pop('volumnN'))
            ti.insert(0, 'amount'   + str(i), ti.pop('amount'))
            ti.insert(0, 'volumn'   + str(i), ti.pop('volumn'))

            #print(ti.head(5))
            data = pd.concat([data, ti], axis = 1)

        del data['size']
        del data['price']
        del data['bs']
        data = data.fillna(0)

        return data

    def computeOneMode(self, data, dMode, tMode):
        def getM(t):
            step = t*60
            def getM_(x):
                #x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
                if x.hour == 9 and x.minute < 30:
                    allseconds = x.hour*3600 + 30*60
                elif x.hour == 15:
                    allseconds = (x.hour - 1 )*3600 + 59*60
                else:
                    allseconds = x.hour*3600 + x.minute*60 + x.second

                if x.hour < 13:
                    deltaseconds = allseconds - (9*3600 + 30*60)

                    dti = datetime.timedelta(seconds = deltaseconds - deltaseconds % step )
                    d = datetime.datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d')
                    time = d + datetime.timedelta(seconds = 9*3600 + 30*60) + dti
                else:
                    deltaseconds = allseconds - (13*3600)

                    dti = datetime.timedelta(seconds = deltaseconds - deltaseconds % step )
                    d = datetime.datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d')
                    time = d + datetime.timedelta(seconds = 13*3600) + dti

                return time
            #f  = lambda x: datetime.timedelta(seconds = (x.item() - x.item() % step )/1000000000)
            return getM_

        if tMode[0] == 'M':
            getMt = getM(int(tMode[1:]))
        else:
            print('only support M timeMode')
            return

        data['date'] = data['date'].apply(getMt)

        grouped = data.groupby(['date'], as_index=True)

        group = grouped.agg('sum')

        group.reset_index(inplace = True)

        #group['timeIndex'] = group['timeIndex']
        return group

    def checkModes(self, tModes):
        t = 0
        for tMode in tModes:
            if t < int(tMode[1:]):
                t = int(tMode[1:])
                continue
            else:
                return False
        return  True

    def computeModes(self, code, L2Data, lastDays, threadindex = 0):
        tModes = []
        maxDay = _const.minDate
        for lastDay in lastDays:
            tModes.append(lastDay[0])
            if lastDay[1] > maxDay:
                maxDay = lastDay[1]

       # print('maxDay = ' + str(maxDay))

        if self.checkModes(tModes) == False:
            print('the tmode sequence is not correct')
            print(tModes)

        for lastDay in lastDays:
            if lastDay[1] < maxDay:
                #print("process single tMode:%s" %(lastDay[0]))
                begt = time.time()
                #queryStr = 'date > "' + str(lastDay[1]) + '" & ' + 'date <= "' + str(maxDay) + '"'
                queryStr = '"' + str(lastDay[1]) + '" < date <= "' + str(maxDay) + '"'
                print(queryStr)

                data = L2Data.query(queryStr).copy(True)
                result = self.computeOneMode(data, 'L2', lastDay[0])
                #print(result.head(15))
                #print(result.tail(15))
                self.storeToDB(code, result, lastDay[0])
                endt = time.time()


                print("%s process single tMode:%s end, time: %f" %(code, lastDay[0], endt - begt))

        print(str(maxDay))
        lastMode = 'L2'
        lastData = L2Data.query('date > "' + str(maxDay + datetime.timedelta(days = 1)) + '"').copy(True)
        #lastData = L2Data[L2Data.date > maxDay].copy(True)
        if len(lastData) == 0:
            return
        print(len(lastData))

        print(lastData.head())
        for lastDay in lastDays:
            begt = time.time()

            lastData = self.computeOneMode(lastData, lastMode, lastDay[0])
            #print(lastData.head(15))
            #print(lastData.tail(15))
            self.storeToDB(code, lastData, lastDay[0])
            lastMode = lastDays[0]
            endt = time.time()

            print("%s  computeOneMode tMode:%s end, time: %f" %(code, lastDay[0], endt - begt))


def main(argv):
    code = '000008'
    L2Path = os.path.join(_const.Level2Path, code + ".db")
    con = sqlite3.connect(L2Path)

    sql = 'SELECT * from trans'

    try:
        data = pd.read_sql(sql, con)
    except Exception as e:
        print('read L2 data exception')

    con.close()

    lt1 = datetime.datetime.strptime('2015-02-05 00:00:00', "%Y-%m-%d %H:%M:%S")
    lt5 = datetime.datetime.strptime('1980-10-24 08:00:00', "%Y-%m-%d %H:%M:%S")

    #lastDays=[('M1', lt, True), ('M5', lt, True), ('M15', lt, True), ('M30', lt, True), ('M60', lt, True), ('M120', lt, True)]
    lastDays=[('M1', lt1), ('M5', lt5)]

    dde = DDE(_const.DDEPath)

    dde.computeModes(code, data, lastDays)

if __name__ == '__main__':
    main(sys.argv)