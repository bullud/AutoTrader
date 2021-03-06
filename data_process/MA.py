import sys
import os
import os.path
import pandas as pd
import sqlite3
import datetime
import threading
import time

from utils import _const
from utils import config_test


diskIOlock = threading.Lock()

class MACache:
    def __init__(self, DDEPath):
        self._cache = {}
        self._DDEPath = DDEPath

    def regData(self, code, tMode, data):
        return
        #if tMode in self._cache.keys():
        #    df = self._cache[tMode]
        #    df.append(data, ignore_index=True)
        #else:
        #    self._cache[tMode] = data

    def getData(self, code, tMode, begTime):
        DBfilepath = self.getDBPath(code, tMode)

        if os.path.exists(DBfilepath) == False:
            return None

        con = sqlite3.connect(DBfilepath)
        sql = ''
        if begTime is None:
            sql = 'SELECT * from DDEs'
        else:
            sql = 'SELECT * from DDEs where date >= "' + str(begTime) + '"'

        try:
            data = pd.read_sql(sql, con)
        except Exception as e:
            print('read %s data begin from % exception' %(tMode, str(begTime)))

        con.close()

        return data

    def getDBPath(self, code, tMode):
        DBPath = os.path.join(self._DDEPath, tMode, code + '_DDE_' + tMode + '.db')

        return DBPath

class MA:
    def __init__(self, MAPath, timeModes = None):
        self._table='MAs'
        self._MAPath = MAPath
        #self._timeModes = ['M1', 'M5', 'M15', 'M30', 'M60', 'M120', 'D1', 'W1', 'mm1', 'mm3', 'mm6', 'mm12']
        self._timeModes = ['M1', 'M5', 'M15', 'M30', 'M60', 'M120', 'D1', 'W1', 'mm1', 'mm3', 'mm6', 'mm12']
        self._lastMode = {'M1':'L2',   'M5':'M1', 'M15':'M5', 'M30':'M15', 'M60':'M30', 'M120':'M60', \
                          'D1':'M120', 'W1':'D1', 'mm1':'D1', 'mm3':'mm1', 'mm6':'mm3', 'mm12':'mm6'}

        if timeModes != None:
            self._timeModes = timeModes

        for tMode in self._timeModes:
            MA_dir = os.path.join(self._MAPath, tMode)
            if os.path.exists(MA_dir) == False:
                try:
                    os.makedirs(MA_dir)
                except:
                    return

    def getDBPath(self, code, tMode, ):
        DBPath = os.path.join(self._MAPath, tMode, code + '_MA_' + tMode + '.db')

        return DBPath

    def getLastTime(self, code, tMode):
        complete = False
        lastTime = _const.minDate

        DBPath = self.getDBPath(code, tMode)

        if os.path.exists(DBPath) == False:
            return (tMode, lastTime, complete)

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
            #always recalculate the last period data for sake the possible new data

            self.cleanEntry(code, tMode, lt, None)
        #print('lastTime = '+ str(lt))
        return (tMode, lt)

    def getLastTimes(self, code):
        lastTimes = []
        for tMode in self._timeModes:
            lastTime = self.getLastTime(code, tMode)
            lastTimes.append(lastTime)

        return lastTimes

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
        print("DBPath = " + DBPath)
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

    def computeOneMode(self, data, tMode):
        def getM(t):
            step = t*60
            def getM_(x):
                #print(type(x))
                if isinstance(x, str):
                    x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')


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

        def getD(x):
            if isinstance(x, str):
                x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

            return datetime.datetime(x.year, x.month, x.day)

        def getW(x):
            if isinstance(x, str):
                x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

            return datetime.datetime(x.year, x.month, x.day) - datetime.timedelta(days = x.weekday())

        def getmm(t):
            def getmm_(x):
                if isinstance(x, str):
                    x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

                return datetime.datetime(x.year, x.month - (x.month - 1)%t, 1)

            return getmm_

        if tMode[0] == 'M':
            getTt = getM(int(tMode[1:]))
        elif tMode[0] == 'D':
            getTt = getD
        elif tMode[0] == 'W':
            getTt = getW
        elif tMode[0:2] == 'mm':
            getTt = getmm(int(tMode[2:]))
        else:
            print('not support %s timeMode' %(tMode))
            return None

        data['date'] = data['date'].apply(getTt)

        grouped = data.groupby(['date'], as_index=True)
        #print(grouped.head(10))
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

    def getLastMode(self, curtMode):
        return self._lastMode[curtMode]

    def computeModes(self, code, L2Data, lastTimes, threadindex = 0):
        cache = DDECache(self._DDEPath)
        cache.regData(code, 'L2', L2Data)

        #lastMode = 'L2'
        for lastTime in lastTimes:
            begt = time.time()
            curMode = lastTime[0]
            lastMode = self.getLastMode(curMode)
            if lastMode == 'L2':
                lastData = L2Data
            else:
                lastData = cache.getData(code, lastMode, lastTime[1])

            print(len(lastData))
            if lastData is None or len(lastData) == 0:
                lastMode = curMode
                continue

            curData = self.computeOneMode(lastData, curMode)
            #print(lastData.head(15))

            if curData is None or len(curData) == 0:
                continue

            cache.regData(code, curMode, curData)
            self.storeToDB(code, curData, curMode)
            endt = time.time()

            print("%s  computeOneMode tMode:%s end, time: %f" %(code, curMode, endt - begt))


def main(argv):
    code = '000008'
    L2Path = os.path.join(_const.Level2Pathw, code + ".db")
    con = sqlite3.connect(L2Path)

    sql = 'SELECT * from trans'

    try:
        data = pd.read_sql(sql, con)
    except Exception as e:
        print('read L2 data exception')

    con.close()

    lt1 = datetime.datetime.strptime('2015-02-05 00:00:00', "%Y-%m-%d %H:%M:%S").date()
    lt5 = datetime.datetime.strptime('1980-10-24 08:00:00', "%Y-%m-%d %H:%M:%S").date()

    #lastDays=[('M1', lt, True), ('M5', lt, True), ('M15', lt, True), ('M30', lt, True), ('M60', lt, True), ('M120', lt, True)]
    #lastDays=[('M1', lt1), ('M5', lt5)]
    lastDays=[('D1', lt1)]

    dde = DDE(_const.DDEPathw)

    dde.computeModes(code, data, lastDays)

if __name__ == '__main__':
    main(sys.argv)