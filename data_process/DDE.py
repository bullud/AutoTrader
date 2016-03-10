import sys
import getopt
import os
import os.path
import pandas as pd
from pandas import DataFrame
from peewee import *
import sqlite3
import datetime
import math
import queue
import threadpool
import threading

from utils import _const

_const.threadNum = 4
_const.BasicInfoPath='I:\\StockData\\BasicInfo\\basics.sqlite'
_const.DDEPath='I:\\StockData\\DDE\\'
_const.Level2Path='I:\\StockData\\level2_dst\\'

_const.small = 100000
_const.middle = 500000
_const.large = 1000000

#paramWrapper for thread
class pWrapper:
    def __init__(self, queue = None, index = None):
        self._queue = queue
        self._index = index

def getCodes(dbpath):
    con = sqlite3.connect(dbpath)
    sql = "SELECT code from basics"
    codes = pd.read_sql(sql, con)
    con.close()
    if codes is not None:
        return codes['code']
    else:
        return []


def cleanEntry(tpoint, tspan, table, dbpath):
    con = sqlite3.connect(dbpath)
    cursor = con.cursor()
    print(dbpath)

    if tspan is not None:
        sql = 'DELETE from ' + table + ' where date >= "' + str(tpoint) + '" and date < "' + str(tpoint + tspan) + '"'
    else:
        sql = 'DELETE from ' + table + ' where date >= "' + str(tpoint) + '"'

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

def getM(t):
    step = t*60

    def getM_(x):

        x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

        allseconds = x.hour*3600 + x.minute*60 + x.second

        dti = datetime.timedelta(seconds = allseconds - allseconds % step )

        d = datetime.datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d')

        time = d + dti

        return time

    #f  = lambda x: datetime.timedelta(seconds = (x.item() - x.item() % step )/1000000000)

    return getM_

def setSize(x):
    return abs(x) <= _const.small and 1 or (abs(x) <= _const.middle and 2 or (abs(x) <= _const.large and 3 or 4))

def getLastDay(table, dbpath):
    complete = False
    lastTime = None

    if os.path.exists(dbpath) == False:
        return lastTime, complete

    sql = "SELECT date from " + table +  " ORDER by date DESC LIMIT 1"
    con = sqlite3.connect(dbpath)

    try:
        lastTime = pd.read_sql(sql, con)
    except Exception as e:
        print(e)

    if lastTime is not None and len(lastTime) != 0:
        #read_sql默认读出的date数据类型是str，需要转换为datetime类型
        lt = datetime.datetime.strptime(lastTime['date'][0], "%Y-%m-%d %H:%M:%S")

        #print('hour = ' + str(lt.hour))
        if lt.hour == 15:
            #暂不考虑熔断提前收盘的情况,默认15点收盘
            complete = True
        else:
            complete = False

        lastDay = lt.date()
    else:
        lastDay = None

    con.close()

    return lastDay, complete

def getL2Data(L2filepath, beginDay):
    data = None

    if os.path.exists(L2filepath) == False:
        return None

    con = sqlite3.connect(L2filepath)
    sql = ''
    if beginDay == None:
        sql = 'SELECT * from trans'
    else:
        sql = 'SELECT * from trans where date >= "' + str(beginDay) + '"'

    try:
        data = pd.read_sql(sql, con)
    except Exception as e:
        print('read L2 data exception')

    con.close()

    return data

def computeDDE(code, threadindex):
    L2filepath = os.path.join(_const.Level2Path, code + ".db")
    print(str(threadindex) + " Processing " + L2filepath)

    DDE_M1_dir = os.path.join(_const.DDEPath, 'M1\\')
    if os.path.exists(DDE_M1_dir) == False:
        os.makedirs(DDE_M1_dir)

    DDE_M1_dbpath = os.path.join(DDE_M1_dir, code + '_DDE_M1.db')
    #createTable(DDE_M1_dbpath, 'DDEs(date TIMESTAMP, price REAL, bs TEXT, volumn INTEGER )')

    lastDay, complete = getLastDay('DDEs', DDE_M1_dbpath)

    if lastDay == None:
        print(code + ' no LastDay data')
        stock = getL2Data(L2filepath, None)
    else:
        if complete == True:
            stock = getL2Data(L2filepath, lastDay + datetime.timedelta(days = 1))
        else:
            stock = getL2Data(L2filepath, lastDay)

    if stock is None or len(stock) == 0:
        print(code + ' no L2 data')
        return

    print(stock.head())

    if lastDay is not None and complete == False:
        cleanEntry(lastDay, datetime.timedelta(days = 1), 'DDEs', DDE_M1_dbpath)

    f = lambda x: x=='B' and 1 or -1
    stock['bs'] = stock['bs'].apply(f)

    stock['volumnN'] = stock['volumn'] * stock['bs']
    stock['amount'] = stock['volumn'] * stock['price']
    stock['amountN'] = stock['amount']* stock['bs']

    stock['size'] = stock['amount'].apply(setSize)

    getM1 = getM(t = 1)
    stock['date'] = stock['date'].apply(getM1)
    print(stock.head())

    grouped = stock.groupby(['date'], as_index=True)
    group = grouped.agg({'volumn':'sum', 'volumnN':'sum', 'amount':'sum', 'amountN':'sum'})

    grouped = stock.groupby(['date', 'size'], as_index=True)
    group2 = grouped.agg({'volumn':'sum', 'volumnN':'sum', 'amount':'sum', 'amountN':'sum'})#.reset_index()

    for i in range(4, 0, -1):
        ti = group2.query('size == ' + str(i)).reset_index()
        del ti['size']
        ti.set_index('date', inplace = True)
        volumnTi = 'volumnT' + str(i)
        volumnNTi = 'volumnNT' + str(i)
        amountTi = 'amountT' + str(i)
        amountNTi = 'amountNT' + str(i)

        t = ti.reindex(group.index, fill_value = 0)

        group.insert(0, amountNTi, t['amountN'])
        group.insert(0, amountTi, t['amount'])
        group.insert(0, volumnNTi, t['volumnN'])
        group.insert(0, volumnTi, t['volumn'])

    group.insert(0, 'amountN', group.pop('amountN'))
    group.insert(0, 'amount', group.pop('amount'))
    group.insert(0, 'volumnN', group.pop('volumnN'))
    group.insert(0, 'volumn', group.pop('volumn'))

    #group.reset_index(inplace = True)

    #group['timeIndex'] = group['timeIndex']

    con = sqlite3.connect(DDE_M1_dbpath)
    group.to_sql('DDEs', con, if_exists = 'append', index = True)
    con.close()


def job(args):
    print('thread: %d start' %(args._index))
    while 1:
        try:
            code = args._queue.get(block = False)
            computeDDE(code, args._index)
        except Exception as e:
            print(e)
            #print('exception raise, thread exit')
            break

    print('thread: %d exit' %(args._index))

def main(argv):
    jobqueue = queue.Queue()

    codes = getCodes(_const.BasicInfoPath)
    beg = 1000
    end = 10000
    i = 0
    for code in codes:
        if i < beg or i >= end:
            i+=1
            continue
        i+=1
        jobqueue.put(code)

    Pool = threadpool.ThreadPool(_const.threadNum)

    for i in range(0,_const.threadNum):
        args = []
        args.append(pWrapper(jobqueue, i))
        request = threadpool.WorkRequest(job, args)
        Pool.putRequest(request)

    Pool.wait()


if __name__ == '__main__':
    main(sys.argv)