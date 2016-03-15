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

from data_process import DDE



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

def getL2Data(L2filepath, beginDay):
    data = None

    if os.path.exists(L2filepath) == False:
        return None

    con = sqlite3.connect(L2filepath)
    sql = ''
    if beginDay == None:
        sql = 'SELECT * from DDEs'
    else:
        sql = 'SELECT * from DDEs where date >= "' + str(beginDay) + '"'

    try:
        data = pd.read_sql(sql, con)
    except Exception as e:
        print('read L2 data exception')

    con.close()

    return data

def createTable(dbpath, schema):
    con = sqlite3.connect(dbpath)
    cur = con.cursor()
    cur.execute('create table if not exists ' + schema)
    con.commit()


def getDBPath(code, type):
    DBPath = ''
    #if type == 'L2':
    DBPath = os.path.join(_const.Level2Path2, code + ".db")

    return DBPath


def computeALL(code, tasks, threadindex):
    #print('computeALL')

    for task in tasks:
        if task == 'DDE':
            dde = DDE.DDE(_const.DDEPath)

            #print('%d, %s, loading L2 Data begin'%(threadindex, code), end='')
            begt = time.time()
            L2Data = getL2Data(getDBPath(code), 'L2', None)

            endt = time.time()
            print('%s loading L2 Data %d, time: %f'%(code, len(L2Data), endt - begt))


            if len(L2Data) == 0:
                return

            begt= time.time()
            L2Data = dde.preProcess(L2Data)
            endt = time.time()
            print('%s preProcess L2 data end, time: %f' %(code, endt - begt))

            dde.computeModes(code, L2Data, None, threadindex)


def job(args):
    print('thread: %d start' %(args._index))
    while 1:
        try:
            j = args._queue.get(block = False)
            if j[0] != '000004':
                continue
            print('thread %d: %s'%(args._index, j[0]))
            computeALL(j[0], j[1], args._index)
        except Exception as e:
            print(e)
            #print('exception raise, thread exit')
            break

    print('thread: %d exit' %(args._index))


def main(argv):
    jobqueue = queue.Queue()

    tasks = ['DDE', 'MA', 'EMA', 'MACD']

    print(_const.BasicInfoPath)
    codes = getCodes(_const.BasicInfoPath)

    for code in codes:
        jobqueue.put((code, tasks))

    Pool = threadpool.ThreadPool(_const.threadNum)

    for i in range(0,_const.threadNum):
        args = []
        args.append(pWrapper(jobqueue, i))
        request = threadpool.WorkRequest(job, args)
        Pool.putRequest(request)

    Pool.wait()


if __name__ == '__main__':
    main(sys.argv)