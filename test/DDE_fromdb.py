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

from utils import _const

_const.DDEPath='I:\\StockData\\DDE\\'
_const.Level2Path='I:\\StockData\\level2_dst\\'

_const.small = 100000
_const.middle = 500000
_const.large = 1000000

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

#def getM_(x, step):
#    d, dt = getTime(x)
#    dti = datetime.timedelta(seconds = (dt.item() - dt.item() % step )/1000000000)
#    return d + dti

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

def getLastDay(code):
    lastDay = None
    return lastDay

def getL2Data(L2filepath, beginDay):
    data = None

    con = sqlite3.connect(L2filepath)

    sql = ''
    if beginDay == None:
        sql = 'SELECT * from trans'
    else:
        sql = 'SELECT * from trans where date >= "' + str(beginDay) + '"'

    print(sql)

    data = pd.read_sql(sql, con )
    con.close()

    return data

def main(argv):
    #codes = ['SZ002466']

    #date = pd.to_datetime('20160205')

    for parent, dirnames, filenames in os.walk(_const.Level2Path):
        for filename in filenames:
            code = filename[0:6]
            print(code)

            L2filepath = os.path.join(parent, filename)
            print(L2filepath)

            lastDay = getLastDay(code)

            if lastDay == None:
                stock = getL2Data(L2filepath, None)
            else:
                stock = getL2Data(L2filepath, lastDay + datetime.timedelta(days = 1))

            #f = lambda x: datetime.datetime.strptime(x, "%H%M%S")
            #stock['time'] = stock['time'].apply(getTime)
            print(stock.head())

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

            print(group.head())

            DDE_M1_dir = os.path.join(_const.DDEPath, 'M1\\')
            if os.path.exists(DDE_M1_dir) == False:
                os.makedirs(DDE_M1_dir)

            DDE_M1_dbpath = os.path.join(DDE_M1_dir, code + '_DDE_M1.db')

            con = sqlite3.connect(DDE_M1_dbpath)
            group.to_sql('DDEs', con, if_exists = 'replace', index = True)
            con.close()

            #return


if __name__ == '__main__':
    main(sys.argv)