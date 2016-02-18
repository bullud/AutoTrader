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

_const.DDEPath='G:\\DDE'
_const.Level2Path='G:\\level2_sqlite\\'

_const.small = 50000
_const.middle = 200000
_const.large = 1000000

def time2Str(x):
    #0 days 13:22:00.000000000
    return x[7:15]

def getTime(x):
    d = datetime.datetime.strptime(x, "%H%M%S")
    t = datetime.timedelta(hours = d.hour, minutes = d.minute, seconds = d.second)
    return t

def getM(t):
    step = t*60000000000
    f  = lambda x: datetime.timedelta(seconds = (x.item() - x.item() % step )/1000000000)
    return f,

def setSize(x):
    return abs(x) <= _const.small and 1 or (abs(x) <= _const.middle and 2 or (abs(x) <= _const.large and 3 or 4))

def getLastDay(code):
    return

def getL2Data()

def main(argv):
    #dirpath = 'G:\\level2_sqlite\\'
    #DDEpath = 'G:\\DDE'

    codes = ['SZ002466']

    date = pd.to_datetime('20160205')
    #for code in codes:

    for parent, dirnames, filenames in os.walk(_const.Level2Path):
        for filename in filenames:
            code = filename[0:8]
            print(code)

            L2filepath = os.path.join(parent, filename)
            print(L2filepath)

            lastDay = getLastDay(code)

            #stock = pd.read_csv(filepath, header=None, names=['time', 'price', 'bs', 'volumn'],\
            #                    converters={'time':str})
            stock = getL2Data(L2filepath, lastDay)

            f = lambda x: datetime.datetime.strptime(x, "%H%M%S")
        stock['time'] = stock['time'].apply(getTime)

        f = lambda x: x=='B' and 1 or -1
        stock['bs'] = stock['bs'].apply(f)

        stock['volumnN'] = stock['volumn'] * stock['bs']
        stock['amount'] = stock['volumn'] * stock['price']
        stock['amountN'] = stock['amount']* stock['bs']

        stock['size'] = stock['amount'].apply(setSize)

        getM1 = getM(t = 1)
        stock['timeIndex'] = stock['time'].apply(getM1)

        grouped = stock.groupby(['timeIndex'], as_index=True)
        group = grouped.agg({'volumn':'sum', 'volumnN':'sum', 'amount':'sum', 'amountN':'sum'})

        grouped = stock.groupby(['timeIndex', 'size'], as_index=True)
        group2 = grouped.agg({'volumn':'sum', 'volumnN':'sum', 'amount':'sum', 'amountN':'sum'})#.reset_index()


        for i in range(4, 0, -1):
            ti = group2.query('size == ' + str(i)).reset_index()
            del ti['size']
            ti.set_index('timeIndex', inplace = True)
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

        group.reset_index(inplace = True)

        group['timeIndex'] = group['timeIndex'] + date
        #group['timeIndex'] = group['timeIndex'].astype(str).apply(time2Str)
        #group['date'] = date
        #group.insert(0, 'date', group.pop('date'))

        print(group.head(96))

        DDE_M1_dbpath = os.path.join(DDEpath, 'M1\\' +  code + '_DDE_M1.db')
        con = sqlite3.connect(DDE_M1_dbpath)
        group.to_sql('DDEs', con, if_exists = 'replace', index = False)
        con.close()


if __name__ == '__main__':
    main(sys.argv)