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

_const.small = 50000
_const.middle = 200000
_const.large = 1000000

def getTime(x):
    d = datetime.datetime.strptime(x, "%H%M%S")
    t = datetime.timedelta(hours = d.hour, minutes = d.minute, seconds = d.second)
    return t

def getM(t):
    step = t*60000000000
    f  = lambda x: datetime.timedelta(seconds = (x.item() - x.item() % step )/1000000000)
    return f

def setType(x):
    return abs(x) <= _const.small and 1 or (abs(x) <= _const.middle and 2 or (abs(x) <= _const.large and 3 or 4))

def main(argv):
    dirpath = 'E:\\BaiduYunDownload\\level2\\2016\\201602\\20160205\\'
    files = ['SZ002466']
    DDEpath = 'E:\\BaiduYunDownload\\DDE'

    for file in files:
        filepath = dirpath + file + '.csv'
        print(filepath)
        stock = pd.read_csv(filepath, header=None, names=['time', 'price', 'bs', 'volumn'],\
                            converters={'time':str} )
        #print(stock.head())

        f = lambda x: datetime.datetime.strptime(x, "%H%M%S")
        stock['time'] = stock['time'].apply(getTime)

        getM1 = getM(t = 1)
        stock['timeM1'] = stock['time'].apply(getM1)

        f = lambda x: x=='B' and 1 or -1
        stock['bs'] = stock['bs'].apply(f)

        stock['amount'] = stock['volumn'] * stock['price']
        stock['amountN'] = stock['amount']* stock['bs']

        stock['type'] = stock['amount'].apply(setType)

        #stock['amount'] = ddf.apply(f, axis = 1)
        grouped = stock.groupby(['timeM1'], as_index=True)
        group = grouped.agg({'amount':'sum', 'amountN':'sum'})
        #print(group.tail(96))

        grouped = stock.groupby(['timeM1', 'type'], as_index=True)
        group2 = grouped.agg({'amount':'sum', 'amountN':'sum'})#.reset_index()
        #print(group2.index)
        #re = group2.reindex( level='type', columns= group2.columns, fill_value=0)
        #print(re)
        #group2.set_index('time')
        t4 = group2.query('type == 4').reset_index()
        del t4['type']
        t4.set_index('timeM1', inplace = True)

        t4.columns = ['amountT4', 'amountNT4']

        t = t4.reindex(group.index, fill_value = 0)
        group['amountT4'] = t['amountT4']
        group['amountNT4'] = t['amountNT4']
        print(group)
        #t = t4 + group
        #print(t)
        #group['amountT1'] = group2[group2.type == 1]['amount']
        #group['amountT2'] = group2[group2.type == 2]
        #group['amountT3'] = group2[group2.type == 3]
        #group['amountT4'] = group2[group2.type == 4]


        #print(group.tail(96))
        #print(stock[stock['type'] == 2])

if __name__ == '__main__':
    main(sys.argv)