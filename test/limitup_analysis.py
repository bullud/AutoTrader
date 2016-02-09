import sys
import getopt
import os
import os.path
import pandas as pd
from pandas import DataFrame
from peewee import *
import sqlite3
import datetime
import numpy as np
from decimal import Decimal
from utils import _const

_const.limitDate = '1997-12-31'

def getLastDay(con, code):
    lastDay = None

    return lastDay

#def anlyze():


def main(argv):

    daydbpath = 'day_bfq.sqlite'
    limitdbpath = 'limit.sqlite'

    dcon = sqlite3.connect(daydbpath)
    sql = 'select distinct code from days'
    cdf = pd.read_sql(sql, dcon)


    codes = cdf['code']

    lcon = sqlite3.connect(limitdbpath)
    for code in codes:
        if code != '002466':
            continue

        date = None
        lastDay = getLastDay(lcon, code)
        print(code + ': lastDay=' + str(lastDay))

        if lastDay == None:
            date = _const.limitDate
        else:
            date = str(lastDay)

        sql = "select * from days where code = '" + code + "' and date >= '" + date + "'"

        print(sql)
        ddf = pd.read_sql(sql, dcon)
        ddf['lclose'] = ddf['close'].shift()
        ddf.drop(0, axis = 0, inplace= True)

        uf = lambda x:round(x * 1.1 + 0.00001, 2)
        df = lambda x:round(x * 0.9 + 0.00001, 2)
        ddf['ulimit'] = ddf['lclose'].apply(uf)
        ddf['dlimit'] = ddf['lclose'].apply(df)



        f = lambda x:x['close'] == x['ulimit'] and 1 \
                     or (x['close'] == x['dlimit'] and -1 \
                     or (x['close'] - x['lclose'])*2/(x['ulimit'] - x['dlimit']))
        ddf['limit'] = ddf.apply(f, axis = 1)


        print(ddf[ ddf['limit'] == 1])
        #print(ddf)
        #print(ddf.tail())
        break

    lcon.close()
    dcon.close()

if __name__ == '__main__':
    main(sys.argv)