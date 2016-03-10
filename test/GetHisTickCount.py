import os
import os.path
import pandas as pd
from pandas import DataFrame
from peewee import *
import sqlite3
import datetime
import numpy as np
import tushare as ts
import msvcrt
import math
from utils import _const

_const.baseInfo='basics.sqlite'
_const.countDB='F:\\stock\\ticks\\'

def main():
    if os.path.exists(_const.countDB) == False:
         os.makedirs(_const.countDB)

    con = sqlite3.connect(_const.baseInfo)
    sql = "SELECT code, timeToMarket from basics"
    codeandtime = pd.read_sql(sql, con,  index_col='code')
    con.close()
    print(codeandtime.head())

    codes = codeandtime.index
    print(type(codes))

    beg = 0
    end = 1000
    b = 0
    ee = 0
    if end <= beg:
        print('end must > beg')
        exit()

    for code in codes:
        if b < beg:
            b += 1
            ee +=1
            continue

        if ee == end:
            break

        ee += 1

        dbpath = _const.countDB + code + '_counts.sqlite'
        con = sqlite3.connect(dbpath)
        cur = con.cursor()

        cur.execute('create table if not exists counts(date TIMESTAMP, count INTEGER)')
        con.commit()
        cur.close()

        print(dbpath)

        date = None
        lastcounts = None
        sql = "SELECT date from counts ORDER by date DESC LIMIT 1"
        try:
            lastcounts = pd.read_sql(sql, con)

        except Exception as e:
            print(e)

        if lastcounts is None or len(lastcounts) == 0:
            dateS = codeandtime.ix[code]['timeToMarket'].astype(str)
            if dateS == '0':
                print('not time for code:' + code )
                con.close()
                continue

            date = datetime.datetime.strptime(dateS, "%Y%m%d").date()
        else:
            #print(str(lastticks['date'][0] + datetime.timedelta(1)))
            date = datetime.datetime.strptime(lastcounts["date"][0], "%Y-%m-%d").date()  + datetime.timedelta(1)
            print("start date:" + str(date))


        dayCount = (datetime.datetime.today().date() - date).days + 1

        counts = []
        for oneDay in [date + datetime.timedelta(n) for n in range(dayCount)]:
            it = 0
            while(True):
                try:
                    l = 0
                    tickdf = ts.get_tick_data(code, date=oneDay)
                    if tickdf is not None:
                        l = len(tickdf)

                    print(code + " " + str(oneDay) + ' tick num = ' + str(l))

                    counts.append((oneDay, l))
                    if len(counts) > 10:
                        cur = con.cursor()
                        sql = 'INSERT into counts(date, count) values(?, ?)'
                        cur.executemany(sql, counts)
                        con.commit()

                        cur.close()
                        counts = []

                    break
                except Exception as e:
                    print(e)

                it += 1
                if it == 3:
                    print('try 3 time for code:' + code + ' ' + str(oneDay))
                    break

                print('try more time ' + str(it))
                continue

        if len(counts) != 0:
            cur = con.cursor()
            sql = 'INSERT into counts(date, count) values(?, ?)'
            cur.executemany(sql, counts)
            con.commit()
            cur.close()


        con.close()

    return

if __name__ == '__main__':
    main()