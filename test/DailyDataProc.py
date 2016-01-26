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


def main():
    con = sqlite3.connect('basics.sqlite')

    dfb = ts.get_stock_basics()
    #print(dfb.head())
    #print(len(dfb))
    dfb.to_sql('basics', con, if_exists = 'replace')

    con.close()

    #print(dfb.index)
    for i in dfb.index:
        dateS = dfb.ix[i]['timeToMarket'].astype(str)
        if dateS == '0':
            print('not time for code:' + i )
            continue

        date = datetime.datetime.strptime(dateS, "%Y%m%d").date()

        dayCount = (datetime.datetime.today().date() - date).days + 1

        #for oneDate in [date + datetime.timedelta(years = n) for n in range(yearCount)]:

        #dfi = ts.get_h_data(i)

    #con = sqlite3.connect('day.sqlite')
    #df = ts.get_today_all()
    #print(len(dfa))
    return

    con = sqlite3.connect('day.sqlite')
    df = ts.get_today_all()

    now = datetime.datetime.today().date()
    print(now)

    df.insert(0, 'Date', now)

    print(df.head())

    pd.io.sql.write_frame(df, 'day', con)
    
    con.close()
    #print('press q to exist')
    #ch = ''
    #while ch != b'q':
    #    ch = msvcrt.getch()

if __name__ == '__main__':
    main()