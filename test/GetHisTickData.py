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

def main():
    con = sqlite3.connect('basics.sqlite')
    sql = "SELECT code, timeToMarket from basics"
    codeandtime = pd.read_sql(sql, con,  index_col='code')
    con.close()
    print(codeandtime.head())

    codes = codeandtime.index
    print(type(codes))

    skip = 0
    for code in codes:
        skip += 1
        if skip < 1000:
            continue

        dbpath = 'ticks/' + code + '_ticks.sqlite'
        con = sqlite3.connect(dbpath)

        print(dbpath)

        dateS = codeandtime.ix[code]['timeToMarket'].astype(str)
        if dateS == '0':
            print('not time for code:' + code )
            con.close()
            continue

        date = datetime.datetime.strptime(dateS, "%Y%m%d").date()

        dayCount = (datetime.datetime.today().date() - date).days + 1

        ticks = []
        for oneDay in [date + datetime.timedelta(n) for n in range(dayCount)]:
            print(oneDay)

            it = 0
            while(True):
                try:
                    tickdf = ts.get_tick_data(code, date=oneDay)
                    if tickdf is not None and len(tickdf) > 5:
                        tickdf.insert(0, 'date', oneDay)
                        tickdf.sort_values(by = 'time', ascending = True, inplace = True)
                        print(tickdf.head())

                        ticks.append(tickdf)

                        if len(ticks) >= 100:
                            ticks_df = pd.concat(ticks)
                            ticks_df.to_sql('ticks', con, if_exists = 'append')
                            ticks = []

                    break
                except Exception as e:
                    print(e)

                it += 1
                if it == 3:
                    print('try 3 time for code:' + code + ' ' + str(oneDay))
                    break

                print('try more time ' + str(it))
                continue

        if len(ticks) != 0:
            ticks_df = pd.concat(ticks)
            ticks_df.to_sql('ticks', con, if_exists = 'append')

        con.close()

    #df = ts.get_tick_data('600848',date='2014-01-09')
    #print(df.head(10))
    return

if __name__ == '__main__':
    main()