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
import time
import logging
import logging.handlers

LOG_FILE = 'DailyDataProc.log'

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024*1024, backupCount = 5) # 实例化handler
fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'

formatter = logging.Formatter(fmt)   # 实例化formatter
handler.setFormatter(formatter)      # 为handler添加formatter

logger = logging.getLogger('tst')    # 获取名为tst的logger
logger.addHandler(handler)           # 为logger添加handler
logger.setLevel(logging.DEBUG)

def basicUP(dbpath):
    #update basic infos
    print("update basic info")

    con = sqlite3.connect(dbpath)

    bdb = ts.get_stock_basics()

    #print(bdb.head())

    bdb.to_sql('basics', con, if_exists = 'replace')

    con.close()

    return bdb

def getLastDay(con, code):
    lastday = None

    sql = "SELECT date from days where code = '" + code + "' ORDER by date DESC LIMIT 1"
    print(sql)
    try:
        lastday = pd.read_sql(sql, con)
    except Exception as e:
        print(e)

    if lastday is not None and len(lastday) != 0:
        lastday = datetime.datetime.strptime(lastday["date"][0], "%Y-%m-%d").date()
    else:
        lastday = None

    return lastday

def dayUP(dbpath, bdf, autype):
    con = sqlite3.connect(dbpath)
    for code in bdf.index:
        print('processing: ' + code)
        dateS = bdf.ix[code]['timeToMarket'].astype(str)
        if dateS == '0':
            print('not time for code:' + code )
            logger.debug('not time for code:' + code)
            continue


        date = None
        lastday = getLastDay(con, code)
        print('lastday=' + str(lastday))

        if lastday == None:
            date = datetime.datetime.strptime(dateS, "%Y%m%d").date()
        else:
            date = lastday + datetime.timedelta(1)

        dayCount = (datetime.datetime.today().date() - date).days + 1

        round =  math.floor(dayCount / 365) + 1

        dfis = []
        #print("ttmTime:" + str(date))
        for oneDate in [date + datetime.timedelta(days = n * 365) for n in range(round)]:
            print(str(oneDate) + ":" + str(oneDate + datetime.timedelta(days = 364)), end=' ')
            it = 0
            while(True):
                try:
                    dfi = ts.get_h_data(code, start = str(oneDate), \
                                        end = str(oneDate + datetime.timedelta(days = 364)), \
                                        autype=autype)
                    if dfi is not None:
                        dfis.append(dfi)
                    #    break
                    #else:
                    #    time.sleep(1)
                    #    print("None data, try again")
                    #    logger.debug("None data, try again:" + code + ":" + str(oneDate))
                    break
                except Exception as e:
                    print("exception try again")
                    time.sleep(5)
                    logger.debug(e)
                    #print(e)

                it += 1
                if it == 3:
                    print('try 3 timse for code:' + code + ' ' + str(oneDate))
                    logger.debug('try 3 times for code:' + code + ' ' + str(oneDate))
                    break
                continue

            print('')

        if len(dfis) == 0:
            continue

        dfi_all = pd.concat(dfis)
        dfi_all.insert(0, 'code', code)
        dfi_all['index'] = 0
        dfi_all.sort_index(ascending = True, inplace = True)
        dfi_all.reset_index(inplace = True)

        f = lambda x:x.date()
        dfi_all['date'] = dfi_all['date'].map(f)

        #print(dfi_all.head())
        dfi_all.to_sql('days', con, if_exists = 'append', index = False)

    con.close()

def main():
    bdbpath = 'basics.sqlite'
    daydbpath = 'day_bfq.sqlite'

    bdb = basicUP(bdbpath)
    dayUP(daydbpath, bdb, None)



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