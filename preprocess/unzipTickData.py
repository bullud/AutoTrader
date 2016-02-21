import os
import os.path
import sys
import shutil
import pandas as pd
from pandas import DataFrame
from peewee import *
import sqlite3
import MySQLdb
import datetime
import numpy as np
import threadpool
from utils import _const

_const.level2_sqlite = 'G:\\level2_sqlite_16'
_const.multiThread = False

Pool = threadpool.ThreadPool(1)

def storeProc(args):
    con = sqlite3.connect(args._sqlite)
    args._data.to_sql(name = 'trans', con = con, if_exists = 'append', index = True)
    con.close()

def getTime(x):
    d = datetime.datetime.strptime(x, "%H%M%S")
    t = datetime.timedelta(hours = d.hour, minutes = d.minute, seconds = d.second)
    return t

def getLastEntry(con, table):
    return

    lastentry = None
    lastentries = None

    sql = "SELECT date from " + table + " ORDER by date DESC LIMIT 1"

    try:
        lastentries = pd.read_sql(sql, con)
    except Exception as e:
        print(e)
    #print(lastdays)

    if lastentries is not None and len(lastentries) != 0:
        lastentry = datetime.datetime.strptime(lastentries["date"][0], "%Y-%m-%d %H:%M:%S")#.date()
    else:
        lastentry = None

    return lastentry

class wrapper:
    def __init__(self, sqlite = None, data = None):
        self._sqlite = sqlite
        self._data = data

def extractData(zipfile, dst):
    print(zipfile, dst)
    d = datetime.datetime.strptime(zipfile[0:8], '%Y%m%d')

    year = d.year
    month = d.month
    day = d.day
    print(year, month, day)

    #con=MySQLdb.connect(host='localhost', db='trans', user='lidian', passwd='123@321ld')

    for parent, dirnames, filenames in os.walk(dst):
        for filename in filenames:
            ext = os.path.splitext(filename)[1][1:].lower()
            #print(ext)
            if ext != 'csv':
                continue

            code = filename[2:8]

            #not necessary for mysql
            sqlitefile = os.path.join(_const.level2_sqlite, code + '.db')
            con = sqlite3.connect(sqlitefile)

            lastEntry = getLastEntry(con, 'trans')

            #print('lastEntry=' + str(lastEntry))

            if lastEntry is not None and lastEntry.date() > d.date():
                con.close()
                continue


            csvfile = os.path.join(parent, filename)

            print(csvfile)
            transactions = pd.read_csv(csvfile, header=None, names=['time', 'price', 'bs', 'volumn'],\
                            converters={'time':str})

            transactions['date'] = d + transactions['time'].apply(getTime)
            del transactions['time']
            transactions.set_index('date', inplace=True)

            args = []

            if lastEntry is not None:
                trans = transactions.query('date  > "' + str(lastEntry) + '"')
                #trans.to_sql(name = code, con = con, flavor = 'mysql', if_exists = 'append', index = True)
                if _const.multiThread == False:
                    trans.to_sql(name = 'trans', con = con, if_exists = 'append', index = True)
                else:
                    args.append(wrapper(sqlitefile, trans))
            else:
                #transactions.to_sql(name = code, con = con, flavor = 'mysql', if_exists = 'append', index = True)
                if _const.multiThread == False:
                    transactions.to_sql(name = 'trans', con = con, if_exists = 'append', index = True)
                else:
                    args.append(wrapper(sqlitefile, transactions))

            if _const.multiThread == True:
                Pool.wait()

                StoreRequest = threadpool.WorkRequest(storeProc, args)

                Pool.putRequest(StoreRequest)

            con.close()

    Pool.wait()

def main(argv):
    rarcmd = '"C:\\Program Files\\WinRAR\\unRar.exe" x '
    z7cmd = '"D:\\Program Files\\7-Zip\\7z.exe" x '
    for parent, dirnames, filenames in os.walk('G:\\level2\\2016\\'):
        for filename in filenames:
            file = os.path.join(parent,filename)

            ext = os.path.splitext(filename)[1][1:].lower()
            dst = parent + "\\temp"

            if ext in ['7z', 'rar']:
                cmd = z7cmd + file + " -y -o" + dst
                rmcmd = "rd/s/q " + dst

            else:
                continue

            '''
            elif ext == 'rar':
                cmd = rarcmd + file + " * " + dst
                rmcmd = "rd/s/q " + dst
            '''

            if os.path.exists(dst) == False:
                os.makedirs(dst)

            print(cmd)
            if os.system(cmd) == 0:
                #print("unrar success")
                extractData(filename, dst)
                #
                if os.system(rmcmd) == 0:
                    print('del temp success')
                else:
                    print('failed')
                    return

            else:
                print('unrar failed')
                return


if __name__ == '__main__':
    main(sys.argv)