import os
import os.path
import sys
import shutil
import pandas as pd
from pandas import DataFrame
from peewee import *
import sqlite3
import datetime
import numpy as np
from utils import _const

_const.level2_sqlite='E:\\BaiduYunDownload\\level2_sqlite'

def getTime(x):
    d = datetime.datetime.strptime(x, "%H%M%S")
    t = datetime.timedelta(hours = d.hour, minutes = d.minute, seconds = d.second)
    return t

def extractData(zipfile, dst):
    print(zipfile, dst)
    d = datetime.datetime.strptime(zipfile[0:8], '%Y%m%d')

    year = d.year
    month = d.month
    day = d.day
    print(year, month, day)


    for parent, dirnames, filenames in os.walk(dst):
        for filename in filenames:
            code = filename[2:8]

            sqlitefile = os.path.join(_const.level2_sqlite, code + '.db')
            con = sqlite3.connect(sqlitefile)

            csvfile = os.path.join(parent, filename)
            print(code)
            transactions = pd.read_csv(csvfile, header=None, names=['time', 'price', 'bs', 'volumn'],\
                            converters={'time':str})

            transactions['date'] = d + transactions['time'].apply(getTime)
            del transactions['time']
            transactions.set_index('date', inplace=True)
            print(transactions.head())
            con.close()
            #print(transactions)
            return

def main(argv):
    rarcmd = '"C:\\Program Files\\WinRAR\\unRar.exe" x '
    z7cmd = '"D:\\Program Files\\7-Zip\\7z.exe" x '
    for parent, dirnames, filenames in os.walk('E:\\BaiduYunDownload\\level2\\2016'):
        for filename in filenames:
            file = os.path.join(parent,filename)

            ext = os.path.splitext(filename)[1][1:].lower()
            dst = parent + "\\temp"

            if os.path.exists(dst) == False:
                os.makedirs(dst)

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