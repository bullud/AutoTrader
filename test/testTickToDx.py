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


def _high_price(g):
    gf = g.astype(float)
    return gf.idxmin() <= gf.idxmax() and np.max(gf) or (-np.max(gf))

def usage():
    print("usage")

def main(argv):
    try:
        options, args= getopt.getopt(argv[1:], "hm:p:t:", ["help", "mtype=", "mpath=", "tpath="])
    except getopt.GetoptError as e:
        print('end')
        sys.exit()

    mt = 0
    mrootdir = ''
    trootdir = ''
    for name, value in options:
        print(value)
        if name in ('-h', '--help'):
            usage()
        if name in ('-m', '--mtype'):
            mt = int(value)
        if name in ('-p', '--mpath'):
            mrootdir = value
        if name in ('-t', '--tpath'):
            trootdir = value


    stocks = []
    i = 0
    for parent, dirnames, filenames in os.walk(trootdir):
        for filename in filenames:
            #print(os.path.join(parent,filename))

            (shotname, extension) = os.path.splitext(filename)
            parts = shotname.split('_')
            print(parts)
            if parts[0] != '002536':
                continue

            #sql = "select name from sqlite_master where type = 'table' order by name"
            #c = sqlite3.Connection('xx.db').cursor()
            #print c.execute(sql).fetchall()

            mxs = None
            dbpath = os.path.join(mrootdir, parts[0] + '_m' + str(mt) + '.db')
            if os.path.exists(dbpath) == True:
                con2 = sqlite3.connect(dbpath)

                sql = "SELECT date from m" + str(mt) + " ORDER BY date DESC LIMIT 1"
                try:
                    mxs = pd.read_sql(sql, con2)
                except Exception as e:
                    print(e)
                finally:
                    con2.close()

            sqltime = ''
            ticks = None
            if mxs is not None:
                sqltime = "WHERE date > '" + str(mxs['date'][0]) + "'"

                #print(sqltime)

            con = sqlite3.connect(os.path.join(parent,filename))
            sql = "SELECT * from ticks " + sqltime

            ticks = None
            print(sql)
            try:
                ticks = pd.read_sql(sql, con)
            except Exception as e:
                print('read ticks failed for: ' + parts[0])
            finally:
                con.close()

            if ticks is None or len(ticks) == 0:
                continue

            print(len(ticks))
            #print(ticks)
            #t = ticks['time']
            #print(type(t[0]))

            del ticks['index']
            del ticks['type']
            del ticks['change']

            #print(ticks.head())

            ticks.insert(3, 'low',   ticks['price'])
            ticks.insert(4, 'open',  ticks['price'])
            ticks.insert(5, 'close', ticks['price'])
            #print(ticks.head())


            TTime = pd.to_timedelta(ticks['time'])
            #stock['Time'] = TTime
            #TTime = pd.to_timedelta(TTime.dt.seconds - (TTime.dt.seconds % 60))
            print(mt)
            step = mt*60000000000
            f  = lambda x: datetime.timedelta(seconds = (x.item() - x.item() % step + step)/1000000000)
            f2 = lambda x: str(datetime.timedelta(seconds = (x.item() - x.item() % step + step)/1000000000))

            ticks.insert(2, 'timeIndex', TTime.map(f))
            ticks.insert(3, 'timeIndex2', TTime.map(f2))

            #ticks.columns = ['date', 'time', 'price', '', 'HighPrice', 'Volume', 'Acount']
            ticks.columns = ['date', 'time', 'timeIndex', 'timeIndex2', 'high', 'low', 'open', 'close', 'volume', 'amount']

            #print(ticks.head())
            #stocks.append(ticks)

            grouped = ticks.groupby(['date', 'timeIndex'])
            group = grouped.agg({'low':'min', \
                             'high': _high_price, \
                             'open':'first',\
                             'close':'last', \
                             'volume':'sum', \
                             'amount':'sum', \
                             'timeIndex2':'first'})

            group.reset_index(inplace = True)

            f2 = lambda x:x.astype(datetime)
            #group['timeIndex'] = group['timeIndex'].astype(datetime)
            group['timeIndex'] = group['timeIndex2']
            del group['timeIndex2']

            print(group.head(10))
            print(os.path.join(mrootdir, parts[0] + '_m'+ str(mt) +'.db'))


            con2 = sqlite3.connect(os.path.join(mrootdir, parts[0] + '_m'+ str(mt) +'.db'))
            group.to_sql('m'+ str(mt), con2, if_exists = 'append', index = False)
            con2.close()

            break

            i += 1
            if i == 350:
                break


if __name__ == '__main__':
    main(sys.argv)