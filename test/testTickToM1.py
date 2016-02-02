import os
import os.path
import pandas as pd
from pandas import DataFrame
from peewee import *
import sqlite3
import datetime
import numpy as np
'''
con = sqlite3.connect('bids.db')
sql = 'SELECT * from bid'

df = pd.read_sql(sql, con)
print(df)
'''

def _high_price(g):
    gf = g.astype(float)
    return gf.idxmin() <= gf.idxmax() and np.max(gf) or (-np.max(gf))


rootdir = "G:\\work\\sourcecode\\AutoTrader\\test\\ticks"

m1rootdir = "G:\\work\\sourcecode\\AutoTrader\\test\\m1s"

stocks = []
i = 0
for parent, dirnames, filenames in os.walk(rootdir):
    for filename in filenames:
        #print(os.path.join(parent,filename))

        (shotname, extension) = os.path.splitext(filename)
        parts = shotname.split('_')
        print(parts)
        if parts[0] == '002536':
            continue

        con = sqlite3.connect(os.path.join(parent,filename))
        sql = "SELECT * from ticks"
        ticks = pd.read_sql(sql, con)
        con.close()

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
        f = lambda x: datetime.timedelta(seconds = (x.item() - x.item() %60000000000)/1000000000)
        f2 = lambda x: str(datetime.timedelta(seconds = (x.item() - x.item() %60000000000)/1000000000))

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
                             'amount':'sum',
                             'timeIndex2':'first'})

        group.reset_index(inplace = True)

        f2 = lambda x:x.astype(datetime)
        #group['timeIndex'] = group['timeIndex'].astype(datetime)
        group['timeIndex'] = group['timeIndex2']
        del group['timeIndex2']

        print(group.head())
        print(os.path.join(m1rootdir, parts[0] + '_m1.db'))


        con2 = sqlite3.connect(os.path.join(m1rootdir, parts[0] + '_m1.db'))
        group.to_sql('m1', con2, if_exists = 'replace', index = False)
        con2.close()
        i += 1
        if i == 350:
            break

exit()

stock_data = pd.concat(stocks)
#print(stock_data.head(10))
#stock_data.to_csv('G:\\BaiduYunDownload\\zhubi-2015-05-19\\2015-05-19_stocks.csv')

grouped = stock_data.groupby(['Code', 'Date', 'TimeIndex'])

print(grouped)

group = grouped.agg({'LowPrice':'min', \
                     'HighPrice': _high_price, \
                     'Volume':'sum', \
                     'Acount':'sum'})

print(group)
group.to_csv('G:\\BaiduYunDownload\\zhubi-2015-05-19\\2015-05-19_stock_m1.csv')