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


rootdir = "G:\\BaiduYunDownload\\zhubi-2015-05-19\\data\\"



stocks = []
i = 0
for parent, dirnames, filenames in os.walk(rootdir):
    for filename in filenames:
        print(os.path.join(parent,filename))

        (shotname, extension) = os.path.splitext(filename)
        parts = shotname.split(' ')

        stock=pd.read_csv(os.path.join(parent,filename), parse_dates=[1], converters={'Price':float})
        del stock['BuySell']



        stock.insert(0, 'Date', pd.to_datetime(parts[0]))
        stock.insert(0, 'Code', parts[1])
        stock.insert(3, 'LowPrice', stock['Price'])

        stock['Acount']= stock.LowPrice.astype(float) * stock.Volume.astype(float)

        TTime = pd.to_timedelta(stock['Time'])
        #stock['Time'] = TTime
        #TTime = pd.to_timedelta(TTime.dt.seconds - (TTime.dt.seconds % 60))
        f = lambda x: datetime.timedelta(seconds = (x.item() - x.item() %60000000000)/1000000000)

        stock.insert(2,'TimeIndex', TTime.map(f))

        stock.columns = ['Code', 'Date', 'TimeIndex', 'Time', 'LowPrice', 'HighPrice', 'Volume', 'Acount']
        #print(stock.head())
        stocks.append(stock)

        i += 1
        if i == 1:
            break

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