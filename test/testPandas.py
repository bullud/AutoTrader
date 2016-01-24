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

rootdir = "G:\\BaiduYunDownload\\zhubi-2015-05-19\\data\\"

stocks = []
i = 0
for parent, dirnames, filenames in os.walk(rootdir):
    for filename in filenames:
        print(os.path.join(parent,filename))

        (shotname, extension) = os.path.splitext(filename)
        parts = shotname.split(' ')

        stock=pd.read_csv(os.path.join(parent,filename), parse_dates=[1])
        stock.insert(0, 'Date', pd.to_datetime(parts[0]))
        stock.insert(0, 'Code', parts[1])
        TTime = pd.to_timedelta(stock['Time'])
        #stock['Time'] = TTime
        #TTime = pd.to_timedelta(TTime.dt.seconds - (TTime.dt.seconds % 60))
        f = lambda x: str(datetime.timedelta(seconds = (x.item() - x.item() %60000000000)/1000000000))

        stock.insert(1,'TimeIndex', TTime.map(f))

        #print(stock)
        stocks.append(stock)

        i += 1
        if i == 2:
            break

stock_data = pd.concat(stocks)

stock_data.to_csv('G:\\BaiduYunDownload\\zhubi-2015-05-19\\2015-05-19_stocks.csv')

print(stock_data.head(1))
#f = lambda x: datetime.timedelta(seconds = (x.item() - x.item() %60000000000)/1000000000)


#stock_data.insert(1,'TimeIndex', stock_data['Time'].map(f))
#print(stock_data.head(5))


#key = lambda x: x.minute
#grouped = stock_data.groupby(['Code', 'Date', 'TimeIndex'])


#print(grouped.head(1))

