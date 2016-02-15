import numpy as np
import matplotlib.pyplot as plt
import tushare as ts
import pandas as pd
import datetime

def getTime(x):
    print(x)
    d = datetime.datetime.strptime(x, "%H%M%S")
    t = datetime.timedelta(hours = d.hour, minutes = d.minute, seconds = d.second)
    return t

csvfile = 'E:\\BaiduYunDownload\\level2\\2010\\201009\\temp\\20100927\\SZ000680.csv'

print(csvfile)
transactions = pd.read_csv(csvfile, header=None, names=['time', 'price', 'bs', 'volumn'],\
                            converters={'time':str})

print(transactions['time'].apply(getTime))

exit(0)

d = ts.get_hist_data('600848')

#print(len(d))

#print(type(d))

print(d.head(2))

#print(d.head(1).describe())

#print(d.loc[dates[0]])
#print(d.index)

#print(d.loc[:,['ma5', 'ma10']])
#print(d.loc['2016-01-06':'2016-01-07'])
print(d[['ma5', 'ma10']][d['ma5'] < 20])
#print(d['ma5'])