import numpy as np
import matplotlib.pyplot as plt
import tushare as ts

df = ts.get_h_data('600656', start = '1990-12-19', end = '1990-12-21')


print(len(df))
print(df)

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