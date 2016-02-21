import tushare as ts
import os
import datetime
import sqlite3

from utils import _const

_const.level2_sqlite = 'G:\\level2_sqlite_16'

_const.deleteDays=['20160128']





for parent, dirnames, filenames in os.walk(_const.level2_sqlite):
    for filename in filenames:
        ext = os.path.splitext(filename)[1][1:].lower()
        if ext != 'db':
            continue

        sqlitefile = os.path.join(parent, filename)

        con = sqlite3.connect(sqlitefile)
        cursor = con.cursor()
        print(sqlitefile)
        for day in _const.deleteDays:
            d = datetime.datetime.strptime(day, "%Y%m%d")
            #print(d)
            sql = 'DELETE from trans where date >= "' + str(d) + '" and date < "' + str(d + datetime.timedelta(1)) +'"'
            print(sql)
            cursor.execute(sql)
            con.commit()

            print(cursor.rowcount)
            cursor.close()


        con.close()
