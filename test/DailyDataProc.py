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


def main():


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