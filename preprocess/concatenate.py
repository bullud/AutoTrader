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

dirs = ['G:\\level2_dst', \
        'G:\\level2_test']
dst = ''
for dir in dirs:
    if dst == '':
        dst = dir
        continue

    for parent, dirnames, filenames in os.walk(dir):
        for filename in filenames:
            ext = os.path.splitext(filename)[1][1:].lower()
            if ext != 'db':
                continue

            code = filename[0:6]
            print(code)

            srcfile = os.path.join(parent, filename)
            cons = sqlite3.connect(srcfile)
            curs = cons.cursor()
            sqls = 'SELECT * from trans'
            curs.execute(sqls)
            cons.commit()

            distfile = os.path.join(dst, code + '.db')
            cond = sqlite3.connect(distfile)
            curd = cond.cursor()
            curd.execute('create table trans(date TIMESTAMP, price REAL, bs TEXT, volumn INTEGER )')
            cond.commit()

            sqld = 'INSERT into trans(date, price, bs, volumn) values(?, ?, ?, ?)'
            entries = curs.fetchmany(100000)

            while len(entries) > 0:
                print(len(entries))
                print(entries[0])
                curd.executemany(sqld, entries)
                cond.commit()
                print(curd.rowcount)

                entries = curs.fetchmany(100000)




            curs.close()
            cons.close()
            curd.close()
            cond.close()

