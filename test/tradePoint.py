import sys
import os
import os.path
import pandas as pd
from pandas import DataFrame
from peewee import *
import sqlite3
import datetime
import numpy as np
import getopt

def usage():
    print('usage')

def doAllSearch(stype, mpath):
    return

def doSingleSearch(code, stype, mpath):
    m1path = os.path.join(mpath, code + "_m1.db")

    print('m1path=' + m1path)

    con2 = sqlite3.connect(m1path)

    sql = "SELECT * from m1 "

    m1s = None
    try:
        m1s = pd.read_sql(sql, con2)
    except Exception as e:
        print(e)
    finally:
        con2.close()

    print(m1s.head())

    if m1s is None or len(m1s) == 0:
        return

    print(m1s.head())


def main(argv):
    try:
        options, args= getopt.getopt(argv[1:], "hc:s:m:", ["help", "code=", "stype=", "mpath="])
    except getopt.GetoptError as e:
        print('end')
        sys.exit()

    mrootdir = ''
    stype = ''
    for name, value in options:
        print(value)
        if name in ('-h', '--help'):
            usage()
        if name in ('-c', '--code'):
            code = value
        if name in ('-s', '--stype'):
            stype = value
        if name in ('-m', '--mpath'):
            mrootdir = value

    if code == '0':
        doAllSearch(stype, mrootdir)
    else:
        doSingleSearch(code, stype, mrootdir)


if __name__ == "__main__":
    main(sys.argv)

