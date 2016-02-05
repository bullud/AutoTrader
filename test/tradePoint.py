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


def main(argv):
    try:
        options, args= getopt.getopt(argv[1:], "hc:s:t:", ["help", "code=", "stype=", "mpath="])
    except getopt.GetoptError as e:
        print('end')
        sys.exit()

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

if __name__ == "__main__":
    main(sys.argv)

