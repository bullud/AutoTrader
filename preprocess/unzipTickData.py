import os
import os.path
import sys
import shutil
import pandas as pd
from pandas import DataFrame
from peewee import *
import sqlite3
import datetime
import numpy as np


def main(argv):
    rarcmd = '"C:\\Program Files\\WinRAR\\unRar.exe" x '
    z7cmd = '"D:\\Program Files\\7-Zip\\7z.exe" x '
    for parent, dirnames, filenames in os.walk('E:\\BaiduYunDownload\\level2\\2016'):
        for filename in filenames:
            file = os.path.join(parent,filename)

            ext = os.path.splitext(filename)[1][1:].lower()
            dst = parent + "\\temp"

            if os.path.exists(dst) == False:
                os.makedirs(dst)

            if ext in ['7z', 'rar']:
                cmd = z7cmd + file + " -o" + dst
                rmcmd = "rd/s/q " + dst

            else:
                continue

            '''
            elif ext == 'rar':
                cmd = rarcmd + file + " * " + dst
                rmcmd = "rd/s/q " + dst
            '''

            print(cmd)
            if os.system(cmd) == 0:
                print("unrar success")

                #
                if os.system(rmcmd) == 0:
                    print('del temp success')
                else:
                    print('failed')
                return
            else:
                print('unrar failed')
                return

if __name__ == '__main__':
    main(sys.argv)