import http.client
import threading
import time
import datetime
import urllib
from common.bid import *

#global db
db = SqliteDatabase('base2.db')


def main():
    bid._meta.database = db
    #db.connect()
    #db.drop_table(bid)
    #db.create_table(bid)
    #db.drop_table(bid)
    bd = bid(code=112)
    bd.save()



if __name__ == '__main__':
    main()