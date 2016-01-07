import peewee
from common.bid import *
from common.day import *

db = SqliteDatabase('bid.db')

bid._meta.database = db

if bid.table_exists() == False:
    bid.create_table()
    #db.connect()
    #db.create_table(bid)


dailydb = SqliteDatabase('daily.db')
day._meta.database = dailydb


if day.table_exists() == False:
    day.create_table()
    #dailydb.connect()
    #dailydb.create_table(day)
else:
    day.drop_table()
    day.create_table()