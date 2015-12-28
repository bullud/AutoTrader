import peewee
from common.bid import *

db = SqliteDatabase('test_bids.db')

bid._meta.database = db

db.connect()
db.create_table(bid)

