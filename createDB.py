import peewee
from common.bid import *

db = SqliteDatabase('bid.db')

bid._meta.database = db

db.connect()
db.create_table(bid)

