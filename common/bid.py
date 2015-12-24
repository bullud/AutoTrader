from peewee import *
from datetime import date

db = SqliteDatabase('bids.db')

class bid(Model):
    code = IntegerField(primary_key=True)
    market = CharField(max_length=2)
    name = CharField(default='')
    to_open_price = FloatField(default=0)
    ye_close_price = FloatField(default=0)
    cur_price = FloatField(default=0)
    to_high_price = FloatField(default=0)
    to_low_price  = FloatField(default=0)
    traded_share  = IntegerField(default=0)
    traded_money  = FloatField(default=0)
    buy1_count    = IntegerField(default=0)
    buy1_price    = FloatField(default=0)
    buy2_count    = IntegerField(default=0)
    buy2_price    = FloatField(default=0)
    buy3_count    = IntegerField(default=0)
    buy3_price    = FloatField(default=0)
    buy4_count    = IntegerField(default=0)
    buy4_price    = FloatField(default=0)
    buy5_count    = IntegerField(default=0)
    buy5_price    = FloatField(default=0)
    buy6_count    = IntegerField(default=0)
    buy6_price    = FloatField(default=0)
    buy7_count    = IntegerField(default=0)
    buy7_price    = FloatField(default=0)
    buy8_count    = IntegerField(default=0)
    buy8_price    = FloatField(default=0)
    buy9_count    = IntegerField(default=0)
    buy9_price    = FloatField(default=0)
    buy10_count   = IntegerField(default=0)
    buy10_price   = FloatField(default=0)
    date_time     = DateTimeField(default=date(1980, 10, 24))
    #time          = TimeField()

    class Meta:
        database = db


def main():
    db.connect()
    db.create_table(bid)
    #bd = bid()
    #bd.save()
    return

if __name__ == '__main__':
    main()