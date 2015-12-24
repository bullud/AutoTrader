from peewee import *

db = SqliteDatabase('bids.db')

class bid(Model):
    code = IntegerField()
    name = CharField()
    to_open_price = FloatField()
    ye_close_price = FloatField()
    cur_price = FloatField()
    to_high_price = FloatField()
    to_low_price  = FloatField()
    traded_share  = IntegerField()
    traded_money  = FloatField()
    buy1_count    = IntegerField()
    buy1_price    = FloatField()
    buy2_count    = IntegerField()
    buy2_price    = FloatField()
    buy3_count    = IntegerField()
    buy3_price    = FloatField()
    buy4_count    = IntegerField()
    buy4_price    = FloatField()
    buy5_count    = IntegerField()
    buy5_price    = FloatField()
    buy6_count    = IntegerField()
    buy6_price    = FloatField()
    buy7_count    = IntegerField()
    buy7_price    = FloatField()
    buy8_count    = IntegerField()
    buy8_price    = FloatField()
    buy9_count    = IntegerField()
    buy9_price    = FloatField()
    buy10_count   = IntegerField()
    buy10_price   = FloatField()
    date_time     = DateTimeField()
    #time          = TimeField()

    class Meta:
        database = db


    def __init__(self):
        return


def main():
    db.connect()
    #db.create_table(bid)
    bd = bid()
    bd.save()
    return

if __name__ == '__main__':
    main()