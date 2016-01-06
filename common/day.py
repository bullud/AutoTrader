from peewee import *
from datetime import date

class day(Model):
    code = CharField(default='000000')
    market = CharField(max_length=2, default='')
    name = CharField(default='')
    open_price  = FloatField(default=0)
    close_price = FloatField(default=0)
    high_price = FloatField(default=0)
    low_price  = FloatField(default=0)
    traded_share = IntegerField(default=0)
    traded_money = IntegerField(default=0)
