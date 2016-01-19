from peewee import *
from datetime import date

class oneMinute(Model):
    code = CharField(default='000000')
    open_price  = FloatField(default=0)
    close_price = FloatField(default=0)
    high_price = FloatField(default=0)
    low_price  = FloatField(default=0)
    traded_share = IntegerField(default=0)
    traded_money = IntegerField(default=0)
    minuteT      = DateTimeField(default=date(1980, 10, 24))
    begBDT        = DateTimeField(default=date(1980, 10, 24))
    endBDT        = DateTimeField(default=date(1980, 10, 24))