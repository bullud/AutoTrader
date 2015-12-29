from peewee import *

class index(Model):
    code = CharField(default='000000')
    name = CharField(default='')
    point = FloatField(default=0.0)
    change = FloatField(default=0.0)
    ratio  = FloatField(default=0.0)
    count = IntegerField(default=0)
    money = IntegerField(default=0)

