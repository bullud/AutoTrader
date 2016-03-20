from utils import _const
import datetime

_const.threadNum = 1
_const.BasicInfoPath='I:\\StockData\\BasicInfo\\basics.sqlite'
_const.DDEPath='I:\\StockData\\DDEtest'
_const.DDEPath2='F:\\stock\\DDEtest'
_const.MAPath='F:\\stock\\MAtest'
_const.Level2Path='I:\\StockData\\level2_dst'
_const.Level2Path2='F:\\stock\\DDEtestL2\\M1'
_const.minDate = datetime.datetime.strptime('1980-10-24 08:00:00', "%Y-%m-%d %H:%M:%S").date()
_const.maxDate = datetime.datetime.strptime('2080-10-24 08:00:00', "%Y-%m-%d %H:%M:%S").date()

_const.minTime = datetime.datetime.strptime('1980-10-24 08:00:00', "%Y-%m-%d %H:%M:%S")
_const.maxTime = datetime.datetime.strptime('2080-10-24 08:00:00', "%Y-%m-%d %H:%M:%S")

_const.small = 100000
_const.middle = 500000
_const.large = 1000000