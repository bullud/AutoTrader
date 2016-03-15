from utils import _const
import datetime

_const.threadNum = 4
_const.BasicInfoPath='F:\\stock\\BasicInfo\\basics.sqlite'
_const.DDEPath='F:\\stock\\DDE'
_const.MAPath='F:\\stock\\MA'
_const.Level2Path='F:\\stock\\level2_dst\\'
_const.minDate = datetime.datetime.strptime('1980-10-24 08:00:00', "%Y-%m-%d %H:%M:%S").date()
_const.maxDate = datetime.datetime.strptime('2080-10-24 08:00:00', "%Y-%m-%d %H:%M:%S").date()

_const.small = 100000
_const.middle = 500000
_const.large = 1000000