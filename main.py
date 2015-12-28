import configparser
from common.bid import *
from peewee import *
import threading
import time


from data_process.manager import *

cf = configparser.ConfigParser()

o = cf.read('config.conf')
if len(o) == 0:
    print('no config.conf found, exit')
    exit()

ss = cf.sections()
if len(ss) == 0:
    print('no section in the config.conf, exit')
    exit()

#opts = cf.options('db')
#print('options', opts)

#kvs = cf.items("db")
#print('items', kvs)

print('show config below:')
dbtype = cf.get('db', 'type')
bidfile = cf.get('db', 'bidfile')
if dbtype == '' or bidfile == '':
    print('get dbtype or bidfile failed, exit')
    exit()
print('dbtype:', dbtype)
print('bidfile:', bidfile)

if dbtype != 'sqlite':
    print('unsupported dbtype, exit')
    exit()

codefile = cf.get('data', 'codefile')
allcodes = open(codefile).read()
codes = allcodes.split('\n')
print('codes:', codes)

source = cf.get('data', 'source')
Ltype  = cf.get('data', 'Ltype')

processor = manager(cf)
processor.start()

time.sleep(10)
processor.stop()
processor.join()


#sinaL1 = SinaLevel1(monitor_list, 2)
#sinaL1.start()
#time.sleep(20)
#sinaL1.stop()