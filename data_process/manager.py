import threading
import queue
import time
import threadpool
from data_process.OneMinuteProc import *
from data_process.RealTimeProc import *
from data_source.sina_level1 import *
from common.oneMinute import *


class wrapper:
    def __init__(self, args = None, objs = None):
        self._args = args
        self._objs = objs


def store_bids(bidswp):
    print('store bids')
    for bd in bidswp._args:
        c = bd.save()
        if c != 1:
            print('save failed')

def store_m1s(wp):
    objs = wp._objs
    objs.storeM1()
    #for om in oneMinutes:
    #    om.save()
    return

def store_m5s(fiveMinutes):
    return

def dorealtimeProc(wp):
    bids = wp._args
    objs = wp._objs
    objs.proc(bids)
    return

class manager(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.thread_stop = True
        self.jobqueue = queue.Queue(maxsize = 10)
        self.config = config

        dbtype = self.config.get('db', 'type')
        bidfile = self.config.get('db', 'bidfile')
        bid._meta.database = SqliteDatabase(bidfile)

        m1file = self.config.get('db', '1minfile')
        oneMinute._meta.database = SqliteDatabase(m1file)

        m5file = self.config.get('db', '5minfile')
        m15file = self.config.get('db', '15minfile')

        codefile = self.config.get('data', 'codefile')
        allcodes = open(codefile).read()
        codes = allcodes.split('\n')
        #print('codes:', codes)

        self.datasource =  sinaLevel1(codes, 2, self)
        self.storePool = threadpool.ThreadPool(3)
        self.calcPool = threadpool.ThreadPool(2)

        self.realTimeP = realtimeProc(codes, self)

        return

    def update(self, sourcename, bids):
        #for bi in bids:
        #    print(bi.code)
        #print('put in queue')
        self.jobqueue.put(bids)

        return

    def stop(self):
        self.datasource.stop()
        self.thread_stop = True
        return

    def start(self):
        self.thread_stop = False
        super().start()
        self.datasource.start()

        return

    def run(self):
        while self.thread_stop == False:
            if self.jobqueue.empty() == False:
                bds = self.jobqueue.get()
                print('deal bid:', len(bds))

                bdargs = []
                bdargs.append(wrapper(bds))
                bidStoreRequest = threadpool.WorkRequest(store_bids, bdargs)
                self.storePool.putRequest(bidStoreRequest)

                m1args = []
                m1args.append(wrapper(bds, self.realTimeP))
                m1ProcRequest = threadpool.WorkRequest(dorealtimeProc, m1args)
                self.calcPool.putRequest(m1ProcRequest)
                self.calcPool.wait()

                m1Sags = []
                m1Sags.append(wrapper(None, self.realTimeP))
                m1SProcRequest = threadpool.WorkRequest(store_m1s, m1Sags)
                self.storePool.putRequest(m1SProcRequest)

                self.storePool.wait()

            else:
                time.sleep(0.01)
        return


def main():
    processor = manager(None)
    processor.start()

    time.sleep(10)
    processor.stop()
    #processor.join()
    return

if __name__ == '__main__':
    main()