import threading
import queue
import time
import threadpool
from data_process.OneMinuteProc import *
from data_process.RealTimeProc import *
from data_source.sina_level1 import *

class wrapper:
    def __init__(self, args = None, objs = None):
        self._args = args
        self._objs = objs


def store_bids(bidswp):
    for bd in bidswp._args:
        c = bd.save()
        if c != 1:
            print('save failed')

def store_1Minute(oneMinutes):
    for om in oneMinutes:
        om.save()
    return

def store_5Minute(fiveMinutes):
    return

def realtimeProc(wp):
    bids = wp._args
    objs = wp._objs
    objs.proc(bids)

class manager(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.thread_stop = True
        self.jobqueue = queue.Queue(maxsize = 10)
        self.config = config

        dbtype = self.config.get('db', 'type')
        bidfile = self.config.get('db', 'bidfile')

        bid._meta.database = SqliteDatabase(bidfile)

        codefile = self.config.get('data', 'codefile')
        allcodes = open(codefile).read()
        codes = allcodes.split('\n')
        #print('codes:', codes)

        self.datasource =  sinaLevel1(codes, 2, self)
        self.storePool = threadpool.ThreadPool(3)
        self.calcPool = threadpool.ThreadPool(2)

        self.oneMinuteP = oneMinuteProc(self)

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

                omargs = []
                omargs.append(wrapper(bds, self.oneMinuteP))
                omProcRequest = threadpool.WorkRequest(realtimeProc, omargs)
                self.calcPool.putRequest(omProcRequest)
                self.calcPool.wait()



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