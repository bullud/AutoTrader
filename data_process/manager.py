import threading
import queue
import time
from data_source.sina_level1 import *

class manager(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.thread_stop = True
        self.jobqueue = queue.Queue(maxsize = 10)
        self.config = config

        codefile = self.config.get('data', 'codefile')
        allcodes = open(codefile).read()
        codes = allcodes.split('\n')
        print('codes:', codes)

        self.datasource =  sinaLevel1(codes, 2, self)

        return

    def update(self, sourcename, bids):
        for bi in bids:
            print(bi.code)

        #self.jobqueue.put(bids)

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
                bd = self.jobqueue.get()
                print(bd)
            else:
                time.sleep(10)
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