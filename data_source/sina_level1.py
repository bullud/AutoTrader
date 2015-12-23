#coding=utf-8
import http.client
import threading
import time
import urllib

class SinaLevel1(threading.Thread):
    def __init__(self, monitor_list, interval):
        threading.Thread.__init__(self)
        self.base_url = 'hq.sinajs.cn'
        self.query = '/list='

        for code in monitor_list:
            self.query += code + ','

    def run(self):
        self.get_data()

    def stop(self):
        self.thread_stop = True

    def parse_data(self, data):
        print(data)

    def get_data(self):
        httpClient = None
        try:
            httpClient = http.client.HTTPConnection(self.base_url)
            httpClient.request("GET", self.query)
            res = httpClient.getresponse()
            #print(res.status)
            if res.status == 200:
                data = res.read().decode('gbk')
                self.parse_data(data)
            else:
                print(res.status)
                return
        except Exception as e:
            print(e)
        finally:
            if httpClient:
                httpClient.close()

def main():
    monitor_list = ['sz002466', 'sz002460', 'sz300073', 'sz000558', 'sz300151', 'sz000952', 'sz000004', 'sz002421']
    sinaL1 = SinaLevel1(monitor_list, 2)
    sinaL1.start()
    time.sleep(3)
    sinaL1.stop()

if __name__ == '__main__':
    main()