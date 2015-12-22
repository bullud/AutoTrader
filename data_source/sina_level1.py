#coding=utf-8
import http.client
import threading
import time
import urllib

class SinaLevel1(threading.Thread):
    def __init__(self, monitor_list, interval):
        threading.Thread.__init__(self)
        self.base_url = 'hq.sinajs.cn'
        self.query_list = '/list='

        for code in monitor_list:
            self.query_list += code + ','

        print(code)

    def run(self):
        self.get_data()

    def get_data(self):
        httpClient = None
        try:
            httpClient = http.client.HTTPConnection(self.base_url)
            httpClient.request("GET", "/list=sz002466")
            res = httpClient.getresponse()
            print(res.status)
            print(res.msg)
            print(res.read().decode('gbk'))
        except Exception as e:
            print(e)
        finally:
            if httpClient:
                httpClient.close()

def main():
    monitor_list = ['sz002466', 'sz002460', 'sz300073', 'sz000558', 'sz300151', 'sz000952']
    sl1 = SinaLevel1(monitor_list, 2)

if __name__ == '__main__':
    main()