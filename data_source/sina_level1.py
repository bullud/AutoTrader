#coding=utf-8
import http.client
import threading
import time
import urllib

class SinaLevel1(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        self.get_data()

    def get_data(self):
        httpClient = None
        try:
            httpClient = http.client.HTTPConnection('hq.sinajs.cn')
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
   print('Hello world!')
   httpClient = None
   try:
      httpClient = http.client.HTTPConnection('hq.sinajs.cn')
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

if __name__ == '__main__':
    main()