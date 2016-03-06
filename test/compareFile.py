import os
import sys
from utils import _const

_const.dir1='I:\\StockData\\level2_gongsi\\'
_const.dir2='I:\\StockData\\level2\\'

def main(argv):
    i = 0
    j = 0
    for parent, dirnames, filenames in os.walk(_const.dir1):
        for filename in filenames:
            fullpath = os.path.join(parent, filename)
            #print(fullpath)
            #print(fullpath[27:])
            i+=1

            peerpath = os.path.join(_const.dir2, fullpath[27:])
            if os.path.exists(peerpath) == False:
                print(fullpath)

            s1 = os.path.getsize(fullpath)
            s2 = os.path.getsize(peerpath)

            if s1 != s2:
                print(fullpath)

            t1 = os.path.getmtime(fullpath)
            t2 = os.path.getmtime(peerpath)
            if t1 != t2:
                print(fullpath)



    print(str(i))

if __name__ == '__main__':
    main(sys.argv)