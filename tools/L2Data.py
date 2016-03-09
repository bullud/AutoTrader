import getopt
import sys

def main(argv):
    try:
        options, args= getopt.getopt(argv[1:], "hL:D:", ["help", "dtype=", "dpath=", "tpath="])
    except getopt.GetoptError as e:
        print('end')
        sys.exit()