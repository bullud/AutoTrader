from common.day import *
from common.bid import *
import datetime


    #for d in input:
def maCalc():
    return

def dayArchiving(inp, oup):
    d = inp[0]
    oup.open_price  = d.to_open_price
    oup.close_price = d.cur_price
    oup.high_price  = d.to_high_price
    oup.low_price   = d.to_low_price
    oup.traded_share = d.traded_share
    oup.traded_money = d.traded_money
    return

def procOneDayData(inp, oup):

    return



def main():
    dailydb = SqliteDatabase('../daily.db')

    day._meta.database = dailydb

    biddb = SqliteDatabase('../bid.db')
    bid._meta.database = biddb

    stocks = bid.select(bid.code).distinct()
    #print(result[0].name)

    for stock in stocks:
        #print(stock.code)

        lastBid = bid.select().where(bid.code == stock.code).order_by(bid.date_time.desc()).limit(1)
        lastDateT = lastBid[0].date_time.date()

        if len(lastBid) == 0:
            print("his code doesn't have bid data: " + stock.code + ":" + stock.name )
            continue

        procTime = day.select(day.dateT).where(day.code == stock.code).order_by(day.dateT.desc()).limit(1)

        if len(procTime) == 0:
            firstBid = bid.select().where(bid.code == stock.code).order_by(bid.date_time.asc()).limit(1)
            firstDateT = firstBid[0].date_time.date()
        else:
            firstDateT = procTime[0].dateT + datetime.timedelta(1)


        #print(firstDateT)
        #print(lastDateT)
        if lastDateT < firstDateT:
            print("%s no more new bid need to proc" % stock.code)
            continue

        dayCount = (lastDateT - firstDateT).days + 1
        for oneDate in [firstDateT + datetime.timedelta(n) for n in range(dayCount)]:
            oneDateBegTime = datetime.datetime.strptime(str(oneDate), '%Y-%m-%d')
            oneDateEndTime = datetime.datetime.strptime(str(oneDate + datetime.timedelta(1)), '%Y-%m-%d')

            #print(oneDateEndTime)

            oneDateData = bid.select().where(bid.code == stock.code, \
                                             bid.date_time>= oneDateBegTime, \
                                             bid.date_time<oneDateEndTime).order_by(bid.date_time.desc())
            #print("begTime:%s -- endTime:%s: %d" % (oneDateBegTime, oneDateEndTime, len(oneDateData)))
            print("%s, %s : %d" %(stock.code, oneDate, len(oneDateData)))

            if len(oneDateData) == 0:
                continue

            oneDay = day()
            oneDay.code = stock.code
            oneDay.dateT = oneDate
            procOneDayData(oneDateData, oneDay)
            oneDay.save()


if __name__ == '__main__':
    main()