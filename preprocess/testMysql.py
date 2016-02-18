import MySQLdb
import tushare as ts

con=MySQLdb.connect(host='localhost', db='trans', user='lidian', passwd='123@321ld')

df = ts.get_hist_data('600848')

df.to_sql(name = '600848', con = con, flavor = 'mysql', if_exists = 'append')

con.close()