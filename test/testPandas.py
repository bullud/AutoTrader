import pandas as pd
from pandas import DataFrame
from peewee import *
import sqlite3

con = sqlite3.connect('bid.db')
sql = 'SELECT * from bid'

df = pd.read_sql(sql, con)
print(df)