import mysql.connector
from tabulate import tabulate
import pandas as pd
import math
import sys

HOST = "147.46.15.238"
PORT = "7000"
USER = "DS2024_0022"
PASSWD = "DS2024_0022"
DB = "DS_proj_15"

connection = mysql.connector.connect(
    host=HOST,
    port=7000,
    user=USER,
    passwd=PASSWD,
    db=DB,
    autocommit=True  # to create table permanently
)

cur = connection.cursor(dictionary=True)

def get_output(query):
    cur.execute(query)
    out = cur.fetchall()
    df = pd.DataFrame(out)
    return df

if __name__ == "__main__":
    query = """select * from ratings where item >= 10 and item < 15 and user in (0, 5)"""
    print(get_output(query))
    query = """select item, avg(rating) from ratings where item >= 10 and item < 15 and user in (0, 5) group by item"""
    print(get_output(query))
    query = """select item, avg(rating) from ratings where item >= 10 and item < 15 and user in (0, 5) group by item"""
    print(get_output(query))