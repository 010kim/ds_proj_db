#!/usr/bin/env python
# coding: utf-8

###################################################################
#                                                                 #
#   2024 DS2 Database Project : Recommendation using SQL-Python   #
#                                                                 #
###################################################################

import mysql.connector
from tabulate import tabulate
import pandas as pd
import math
import sys

## Connect to Remote Database
## Insert database information

HOST = "147.46.15.238"
PORT = "7000"
USER = ""
PASSWD = ""
DB = ""

connection = mysql.connector.connect(
    host=HOST,
    port=7000,
    user=USER,
    passwd=PASSWD,
    db=DB,
    autocommit=True  # to create table permanently
)

cur = connection.cursor(dictionary=True)

## 수정할 필요 없는 함수입니다.
# DO NOT CHANGE INITIAL TABLES IN prj.sql
def get_dump(mysql_con, filename):
    '''
    connect to mysql server using mysql_connector
    load .sql file (filename) to get queries that create tables in an existing database (fma)
    '''
    query = ""
    try:
        with mysql_con.cursor() as cursor:
            for line in open(filename, 'r'):
                if line.strip():
                    line = line.strip()
                    if line[-1] == ";":
                        query += line
                        cursor.execute(query)
                        query = ""
                    else:
                        query += line

    except Warning as warn:
        print(warn)
        sys.exit()


## 수정할 필요 없는 함수입니다.
# SQL query 를 받아 해당 query를 보내고 그 결과 값을 dataframe으로 저장해 return 해주는 함수
def get_output(query):
    cur.execute(query)
    out = cur.fetchall()
    df = pd.DataFrame(out)
    return df


# [Algorithm 1] Popularity-based Recommendation - 1 : Popularity by rating count
def popularity_based_count(user_input=True, item_cnt=None):
    if user_input:
        rec_num = int(input('Number of recommendations?: '))
    else:
        assert item_cnt is not None
        rec_num = int(item_cnt)
    print(f"Popularity Count based recommendation")
    print("=" * 99)

    # TODO: remove sample, return actual recommendation result as df
    # YOUR CODE GOES HERE !
    # 쿼리의 결과를 sample 변수에 저장하세요.
    QUERY = "select * from ratings limit 5"
    
    sample = [(x, 5.0-0.1*x) for x in range(rec_num)]

    # do not change column names
    df = pd.DataFrame(sample, columns=['item', 'count'])

    # TODO end

    # Do not change this part
    with open('pbc.txt', 'w') as f:
        f.write(tabulate(df, headers=df.columns, tablefmt='psql', showindex=False))
    print("Output printed in pbc.txt")


# [Algorithm 1] Popularity-based Recommendation - 2 : Popularity by average rating
def popularity_based_rating(user_input=True, item_cnt=None):
    if user_input:
        rec_num = int(input('Number of recommendations?: '))
    else:
        assert item_cnt is not None
        rec_num = int(item_cnt)
    print(f"Popularity Rating based recommendation")
    print("=" * 99)

    # TODO: remove sample, return actual recommendation result as df
    # YOUR CODE GOES HERE !
    # 쿼리의 결과를 sample 변수에 저장하세요.
    sample = [(x, 5.0-0.1*x) for x in range(rec_num)]

    # do not change column names
    df = pd.DataFrame(sample, columns=['item', 'prediction'])
    # TODO end

    # Do not change this part
    with open('pbr.txt', 'w') as f:
        f.write(tabulate(df, headers=df.columns, tablefmt='psql', showindex=False))
    print("Output printed in pbr.txt")


# [Algorithm 2] Item-based Recommendation
def ibcf(user_input=True, user_id=None, rec_threshold=None, rec_max_cnt=None):
    if user_input:
        user = int(input('User Id: '))
        rec_cnt = int(input('Recommend Count: '))
        rec_num = float(input('Recommendation Threshold: '))
    else:
        assert user_id is not None
        assert rec_max_cnt is not None
        assert rec_threshold is not None
        user = int(user_id)
        rec_cnt = int(rec_max_cnt)
        rec_num = float(rec_threshold)

    print("=" * 99)
    print(f'Item-based Collaborative Filtering')
    print(f'Recommendations for user {user}')

    # TODO: remove sample, return actual recommendation result as df
    # YOUR CODE GOES HERE !
    # 쿼리의 결과를 sample 변수에 저장하세요.
    sample = [(user, 50-x, x/10)
              for x in range(50, math.ceil(rec_num * 10) - 1, -1)]

    # do not change column names
    df = pd.DataFrame(sample, columns=['user', 'item', 'prediction'])
    # TODO end

    # Do not change this part
    with open('ibcf.txt', 'w') as f:
        f.write(tabulate(df, headers=df.columns, tablefmt='psql', showindex=False))
    print("Output printed in ibcf.txt")



# [Algorithm 3] (Optional) User-based Recommendation
def ubcf(user_input=True, user_id=None, rec_threshold=None, rec_max_cnt=None):
    if user_input:
        user = int(input('User Id: '))
        rec_cnt = int(input('Recommend Count: '))
        rec_num = float(input('Recommendation Threshold: '))
    else:
        assert user_id is not None
        assert rec_max_cnt is not None
        assert rec_threshold is not None
        user = int(user_id)
        rec_cnt = int(rec_max_cnt)
        rec_num = float(rec_threshold)

    print("=" * 99)
    print(f'User-based Collaborative Filtering')
    print(f'Recommendations for user {user}')

    # TODO: remove sample, return actual recommendation result as df
    # YOUR CODE GOES HERE !
    # 쿼리의 결과를 sample 변수에 저장하세요.
    sample = [(user, 50-x, x/10)
              for x in range(50, math.ceil(rec_num * 10) - 1, -1)]

    # do not change column names
    df = pd.DataFrame(sample, columns=['user', 'item', 'prediction'])
    # TODO end

    # Do not change this part
    with open('ubcf.txt', 'w') as f:
        f.write(tabulate(df, headers=df.columns, tablefmt='psql', showindex=False))
    print("Output printed in ubcf.txt")


## 수정할 필요 없는 함수입니다.
# Print and execute menu 
def menu():
    print("=" * 99)
    print("0. Initialize")
    print("1. Popularity Count-based Recommendation")
    print("2. Popularity Rating-based Recommendation")
    print("3. Item-based Collaborative Filtering")
    print("4. User-based Collaborative Filtering")
    print("5. Exit database")
    print("=" * 99)

    while True:
        m = int(input("Select your action : "))
        if m < 0 or m > 5:
            print("Wrong input. Enter again.")
        else:
            return m

def execute(argv):
    terminated = False
    while not terminated:
        if len(argv)<2:
            m = menu()
            if m == 0:
                # 수정할 필요 없는 함수입니다.
                # Upload prj.sql before this
                # If autocommit=False, always execute after making cursor
                get_dump(connection, 'prj.sql')
            elif m == 1:
                popularity_based_count()
            elif m == 2:
                popularity_based_rating()
            elif m == 3:
                ibcf()
            elif m == 4:
                ubcf()
            elif m == 5:
                terminated = True
            

        # 평가를 위한 코드입니다. 수정하지 마세요.
        else:
            with open(argv[1], 'r') as f:
                lines = f.readlines()
                for line in lines:
                    rec_args = list(map(float, line.split(',')))
                    if len(rec_args) > 1:
                        rec_args[1] = int(rec_args[1])
                    m = rec_args[0]
                    if m==0:
                        get_dump(connection, 'prj.sql')
                    elif m == 1:
                        popularity_based_count(False, *rec_args[1:])
                    elif m == 2:
                        popularity_based_rating(False, *rec_args[1:])
                    elif m == 3:
                        ibcf(False, *rec_args[1:])
                    elif m == 4:
                        ubcf(False, *rec_args[1:])
                    elif m == 5:
                        terminated = True
                    else:
                        print('Invalid menu option')

# DO NOT CHANGE
if __name__ == "__main__":
    execute(sys.argv)
