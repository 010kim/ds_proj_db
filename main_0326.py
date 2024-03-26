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
    query = f"""select item, count
                from (  select item, count(user) as count
                        from ratings
                        where rating is not Null
                        group by item) R1
                where item >=150 and item < 350
                order by count desc limit {rec_num}"""
    
    # 쿼리의 결과를 sample 변수에 저장하세요.
    sample = get_output(query)

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
    query = f"""select R3.item, round(avg(R3.P_rating),4) as prediction
                from (
                    select R1.item, R1.user, round((R1.rating - R2.min_rating)/(R2.max_rating-R2.min_rating),4) as P_rating  
                    from (select user, item, rating
                        from ratings
                        where user is Not Null and rating is Not Null) as R1
                        left outer join
                        (select user, max(rating) as max_rating, min(rating) as min_rating
                        from ratings
                        where user is Not Null and rating is Not Null
                        group by user) as R2
                        on R1.user = R2.user
                ) R3
                where R3.item >=150 and R3.item < 350
                group by R3.item
                order by prediction desc limit {rec_num}"""
    # 쿼리의 결과를 sample 변수에 저장하세요.
    sample = get_output(query)

    # do not change column names
    df = pd.DataFrame(sample, columns=['item', 'prediction'])
    # TODO end

    # Do not change this part
    with open('pbr.txt', 'w') as f:
        f.write(tabulate(df, headers=df.columns, tablefmt='psql', showindex=False))
    print("Output printed in pbr.txt")


# [Algorithm 2] Item-based Recommendation
def ibcf(user_input=True, user_id=None, item_cnt=None):
    if user_input:
        user = int(input('User Id: '))
        rec_num = int(input('Number of recommendations?: '))
    else:
        assert user_id is not None
        assert item_cnt is not None
        user = int(user_id)
        rec_num = int(item_cnt)

    print("=" * 99)
    print(f'Item-based Collaborative Filtering')
    print(f'Recommendations for user {user}')

    # TODO: remove sample, return actual recommendation result as df
    # YOUR CODE GOES HERE !
    query = f"""select F.user as user, F.item_2 as item, round(sum(F.sim_adj * F.rating_adj), 4) as prediction from 
                (select D.item_1, D.item_2, D.sim_adj, E.user, E.rating_adj 
                from (
                    select B.item_1, B.item_2, round(B.sim / B.sim_sum, 4) as sim_adj
                    from (
                    select A.item_1, A.item_2, A.sim, sum(A.sim) over (partition by A.item_1) as sim_sum 
                    from (select item_1, item_2, sim, row_number() over (partition by item_1 order by sim desc, item_2 asc) as rn from item_similarity) as A 
                    where A.rn <= 5) as B) as D
                left join (
                    select C.item, C.user, case when rating is not null then C.rating else C.avg_rating end as rating_adj from (
                    select item, user, rating, avg(rating) over (partition by item) as avg_rating from ratings) as C
                    ) as E
                on D.item_1 = E.item) as F
                group by F.user, F.item_2 having F.user = {user} 
                order by prediction desc, item asc limit {rec_num}"""
    
    # 쿼리의 결과를 sample 변수에 저장하세요.
    #sample = [(user, 50-x, x/10)
    #          for x in range(50, math.ceil(rec_num * 10) - 1, -1)]

    sample = get_output(query)

    # do not change column names
    df = pd.DataFrame(sample, columns=['user', 'item', 'prediction'])
    # TODO end

    # Do not change this part
    with open('ibcf.txt', 'w') as f:
        f.write(tabulate(df, headers=df.columns, tablefmt='psql', showindex=False))
    print("Output printed in ibcf.txt")



# [Algorithm 3] (Optional) User-based Recommendation
def ubcf(user_input=True, user_id=None, item_cnt=None):
    if user_input:
        user = int(input('User Id: '))
        rec_num = int(input('Number of recommendations?: '))
    else:
        assert user_id is not None
        assert item_cnt is not None
        user = int(user_id)
        rec_num = int(item_cnt)

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
