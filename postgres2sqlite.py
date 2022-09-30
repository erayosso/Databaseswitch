#!/usr/bin/env python3

import argparse
import sqlite3, psycopg2, sys

parser = argparse.ArgumentParser()
parser.add_argument('--sqlite', type=str, required=True)
parser.add_argument('--pgdb', type=str, required=True)
parser.add_argument('--pguser', type=str, required=True)
parser.add_argument('--pgpwd', type=str, required=True)
parser.add_argument('--pghost', type=str, required=True)
parser.add_argument('--pgport', type=int, required=True)
args = parser.parse_args()



#SqLite database to connect to
con = sqlite3.connect(args.sqlite)



#Postgres database parameters
conpg = psycopg2.connect(database=args.pgdb,
                         user=args.pguser,
                         password=args.pgpwd,
                         host=args.pghost,
                         port=args.pgport)





def get_psql_tables(conpg):
    ''':returns: all tables in given database'''
    tabnames=[]
    curpg = conpg.cursor()
    curpg.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;") 

    tabgrab = curpg.fetchall()
    for item in tabgrab:
        tabnames.append(item[0])
    return tabnames



def postgresColNames(conpg, table):
    ''':returns: column names in given table'''
    colnames = []
    curpg = conpg.cursor()
    query = f"SELECT * FROM {table} LIMIT 1"
    curpg.execute(query)
    col_names=[i for i in curpg.description]
    for col in col_names:
        colnames.append(col[0])
    return colnames




def get_col_datatype(conpg,table):
    ''':returns: datatype of all columns in given table'''
    curpg = conpg.cursor()
    list = []
    for col in postgresColNames(conpg, table):
        curpg.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' AND column_name = '{col}';")
        list  += curpg.fetchall()
    return list

def get_create_statement(conpg, table):
    ''':returns: create statement of given table'''
    query = f"CREATE TABLE IF NOT EXISTS {table} ("
    query += ', '.join([f'{column} {type}' for column, type in get_col_datatype(conpg, table)])
    query+= ");"
    query = query.replace("character varying", "varchar(256)").replace("key integer", "key integer NOT NULL PRIMARY KEY AUTOINCREMENT")
    return query




def get_all_creates(conpg,tables):
    ''':returns: all creates of a database in a list'''
    return [get_create_statement(conpg, table) for table in tables]


cursq = con.cursor()
for create in get_all_creates(conpg,get_psql_tables(conpg)):
    cursq.execute(create)
    print(create)


for table in get_psql_tables(conpg):
    curpg = conpg.cursor()
    curpg.execute("SELECT * FROM %s;" %table)
    rows = curpg.fetchall()
    try:
        if len(rows) > 0:
            colcount = len(rows[0])
            pholder = '?,' * colcount
            newholder = pholder[:-1]
            cursq.executemany(f'INSERT INTO {table} VALUES ({newholder});', rows)
            print("Inserted into", table)
            con.commit()

    except psycopg2.DatabaseError as e:
        print(f'Error {e}')
        sys.exit(1)

conpg.close()
con.close()
 
