#!/usr/bin/env python3

import argparse
from cmath import inf
import sqlite3, psycopg2, sys
from webbrowser import get
from venv import create




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
pgdb=args.pgdb
pguser=args.pguser
pgpswd=args.pgpwd
pghost=args.pghost
pgport=args.pgport
pgschema='public'
conpg = psycopg2.connect(database=pgdb, user=pguser, password=pgpswd,
                               host=pghost, port=pgport)





def get_psql_tables(conpg):
    ''':returns: all tables in given database'''
    con = conpg
    tabnames=[]
    curpg = con.cursor()
    curpg.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name;") 

    tabgrab = curpg.fetchall()
    for item in tabgrab:
        tabnames.append(item[0])
    return tabnames



def postgresColNames(conpg,table):
    ''':returns: column names in given table'''
    con = conpg
    colnames= []
    curpg =con.cursor()
    query = "select * from " + table + " limit 1"
    curpg.execute(query)
    col_names=[i for i in curpg.description]
    for col in col_names:
        colnames.append(col[0])
    return colnames




def get_col_datatype(conpg,table):
    ''':returns: datatype of all columns in given table'''
    con = conpg
    curpg = con.cursor()
    list = []
    for col in postgresColNames(con,table):
        curpg.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '"+ table + "' AND column_name = '"+ col+"';")
        list  += curpg.fetchall()
    return list




def split_col_tuple(lst):
    ''':returns: two seperate lists of given name/type tuple'''
    lst1 = []
    lst2 = []
    for x, y in lst:
        lst1.append(x)
        lst2.append(y)
    return (lst1, lst2)





def get_create_statement(conpg,table):
    ''':returns: create statement of given table'''
    query = "CREATE TABLE IF NOT EXISTS " +  table + "("
    col, typ = split_col_tuple(get_col_datatype(conpg,table))
    for i in range(len((col))):
        query += col[i]
        query += " "
        query += typ[i]
        query += ", "
    query = query[:-2]
    query+= ");"
    query = str(query).replace("character varying","varchar(256)").replace("key integer", "key integer NOT NULL PRIMARY KEY AUTOINCREMENT")
    return query




def get_all_creates(conpg,tables):
    ''':returns: all creates of a database in a list'''
    list = []
    for table in tables:
        x = get_create_statement(conpg,table)
        list.append(x)
    return list




cursq = con.cursor()
for create in get_all_creates(conpg,get_psql_tables(conpg)):
    if create == get_all_creates(conpg,get_psql_tables(conpg))[:-1]:
        continue
    else:
        cursq.execute(create)
        print(create)


for table in get_psql_tables(conpg):
    curpg = conpg.cursor()
    curpg.execute("SELECT * FROM %s;" %table)
    rows=curpg.fetchall()
    try:
        if len(rows) > 0:
            colcount=len(rows[0])
            pholder='?,'*colcount
            newholder=pholder[:-1]
            cursq.executemany(f'INSERT INTO {table} VALUES ({newholder});', rows)
            print("Inserted into", table)
            con.commit()

    except psycopg2.DatabaseError as e:
        print(f'Error {e}')
        sys.exit(1)

conpg.close()
con.close()
 

        

    










#get create statements for tables and change them to sqlite syntax
#for table in showPostgresTables(con):
#   curpg = con.cursor