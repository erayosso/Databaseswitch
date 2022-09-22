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


#Postgres database stuff
pgdb = args.pgdb
pguser = args.pguser
pgpswd = args.pgpwd
pghost = args.pghost
pgport = args.pgport
pgschema = 'public'




def get_sqlite_tables(con):
    ''' :returns: all tables in given database '''
    tabnames=[]
    cursorObj = con.cursor()
    cursorObj.execute('SELECT name from sqlite_master where type= "table"')  

    tables = cursorObj.fetchall()
    for table in tables:
        tabnames.append(table[0])
    return tabnames


def sqliteColNames(con,table):
    ''' :returns: column names in given table '''
    cur = con.cursor()
    query = "select * from " + table + " limit 1"
    cur.execute(query)
    return [column[0] for column in cur.description]


def get_boolean_columns(table_name):
    ''' :returns: columns with type boolean '''
    cursorObj = con.cursor()
    cursorObj.execute("PRAGMA table_info(" +table_name +");")
    tables = cursorObj.fetchall()
    return [item[1] for item in tables if item[2] == "boolean"]



#switch 1/0 to t/f for psql
def fromSqliteToPostgresBool(el):
    return "True" if el == 1 else "False"


#replace 1/0 to t/f if column is integer
def replaceValuesInColumn(values, index):
    for i in range(0, len(values)):
        row = list(values[i])
        for j in range(0, len(row)):
            if j == index:
              row[j] = fromSqliteToPostgresBool(row[j])
        values[i] = tuple(row)
    return values



#get create statements for tables and change them to psql syntax
for table in get_sqlite_tables(con):
    if table == "sqlite_sequence":
        continue
    cursq=con.cursor()
    cursq.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name = ?;", (table,))
    create = cursq.fetchone()[0]
    create = str(create).replace("`", "") \
                        .replace("integer NOT NULL PRIMARY KEY AUTOINCREMENT","SERIAL PRIMARY KEY") \
                        .replace("blob","BYTEA") \
                        .replace("bigint","BIGINT") \
                        .replace("varchar","VARCHAR")
    print(create)

    
    #connect to psql and insert values
    try:
 
        conpg = psycopg2.connect(database=pgdb, user=pguser, password=pgpswd,
                               host=pghost, port=pgport)
        curpg = conpg.cursor()
        curpg.execute(f'SET search_path TO {pgschema};')
        curpg.execute(f'DROP TABLE IF EXISTS {table};')
        curpg.execute(create)
        column_names = sqliteColNames(con, table)
        cursq.execute(f'SELECT * FROM {table};')
        rows = cursq.fetchall()
        names = list(map(lambda x: x[0], cursq.description))
        booleans = get_boolean_columns(table)
        both = set(names).intersection(booleans)
        indices = [names.index(x) for x in both]
        
        if len(rows) > 0: #if table isnt empty
            if len(indices) > 0:
                for index in indices:
                    rows = replaceValuesInColumn(rows, index)
            colcount=len(rows[0])
            pholder='%s,'*colcount
            newholder=pholder[:-1]
            curpg.executemany(f'INSERT INTO {table} VALUES ({newholder});', rows)
        conpg.commit()
        print(f'Created {table}')

 
    except psycopg2.DatabaseError as e:
        print(f'Error {e}')
        sys.exit(1)
 
    finally:
 
        if conpg:
            conpg.close()
 
con.close()