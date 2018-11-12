#!/usr/bin/python2.7
#
# Import the bug_all.csv data into opendata database.
# data url: http://54.65.74.29/file/bug_all.csv
#

import psycopg2
import requests
import csv

if __name__ == '__main__':
    try:
        conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
    except:
        print ("I am unable to connect to the database")

    cur = conn.cursor()
    req = requests.get('http://54.65.74.29/file/bug_all.csv')
    data = req.text

    reader = csv.reader(data.splitlines(), delimiter=',')
    cur.execute('DELETE FROM bug')
    conn.commit()
    for index, row in enumerate(reader):
        if index == 0:
            continue
        # date
        row[1] = "-".join(row[1].split('/'))
        cur.execute('INSERT INTO bug values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\
                                                %s,%s,%s,%s,%s,%s,%s,%s)', \
                                                (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],\
                                                row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],\
                                                row[20],row[21],row[22],row[23],row[24],row[25],row[26],row[27],row[28],row[29],\
                                                row[30],row[31],row[32],row[33],row[34],row[35],row[36],row[37]))

    conn.commit()
    cur.close()
    conn.close()



