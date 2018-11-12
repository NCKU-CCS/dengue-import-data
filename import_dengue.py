import psycopg2
import requests
import datetime

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

req= requests.get('http://54.65.74.29/file/dengue_all.csv')
cur.execute("DELETE from dengue")
conn.commit()
for line_index, line in enumerate(req.text.split('\r\n')):
    if line == '' or line_index == 0:
        continue
    line_split = line.split(',')
    line_date = datetime.datetime.strptime(line_split[1], '%Y/%m/%d').date()
    cur.execute('INSERT INTO dengue(no, check_date, dist, vil, road, lat, lon) VALUES (%s, %s, %s, %s, %s, %s, %s)', (line_split[0], line_date, line_split[2], line_split[3], line_split[4], line_split[5], line_split[6]))
    print (line)

conn.commit()
cur.close()
conn.close()
