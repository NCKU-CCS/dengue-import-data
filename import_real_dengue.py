import psycopg2
import requests
import datetime


conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

req= requests.get('https://dl.dropboxusercontent.com/u/13580830/Tainan/realdata.tsv')
## delete all row
cur.execute('DELETE FROM real_dengue')

for line_index, line in enumerate(req.text.split('\n')):
    if line_index == 0 or line == '':
        continue
    line_split = line.split('\t')

    print (line_split)
    line_date = datetime.datetime.strptime(line_split[5]+' 2015', "%m/%d %Y").date()
    sql = "INSERT INTO real_dengue(no, city, dist, vil, age, date, result, lon, lat, location) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GEOMFROMTEXT(%s, 4326))"
    params = [line_split[0], line_split[1], line_split[2], line_split[3], line_split[4], line_date, line_split[7], line_split[9], line_split[10], 'POINT('+line_split[9]+' '+line_split[10]+')']
    cur.execute(sql, params)
    # cur.execute("INSERT INTO real_dengue(no, city, dist, vil, age, date, result, lon, lat, location) VALUES (%s, %s, %s, %s, '%s', %s, %s, %s, %s, ST_SetSRID(ST_MAKEPOINT(%s, %s), 4326))" % (line_split[0], line_split[1], line_split[2], line_split[3], line_split[5], line_date, line_split[8], line_split[9], line_split[10], line_split[9], line_split[10]))

conn.commit()
cur.close()
conn.close()
