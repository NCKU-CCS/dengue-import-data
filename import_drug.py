import psycopg2
import requests
import datetime

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

req = requests.get('http://54.65.74.29/file/drug_all.csv')
cur.execute('DELETE FROM drug')
conn.commit()
for line_index, line in enumerate(req.text.split('\r\n')):
    if line == '' or line_index == 0:
        continue
    line_split = line.split(',')
    if len(line_split) < 16:
        line_split.append('')
    line_date = datetime.datetime.strptime(line_split[4] + '2015', '%m月%d日%Y').date()
    cur.execute('INSERT INTO drug(no, clear_or_drug, dist, vil, date, week, gather_time, gather_address, lat, lon, locksmith, police, criminal_police, spray_worker, support_people, can_support_people) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)', (line_split[0], line_split[1], line_split[2], line_split[3], line_date, line_split[5], line_split[6], line_split[7], line_split[8], line_split[9], line_split[10], line_split[11], line_split[12], line_split[13], line_split[14], line_split[15]))
    print (line)

conn.commit()
cur.close()
conn.close()
