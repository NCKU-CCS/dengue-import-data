import psycopg2
import csv


conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

with open("total_in_each_block.csv","w") as wfile:
	for i in range(16974):
		no = i + 1
		cur.execute("SELECT count(*) from tainan_blocks_500 as B, real_dengue as D where ST_Contains(block_poly, ST_GeomFromText(FORMAT('POINT(%s %s)',lat,lon),4326)) and B.no={0}".format(no))
		count = cur.fetchone()[0]
		wfile.write("{0},{1}".format(no,count))
		break

conn.commit()
cur.close()
conn.close()