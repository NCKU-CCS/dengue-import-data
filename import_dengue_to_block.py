import json
import psycopg2
import requests
import datetime
from subprocess import call

## for range date
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

cur.execute('SELECT * FROM tainan_blocks_500')
tainan_blocks_500_rows = cur.fetchall()
start_date = datetime.date(2015, 7, 20)
end_date = datetime.datetime.now().date()

for tainan_blocks_500_row in tainan_blocks_500_rows:
    print (tainan_blocks_500_row[0])
    all_count = 0
    each_day_count_list = list()
    cur.execute("SELECT COUNT(*) FROM real_dengue WHERE ST_Contains('%s', ST_SetSRID(ST_MakePoint(lat, lon), 4326))" % (tainan_blocks_500_row[2]))
    all_count = cur.fetchone()[0]
    if all_count > 0:
        for single_date in daterange(start_date, end_date):
            cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', ST_SetSRID(ST_MakePoint(lat, lon), 4326))" % (single_date, tainan_blocks_500_row[2]))
            each_day_count = cur.fetchone()[0]
            each_day_count_list.append(single_date.strftime("%Y-%m-%d") + " " + str(each_day_count))

    print(all_count)
    print (each_day_count_list)

    ## Update Database
    if all_count > 0:
        cur.execute("UPDATE tainan_blocks_500 SET dengue='%s' WHERE no=%d" % (','.join(each_day_count_list), tainan_blocks_500_row[0]))
    else:
        cur.execute("UPDATE tainan_blocks_500 SET dengue='%s' WHERE no=%d" % ("", tainan_blocks_500_row[0]))

conn.commit()

## To GeoJSON
# cur.execute('SELECT * FROM tainan_blocks_500')
# tainan_blocks_500_rows = cur.fetchall()
# geojson_dict = {"type": "GeometryCollection"}
# geojson_dict['geometries'] = list()
# for tainan_blocks_500_row in tainan_blocks_500_rows:
    # print(tainan_blocks_500_row[0])
    # if tainan_blocks_500_row[11] == "":
        # continue
    # cur.execute("SELECT ST_AsGeoJSON('%s')" % (tainan_blocks_500_row[2]))
    # tainan_block_geojson = cur.fetchone()[0]

    # tainan_block_dict = json.loads(tainan_block_geojson)
    # tainan_block_dict['properties'] = {"dengue_date_count": tainan_blocks_500_row[11]}
    # geojson_dict['geometries'].append(tainan_block_dict)

# with open('tainan_block_500.geojson', 'w') as myfile:
    # json.dump(geojson_dict, myfile, indent=4)

# call(["topojson", "-o", "tainan_block_500.topojson", "tainan_block_500.geojson"])

## To csv
# cur.execute('SELECT * FROM tainan_blocks_500')
# tainan_blocks_500_rows = cur.fetchall()
# output_str = str()
# for tainan_blocks_500_row in tainan_blocks_500_rows:
    # print(tainan_blocks_500_row[0])
    # if tainan_blocks_500_row[11] == "":
        # continue
    # cur.execute("SELECT ST_AsText('%s')" % tainan_blocks_500_row[1])
    # center_str = cur.fetchone()[0]
    # center_split = center_str[6:-1].split()
    # output_str = output_str + center_split[0] + ',' + center_split[1] + ',' + tainan_blocks_500_row[11] + '\n'

# with open('tainan_block_500.csv', 'w') as myfile:
    # myfile.write(output_str)

cur.close()
conn.close()

