import json
import psycopg2
from pprint import pprint
from subprocess import call

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

geojson_dict = {"type": "GeometryCollection"}
geojson_dict['geometries'] = list()
cur.execute("SELECT * FROM dengue_prevention_block")
dengue_prevention_blocks = cur.fetchall()
for dengue_prevention_block in dengue_prevention_blocks:
    ## get block_poly geojson
    cur.execute("SELECT ST_AsGeoJSON('%s')" % (dengue_prevention_block[3]))
    prevention_block_geojson = cur.fetchone()[0]

    ## get other info
    prevention_block_dict = json.loads(prevention_block_geojson)
    dengue_prev_block_date = dengue_prevention_block[2]
    if dengue_prev_block_date != None:
        dengue_prev_block_date = dengue_prev_block_date.strftime("%Y-%m-%d")
    prevention_block_dict['properties'] = {"way_id": dengue_prevention_block[0], "region": dengue_prevention_block[1], "date": dengue_prev_block_date, "is_origin_effect": dengue_prevention_block[7], "is_50_effect": dengue_prevention_block[8], "is_100_effect": dengue_prevention_block[9], "is_150_effect": dengue_prevention_block[10], "count_origin_14_day": dengue_prevention_block[11], "count_50_14_day": dengue_prevention_block[12], "count_100_14_day": dengue_prevention_block[13], "count_150_14_day": dengue_prevention_block[14], "tainan_same_period": dengue_prevention_block[15]}
    pprint (prevention_block_dict)
    geojson_dict['geometries'].append(prevention_block_dict)


with open('prev_block_geo.geojson', 'w') as myfile:
    json.dump(geojson_dict, myfile, indent=4)

cur.close()
conn.close()

call(["topojson", "-o", "prev_block_geo.topojson", "prev_block_geo.geojson"])
