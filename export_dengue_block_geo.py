import json
import psycopg2
from subprocess import call

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

geojson_dict = {"type": "GeometryCollection"}
geojson_dict['geometries'] = list()
cur.execute("SELECT * FROM dengue_prevention_block")
dengue_prevention_blocks = cur.fetchall()
for dengue_prevention_block in dengue_prevention_blocks:
    cur.execute("SELECT ST_AsGeoJSON('%s')" % (dengue_prevention_block[3]))
    prevention_block_geojson = cur.fetchone()[0]
    prevention_block_dict = json.loads(prevention_block_geojson)
    geojson_dict['geometries'].append(prevention_block_dict)

    cur.execute("SELECT ST_AsGeoJSON('%s')" % (dengue_prevention_block[4]))
    prevention_block_geojson = cur.fetchone()[0]
    prevention_block_dict = json.loads(prevention_block_geojson)
    geojson_dict['geometries'].append(prevention_block_dict)

    cur.execute("SELECT ST_AsGeoJSON('%s')" % (dengue_prevention_block[5]))
    prevention_block_geojson = cur.fetchone()[0]
    prevention_block_dict = json.loads(prevention_block_geojson)
    geojson_dict['geometries'].append(prevention_block_dict)

    cur.execute("SELECT ST_AsGeoJSON('%s')" % (dengue_prevention_block[6]))
    prevention_block_geojson = cur.fetchone()[0]
    prevention_block_dict = json.loads(prevention_block_geojson)
    geojson_dict['geometries'].append(prevention_block_dict)
    break

with open('dengue_prev_geo.geojson', 'w') as myfile:
    json.dump(geojson_dict, myfile, indent=4)

cur.close()
conn.close()

call(["topojson", "-o", "dengue_prev_geo.topojson", "dengue_prev_geo.geojson"])
