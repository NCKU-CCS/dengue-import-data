from shapely.geometry import Polygon
from shapely.ops import cascaded_union

import json
import psycopg2
from ast import literal_eval
from subprocess import call

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

cur.execute("SELECT ST_AsText(block_poly) from dengue_prevention_block where no='%s'" % ('372966466'))
first_polygon_str = cur.fetchone()[0]

cur.execute("SELECT ST_AsText(block_poly) from dengue_prevention_block where no='%s'" % ('372968829'))
second_polygon_str = cur.fetchone()[0]

first_point_list = list()
second_point_list = list()

for point_str in first_polygon_str[9:-2].split(','):
    point_split = point_str.split()
    point_x = int(float(point_split[0]) * 1000000)
    point_y = int(float(point_split[1]) * 1000000)
    first_point_list.append((point_x, point_y))

for point_str in second_polygon_str[9:-2].split(','):
    point_split = point_str.split()
    point_x = int(float(point_split[0]) * 1000000)
    point_y = int(float(point_split[1]) * 1000000)
    second_point_list.append((point_x, point_y))

first_polygon = Polygon(first_point_list)
second_polygon = Polygon(second_point_list)

merge_polygon = cascaded_union([first_polygon, second_polygon])
merge_point_list = list()
for point_str in str(merge_polygon)[10:].split(')')[0].split(','):
    point_split = point_str.split()
    point_x = str(float(point_split[0]) / 1000000)
    point_y = str(float(point_split[1]) / 1000000)
    merge_point_list.append(point_x + ' ' + point_y)

merge_polygon_str = 'POLYGON((' + ','.join(merge_point_list) + '))'
cur.execute("SELECT ST_AsGeoJSON(ST_GeomFromText('%s', 4326))" % (str(merge_polygon_str)))
merge_polygon_geojson = json.loads(cur.fetchone()[0])

cur.close()
conn.close()

with open('tmp_merge.geojson', 'w') as myfile:
    json.dump(merge_polygon_geojson, myfile, indent=4)

call(["topojson", "-o", "tmp_merge.topojson", "tmp_merge.geojson"])
