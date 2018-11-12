import json
import psycopg2
import requests
import datetime

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

## for range date
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

req= requests.get('https://dl.dropboxusercontent.com/u/13580830/dengue_help/new_geo.geojson')
json_data = json.loads(req.text)

cur.execute("SELECT MAX(no) FROM flooding_block")
no = cur.fetchone()
no = no[0]
if no == None:
    no = 0
no += 1
print (no)

for feature in json_data['features']:
    for polygon in feature['geometry']['coordinates']:
        point_list = list()
        for point in polygon[0]:
            point_list.append(str(point[0]) + " " + str(point[1]))
        polygon_str = 'POLYGON((' + ','.join(point_list) + '))'
        sql = "SELECT ST_GeomFromText(%s, 4326)"
        params = [polygon_str]
        cur.execute(sql, params)
        block_geom = cur.fetchone()
        block_geom = block_geom[0]

        #if exist
        sql = "SELECT * FROM flooding_block WHERE block_poly=%s"
        params = [block_geom]
        cur.execute(sql,params)
        is_block_exist = cur.fetchone()
        print (is_block_exist)
        if is_block_exist != None and len(is_block_exist) >= 1:
            continue

        #if not exist
        sql = "INSERT INTO flooding_block (no, block_poly, class, note, each_day_count) VALUES (%s, %s, %s, %s, %s)"
        params = [no, block_geom, str(feature['properties']['CLASS']), feature['properties']['NOTE'], ""]
        cur.execute(sql, params)
        no += 1
conn.commit()

min_date = datetime.date(2015, 7, 20)
max_date = datetime.datetime.now().date()
cur.execute("SELECT * FROM flooding_block")
flooding_blocks = cur.fetchall()
for flooding_block in flooding_blocks:
    print (flooding_block[0])
    each_day_count_list = list()
    for single_date in daterange(min_date, max_date):
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, flooding_block[1]))
        each_day_count = cur.fetchone()
        each_day_count = each_day_count[0]
        each_day_count_list.append(single_date.strftime("%Y-%m-%d"))
        each_day_count_list.append(str(each_day_count))
    sql = "UPDATE flooding_block SET each_day_count=%s WHERE no=%s"
    params = [','.join(each_day_count_list), flooding_block[0]]
    cur.execute(sql, params)

conn.commit()
cur.close()
conn.close()
