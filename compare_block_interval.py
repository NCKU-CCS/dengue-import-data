import datetime
import psycopg2

from clipper import *

## for range date
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

## for expand block
def expand_block(geom_astext, meter):
    meter_gps = 0.00000900900901
    polygons = list()
    poly = list()
    for point_str in geom_astext[9:-2].split(','):
        point_split = point_str.split()
        x = float(point_split[0]) * 1000000
        y = float(point_split[1]) * 1000000
        poly.append(Point(x, y))
    polygons.append(poly)
    expand_poly = OffsetPolygons(polygons, meter_gps * meter * 1000000)
    point_list = list()
    for point_obj in expand_poly[0]:
        point_list.append(str(point_obj.x/1000000) + " " + str(point_obj.y/1000000))
    point_list.append(str(expand_poly[0][0].x/1000000) + " " + str(expand_poly[0][0].y/1000000))
    return 'POLYGON((' + ','.join(point_list) + '))'

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

output_str = str()
cur.execute("SELECT * FROM dengue_prevention_block")
dengue_prevention_blocks = cur.fetchall()
for dengue_prevention_block in dengue_prevention_blocks:
    if dengue_prevention_block[2] == None:
        continue

    print (dengue_prevention_block[0])
    before_start_date = dengue_prevention_block[2] + datetime.timedelta(days=-11)
    before_end_date = dengue_prevention_block[2] + datetime.timedelta(days=3)
    after_start_date = dengue_prevention_block[2] + datetime.timedelta(days=4)
    after_end_date = dengue_prevention_block[2] + datetime.timedelta(days=17)
    today_date = datetime.datetime.now().date()

    cur.execute("SELECT ST_AsText('%s')" % dengue_prevention_block[3])
    origin_polygon_str = cur.fetchone()[0]
    reduce_10_polygon_str = expand_block(origin_polygon_str, -10)
    cur.execute("SELECT ST_GeomFromText('%s', 4326)" % (reduce_10_polygon_str))
    reduce_10_polygon_geom = cur.fetchone()[0]

    expand_20_polygon_str = expand_block(origin_polygon_str, 20)
    cur.execute("SELECT ST_GeomFromText('%s', 4326)" % (expand_20_polygon_str))
    expand_20_polygon_geom = cur.fetchone()[0]

    date_list = list()
    origin_count_list = list()
    for single_date in daterange(before_start_date, after_end_date):
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, reduce_10_polygon_geom))
        each_date_count = cur.fetchone()[0]
        date_list.append(single_date.strftime("%Y-%m-%d"))
        origin_count_list.append(each_date_count)

    expand_20_count_list = list()
    for single_date in daterange(before_start_date, after_end_date):
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, expand_20_polygon_geom))
        each_date_count = cur.fetchone()[0]
        expand_20_count_list.append(each_date_count)

    expand_50_count_list = list()
    for single_date in daterange(before_start_date, after_end_date):
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, dengue_prevention_block[4]))
        each_date_count = cur.fetchone()[0]
        expand_50_count_list.append(each_date_count)

    expand_100_count_list = list()
    for single_date in daterange(before_start_date, after_end_date):
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, dengue_prevention_block[5]))
        each_date_count = cur.fetchone()[0]
        expand_100_count_list.append(each_date_count)

    expand_150_count_list = list()
    for single_date in daterange(before_start_date, after_end_date):
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, dengue_prevention_block[6]))
        each_date_count = cur.fetchone()[0]
        expand_150_count_list.append(each_date_count)

    origin_output_data_list = list()
    for data_index, date_str in enumerate(date_list):
        origin_output_data_list.append(date_str + ' ' + str(origin_count_list[data_index]))
    output_str = output_str + dengue_prevention_block[0] + ',' +\
                 dengue_prevention_block[2].strftime("%Y-%m-%d") + ',' +\
                 'inside-10' + ',' +\
                 ','.join(origin_output_data_list) + '\n'

    expand_20_data_list = list()
    for data_index, date_str in enumerate(date_list):
        expand_20_data_list.append(date_str + ' ' + str(expand_20_count_list[data_index] - origin_count_list[data_index]))
    output_str = output_str + dengue_prevention_block[0] + ',' +\
                 dengue_prevention_block[2].strftime("%Y-%m-%d") + ',' +\
                 'inside-10~20' + ',' +\
                 ','.join(expand_20_data_list) + '\n'

    expand_50_data_list = list()
    for data_index, date_str in enumerate(date_list):
        expand_50_data_list.append(date_str + ' ' + str(expand_50_count_list[data_index] - expand_20_count_list[data_index]))
    output_str = output_str + dengue_prevention_block[0] + ',' +\
                 dengue_prevention_block[2].strftime("%Y-%m-%d") + ',' +\
                 '20~50' + ',' +\
                 ','.join(expand_50_data_list) + '\n'

    expand_100_data_list = list()
    for data_index, date_str in enumerate(date_list):
        expand_100_data_list.append(date_str + ' ' + str(expand_100_count_list[data_index] - expand_50_count_list[data_index]))
    output_str = output_str + dengue_prevention_block[0] + ',' +\
                 dengue_prevention_block[2].strftime("%Y-%m-%d") + ',' +\
                 '50~100' + ',' +\
                 ','.join(expand_100_data_list) + '\n'

    expand_150_data_list = list()
    for data_index, date_str in enumerate(date_list):
        expand_150_data_list.append(date_str + ' ' + str(expand_150_count_list[data_index] - expand_100_count_list[data_index]))
    output_str = output_str + dengue_prevention_block[0] + ',' +\
                 dengue_prevention_block[2].strftime("%Y-%m-%d") + ',' +\
                 '100~150' + ',' +\
                 ','.join(expand_150_data_list) + '\n'

with open('block_interval.csv', 'w') as myfile:
    myfile.write(output_str)
