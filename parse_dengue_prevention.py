import datetime
import psycopg2
import requests
from lxml import etree
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

## Online Request
# req = requests.get('http://overpass-api.de/api/map?bbox=119.7084,22.7357,120.9828,23.5652')
# xml_str = str.encode(req.text)

# Read Local File
# with open('map', 'br') as myfile:
    # xml_str = myfile.read()
# root = etree.XML(xml_str)
# dengue_prev_polygon_list = list()
# nodes = root.findall("node")

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

# for child in root:
    # if child.tag != 'way':
        # continue

    # child_tags = child.findall("tag")
    # for child_tag in child_tags:
        # if child_tag.get('v') != '登革熱防治圈劃區':
            # continue

        # ## get way id
        # way_id = child.get('id')
        # cur.execute("SELECT * FROM dengue_prevention_block WHERE no='%s'" % (way_id))
        # way_list = cur.fetchall()
        # if len(way_list) >= 1:
            # continue

        # print (child.get('id'))
        # ## get date
        # drug_date = None
        # for child_tag in child_tags:
            # if child_tag.get('k') == 'date':
                # drug_date = child_tag.get('v')
                # try:
                    # drug_date = datetime.datetime.strptime(drug_date, "%Y%m%d").date()
                # except:
                    # drug_date = None

        # ## get is_in:region
        # drug_region = str()
        # for child_tag in child_tags:
            # if child_tag.get('k') == 'is_in:region':
                # drug_region = child_tag.get('v')

        # ## get polygon point
        # dengue_prev_point_list = list()
        # child_nds = child.findall("nd")
        # for child_nd in child_nds:
            # for node in nodes:
                # if node.get('id') == child_nd.get('ref'):
                    # dengue_prev_point_list.append(node.get('lon') + " " +  node.get('lat'))
                    # break
        # dengue_prev_polygon_list.append({"way_id": way_id, "drug_date": drug_date, "region": drug_region, "point_list": dengue_prev_point_list})




# for dengue_prev_polygon in dengue_prev_polygon_list:
    # sql = "INSERT INTO dengue_prevention_block (no, region, date, block_poly, block_poly_50, block_poly_100, block_poly_150, is_origin_effect, is_50_effect, is_100_effect, is_150_effect, count_origin_14_day, count_50_14_day, count_100_14_day, count_150_14_day) VALUES (%s, %s, %s, ST_GEOMFROMTEXT(%s, 4326), ST_EXPAND(ST_GEOMFROMTEXT(%s, 4326), 50), ST_EXPAND(ST_GEOMFROMTEXT(%s, 4326), 100), ST_EXPAND(ST_GEOMFROMTEXT(%s, 4326), 150), %s, %s, %s, %s, %s, %s, %s, %s)"

    # polygon_str = 'POLYGON((' + ','.join(dengue_prev_polygon['point_list']) + '))'

    # params = [dengue_prev_polygon['way_id'], dengue_prev_polygon['region'], dengue_prev_polygon['drug_date'], polygon_str, polygon_str, polygon_str, polygon_str, "", "", "", "", "", "", "", ""]
    # cur.execute(sql, params)
# conn.commit()

## Update dengue block expand
# cur.execute("SELECT * FROM dengue_prevention_block")
# dengue_prev_blocks = cur.fetchall()
# for dengue_prev_block in dengue_prev_blocks:
    # print (dengue_prev_block[0])
    # cur.execute("SELECT ST_AsText('%s')" % dengue_prev_block[3])
    # block_poly_str = cur.fetchone()
    # expand_50_poly = expand_block(block_poly_str[0], 50)
    # expand_100_poly = expand_block(block_poly_str[0], 100)
    # expand_150_poly = expand_block(block_poly_str[0], 150)

    # sql = "Update dengue_prevention_block SET block_poly_50=ST_GeomFromText(%s, 4326), block_poly_100=ST_GeomFromText(%s, 4326), block_poly_150=ST_GeomFromText(%s, 4326) where no=%s"
    # params = [expand_50_poly, expand_100_poly, expand_150_poly, dengue_prev_block[0]]
    # cur.execute(sql, params)
# conn.commit()


## Update real_dengue in block
cur.execute("SELECT * FROM dengue_prevention_block")
dengue_prev_blocks = cur.fetchall()
for dengue_prev_block in dengue_prev_blocks:
    if dengue_prev_block[2] == None:
        continue

    print (dengue_prev_block[0])
    before_start_date = dengue_prev_block[2] + datetime.timedelta(days=-11)
    before_end_date = dengue_prev_block[2] + datetime.timedelta(days=3)
    after_start_date = dengue_prev_block[2] + datetime.timedelta(days=4)
    after_end_date = dengue_prev_block[2] + datetime.timedelta(days=17)
    today_date = datetime.datetime.now().date()

    if before_end_date > today_date:
        origin_before = "uncertain"
        block_50_before = "uncertain"
        block_100_before = "uncertain"
        block_150_before = "uncertain"
        tainan_before = "uncertain"
    else:
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (before_start_date, before_end_date, dengue_prev_block[3]))
        origin_before = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (before_start_date, before_end_date, dengue_prev_block[4]))
        block_50_before = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (before_start_date, before_end_date, dengue_prev_block[5]))
        block_100_before = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (before_start_date, before_end_date, dengue_prev_block[6]))
        block_150_before = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s'" % (before_start_date, before_end_date))
        tainan_before = str(cur.fetchone()[0])

    if after_end_date > today_date:
        origin_after = "uncertain"
        block_50_after = "uncertain"
        block_100_after = "uncertain"
        block_150_after = "uncertain"
        tainan_after = "uncertain"
    else:
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (after_start_date, after_end_date, dengue_prev_block[3]))
        origin_after = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (after_start_date, after_end_date, dengue_prev_block[4]))
        block_50_after = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (after_start_date, after_end_date, dengue_prev_block[5]))
        block_100_after = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (after_start_date, after_end_date, dengue_prev_block[6]))
        block_150_after = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s'" % (after_start_date, after_end_date))
        tainan_after = str(cur.fetchone()[0])

    is_origin_effect = origin_before + ", " + origin_after
    is_50_effect = block_50_before + ", " + block_50_after
    is_100_effect = block_100_before + ", " + block_100_after
    is_150_effect = block_150_before + ", " + block_150_after
    tainan_same_period = tainan_before + ", " + tainan_after

    print (is_origin_effect)
    print (is_50_effect)
    print (is_100_effect)

    origin_count_list = list()
    for single_date in daterange(before_start_date, after_end_date):
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, dengue_prev_block[3]))
        each_date_count = cur.fetchone()[0]
        origin_count_list.append(single_date.strftime("%Y-%m-%d"))
        origin_count_list.append(str(each_date_count))

    expand_50_count_list = list()
    for single_date in daterange(before_start_date, after_end_date):
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, dengue_prev_block[4]))
        each_date_count = cur.fetchone()[0]
        expand_50_count_list.append(single_date.strftime("%Y-%m-%d"))
        expand_50_count_list.append(str(each_date_count))

    expand_100_count_list = list()
    for single_date in daterange(before_start_date, after_end_date):
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, dengue_prev_block[5]))
        each_date_count = cur.fetchone()[0]
        expand_100_count_list.append(single_date.strftime("%Y-%m-%d"))
        expand_100_count_list.append(str(each_date_count))

    expand_150_count_list = list()
    for single_date in daterange(before_start_date, after_end_date):
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, dengue_prev_block[6]))
        each_date_count = cur.fetchone()[0]
        expand_150_count_list.append(single_date.strftime("%Y-%m-%d"))
        expand_150_count_list.append(str(each_date_count))

    cur.execute("UPDATE dengue_prevention_block SET is_origin_effect='%s', is_50_effect='%s', is_100_effect='%s', is_150_effect='%s', count_origin_14_day='%s', count_50_14_day='%s', count_100_14_day='%s', count_150_14_day='%s', tainan_same_period='%s' WHERE no='%s'" % (is_origin_effect, is_50_effect, is_100_effect, is_150_effect, ','.join(origin_count_list), ','.join(expand_50_count_list), ','.join(expand_100_count_list), ','.join(expand_150_count_list), tainan_same_period, dengue_prev_block[0]))

conn.commit()
cur.close()
conn.close()
