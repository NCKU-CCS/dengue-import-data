import psycopg2

north_border = (23.441644, 120.408899)
east_border = (23.220398, 120.656291)
south_border = (22.888121, 120.352774)
west_border = (23.102259, 120.035226)

ori_point = (22.89067355, 120.03777855)
space_500 = 0.0045045045
space_250 = 0.0025525526

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

no = 1
lat_list = list()
lon_list = list()
cur_lat = ori_point[0]
cur_lon = ori_point[1]
lat_max = north_border[0]
lon_max = east_border[1]
while True:
    if cur_lat > lat_max:
        break
    lat_list.append(cur_lat)
    cur_lat += space_500

while True:
    if cur_lon > lon_max:
        break
    lon_list.append(cur_lon)
    cur_lon += space_500

for cur_lat in lat_list:
    for cur_lon in lon_list:
        print (no)
        sql = "INSERT INTO tainan_blocks_500 (no, center, block_poly, left_top_lat, left_top_lon, right_top_lat, right_top_lon, right_down_lat, right_down_lon, left_down_lat, left_down_lon, dengue) VALUES (%s, ST_GEOMFROMTEXT(%s, 4326), ST_GEOMFROMTEXT(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        left_top_lat = cur_lat+space_250
        left_top_lon = cur_lon-space_250
        right_top_lat = cur_lat+space_250
        right_top_lon = cur_lon+space_250
        right_down_lat = cur_lat-space_250
        right_down_lon = cur_lon+space_250
        left_down_lat = cur_lat-space_250
        left_down_lon = cur_lon-space_250
        poly_point_list = [str(left_top_lat) + " " + str(left_top_lon),\
                           str(right_top_lat) + " " + str(right_top_lon),\
                           str(right_down_lat) + " " + str(right_down_lon),\
                           str(left_down_lat) + " " + str(left_down_lon),\
                           str(left_top_lat) + " " + str(left_top_lon)]
        params = [str(no), 'POINT('+str(cur_lat)+ " " +str(cur_lon)+')', 'POLYGON(('+','.join(poly_point_list)+'))', str(left_top_lat), str(left_top_lon), str(right_top_lat), str(right_top_lon), str(right_down_lat), str(right_down_lon), str(left_down_lat), str(left_down_lon), ""]
        cur.execute(sql,params)

        no += 1


conn.commit()
cur.close()
conn.close()

