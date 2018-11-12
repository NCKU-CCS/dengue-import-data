import datetime
import psycopg2

with open('Dengue_Daily.csv', 'r') as myfile:
    data = myfile.read()

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

for line_index, line in enumerate(data.split('\n')):
    if line_index == 0 or line == "":
        continue
    print (line_index)
    line_split = line.split(',')
    if line_split[0] == "":
        onset_date = None
    else:
        onset_date = datetime.datetime.strptime(line_split[0], "%Y/%m/%d").date()

    if line_split[1] == "":
        judged_date = None
    else:
        judged_date = datetime.datetime.strptime(line_split[1], "%Y/%m/%d").date()

    if line_split[2] == "":
        bulletin_date = None
    else:
        bulletin_date = datetime.datetime.strptime(line_split[2], "%Y/%m/%d").date()

    if line_split[9] == "":
        lon = None
    else:
        lon = line_split[9]

    if line_split[10] == "":
        lat = None
    else:
        lat = line_split[10]

    if line_split[9] == "" or line_split[10] == "":
        point_str = None
    else:
        point_str = 'POINT(' + line_split[9] + ' ' + line_split[10] +')'

    sql = "INSERT INTO year_daily_dengue (onset_date, judged_date, bulletin_date, gender, age_range, reside_county, reside_town, reside_vil, min_statistical_area, min_statistical_x, min_statistical_y, min_statistical_location, first_statistical_area, second_statistical_area, infect_county, infect_town, infect_vil, is_outside, infect_country, certain_case) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, ST_GeomFromText(%s, 4326), %s, %s, %s, %s, %s, %s, %s, %s)"
    params = [onset_date, judged_date, bulletin_date, line_split[3], line_split[4], line_split[5], line_split[6], line_split[7], line_split[8], lon, lat, point_str, line_split[11], line_split[12], line_split[13], line_split[14], line_split[15], line_split[16], line_split[17], line_split[18]]
    cur.execute(sql, params)

conn.commit()
cur.close()
conn.close()
