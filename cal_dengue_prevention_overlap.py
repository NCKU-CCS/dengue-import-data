import datetime
import psycopg2

# for range date
def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + datetime.timedelta(n)

# calculate prevention overlap
def block_overlap_data(cur, way_id, expand_meter, block_geom, block_date):
    before_start_date = block_date + datetime.timedelta(days=-11)
    before_end_date = block_date + datetime.timedelta(days=3)
    after_start_date = block_date + datetime.timedelta(days=4)
    after_end_date = block_date + datetime.timedelta(days=17)
    today_date = datetime.datetime.now().date()

    date_count_list = list()
    for single_date in daterange(before_start_date, after_end_date):
        if single_date > today_date:
            date_count_list.append(single_date.strftime("%Y-%m-%d") + ' ' + 'uncertain')
            continue
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date='%s' AND ST_Contains('%s', location)" % (single_date, block_geom))
        each_date_count = cur.fetchone()[0]
        date_count_list.append(single_date.strftime("%Y-%m-%d") + ' ' + str(each_date_count))

    cur.execute("SELECT COUNT(*) FROM dengue_prevention_block WHERE no!='%s' AND ST_Overlaps('%s', block_poly) AND date>='%s' AND date<='%s'" % (way_id, block_geom, before_start_date, after_end_date))
    overlap_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM dengue_prevention_block WHERE no!='%s' AND ST_Contains('%s', block_poly) AND date>='%s' AND date<='%s'" % (way_id, block_geom, before_start_date, after_end_date))
    overlap_count += cur.fetchone()[0]

    return way_id + ',' +\
           block_date.strftime("%Y-%m-%d") + ',' +\
           expand_meter + ',' +\
           str(overlap_count) + ',' +\
           ','.join(date_count_list) + '\n'



conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

output_str = str()
cur.execute("SELECT * FROM dengue_prevention_block")
dengue_prevention_blocks = cur.fetchall()
for dengue_prevention_block in dengue_prevention_blocks:
    if dengue_prevention_block[2] == None:
        continue

    print (dengue_prevention_block[0])
    output_str += block_overlap_data(cur, dengue_prevention_block[0], '0', dengue_prevention_block[3], dengue_prevention_block[2])
    output_str += block_overlap_data(cur, dengue_prevention_block[0], '50', dengue_prevention_block[4], dengue_prevention_block[2])
    output_str += block_overlap_data(cur, dengue_prevention_block[0], '100', dengue_prevention_block[5], dengue_prevention_block[2])
    output_str += block_overlap_data(cur, dengue_prevention_block[0], '150', dengue_prevention_block[6], dengue_prevention_block[2])

with open('dengue_prevention_overlap.csv', 'w') as myfile:
    myfile.write(output_str)

