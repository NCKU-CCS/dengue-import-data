import datetime
import psycopg2

conn = psycopg2.connect("dbname='opendata' user='netdb' host='54.178.174.129' password='netdb2602'")
cur = conn.cursor()

cur.execute("SELECT * FROM dengue_prevention_block")
dengue_prevention_blocks = cur.fetchall()
output_str = "block_no,date,origin_before,origin_after,50_before,50_after,100_before,100_after,150_before,150_after,tainan_before,tainan_after\n"
for dengue_prevention_block in dengue_prevention_blocks:
    print (dengue_prevention_block[0])
    if dengue_prevention_block[2] == None:
        continue

    before_start_date = dengue_prevention_block[2] + datetime.timedelta(days=-11)
    before_end_date = dengue_prevention_block[2] + datetime.timedelta(days=3)
    after_start_date = dengue_prevention_block[2] + datetime.timedelta(days=4)
    after_end_date = dengue_prevention_block[2] + datetime.timedelta(days=17)
    today_date = datetime.datetime.now().date()

    origin_before = str()
    origin_after = str()
    block_50_before = str()
    block_50_after = str()
    block_100_before = str()
    block_100_after = str()
    block_150_before = str()
    block_150_after = str()

    if before_end_date > today_date:
        origin_before = "uncertain"
        block_50_before = "uncertain"
        block_100_before = "uncertain"
        block_150_before = "uncertain"
        tainan_before = "uncertain"
    else:
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (before_start_date, before_end_date, dengue_prevention_block[3]))
        origin_before = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (before_start_date, before_end_date, dengue_prevention_block[4]))
        block_50_before = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (before_start_date, before_end_date, dengue_prevention_block[5]))
        block_100_before = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (before_start_date, before_end_date, dengue_prevention_block[6]))
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
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (after_start_date, after_end_date, dengue_prevention_block[3]))
        origin_after = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (after_start_date, after_end_date, dengue_prevention_block[4]))
        block_50_after = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (after_start_date, after_end_date, dengue_prevention_block[5]))
        block_100_after = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s' AND ST_Contains('%s', location)" % (after_start_date, after_end_date, dengue_prevention_block[6]))
        block_150_after = str(cur.fetchone()[0])
        cur.execute("SELECT COUNT(*) FROM real_dengue WHERE date>='%s' AND date<='%s'" % (after_start_date, after_end_date))
        tainan_after = str(cur.fetchone()[0])

    output_str = output_str + dengue_prevention_block[0] + ',' + dengue_prevention_block[2].strftime("%Y-%m-%d") + ',' +\
                 origin_before + ',' + origin_after + ',' +\
                 block_50_before + ',' + block_50_after + ',' +\
                 block_100_before + ',' + block_100_after + ',' +\
                 block_150_before + ',' + block_150_after + ',' +\
                 tainan_before + ',' + tainan_after + '\n'

with open('compare_prevention_block.csv', 'w') as myfile:
    myfile.write(output_str)
