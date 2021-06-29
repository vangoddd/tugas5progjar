import sqlite3, os, sys, time

import hashlib
import random

start = time.time()
db = sqlite3.connect("data.db")
cur = db.cursor()

count = 0
total = 0
value = 0
for i in range(1000):
    n1 = random.randint(1, 99000)
    n2 = random.randint(1, 1000)
    sql = "select count(*) from MOCKDATA where ID>=%s AND ID<=%s;" % (n1, n1+n2)
    h = hash(sql)

    if h % 2 == 0:
        print(sql, " count() = ", 0)
        # pub sub
    else:
        cur.execute(sql)
        value = int(cur.fetchone()[0])
        print(sql, " count() = ", value) 
        
    count += 1
    total += value

print()
print()
print("num query = %d, total return value = %d" % (count, total))

db.close()
print("waktu: = ", time.time()-start)