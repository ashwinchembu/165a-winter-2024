from lstore.py.db import Database
from lstore.py.query import Query
from DBWrapper import *

from random import choice, randint, sample, seed

dbs = Database()
dbs.open("./tables")
table1 = dbs.create_table("myTable",5,0)
query = Query(table1)

records = 1000
keys=[]

updates=64000

for i in range(records):
    next = [i, randint(0,records),
            randint(0,records),randint(0,records),
            randint(0,records)]
    
    query.insert(*next)

for i in range(updates):
    next = [i % records, randint(0,records),
            randint(0,records),randint(0,records),
            randint(0,records)]

    query.update(i % records,*next)

force_write_back_all()
# Table_print_lineage(table1.selfPtr)
# Table_print_table(table1.selfPtr)

dbs.close()




