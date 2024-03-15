from lstore.py.db import Database
from lstore.py.query import Query
from lstore.py.transaction import Transaction
from lstore.py.transaction_worker import TransactionWorker
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []
num_threads = 8
number_of_records = 10000
number_of_transactions = 100


insert_transactions = []
update_transactions = []
select_transactions = []
aggre_transactions = []

for i in range(number_of_transactions):
    insert_transactions.append(Transaction())

for i in range(0, number_of_records):
    key = 906659671 + i
    keys.append(key)
    records[key] = [key, 93, 5, 4, 2]
    t = insert_transactions[i % number_of_transactions]
    t.add_query(query.insert, grades_table, *records[key])

transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker())

for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(insert_transactions[i])

insert_time_0 = process_time()
# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()
insert_time_1 = process_time()



print("Inserting 10K records took:  \t\t\t", insert_time_1 - insert_time_0)
# Measuring update Performance

# for i in range(number_of_transactions):
#     update_transactions.append(Transaction())
#
#
# update_cols = [
#     [None, None, None, None, None],
#     [None, randrange(0, 100), None, None, None],
#     [None, None, randrange(0, 100), None, None],
#     [None, None, None, randrange(0, 100), None],
#     [None, None, None, None, randrange(0, 100)],
# ]
#
# for i in range(0, number_of_records):
#     t = update_transactions[i % number_of_transactions]
#     t.add_query(query.update, grades_table, choice(keys), *(choice(update_cols)))
#
# transaction_workers = []
# for i in range(num_threads):
#     transaction_workers.append(TransactionWorker())
#
# for i in range(number_of_transactions):
#     transaction_workers[i % num_threads].add_transaction(update_transactions[i])
#
# update_time_0 = process_time()
# # run transaction workers
# for i in range(num_threads):
#     transaction_workers[i].run()
#
# # wait for workers to finish
# for i in range(num_threads):
#     transaction_workers[i].join()
# update_time_1 = process_time()
# print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)



for i in range(0, number_of_records):
    t = select_transactions[i % number_of_transactions]
    t.add_query(query.select, grades_table, choice(keys),0 , [1, 1, 1, 1, 1])

transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker())

for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(select_transactions[i])

# Measuring Select Performance
select_time_0 = process_time()
# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)




for i in range(0, number_of_records, 100):
    start_value = 906659671 + i
    end_value = start_value + 100
    t = aggre_transactions[i % number_of_transactions]
    t.add_query(query.sum, grades_table, start_value, end_value - 1, randrange(0, 5))

transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker())

for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(aggre_transactions[i])
# Measuring Aggregate Performance
agg_time_0 = process_time()
# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()
agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)
