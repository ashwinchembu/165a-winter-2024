from lstore.py.db import Database
from lstore.py.query import Query
from lstore.py.transaction import Transaction
from lstore.py.transaction_worker import TransactionWorker
from time import sleep
from random import choice, randint, sample, seed

db = Database()
db.open('./ECS165')

grades_table = db.create_table('Grades', 5, 0)

query = Query(grades_table)

records = {}

seed(3562901)

key_original = 92106429
key = key_original
records[key] = [key, 0, 0, 0, 0]
query.insert(*records[key])
key += 1
records[key] = [key, 1, 2, 3, 4]
query.insert(*records[key])
key += 1
records[key] = [key, 1, 2, 3, 4]
query.insert(*records[key])

keys = sorted(list(records.keys()))
print("Insert finished")


# test 1: select on non-primary columns with index
# test 2: select on non-primary columns without index
# test 3: select that returns multiple records
i = 0
for key in records:
    record = query.select(i, i, [1, 1, 1, 1, 1])
    print('select on', i, ' on ', i, 'th column :', [x.columns for x in record])
    i += 1
print('test 1,2,3 finished')

# test 4: select that returns no records
for key in records:
    record = query.select(key, 5, [1, 1, 1, 1, 1])
    print('select on', key, ': ', [x.columns for x in record])
print('test 4 finished')

# test 5: update on no record (primary key does not exist in the table)
print('successfully updated : ', query.update(90, *[1, 1, 1, 1, 1]))
print('test 5 finished')

# test 6: HUGE SELECT
for _ in range(100):
    key += 1
    query.insert(*[key, 70, 70, 70, 70])
record = query.select(70, 2, [1, 1, 1, 1, 1])
print('number of selected records : ', len(record), '/ 100')
print('test 6 finished')

# test 7: sum and sum version with multi threading
sum_transactions = []
sum_transaction_workers = []

for i in range(1,9):
    sum_transactions.append(Transaction())

for i in range(1,9):
    sum_transaction_workers.append(TransactionWorker())

for i in range(1,5):
    sum_transactions[i-1].add_query(query.sum, grades_table, 70, 70, i)

for i in range(5,9):
    sum_transactions[i-1].add_query(query.sum_version, grades_table, 70, 70, i - 4, 0)

for i in range(1,9):
    sum_transaction_workers[i-1].add_transaction(sum_transactions[i-1])

for i in range(1,9):
    sum_transaction_workers[i-1].run()

for i in range(1,9):
    sum_transaction_workers[i-1].join()

print('test 7 finished')

#test 8: changing primary key while trying to read it on another thread
pk_transactions = []
pk_transaction_workers = []

for i in range(1,2):
    pk_transactions.append(Transaction())

for i in range(1,2):
    pk_transaction_workers.append(TransactionWorker())

pk_transactions[0].add_query(query.update, grades_table, 92106429, [28, None, None, None, None])
pk_transactions[1].add_query(query.update, grades_table, 92106429, [28, None, None, None, None])

for i in range(1,2):
    pk_transaction_workers[i-1].add_transaction(pk_transactions[i-1])

for i in range(1,2):
    pk_transaction_workers[i-1].run()

for i in range(1,2):
    pk_transaction_workers[i-1].join()

print('test 8 finished')

#test 9: insert the same primary key with two different threads

same_pk_transactions = []
same_pk_transaction_workers = []

for i in range(1,2):
    same_pk_transactions.append(Transaction())

for i in range(1,2):
    same_pk_transaction_workers.append(TransactionWorker())

same_pk_transactions[0].add_query(query.insert, grades_table, [264, None, None, None, None])
same_pk_transactions[1].add_query(query.insert, grades_table, [264, None, None, None, None])

for i in range(1,2):
    same_pk_transaction_workers[i-1].add_transaction(same_pk_transactions[i-1])

for i in range(1,2):
    same_pk_transaction_workers[i-1].run()

for i in range(1,2):
    same_pk_transaction_workers[i-1].join()

print('test 9 finished')

# transaction = Transaction()
#
# transaction.add_query(query.insert, grades_table, *records[key])
# transaction.add_query(query.update, grades_table, key, *updated_columns)
# transaction.add_query(query.select, grades_table, key, column, [1, 1, 1, 1, 1])
# transaction.add_query(query.select_version, grades_table, key, column, [1, 1, 1, 1, 1], version)
# transaction.add_query(query.sum, grades_table, start, end, column)
# transaction.add_query(query.sum_version, grades_table, start, end, column, version)
#
# transaction_worker = TransactionWorker()
# transaction_worker.add_transaction(transaction)
#
# transaction_worker.run()
# transaction_worker.join()

db.close()
