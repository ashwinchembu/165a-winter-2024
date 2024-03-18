from lstore.py.db import Database
from lstore.py.query import Query
from lstore.py.transaction import Transaction
from lstore.py.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed     


def correctness_tester1():
    db = Database()
    db.open('./CT1')

    record_num = 20
    thread_cnt = 3

    # creating grades table
    grades_table = db.create_table('CT1', 5, 0)
    query = Query(grades_table)

    for i in range(record_num):
        query.insert(i, 0, 0, 0, 0)

    transaction_workers = []
    for j in range(thread_cnt):
        transaction_workers.append(TransactionWorker())


    # create 60 transactions, each has 1 queries
    # each transaction_worker runs 20 transactions
    for j in range(thread_cnt):
        update_transactions = []
        for i in range(record_num):
            update_transactions.append(Transaction())
            update_transactions[i].add_query(query.update, grades_table, i, *[None, 2+j, 2+j, 2+j, 2+j])
            transaction_workers[j].add_transaction(update_transactions[i])
    
    for j in range(thread_cnt):
        transaction_workers[j].run()

    for j in range(thread_cnt):
        transaction_workers[j].join()

    for i in range(record_num):
        result = query.select(i, 0, [1, 1, 1, 1, 1])[0].columns
        if not (result[1] == result[2] and result[1] == result[3] and result[1] == result[4]):
            print("Wrong Result:", result)
            break

    db.close()


def correctness_tester2():
    db = Database()
    db.open('./CT2')

    record_num = 20
    thread_cnt = 3

    # creating grades table
    grades_table = db.create_table('CT2', 5, 0)
    query = Query(grades_table)

    for i in range(record_num):
        query.insert(i, 0, 0, 0, 0)

    transaction_workers = []
    for j in range(thread_cnt):
        transaction_workers.append(TransactionWorker())

    # create 3 transactions, each has 20 queries
    # each transaction_worker runs 1 transaction
    for j in range(thread_cnt):
        update_transaction = Transaction()
        for i in range(record_num):
            update_transaction.add_query(query.update, grades_table, i, *[None, 2+j, 2+j, 2+j, 2+j])
        transaction_workers[j].add_transaction(update_transaction)
    
    for j in range(thread_cnt):
        transaction_workers[j].run()

    for j in range(thread_cnt):
        transaction_workers[j].join()

    for i in range(record_num):
        result1 = query.select(i, 0, [1, 1, 1, 1, 1])[0].columns
        result2 = query.select((i+1)%record_num, 0, [1, 1, 1, 1, 1])[0].columns
        if not (result1[1] == result1[2] and result1[1] == result1[3] and result1[1] == result1[4]):
            print("Wrong Result:", result1)
            break
        if result1[1] != result2[1] :
            print("Wrong Results:", result1, result2)
            break

    db.close()

correctness_tester1()
correctness_tester2()