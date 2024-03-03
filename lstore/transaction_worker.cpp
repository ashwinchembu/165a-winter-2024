#include <thread>
#include <iostream>
#include "config.h"
#include "transaction_worker.h"


TransactionWorker::TransactionWorker () {
    transactions.resize(MAX_THREADS);
}

TransactionWorker::~TransactionWorker () {

}

// Append transaction t to the appropriate place.
void TransactionWorker::add_transaction(const Transaction& t) {

}

// Start all the transactions. Create thread and run.
void TransactionWorker::run() {
    for (int i = 0; i < transactions.size(); i++) {
        int num_transaction = transactions[i].size();
        if (num_transaction > 0) {
            for (int j = 0; j < num_transaction; j++) {
                threads.push_back(std::thread(&Transaction::run, &(transactions[i][j])));
            }
        }
    }
}

// call all the join function for the thread we have.
void TransactionWorker::join() {
    for (size_t i; i < threads.size(); i++) {
        if (threads[i].joinable()) {
            threads[i].join();
        }
    }
    threads.clear();
}
