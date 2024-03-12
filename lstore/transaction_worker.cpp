#include <thread>
#include <iostream>
#include "config.h"
#include "../DllConfig.h"
#include "transaction_worker.h"


TransactionWorker::TransactionWorker () {
    transactions.resize(MAX_THREADS);
}

TransactionWorker::~TransactionWorker () {

}

// Append transaction t to the appropriate place.
// Operation that happens on same place in same page goes to same place. Best effort model.
void TransactionWorker::add_transaction(const Transaction& t) {
    transactions.push_back(t);
}

// Start all the transactions. Create thread and run.
void TransactionWorker::run() {
    thread = std::thread(&TransactionWorker::_run, this);
    // thread = std::thread(&TransactionWorker::_run, this, );
}

// void TransactionWorker::_run_visualize(int id) {
//
// }

void TransactionWorker::_run() {
    for (size_t i = 0; i < transactions.size(); i++) {
        bool result = transactions[i].run();
        if(!result){
          transactions.push_back(transactions[i]);
        }
        std::this_thread::sleep_for(2000ms);
    }
}

// call all the join function for the thread we have.
void TransactionWorker::join() {
    if (thread.joinable()) {
        thread.join();
    }
}



COMPILER_SYMBOL void TransactionWorker_add_transaction(int* obj,int* transaction){
	((TransactionWorker*)obj)->add_transaction(*(Transaction*)transaction);
}

COMPILER_SYMBOL int* TransactionWorker_constructor(){
	return (int*)(new TransactionWorker());
}

COMPILER_SYMBOL void TransactionWorker_destructor(int* obj){
	delete ((TransactionWorker*)obj);
}

COMPILER_SYMBOL void TransactionWorker_run(int* obj){
	((TransactionWorker*)obj)->run();
}

COMPILER_SYMBOL void TransactionWorker_join(int* obj){
	((TransactionWorker*)obj)->join();
}
