#include <thread>
#include <iostream>
#include <chrono>
#include "config.h"
#include "../DllConfig.h"
#include "transaction_worker.h"


TransactionWorker::TransactionWorker () {
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
    query_thread = std::thread(&TransactionWorker::_run, this);
    // std::cout << "Running transaction with id " << query_thread.get_id() << std::endl;
    // thread = std::thread(&TransactionWorker::_run, this);
}

// void TransactionWorker::_run_visualize() {
//
// }


std::mutex threadGate;

void TransactionWorker::_run() {
    for (size_t i = 0; i < transactions.size(); i++) {
        std::unique_lock<std::mutex> gate(threadGate);
        bool result = transactions[i].run();
        if(!result){
          transactions.push_back(transactions[i]);
        }
        gate.unlock();
	}
}

// call all the join function for the thread we have.
void TransactionWorker::join() {
    if (query_thread.joinable()) {
		// std::cout << "Joining transaction " << query_thread.get_id() << std::endl;
        query_thread.join();
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
