#include <thread>
#include <iostream>
#include <chrono>
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
    query_thread = std::thread(&TransactionWorker::_run, this);
    // thread = std::thread(&TransactionWorker::_run, this);
}

// void TransactionWorker::_run_visualize() {
//
// }

void TransactionWorker::_run() {
    std::cout << "Running transaction with id " << std::this_thread::get_id() << std::endl;
    for (size_t i = 0; i < transactions.size(); i++) {
        // std::cout << "before transaction" << std::endl;
        bool result = transactions[i].run();
        // std::cout << "after transaction: " << std::this_thread::get_id() << std::endl;
        if(!result){
            // std::cout << "error 1" << std::endl;
          transactions.push_back(transactions[i]);
        }
        // std::cout << "error 2" << std::endl;
    }
    // std::cout << "error 3" << std::endl;
}

// call all the join function for the thread we have.
void TransactionWorker::join() {
	try
    {
        if (query_thread.joinable()) {
		std::cout << "Joining transaction " << query_thread.get_id() << std::endl;
        query_thread.join();
std::cout << "Joined" << std::endl;
    }
    }
    catch(const std::system_error& e)
    {
        std::cerr << "Caught system_error with code "
                     "[" << e.code() << "] meaning "
                     "[" << e.what() << "]\n";
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
