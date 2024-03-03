#include <thread>
#include "table.h"
#include "index.h"
#include "transaction.h"

class TransactionWorker {
public:
    std::vector<std::vector<Transaction>> transactions; // List of transactions to run
    std::vector<std::thread> threads;
    int result = 0;
    TransactionWorker ();
    virtual ~TransactionWorker ();
    void add_transaction(const Transaction& t); // Append transaction t to the appropriate place.
    void run(); // Start all the transactions. Create thread and run.
    void join(); // call all the join function for the thread we have.
};
