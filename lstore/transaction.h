#ifndef TRANSACTION_H
#define TRANSACTION_H
#include <thread>
#include "table.h"
#include "index.h"
#include "query.h"
#include "config.h"

enum OpCode { NOTHING, INSERT, UPDATE, SELECT, SELECT_VER, SUM, SUM_VER, INCREMENT };

class QueryOperation {
public:
    /* Data for things necessary to run query, i.e. table, key, columns... */
    Query* q = nullptr;
    OpCode type = OpCode::NOTHING;
    Table* table = nullptr;

    int key = NONE; // Update, Select, Select version, Increment
    std::vector<int> columns; // Columns and projected_columns_index combined. For insert, update, select, select ver.
    int search_key_index = -1; // Select and Select ver
    int relative_version = 1; // Select ver and Sum ver
    int start_range = NONE; // Sum and Sum ver
    int end_range = NONE; // Sum and Sum ver
    int aggregate_column_index = -1; // Sum and Sum ver and Increment

    /* Return values */
    unsigned long int* sum_result = nullptr;
    std::vector<Record> select_result;

    QueryOperation (Query* query, const OpCode& op, Table* t) : q(query), type(op), table(t) {}
    virtual ~QueryOperation ();
    bool run(); // Run the operation
    bool check_req();
};

class Transaction {
public:
    std::vector<QueryOperation> queries; // To hold onto queries
    int num_queries = 0;
    int xact_id = -1;
    Transaction ();
    Transaction (const Transaction& rhs);
    virtual ~Transaction ();

    void add_query(Query& q, Table& t, const std::vector<int>& columns); // Insert
    void add_query(Query& q, Table& t, int& key, const std::vector<int>& columns); // Update
    void add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index); // Select
    void add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index,  const int& relative_version); // Select Ver
    void add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index); // Sum
    void add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index, const int& relative_version); // Sum ver
    void add_query(Query& q, Table& t, int& key, const int& column); // Increment

    bool run(); // Called by transaction worker. Return 0 if need to re-attempt
    void abort(); // Transaction worker check the work. If it contain anomaly, abort.
    void commit(); // Transaction worker check the work. If it run successfully, commit.
};
#endif
