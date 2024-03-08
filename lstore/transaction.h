#include "table.h"
#include "index.h"
#include "query.h"

enum OpCode { NOTHING, INSERT, UPDATE, SELECT, SELECT_VER, SUM, SUM_VER };

class QueryOperation {
public:
    /* Data for things necessary to run query, i.e. table, key, columns... */
    Query* q = nullptr;
    const OpCode type = OpCode::NOTHING;
    Table* table = nullptr;

    int* key = nullptr; // Update, Select, Select version
    std::vector<int> columns; // Columns and projected_columns_index combined. For insert, update, select, select ver.
    int search_key_index = -1; // Select and Select ver
    int relative_version = 1; // Select ver and Sum ver
    int* start_range = nullptr; // Sum and Sum ver
    int* end_range = nullptr; // Sum and Sum ver
    int aggregate_column_index = -1; // Sum and Sum ver

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
    int hash_key = 0;
    int xact_id;
    Transaction ();
    virtual ~Transaction ();
    // I believe wrapper can simplify these function pointers
    // void add_query(bool (*insert_func)(std::vector<int>), Table& t, const std::vector<int>& columns);
    // void add_query(bool (*update_func)(int, std::vector<int>), Table& t, const int& key, const std::vector<int>& columns);
    // void add_query(std::vector<Record> (*select_func)(int, int, std::vector<int>), Table& t, const int& key, const int& search_key_index, const std::vector<int>& projected_columns_index);
    // void add_query(std::vector<Record> (*select_version_func)(int, int, std::vector<int>, int), Table& t, const int& key, const int& search_key_index, const std::vector<int>& projected_columns_index,  const int& relative_version);
    // void add_query(unsigned long int (*sum_func)(int, int, int), Table& t, const int& start_range, const int& end_range, const int& aggregate_column_index);
    // void add_query(unsigned long int (*sum_version_func)(int, int, int, int), Table& t, const int& start_range, const int& end_range, const int& aggregate_column_index, const int& relative_version);


    void add_query(Query& q, Table& t, const std::vector<int>& columns);
    void add_query(Query& q, Table& t, int& key, const std::vector<int>& columns);
    void add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index);
    void add_query(Query& q, Table& t, int& key, const int& search_key_index, const std::vector<int>& projected_columns_index,  const int& relative_version);
    void add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index);
    void add_query(Query& q, Table& t, int& start_range, int& end_range, const int& aggregate_column_index, const int& relative_version);

    void run(); // Called by transaction worker. Return 0 if need to re-attempt
    void abort(); // Transaction worker check the work. If it contain anomaly, abort.
    void commit(); // Transaction worker check the work. If it run successfully, commit.
};
