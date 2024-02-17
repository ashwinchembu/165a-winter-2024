#ifndef QUERY_H
#define QUERY_H

#include "table.h" // Assuming Table, Record, and other dependencies are defined in their respective header files.
#include "page.h"
#include "index.h"
#include <vector>
#include <string>

class Query {
public:
    Table* table;

    explicit Query(Table* _table) : table(_table) {};
    bool deleteRecord(const int& primary_key);
    bool insert(const std::vector<int>& columns);
    std::vector<Record> select(const int& search_key, const int& search_key_index, const std::vector<int>& projected_columns_index);
    std::vector<Record> select_version(const int& search_key, const int& search_key_index, const std::vector<int>& projected_columns_index, const int& _relative_version);
    bool update(const int& primary_key, const std::vector<int>& columns);
    unsigned long int sum(const int& start_range, const int& end_range, const int& aggregate_column_index);
    unsigned long int sum_version(const int& start_range, const int& end_range, const int& aggregate_column_index, const int& _relative_version);
    bool increment(const int& key, const int& column);
};

#endif // QUERY_H
