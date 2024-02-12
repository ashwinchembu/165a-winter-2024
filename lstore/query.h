#ifndef QUERY_H
#define QUERY_H

#include "table.h" // Assuming Table, Record, and other dependencies are defined in their respective header files.
#include "page.h"
#include "index.h"
#include <vector>
#include <string>

class Query {
public:
    explicit Query(Table* _table);
    bool deleteRecord(int primary_key);
    bool insert(const std::vector<int>& columns);
    std::vector<Record> select(int search_key, int search_key_index, const std::vector<int>& projected_columns_index);
    std::vector<Record> select_version(int search_key, int search_key_index, const std::vector<int>& projected_columns_index, int relative_version);
    bool update(int primary_key, const std::vector<int>& columns);
    int sum(int start_range, int end_range, int aggregate_column_index);
    int sum_version(int start_range, int end_range, int aggregate_column_index, int relative_version);
    bool increment(int key, int column);

    Table* table;
};

#endif // QUERY_H
