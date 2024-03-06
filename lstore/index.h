#ifndef INDEXH
#define INDEXH
#include <unordered_map>
#include <vector>
#include <mutex>
#include "RID.h"
#include "table.h"

class Table;

class Index {
public:
    std::mutex mutex;
    Table* table = nullptr;
    std::unordered_map<int, std::unordered_multimap<int, int>> indices; //column, (value, rid id)

    Index (){};
    virtual ~Index ();
    std::vector<int> locate(const int& column_number, const int& value);
    std::vector<int> locate_range(const int& begin, const int& end, const int& column_number);
    void create_index(const int& column_number);
    void drop_index(const int& column_number);
    void setTable(Table* t);
    //each index is a map, each element in the map corresponds to a key and a vector of rids
    void insert_index(int& rid, std::vector<int> columns);
    void update_index(int& rid, std::vector<int> columns, std::vector<int> old_columns);

    void printData();

   int write(FILE* fp);
   int read(FILE* fp);
};

#endif
