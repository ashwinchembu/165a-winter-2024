#ifndef INDEXH
#define INDEXH
#include <unordered_map>
#include <vector>
#include "RID.h"
#include "table.h"

class Table;

class Index {
public:
    /* data */
    Table* table = nullptr;
    std::unordered_map<int, std::unordered_multimap<int, RID> > indices; //column, (value, RID)

    Index (){};
    virtual ~Index () {};
    std::vector<RID> locate(const int& column_number, const int& value);
    std::vector<RID> locate_range(const int& begin, const int& end, const int& column_number);
    void create_index(const int& column_number);
    void drop_index(const int& column_number);
    void setTable(Table* t);
    //each index is a map, each element in the map corresponds to a key and a vector of rids
    void insert_index(RID& rid, std::vector<int> columns);
    void update_index(RID& rid, std::vector<int> columns, std::vector<int> old_columns);

    void printData();
};

#endif
