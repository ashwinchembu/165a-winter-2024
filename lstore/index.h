#ifndef INDEXH
#define INDEXH
#include <unordered_map>
#include <vector>
#include "RID.h"

class Index {
private:
    /* data */
    Table* table;
    std::unordered_map<int, std::unordered_multimap<int, RID>> indices; //column, (value, RID)
public:
    Index () {};
    virtual ~Index ();
    std::vector<RID> locate(int column_number, int value);
    std::vector<RID> locate_range(int begin, int end, int column_number);
    void create_index(int column_number);
    void drop_index(int column_number);
    void setTable(Table* t){this->table = t;}
    //each index is a map, each element in the map corresponds to a key and a vector of rids
};

#endif
