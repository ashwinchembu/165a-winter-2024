#ifndef INDEXH
#define INDEXH
#include "table.h"
#include "RID.h"

class Index {
public:
    Index () {};
    virtual ~Index ();
    // Return pointer to array of RIDs
    std::vector<RID> locate(std::string column, int value);
    std::vector<RID> locate_range(int begin, int end, std::string column);
    void create_index(int column_number);
    void drop_index(int column_number);
    void setTable(Table* t){this->table = t;}
private:
    /* data */
    Table* table;
    std::map<int, std::map<int, std::vector<RID>>> indices;
    //each index is a map, each element in the map corresponds to a key and a vector of rids
};

#endif
