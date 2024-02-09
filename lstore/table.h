#ifndef TABLEH
#define TABLEH
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <string>
#include "index.h"
#include "page.h"
#include "RID.h"

const int INDIRECTION_COLUMN = 0;
const int TIMESTAMP_COLUMN = 1;
const int SCHEMA_ENCODING_COLUMN = 2;

// param name: string         #Table name
// param num_columns: int     #Number of Columns: all columns are integer
// param key: int             #Index of table key in columns
class Record {
    public:
    Record(int rid_in, int key_in, std::vector<int> columns_in) : rid(rid_in), key(key_in), columns(columns_in) {};
    int rid;
    int key;
    std::vector<int> columns;
};

class Table {
    public:
    Table(std::string name_in, int key_in, int num_columns_in): name(name_in), key(key_in), num_columns(num_columns_in){};
    friend class Index;
    friend class Query;

    RID insert(const std::vector<int>& columns);
    RID update(RID rid, const std::vector<int>& columns);
    int merge();

    private:
    std::string name;
    int key; //primary key
    int num_columns; //number of columns of actual data, excluding the metadata
    std::map<int, RID> page_directory; //<RID.id, RID>
    Index* index;
    int last_page_range = -1;
    std::vector<PageRange> page_range;
    int num_update = 0;
    int num_insert = 0;
};

#endif
