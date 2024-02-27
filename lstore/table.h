#ifndef TABLEH
#define TABLEH
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <string>
#include "RID.h"
#include <memory>
#include <queue>
#include "config.h"
// Avoid recursive include
// #include "index.h"
// #include "page.h"
// #include "bufferpool.h"


// param name: string         #Table name
// param num_columns: int     #Number of Columns: all columns are integer
// param key: int             #Index of table key in columns
class Record {
public:
    Record(int rid_in, int key_in, std::vector<int> columns_in) : rid(rid_in), key(key_in), columns(columns_in) {};
    ~Record(){}
    int rid;
    int key;
    std::vector<int> columns;
};

class Frame;
class Index;
class PageRange;

class Table {
public:
    std::string name;
    int key; //primary key
    std::map<int, RID> page_directory; //<RID.id, RID>
    std::queue<std::vector<Frame*>> merge_queue;
    //std::queue<std::vector<std::shared_ptr<Page>>> merge_queue;
    std::vector<std::shared_ptr<PageRange>> page_range;
    std::map<int, int> page_range_update;
    Index* index = nullptr;
    // int last_page_range = -1;
    int num_update = 0;
    int num_insert = 0;

    int num_columns; //number of columns of actual data, excluding the metadata

    Table () {};
    ~Table ();
    Table(const std::string& name, const int& num_columns, const int& key);

    friend class Index;
    friend class Query;

    RID insert(const std::vector<int>& columns);
    RID update(RID& rid, const std::vector<int>& columns);
    int merge();
    int write(FILE* fp);
    int read(FILE* fp);

};

#endif
