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
#include <mutex>
#include "config.h"
#include "../Toolkit.h"
// Avoid recursive include
// #include "index.h"
// #include "page.h"
// #include "bufferpool.h"

class Frame;

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

extern const int DELETE_NONE;
extern const int DELETE_CASCADE;
extern const int DELETE_NULL;

class RIDJoin;

class Table {
public:
	std::shared_ptr<std::mutex> mutex_insert = std::make_shared<std::mutex>();
	std::shared_ptr<std::mutex> mutex_update = std::make_shared<std::mutex>();

    std::string name;
    int key; //primary key
    std::map<int, RID> page_directory; //<RID.id, RID>
    std::queue<std::vector<Frame*>> merge_queue;
    std::vector<std::shared_ptr<PageRange>> page_range;
    std::map<int, int> page_range_update;
    Index* index = nullptr;
    int num_update = 0;
    int num_insert = 0;

    int TPS = 0;

    std::map<int,std::vector<RIDJoin>>referencesOut;

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
    void PrintData();

    //////////////////////////
    //unused code for testing
//
//    RID update2(RID& rid, const std::vector<int>& columns);
//    int merge2();

    bool ridIsJoined(RID rid,int col);
    RIDJoin getJoin(RID rid, int col);
};

class RIDJoin{
	public:
		RID ridSrc;
		RID ridTarget;

		int srcCol;
		int targetCol;

		int modificationPolicy;
		Table* targetTable;
};

#endif
