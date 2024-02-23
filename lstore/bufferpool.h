#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <string>
#include "config.h"
#include "page.h"
#include <vector>
#include <fstream>

std::ofstream dbMetadataOut("../Disk/DiskMetadata.dat",
		std::ofstream::in | std::ofstream::binary);

std::ifstream dbMetadataIn("../Disk/DiskMetadata.dat",
		std::ifstream::in | std::ifstream::binary);

int ENTRIES_PER_PAGE = 256;

int TABLE_NAME_BYTES = 256;
int TABLE_NAME_OFFSET = 0;
int START_BASE_ID_OFFSET = TABLE_NAME_BYTES;
int START_TAIL_ID_OFFSET = START_BASE_ID_OFFSET + sizeof(double);
int NUMBER_OF_COLUMNS_OFFSET = START_TAIL_ID_OFFSET + sizeof(int);
int NUMBER_OF_ENTRIES_OFFSET = NUMBER_OF_COLUMNS_OFFSET + sizeof(int);

/*startBaseId and startTailId can be set to Nan if
 * not in table*/

/*name startBaseId startTailId numberOfColumns numberOfEntries*/
int METADATA_OFFSET =
		TABLE_NAME_BYTES + sizeof(double) * 2 + sizeof(int) * 2;

class Frame {
public:
    Frame ();
    virtual ~Frame ();
    Page* page = nullptr;
    int first_rid_page = 0; //first rid in the page
    // Can we hold first rid of the page range and table for eviction? <= Should not be hard because it is passed on load / make new
    std::string table_name = "";
    int first_rid_page_range = 0;
    int column = -1;
    bool valid = false;
    int pin = 0;
    bool dirty = false;
    int age = -1;
    Frame* next;
    Frame* prev;
};

class BufferPool {
public:
    BufferPool (const int& num_pages);
    virtual ~BufferPool ();
    Frame* head;
    Frame* tail;
    int get (const RID& rid, const int& column); // given a rid and column, returns the value in that location
    void set (const RID& rid, const int& column, int value); // given a rid and column, changes the value in that location
    void load (const RID& rid, const int& column);
    void insert_new_page(const RID& rid, const int& column, int value);
    void update_ages(const int& index_of_just_accessed);
    void evict ();
    void evict_all ();
    void pin (const int& rid, const int& page_num);
    void unpin (const int& rid, const int& page_num);
    vector<Frame*> hash_vector;
    int bufferpool_size;

};


extern BufferPool buffer_pool;

#endif
