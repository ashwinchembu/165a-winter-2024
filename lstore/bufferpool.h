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
    Frame* next = nullptr;
    Frame* prev = nullptr;
};

class BufferPool {
public:
    BufferPool (const int& num_pages);
    virtual ~BufferPool ();
    Frame* head;
    Frame* tail;
    int get (const RID& rid, const int& column); // given a rid and column, returns the value in that location
    void set (const RID& rid, const int& column, int value); // given a rid and column, changes the value in that location
    Frame* load (const RID& rid, const int& column); //from disk to bufferpool
    Frame* search(const RID& rid, const int& column); //search in specific hash range
    void insert_new_page(const RID& rid, const int& column, int value);
    void update_ages(Frame* just_accessed, Frame* range_begin); // update all the ages in hash range based on which frame was just accessed
    Frame* evict (const RID& rid); //evict the oldest that is not pinned
    void write_back(Frame* frame); //write back to disk if dirty
    void write_back_all ();
    void pin (const RID& rid, const int& column);
    void unpin (const RID& rid, const int& column);
    std::vector<Frame*> hash_vector;
    //vector<int> frame_directory; //keep track of which frames are full, 1s and 0s
    int bufferpool_size;

};


extern BufferPool buffer_pool;

#endif
