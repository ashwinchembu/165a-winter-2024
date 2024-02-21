#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <string>
#include "page.h"
#include <vector>

class BufferPool {
public:
    BufferPool ();
    virtual ~BufferPool ();
    std::vector<Frame> buffer(BUFFER_POOL_SIZE);
    int get (const RID& rid, const int& column); // given a rid and column, returns the value in that location
    void set (const RID& rid, const int& column, int value); // given a rid and column, changes the value in that location
    int load (const RID& rid, const int& column);
    void insert_new_page();
    void evict ();
    void evict_all ();
    void pin (const int& rid, const int& page_num);
    void unpin (const int& rid, const int& page_num);
};

class Frame {
public:
    Frame ();
    virtual ~Frame ();
    Page* page = nullptr;
    int first_rid_page = -1; //first rid in the page
    int column = -1;
    bool valid = FALSE;
    bool pin = FALSE;
    bool dirty = FALSE;
    int age = -1;
};

extern BufferPool buffer_pool;

#endif
