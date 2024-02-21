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
    int rid_id = -1;
    int column = -1;
    bool valid = 0;
    bool pin = 0;
    bool dirty = 0;
    int age = -1;
};

extern BufferPool buffer_pool;

#endif
