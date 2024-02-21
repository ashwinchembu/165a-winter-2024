#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <vector>
#include <string>
#include "page.h"

class BufferPool {
public:
    BufferPool ();
    virtual ~BufferPool ();
    const int column = 1;
    const int row = 1;
    std::vector<std::vector<Page*>> buffer;
    std::vector<std::vector<int>> pin_dirty_age; // Can we conbine? E.g. Using most significant bit for dirty or not. Next 5 bits for pin, rest of 26 bits for age?
    int get (const RID& rid, const int& column); // given a rid and column, returns the value in that location
    void set (const RID& rid, const int& column); // given a rid and column, changes the value in that location
    int load ();
    void evict ();
    void evict_all ();
    void pin (const int& rid, const int& page_num);
    void unpin (const int& rid, const int& page_num);
};

extern BufferPool buffer_pool;

#endif
