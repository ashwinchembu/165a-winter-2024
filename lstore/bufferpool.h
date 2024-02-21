#ifndef BUFFERPOOL_H
#define BUFFERPOOL_H

#include <vector>
#include "page.h"

class BufferPool {
public:
    BufferPool ();
    virtual ~BufferPool ();
    const int column = 1;
    const int row = 1;
    std::vector<std::vector<Page*>> buffer;
    std::vector<std::vector<int>> pin_dirty_age; // Can we conbine? E.g. 0 is all clean, 1 is pin, 2 is dirty, 4-16 is age?
    int* get ();
    int* load ();
    void evict ();
    void evict_all ();
    void pin (const int& rid, const int& page_num);
    void unpin (const int& rid, const int& page_num);
};

extern BufferPool buffer_pool;

#endif
