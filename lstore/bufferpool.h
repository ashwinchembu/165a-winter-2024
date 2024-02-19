#include <vector>
#include "page.h"

class BufferPool {
public:
    BufferPool ();
    virtual ~BufferPool ();
    const int column = 1;
    const int row = 1;
    std::vector<std::vector<Page*>> buffer;
    std::vector<std::vector<int>> pin_dirty; // Can we conbine? E.g. 0 is all clean, 1 is pin, 2 is dirty.+*
    int* get ();
    int* load ();
    void evict ();
    void evict_all ();
};
