#include <vector>
#include "page.h"
#include "bufferpool.h"


BufferPool::BufferPool () {
    // I don't think we need to initialize with anything additional.
}

BufferPool::~BufferPool () {
    // Make sure everything is evicted?
    evict_all();
}

int* BufferPool::get() {
    // Get a page using some sort of identifier.
    // Check if it is in the buffer pool or not.
    // If not, call load and return the pointer.
    return nullptr;
}
int* BufferPool::load (){
    // Called by get.
    // There should be a option to not actually read file from storage, which we use it when we make a new file and write things in.
    // Check the availability of the pool. There should be some identifier.
    // Find the file, get the specific part and load into a memory. For now, I'm thinking about saving per table.
    // If it is saved per page range, we need to find a way to get the specific page.
    return nullptr;
}
void BufferPool::evict (){
    // Called by load with which area to have a file evicted.
    // Evict a page that has no pin. Write on disk if it is dirty.
}
void BufferPool::evict_all (){
    // Called by close from db.cpp.
    // This will cause all the pages to be evicted.
    // This have separated file in case if there is some optimization if we can not think about which and order and such.
}
void BufferPool::pin (const int& rid, const int& page_num) {
    // Pin a page
}
void BufferPool::unpin (const int& rid, const int& page_num) {
    // Unpin a page
}
