#include <vector>
#include "page.h"
#include "config.h"
#include "bufferpool.h"


BufferPool::BufferPool ();

BufferPool::~BufferPool () {
    evict_all();
}

int BufferPool::get (const RID& rid, const int& column) {
    // Given desired RID and desired column
    // Check if it is already in the buffer pool or not.
    // If not, call load to retrieve from disk and return the value
    // Update all ages
}

void BufferPool::set (const RID& rid, const int& column){
    // Given desired RID and desired column
    // Check if it is already in the buffer pool or not.
    // If not, call load to retrieve from disk and return the value
    // Update all ages
}

void BufferPool::load (){
    // Called by get
    // There should be an option to not actually read file from storage, which we use it when we make a new file and write things in.
    // Check the availability of the pool. There should be some identifier.
    // Find the file, get the specific part and load into a memory. For now, I'm thinking about saving per table.
    // If bufferpool is full, call evict
}

void BufferPool::evict (){
    // Called by load with which area to have a file evicted.
    // Evict a page using LRU that has no pin. Write back to disk if it is dirty.
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
