#include <vector>
#include "page.h"
#include "config.h"
#include "bufferpool.h"


BufferPool::BufferPool () {
  for(int i = 0; i < BUFFER_POOL_SIZE; i++){ //each frame is initialized with an age
    buffer[i].age = i;
  }
}

BufferPool::~BufferPool () {
    evict_all();
}

int BufferPool::get (const RID& rid, const int& column) {
    bool found = FALSE;
    int index_of_found = -1;

    for(int i = 0; i < BUFFER_POOL_SIZE; i++){ //scan through bufferpool to find desired page
      if(buffer[i].valid && rid.first_rid_page == buffer[i].first_rid_page && column == buffer[i].column){ //if it is valid and the value that we want
        found = TRUE;
        index_of_found = i;
        break;
      }
    }
    if(!found){ //if not already in the bufferpool, load into bufferpool
      index_of_found = load(rid, column);
    }
    int age_of_found = buffer[index_of_found].age;
    // Update all ages
    return *(buffer[index_of_found].page->data + rid.offset); //return the value we want
}

void BufferPool::set (const RID& rid, const int& column, int value){ //return the index of where you placed it
    // Given desired RID and desired column
    // Check if it is already in the buffer pool or not.
    // If not, call load to retrieve from disk and return the value
    // Update all ages
    //set dirty bit 1
}

int BufferPool::load (){ //return the index of where you placed it
    // Called by get
    // There should be an option to not actually read file from storage, which we use it when we make a new file and write things in.
    // Check the availability of the pool. There should be some identifier.
    // Find the file, get the specific part and load into a memory. For now, I'm thinking about saving per table.
    // If bufferpool is full, call evict
    // Set the valid bit of the frame to 1;
    // Set the dirty bit of the frame to 0;
}

void BufferPool::insert_new_page() {

};
int BufferPool::evict (){ //return the index of evicted
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

Frame::Frame ();

virtual Frame::~Frame ();
