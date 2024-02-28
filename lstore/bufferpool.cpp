#include <vector>
#include <stdio.h>
#include <string>
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <cmath>
#include <stdexcept> // Throwing errors
#include <iostream>

#include "page.h"
#include "config.h"
#include "bufferpool.h"
#include "../Toolkit.h"

BufferPool::BufferPool (const int& num_pages) : bufferpool_size(num_pages){
  head = new Frame; //create head
  hash_vector.push_back(head); //head will be the first hash range beginning
  frame_directory.push_back(0); //each hash range begins empty

  Frame* old_frame = head; //create number of frames according to bufferpool size
  for(int i = 1; i < (bufferpool_size - 1); i++){
    Frame* new_frame = new Frame;
    old_frame->next = new_frame;
    new_frame->prev = old_frame;
    if(i % (bufferpool_size / NUM_BUFFERPOOL_HASH_PARTITIONS) == 0){ //check if the frame should be a hash range beginning
      hash_vector.push_back(new_frame);
      frame_directory.push_back(0);
    }
    old_frame = new_frame;
  }

  tail = new Frame; //create tail
  old_frame->next = tail;
  tail->prev = old_frame;
}

BufferPool::~BufferPool () {
  std::cout << "Bufferpool Destructor In" << std::endl;
  Frame* current_frame = head;
  while(current_frame != nullptr){ //iterate through entire bufferpool
    Frame* old = current_frame;
    current_frame = current_frame->next;
    delete old;
  }
  std::cout << "Bufferpool Destructor Out" << std::endl;
}

int BufferPool::hash_fun(unsigned int x) {
  return (x / PAGE_SIZE) % NUM_BUFFERPOOL_HASH_PARTITIONS;
}

int BufferPool::get (const RID& rid, const int& column) {
  Frame* found = search(rid, column);
  if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
    //std::cout << "Couldn't find it :(" << std::endl;
  //   Frame* current_frame = head;
  //   while(current_frame != nullptr){ //iterate through entire bufferpool
  //   // if(current_frame->page != nullptr){
  //   //   std::cout << "first rid page is " << current_frame->first_rid_page << " column is " << current_frame->column << std::endl;
  //   // }
  //   current_frame = current_frame->next;
  // }
    found = load(rid, column);
  }
  std::cout << "Update ages" << std::endl;
  std::cout << "found first rid" << found->first_rid_page;
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  std::cout << "base is returning" << *(found->page->data + rid.offset) << std::endl;
  return *(found->page->data + rid.offset); //return the value we want
}

Frame* BufferPool::get_page(const RID& rid, const int& column){
  Frame* found = search(rid, column);
  if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  return found; //return the value we want
}

void BufferPool::set (const RID& rid, const int& column, const int& value, const bool& is_new){
  //std::cout << "hello1" << rid.id << std::endl;
  pin(rid, column);
  //std::cout << "hello2" << std::endl;
  Frame* found = search(rid, column);
  //std::cout << "hello3" << std::endl;
  if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
    //std::cout << "Couldn't find it :(" << std::endl;
    Frame* current_frame = head;
    while(current_frame != nullptr){ //iterate through entire bufferpool
    // if(current_frame->page != nullptr){
    //   std::cout << "first rid page is " << current_frame->first_rid_page << " column is " << current_frame->column << std::endl;
    // }
    current_frame = current_frame->next;
  }
    found = load(rid, column);
  }
  *(found->page->data + rid.offset) = value;
  if(is_new){
    found->page->num_rows++;
  }
  found->dirty = true; //the page has been modified
  unpin(rid, column);
  //std::cout << "hello4" << std::endl;
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  return;
}

Frame* BufferPool::search(const RID& rid, const int& column){
  // if(path == "./ECS165/Merge"){
  //   std::cout << "we are looking for rid " << rid.first_rid_page << " and " << column << std::endl;
  // }
  // std::cout << "we are looking for rid " << rid.first_rid_page << " and " << column << std::endl;
  size_t hash = hash_fun(rid.first_rid_page); //perform hash on rid
  Frame* range_begin = hash_vector[hash]; //beginning of hash range
  // std::cout << "0 range begin is " << range_begin << ", rid " << range_begin->first_rid_page << ", column " << range_begin->column << std::endl;
  // if(path == "./ECS165/Merge"){
  //  std::cout << "hash for " << rid.first_rid_page << " is " << hash << std::endl;
  //  std::cout << "1 range begin is " << hash_vector[0] << std::endl;
  //  std::cout << "2 range begin is " << range_begin << std::endl;
  // }
//  std::cout << "1 head is" << head << std::endl;
  Frame* range_end = (hash == hash_vector.size() - 1) ? tail : hash_vector[hash + 1]->prev; //end of hash range
  // std::cout << "0 range end is " << range_end << ", rid " << range_end->first_rid_page << ", column " << range_end->column << std::endl;
  Frame* current_frame = range_begin; //iterate through range
  while(current_frame != range_end->next){
    if ((current_frame->valid)) {
      // if(path == "./ECS165/Merge"){
        // std::cout << "we are looking at " << current_frame->first_rid_page << " and " << current_frame->column << std::endl;
      // }
      if(rid.first_rid_page == current_frame->first_rid_page && column == current_frame->column){
        // std::cout << "found and exititing" << std::endl;
        return current_frame;
      }
    }
    current_frame = current_frame->next;
  }
  // std::cout << "Miss" << std::endl;
  return nullptr; //if not found in the range
}

/*
Frame* BufferPool::search(const RID& rid, const int& column, std::string merge){
  Frame* current_frame = head; //iterate through range
  while(current_frame != nullptr){
    if ((current_frame->valid)) {
      if(rid.first_rid_page == current_frame->first_rid_page && column == current_frame->column){
        return current_frame;
      }
    }
    current_frame = current_frame->next;
  }
  return nullptr; //if not found in the range
}*/

void BufferPool::update_ages(Frame*& just_accessed, Frame*& range_begin){ //change ages and reorder linked list
  std::cout << "Error 1" << std::endl;
  std::cout << just_accessed->first_rid_page << std::endl;
  std::cout << range_begin->first_rid_page << std::endl;
  if(just_accessed != range_begin){ //if not already the range beginning / most recently accessed
    std::cout << "Error 2" << std::endl;
    if(just_accessed->next == nullptr ){ //if just_accessed is the tail
      std::cout << "Error 3" << std::endl;
      tail = just_accessed->prev;
      std::cout << "Error 4" << std::endl;
    } else if (range_begin->prev == nullptr) { //if range_begin is the head
      std::cout << "Error 5" << std::endl;
      head = just_accessed;
      std::cout << "Error 6" << std::endl;
    }
    std::cout << "Error 7" << std::endl;

    just_accessed->prev->next = just_accessed->next; //close gap where just_accessed used to be
    std::cout << "Error 8" << std::endl;
    if(just_accessed->next != nullptr){ //if just accessed was not tail
      std::cout << "Error 9" << std::endl;
      just_accessed->next->prev = just_accessed->prev;
      std::cout << "Error 10" << std::endl;
    }
    just_accessed->prev = range_begin->prev; //just_accessed becomes the new range beginning
    std::cout << "Error 11" << std::endl;
    just_accessed->next = range_begin;
    std::cout << "Error 12" << std::endl;
    if (range_begin->prev != nullptr) {
      std::cout << "Error 13" << std::endl;
      range_begin->prev->next = just_accessed;
      std::cout << "Error 14" << std::endl;
    }
    range_begin->prev = just_accessed;
    std::cout << "Error 15" << std::endl;
    range_begin = just_accessed;
    std::cout << "Error 16" << std::endl;
  }
  return;
}

// Called by get and set
Frame* BufferPool::load (const RID& rid, const int& column){ //return the frame that the page was loaded into
  int frp = rid.first_rid_page;
  std::string frp_s = std::to_string(rid.first_rid_page);
  if (frp < 0) {
    frp_s = "M" + std::to_string(-1 * (frp));
  }
  std::string data_path = path + "/" + rid.table_name
    + "_" + std::to_string(rid.first_rid_page_range)
    + "_" + frp_s
    + "_" + std::to_string(column) + ".dat";

        //std::cout << "data path: " << data_path << std::endl;

  FILE* fp = fopen((data_path).c_str(),"r");
  if (!fp) {
    throw std::invalid_argument("Couldn't open file " + data_path);
  }
  Frame* frame = nullptr;
  Page* p = new Page();
  int e = fread(&(p->num_rows), sizeof(int), 1, fp);
  e = e + fread(p->data, sizeof(int), p->num_rows, fp);
  fclose(fp);
  frame = insert_into_frame(rid, column, p); //insert the page into a frame in the bufferpool
  frame->dirty = false; //frame has not yet been modified
  // if (e != 1 + p->num_rows) {
  //   std::cout << "error?: " << e << std::endl;
  // }
  return frame;
}

Frame* BufferPool::insert_into_frame(const RID& rid, const int& column, Page* page){ //return the frame that the page was placed into
  Frame* frame = nullptr;
  size_t hash = hash_fun(rid.first_rid_page); //determine correct hash range
  if(frame_directory[hash] == (bufferpool_size / NUM_BUFFERPOOL_HASH_PARTITIONS)){ //if hash range is full
    frame = evict(rid);
  } else{ //find empty frame to fill
    Frame* range_begin = hash_vector[hash]; //beginning of hash range
    Frame* range_end = hash == (hash_vector.size() - 1) ? tail : hash_vector[hash + 1]->prev; //end of hash range
    Frame* current_frame = range_begin; //iterate through range
    while(current_frame != range_end->next){
      if(!current_frame->valid){ //frame is empty
        frame = current_frame;
        break;
      }
      current_frame = current_frame->next;
    }
    if(current_frame == range_end->next){
      throw std::invalid_argument("conflict over whether hash range is full or not");
    }
  }
  frame->page = page;
  frame->first_rid_page = rid.first_rid_page;
  frame->table_name = rid.table_name;
  frame->first_rid_page_range = rid.first_rid_page_range;
  frame->column = column;
  frame->valid = true;
  frame_directory[hash]++; //a frame has been filled
  return frame;
}

void BufferPool::insert_new_page(const RID& rid, const int& column, const int& value) {

  Page* page = new Page();
  *(page->data + rid.offset) = value;
  page->num_rows++;

  Frame* frame = insert_into_frame(rid, column, page); //insert the page into a frame in the bufferpool
  pin(rid, column);
  frame->dirty = true; //make sure data will be written back to disk
  unpin(rid, column);
  update_ages(frame, hash_vector[hash_fun(rid.first_rid_page)]);

  return;
}

Frame* BufferPool::evict(const RID& rid){ //return the frame that was evicted
  size_t hash = hash_fun(rid.first_rid_page); //determine correct hash range
  Frame* range_begin = hash_vector[hash]; //beginning of hash range
  Frame* range_end = (hash == (hash_vector.size() - 1)) ? tail : hash_vector[hash + 1]->prev; //end of hash range

  Frame* current_frame = range_end; //iterate through range
  while(true){ //search until a page with no pins is found
    if(current_frame->pin == 0){ //if not pinned, we can evict
      if(current_frame->dirty && current_frame->valid){ //if dirty and valid write back to disk
        write_back(current_frame);
      }
      // if(path == "./ECS165/Merge"){
      //   std::cout << "evict from merge " << current_frame->first_rid_page << " " << current_frame->column << std::endl;
      // }
      frame_directory[hash]--;
      current_frame->valid = false; //frame is now empty
      return current_frame;
    }
    current_frame = current_frame->prev;
    if(current_frame == range_begin && current_frame->pin != 0){ //if all the pages are pinned
      current_frame = range_end;
    }
  }
}

void BufferPool::write_back(Frame* frame){
  int frp = frame->first_rid_page;
  std::string frp_s = std::to_string(frame->first_rid_page);
  if (frp < 0) {
    frp_s = "M" + std::to_string(-1 * (frp));
  }
  std::string data_path = path + "/" + frame->table_name
    + "_" + std::to_string(frame->first_rid_page_range)
    + "_" + frp_s
    + "_" + std::to_string(frame->column) + ".dat";
  FILE* fp = fopen((data_path).c_str(),"w");
  if (!fp) {
    throw std::invalid_argument("Couldn't open file " + data_path);
  }
  fwrite(&(frame->page->num_rows), sizeof(int), 1, fp);
  fwrite(frame->page->data, sizeof(int), frame->page->num_rows, fp);
  fclose(fp);
  if (frame != nullptr && frame->page != nullptr) {
    delete frame->page;
    // frame->page = nullptr;
    // std::cout << frame << std::endl;

}
}

void BufferPool::write_back_all (){
  Frame* current_frame = head;
  // std::cout << current_frame << std::endl;
  while(current_frame != nullptr){ //iterate through entire bufferpool
    if(current_frame != nullptr && current_frame->dirty && current_frame->valid){
      write_back(current_frame);
    } else {
      current_frame->valid = false;
      if(current_frame->column != -1){
        delete current_frame->page;
        //current_frame->page = nullptr;
      }
    }
    current_frame = current_frame->next;
  }
  return;
}

void BufferPool::pin (const RID& rid, const int& column) {
  Frame* found = search(rid, column);
  // if (found == nullptr) {
  //   std::cout << "sdlkafjlkadsjfkldsajfkldsajfkldsjklfsdklafhadsjlhfjksafhdjk" << std::endl;
  // }
  //std::cout << "found: " << found << " validity: " << !found->valid;
  if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  (found->pin)++;

  return;
}

void BufferPool::unpin (const RID& rid, const int& column) {
  Frame* found = search(rid, column);
  if(found == nullptr || !found->valid){ //if not in the bufferpool
    throw std::invalid_argument("Attempt to unpin record that was not already pinned (No record found)");
  }
  (found->pin)--;
  if(found->pin < 0){ //if pin count gets below 0
    (found->pin) = 0;
    throw std::invalid_argument("Attempt to unpin record that was not already pinned (Pin negative value)");
  }
  return;
}

void BufferPool::set_path (const std::string& path_rhs) {
  path = path_rhs;
}


Frame::Frame () {}

Frame::~Frame () {
  std::cout << "/* message */" << std::endl;
}

bool Frame::operator==(const Frame& rhs) {
  return ((first_rid_page_range == rhs.first_rid_page_range) && (first_rid_page == rhs.first_rid_page) && (column == rhs.column));
}

void Frame::operator=(const Frame& rhs)
    {
      page = rhs.page;
      first_rid_page = rhs.first_rid_page; //first rid in the page
      table_name = rhs.table_name;
      first_rid_page_range = rhs.first_rid_page_range; //first rid in the page range
      column = rhs.column;
      valid = rhs.valid; //whether the frame contains data
      pin = rhs.pin; //how many transactions have pinned the page
      dirty = rhs.dirty; //whether the page was modified
    }
