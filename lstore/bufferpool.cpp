// #include <bits/types/FILE.h>
#include <vector>
#include <stdio.h>
#include <string>
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <cmath>
#include <stdexcept> // Throwing errors
#include <iostream>
// #include <unistd.h>

#include "page.h"
#include "config.h"
#include "bufferpool.h"
#include "../Toolkit.h"

// BufferPool buffer_pool(BUFFER_POOL_SIZE);

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
  std::cout << "Bufferpool destructor?" << std::endl;
  write_back_all(); //make sure all unsaved data gets back to disk
  Frame* current_frame = head;
  while(current_frame != nullptr){ //iterate through entire bufferpool
    Frame* old = current_frame;
    current_frame = current_frame->next;
    delete old;
  }
}

int BufferPool::hash_fun(unsigned int x) {
  return (x / 4096) % 4;
}

int BufferPool::get (const RID& rid, const int& column) {
  Frame* found = search(rid, column);
  if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  return *(found->page->data + rid.offset); //return the value we want
}

void BufferPool::set (const RID& rid, const int& column, int value){
  pin(rid, column);
  Frame* found = search(rid, column);
  if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  // found->page->write(value);
  *(found->page->data + rid.offset) = value;
  found->dirty = true; //the page has been modified
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  unpin(rid, column);
  return;
}

Frame* BufferPool::search(const RID& rid, const int& column){
  size_t hash = hash_fun(rid.first_rid_page); //perform hash on rid
  Frame* range_begin = hash_vector[hash]; //beginning of hash range
  Frame* range_end = (hash == hash_vector.size() - 1) ? tail : hash_vector[hash + 1]->prev; //end of hash range
  Frame* current_frame = range_begin; //iterate through range
    std::cout << "searching : " << rid.first_rid_page << ", " << column << std::endl;
  while(current_frame != range_end->next){
    std::cout << "search : " <<current_frame << " current frame->next: " << current_frame->next << " first_rid_page: " << current_frame->first_rid_page << " column: " << current_frame->column << " pin: " << current_frame->pin << std::endl;
    if ((current_frame->valid)) {
      if(rid.first_rid_page == current_frame->first_rid_page && column == current_frame->column){
        return current_frame;
      }
    }
    current_frame = current_frame->next;
     std::cout << "search : " <<current_frame << " current frame->next: " << current_frame->next << std::endl;
  }
  // std::cout << "miss!" << std::endl;
  return nullptr; //if not found in the range
}

void BufferPool::update_ages(Frame*& just_accessed, Frame*& range_begin){ //change ages and reorder linked list

  if(just_accessed != range_begin){ //if not already the range beginning / most recently accessed
    if(just_accessed->next == nullptr ){
      tail = just_accessed->prev;
    } else if (range_begin->prev == nullptr) {
      head = just_accessed;
    }

    just_accessed->prev->next = just_accessed->next; //close gap where just_accessed used to be
    if(just_accessed->next != nullptr){ //if just accessed was not tail
      just_accessed->next->prev = just_accessed->prev;
    }
    just_accessed->prev = range_begin->prev; //just_accessed becomes the new range beginning
    just_accessed->next = range_begin;
    if (range_begin->prev != nullptr) {
      range_begin->prev->next = just_accessed;
    }
    range_begin->prev = just_accessed;
    range_begin = just_accessed;
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

  FILE* fp = fopen((data_path).c_str(),"r");
  if (!fp) {
    throw std::invalid_argument("Couldn't open file " + data_path);
  }
  Frame* frame = nullptr;
  Page* p = new Page();
  int e = fread(&(p->num_rows), 1, sizeof(int), fp);
  e = fread(p->data, p->num_rows, sizeof(int), fp);
  fclose(fp);
  frame = insert_into_frame(rid, column, p); //insert the page into a frame in the bufferpool
  frame->dirty = false; //frame has not yet been modified
  if (e != 1 + p->num_rows) {
    std::cout << "error?" << std::endl;
  }
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
  // page->write(value);
  *(page->data + rid.offset) = value;

  Frame* frame = insert_into_frame(rid, column, page); //insert the page into a frame in the bufferpool
  pin(rid, column);
  frame->dirty = true; //make sure data will be written back to disk
  update_ages(frame, hash_vector[hash_fun(rid.first_rid_page)]);
  unpin(rid, column);

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
  fwrite(&(frame->page->num_rows), 1, sizeof(int), fp);
  fwrite(frame->page->data, frame->page->num_rows, sizeof(int), fp);
  fclose(fp);
  delete frame->page;
}

void BufferPool::write_back_all (){
  Frame* current_frame = head;
  while(current_frame != nullptr){ //iterate through entire bufferpool
    if(current_frame->dirty && current_frame->valid){
      write_back(current_frame);
    }
    current_frame = current_frame->next;
  }
  return;
}

void BufferPool::pin (const RID& rid, const int& column) {
  Frame* found = search(rid, column);
    std::cout << found << std::endl;

    std::cout << "Pin : first_rid_page: "<< found->first_rid_page << " column: " << found->column << " pin: " << found->pin << std::endl;

  if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  (found->pin)++;
    std::cout << "Pin3 : first_rid_page: "<< found->first_rid_page << " column: " << found->column << " pin: " << found->pin << std::endl;

  return;
}

void BufferPool::unpin (const RID& rid, const int& column) {
  Frame* found = search(rid, column);
  std::cout << found << std::endl;
  std::cout << "Unpin : first_rid_page: "<< found->first_rid_page << " column: " << found->column << " pin: " << found->pin << std::endl;
  if(found == nullptr || !found->valid){ //if not in the bufferpool
    throw std::invalid_argument("Attempt to unpin record that was not already pinned (No record found)");
  }
  (found->pin)--;
  std::cout << "Unpin2 : first_rid_page: "<< found->first_rid_page << " column: " << found->column << " pin: " << found->pin << std::endl;
  if(found->pin < 0){ //if pin count gets below 0
    (found->pin) = 0;
    std::cout << rid.id << std::endl;
    throw std::invalid_argument("Attempt to unpin record that was not already pinned (Pin negative value)");
  }
  return;
}

void BufferPool::set_path (const std::string& path_rhs) {
  path = path_rhs;
}


Frame::Frame () {}

Frame::~Frame () {}

bool Frame::operator==(const Frame& rhs) {
  return ((first_rid_page_range == rhs.first_rid_page_range) && (first_rid_page == rhs.first_rid_page) && (column == rhs.column));
}
