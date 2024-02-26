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
  write_back_all(); //make sure all unsaved data gets back to disk
  Frame* current_frame = head;
  while(current_frame != nullptr){ //iterate through entire bufferpool
    current_frame = current_frame->next;
    delete current_frame->prev;
  }
}

int BufferPool::hash_fun(int x) {
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
  std::cout << "found before " << found << "\n";
  std::cout << "hash before " << hash_vector[hash_fun(rid.first_rid_page)] << "\n";
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  std::cout << "found after " << found << "\n";
  std::cout << "hash after " << hash_vector[hash_fun(rid.first_rid_page)] << "\n";
  found->page->write(value);
  found->dirty = true; //the page has been modified
  unpin(rid, column);
  return;
}

Frame* BufferPool::search(const RID& rid, const int& column){
  size_t hash = hash_fun(rid.first_rid_page); //perform hash on rid
  Frame* range_begin = hash_vector[hash]; //beginning of hash range
  Frame* range_end = (hash == hash_vector.size() - 1) ? tail : hash_vector[hash + 1]->prev; //end of hash range
  Frame* current_frame = range_begin; //iterate through range
  while(current_frame != range_end->next){
    if(rid.id == 32769){

    std::cout << "Looking for " << rid.first_rid_page << ", found " << current_frame->first_rid_page << std::endl;
    std::cout << "Looking for column " << column << ", found " << current_frame->column << std::endl;
    }
    if ((current_frame->valid)) {
      if(rid.first_rid_page == current_frame->first_rid_page && column == current_frame->column){
        return current_frame;
      }
    }
    current_frame = current_frame->next;
  }
  std::cout << "miss!" << std::endl;
  return nullptr; //if not found in the range
}

void BufferPool::update_ages(Frame*& just_accessed, Frame*& range_begin){ //change ages and reorder linked list
  if(just_accessed != range_begin){ //if not already the range beginning / most recently accessed
    (just_accessed)->prev->next = (just_accessed)->next; //close gap where just_accessed used to be
    if((just_accessed)->next != nullptr){ //if just accessed was not tail
      (just_accessed)->next->prev = (just_accessed)->prev;
    }
    (just_accessed)->prev = (range_begin)->prev; //just_accessed becomes the new range beginning
    (just_accessed)->next = range_begin;
    (range_begin)->prev = just_accessed;
    range_begin = just_accessed;
  }
  return;
}

// Called by get and set
Frame* BufferPool::load (const RID& rid, const int& column){ //return the frame that the page was loaded into
  std::string data_path = path + rid.table_name
    + "_" + std::to_string(rid.first_rid_page_range)
    + "_" + std::to_string(rid.first_rid_page)
    + "_" + std::to_string(column) + ".dat";

  FILE* fp = fopen((data_path).c_str(),"r");
  if (!fp) {
    throw std::invalid_argument("Couldn't open file " + data_path);
  }
  Frame* frame = nullptr;
  Page* p = new Page();
  fread(&(p->num_rows), 1, sizeof(int), fp);
  fread(p->data, p->num_rows, sizeof(int), fp);
  fclose(fp);
  frame = insert_into_frame(rid, column, p); //insert the page into a frame in the bufferpool
  frame->dirty = false; //frame has not yet been modified

  return frame;
}

Frame* BufferPool::insert_into_frame(const RID& rid, const int& column, Page* page){ //return the frame that the page was placed into
  Frame* frame = nullptr;
  size_t hash = hash_fun(rid.first_rid_page); //determine correct hash range

  if(frame_directory[hash] == bufferpool_size / NUM_BUFFERPOOL_HASH_PARTITIONS){ //if hash range is full
    frame = evict(rid);
  } else{ //find empty frame to fill
    std::cout << "Error at 1\n";
    Frame* range_begin = hash_vector[hash]; //beginning of hash range
    std::cout << "Error at 2\n";
    Frame* range_end = hash == hash_vector.size() - 1 ? tail : hash_vector[hash + 1]->prev; //end of hash range
    std::cout << "Error at 3\n";
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
  page->write(value);
  // *(page->data + rid.offset) = value;
    std::cout << "Inserting " << rid.first_rid_page << std::endl;
    std::cout << "Inserting column " << column << std::endl;
  Frame* frame = insert_into_frame(rid, column, page); //insert the page into a frame in the bufferpool
  std::cout << "Pin new page" << std::endl;
  pin(rid, column);
  update_ages(frame, hash_vector[hash_fun(rid.first_rid_page)]);
  std::cout << "updated ages\n";
  frame->dirty = true; //make sure data will be written back to disk
  unpin(rid, column);

  return;
}

Frame* BufferPool::evict(const RID& rid){ //return the frame that was evicted
  size_t hash = hash_fun(rid.first_rid_page); //determine correct hash range
  Frame* range_begin = hash_vector[hash]; //beginning of hash range
  Frame* range_end = (hash == hash_vector.size() - 1) ? tail : hash_vector[hash + 1]->prev; //end of hash range

  Frame* current_frame = range_end; //iterate through range
  while(true){ //search until a page with no pins is found
    if(current_frame->pin == 0){ //if not pinned, we can evict
      if(current_frame->dirty && current_frame->valid){ //if dirty and valid write back to disk
        write_back(current_frame);
      }
      if(rid.id == 32769){
        std::cout << "Evicted " << std::endl;
      }
      return current_frame;
    }
    current_frame = current_frame->prev;
    if(current_frame == range_begin && current_frame->pin != 0){ //if all the pages are pinned
      current_frame = range_end;
    }
  }
}

void BufferPool::write_back(Frame* frame){
  std::string data_path = path + frame->table_name
    + "_" + std::to_string(frame->first_rid_page_range)
    + "_" + std::to_string(frame->first_rid_page)
    + "_" + std::to_string(frame->column) + ".dat";
  std::cout << "Writing back to" << data_path << std::endl;
  FILE* fp = fopen((data_path).c_str(),"w");
  if (!fp) {
    throw std::invalid_argument("Couldn't open file " + data_path);
  }
  fwrite(&(frame->page->num_rows), 1, sizeof(int), fp);
  fwrite(frame->page->data, frame->page->num_rows, sizeof(int), fp);
  fclose(fp);
  delete frame->page;
  frame->valid = false; //frame is now empty
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
  if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  (found->pin)++;
  return;
}

void BufferPool::unpin (const RID& rid, const int& column) {
  Frame* found = search(rid, column);
  if(found == nullptr || !found->valid){ //if not in the bufferpool
    throw std::invalid_argument("Attempt to unpin record that was not already pinned");
  }
  (found->pin)--;
  if(found->pin < 0){ //if pin count gets below 0
    (found->pin) = 0;
    throw std::invalid_argument("Attempt to unpin record that was not already pinned");
  }
  return;
}

void BufferPool::set_path (const std::string& path_rhs) {
  path = path_rhs;
}


Frame::Frame () {}

Frame::~Frame () {}
