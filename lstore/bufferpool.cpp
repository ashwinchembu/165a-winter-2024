// #include <bits/types/FILE.h>
#include <vector>
#include <stdio.h>
#include <string>
#include <algorithm>
#include <cstring>
#include <fcntl.h>
#include <cmath>
#include <stdexcept> // Throwing errors
#include <unistd.h>

#include "page.h"
#include "config.h"
#include "bufferpool.h"
#include "../Toolkit.h"

BufferPool::BufferPool (const int& num_pages) : bufferpool_size(num_pages){
  head = new Frame; //create head
  hash_vector.push_back(head); //head will be the first hash range beginning
  Frame* old_frame = head;

  for(int i = 1; i < (bufferpool_size - 1); i++){
    Frame* new_frame = new Frame;
    old_frame->next = new_frame;
    new_frame->prev = old_frame;
    if(i % (bufferpool_size / NUM_BUFFERPOOL_HASH_PARTITIONS) == 0){ //check if the pointer should be a hash range beginning
      hash_vector.push_back(new_frame);
    }
    old_frame = new_frame;
  }

  tail = new Frame; //create tail
  old_frame->next = tail;
  tail->prev = old_frame;

  //frame_directory.resize(num_pages, 0);
}

BufferPool::~BufferPool () {
  write_back_all();
}

int BufferPool::hash_fun(int x) {
  return (x / 4096) % 4;
}

int BufferPool::get (const RID& rid, const int& column) {
  Frame* found = search(rid, column);
  if(found == nullptr){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  return *(found->page->data + rid.offset * sizeof(int)); //return the value we want
}

void BufferPool::set (const RID& rid, const int& column, int value){ //return the index of where you placed it
  pin(rid, column);
  Frame* found = search(rid, column);
  if(found == nullptr){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  *(found->page->data + rid.offset * sizeof(int)) = value;
  found->dirty = true;
  unpin(rid, column);
  return;
}

Frame* BufferPool::search(const RID& rid, const int& column){
  int hash = hash_fun(rid.first_rid_page); //perform hash on rid
  Frame* range_begin = hash_vector[hash]; //beginning of hash range
  Frame* range_end = (hash == hash_vector.size()) ? tail : hash_vector[hash + 1]; //end of hash range

  Frame* current_frame = range_begin; //iterate through range
  while(current_frame != range_end){
    if(rid.first_rid_page == current_frame->first_rid_page && column == current_frame->column){
      return current_frame;
    }
    current_frame = current_frame->next;
  }

  return nullptr; //if not found in the range
}

void BufferPool::update_ages(Frame* just_accessed, Frame* range_begin){ //change ages and reorder linked list
  if(just_accessed != range_begin){ //if not already the range beginning / most recently accessed
    just_accessed->prev->next = just_accessed->next; //close gap where just_accessed used to be
    if(just_accessed->next != nullptr){ //if just accessed was not tail
      just_accessed->next->prev = just_accessed->prev;
    }
    range_begin->prev = just_accessed; //just_accessed becomes the new head
    just_accessed->next = range_begin;
    just_accessed->prev = nullptr;
    range_begin = just_accessed;
  }
}

bool placeholderForHasSpace;

Frame* placeholderForGetAvaliableFrame(RID rid){
	return 0;
}

// Called by get and set
// There should be an option to not actually read file from storage, which we use it when we make a new file and write things in.
// Check the availability of the pool. There should be some identifier.
// Find the file, get the specific part and load into a memory. For now, I'm thinking about saving per table.
// If bufferpool is full, call evict
// Set the valid bit of the frame to 1;
// Set the dirty bit of the frame to 0;
Frame* BufferPool::load (const RID& rid, const int& column){ //return the index of where you placed it
	std::string path = "..\\Disk\\" + rid.table_name
			+ "_" + std::to_string(rid.first_rid_page_range)
			+ "_" + std::to_string(rid.first_rid_page)
			+ "_" + std::to_string(column) + ".dat";

	int fd = open((const char*)path.c_str(), O_RDWR);

	Frame* evictedFrame = nullptr;

	if(fd != -1){
		Page* p = new Page();
		float buffer;

		while(true){
			read(fd, &buffer, sizeof(float));

			if(std::isnan(buffer)){
				break;
			}

			p->write((int)buffer);
		}

		close(fd);

		if(!placeholderForHasSpace){
			evictedFrame = evict(rid);
		} else {
			evictedFrame = placeholderForGetAvaliableFrame(rid);
		}

		evictedFrame->page = p;
		evictedFrame->table_name = rid.table_name;
		evictedFrame->first_rid_page = rid.first_rid_page;
		evictedFrame->first_rid_page_range = rid.first_rid_page_range;
		evictedFrame->column = column;
		evictedFrame->valid= true;
		evictedFrame->pin = 0;
		evictedFrame->dirty = false;
	}

    return evictedFrame;
}
// std::string file_name = file_path + rid.table_name + "_" + std::to_string(rid.first_rid_page_range) + "_" + std::to_string(rid.first_rid_page) + "_" + std::to_string(column) + ".dat";
// FILE* fp = std::fopen(file_name.c_str(), "r");
// if (!fp) {
//   throw std::invalid_argument("File with given RID does not exist");
// }
// Page* p = new Page();
// fread(&(p->num_rows), sizeof(int), 1, fp);
// fread(p->data, sizeof(int), (p->num_rows)*sizeof(int), fp);

/* TODO: how to Find a appropriate Frame using rid.first_rid_page and insert here */

void BufferPool::insert_new_page(const RID& rid, const int& column, int value) {
  Page* p = new Page();
  /* TODO: Calculate with hash_fun and put into a frame and here */
  pin(rid, column);
  Frame* decoy;
  decoy->table_name = rid.table_name;
  decoy->first_rid_page = rid.first_rid_page;
  decoy->first_rid_page_range = rid.first_rid_page_range;
  decoy->column = column;

  update_ages(decoy, hash_vector[hash_fun(rid.first_rid_page)]);
  *(decoy->page->data + rid.offset * sizeof(int)) = value;

  decoy->dirty = true;
  unpin(rid, column);
}

Frame* BufferPool::evict(const RID& rid){ //return the frame that was evicted
  int hash = hash_fun(rid.first_rid_page); //perform hash on rid
  Frame* range_begin = hash_vector[hash]; //beginning of hash range
  Frame* range_end = (hash == hash_vector.size()) ? tail : hash_vector[hash + 1]; //end of hash range

  Frame* current_frame = range_end; //iterate through range
  while(true){
    if(current_frame->pin == 0){ //if not pinned
      if(current_frame->dirty){ //if dirty
        write_back(current_frame);
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
  // Use fastformat library instead of to_string

	std::string path = "..\\Disk\\" + frame->table_name
				+ "_" + std::to_string(frame->first_rid_page_range)
				+ "_" + std::to_string(frame->first_rid_page)
				+"_" + std::to_string(frame->column) + ".dat";

	int fd = open((const char*)path.c_str(), O_RDWR);

	if(fd != -1){
		write(fd,frame->page->data,frame->page->num_rows* sizeof(int));

		float nan = std::nan("");
		write(fd,&nan,sizeof(float));

		close(fd);
	}

   frame->valid = false;
}

//
//  std::string file_name = file_path + frame->table_name + "_" + std::to_string(frame->first_rid_page_range) + "_" + std::to_string(frame->first_rid_page) + "_" + std::to_string(frame->column) + ".dat";
//
//
//
//  FILE* fp = fopen(file_name.c_str(), "w");
//  fwrite(&(frame->page->num_rows), sizeof(int), 1, fp);
//  fwrite(frame->page->data, sizeof(int), PAGE_SIZE*sizeof(int), fp);
//  // Write in num_rows also into page
//  fclose(fp);

void BufferPool::write_back_all (){
  Frame* current_frame = head;
  while(current_frame != nullptr){
    if(current_frame->dirty){
      write_back(current_frame);
    }
    current_frame = current_frame->next;
  }
  return;
}

void BufferPool::pin (const RID& rid, const int& column) {
  Frame* found = search(rid, column);
  if(found == nullptr){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  (found->pin)++;
  return;
}

void BufferPool::unpin (const RID& rid, const int& column) {
  Frame* found = search(rid, column);
  if(found == nullptr){ //if not in the bufferpool
    throw std::invalid_argument("This record was never pinned to begin with");
  }
  (found->pin)--;
  if(found->pin < 0){ //if pin count gets below 0
    throw std::invalid_argument("This record was never pinned to begin with");
  }
  return;
}

Frame::Frame () {}

Frame::~Frame () {}

