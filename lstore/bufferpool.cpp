#include <vector>
#include <stdio.h>
#include <string>
#include <algorithm>
#include<sys/stat.h>
#include <cstring>
#include <fcntl.h>
#include <cmath>
#include <stdexcept> // Throwing errors
#include <iostream>
#include <thread>

#include "page.h"
#include "config.h"
#include "bufferpool.h"
#include "lock_manager.h"
#include "../Toolkit.h"
#include "../DllConfig.h"

int id_counter = 0;

BufferPool::BufferPool (const int& num_pages) : bufferpool_size(num_pages){ // @suppress("Class members should be properly initialized")
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
  Frame* current_frame = head;
  while(current_frame != nullptr){ //iterate through entire bufferpool
    Frame* old = current_frame;
    current_frame = current_frame->next;
    delete old;
  }
}

int BufferPool::hash_fun(unsigned int x) {
  return (x / PAGE_SIZE) % NUM_BUFFERPOOL_HASH_PARTITIONS;
}

int BufferPool::get (const RID& rid, const int& column) {
  int return_val = NONE - 10;
  Frame* found = pin(rid, column);
  if(found == nullptr){ //if not already in the bufferpool, load into bufferpool
    return return_val;
  }
  return_val = *(found->page->data + rid.offset);
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  unpin(rid, column);
  return return_val; //return the value we want
}

Frame* BufferPool::get_page(const RID& rid, const int& column){
  Frame* found = search(rid, column);
  if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  return found; //return the value we want
}


bool BufferPool::set (const RID& rid, const int& column, const int& value, const bool& is_new){
  Frame* found = pin(rid, column);
  if(found == nullptr){ //if not already in the bufferpool, load into bufferpool
    return false;
  }
  *(found->page->data + rid.offset) = value;
  if(is_new){
    found->page->num_rows++;
  }
  found->dirty = true; //the page has been modified
  update_ages(found, hash_vector[hash_fun(rid.first_rid_page)]);
  unpin(rid, column);
  return true;
}

Frame* BufferPool::search(const RID& rid, const int& column){
  std::shared_lock lock(update_age_lock, std::defer_lock);

  size_t hash = hash_fun(rid.first_rid_page); //perform hash on rid
  lock.lock();
  Frame* range_begin = hash_vector[hash]; //beginning of hash range
  Frame* range_end = (hash == hash_vector.size() - 1) ? tail : hash_vector[hash + 1]->prev; //end of hash range
  Frame* current_frame = range_begin; //iterate through range
  while(current_frame != range_end->next){
    if ((current_frame->valid)) {
      if(rid.first_rid_page == current_frame->first_rid_page && column == current_frame->column){
        lock.unlock();
        return current_frame;
      }
    }
    current_frame = current_frame->next;
  }
  lock.unlock();
  return nullptr; //if not found in the range
}


void BufferPool::update_ages(Frame*& just_accessed, Frame*& range_begin){ //change ages and reorder linked list
  std::unique_lock lock(update_age_lock);
  // update_age_lock.lock();
  if(just_accessed != range_begin){ //if not already the range beginning / most recently accessed
    if(just_accessed->next == nullptr ){ //if just_accessed is the tail
      tail = just_accessed->prev;
    } else if (range_begin->prev == nullptr) { //if range_begin is the head
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
  // update_age_lock.unlock();
  lock.unlock();
  return;
}

std::string BufferPool::buildDatPath(std::string tname,int first_rid_page,int first_rid_page_range,int column){
//	char ret[1024];
//	char* ptr =  ret;

	bool isBase = false;

	std::string buildPath = this->path;
	buildPath.append("/")
			 .append(tname);

//	ptr += sprintf(ptr,"%s/",path.c_str());
//
//	ptr += sprintf(ptr, "%s",tname.c_str());

	if(column < NUM_METADATA_COLUMNS){
//		ptr+= sprintf(ptr," M ");
		buildPath.append("M");

	} else if(first_rid_page < 0){
//		ptr+= sprintf(ptr," T ");
		buildPath.append("T");

	} else if(first_rid_page > 0){
//		ptr+= sprintf(ptr," B ");
		buildPath.append("B");
		isBase = true;
	}

	buildPath.append(std::to_string(first_rid_page))
			 .append("")
			 .append(std::to_string(first_rid_page_range))
			 .append("")
			 .append(std::to_string(column));

//	ptr+=sprintf(ptr,"%d %d %d",first_rid_page,first_rid_page_range,column);

	if(isBase){
//		ptr+=sprintf(ptr," %d",mergeNumber);

		buildPath.append("")
		         .append(std::to_string(tableVersions.find(tname)->second));
	}

	buildPath.append(".dat");

//	sprintf(ptr, ".dat");
//
//	return {ret};

	return buildPath;
}

// Called by get and set
Frame* BufferPool::load (const RID& rid, const int& column){ //return the frame that the page was loaded into
  std::string data_path = buildDatPath(rid.table_name,rid.first_rid_page,
          rid.first_rid_page_range,column);
  FILE* fp = fopen((data_path).c_str(),"r");
  if (!fp) {
    throw std::invalid_argument("Load : Couldn't open file " + data_path);
  }

  Frame* frame = nullptr;
  Page* p = new Page();
  int e = fread(&(p->num_rows), sizeof(int), 1, fp);
  e = e + fread(p->data, sizeof(int), p->num_rows, fp);
  fclose(fp);
  frame = insert_into_frame(rid, column, p); //insert the page into a frame in the bufferpool
  frame->dirty = false; //frame has not yet been modified
  if (e != 1 + p->num_rows) {
    std::cerr << "Possible error (Bufferpool Load : Number of read does not match)" << e << std::endl;
  }
  return frame;
}

Frame* BufferPool::insert_into_frame(const RID& rid, const int& column, Page* page){ //return the frame that the page was placed into
  Frame* frame = nullptr;
  size_t hash = hash_fun(rid.first_rid_page); //determine correct hash range
  std::shared_lock<std::shared_mutex> share_lock(frame_directory_lock);
  // shared_frame_directory_lock.lock();
  if(frame_directory[hash] == (bufferpool_size / NUM_BUFFERPOOL_HASH_PARTITIONS)){ //if hash range is full
    // shared_frame_directory_lock.unlock();
    share_lock.unlock();
    frame = evict(rid);
  } else{ //find empty frame to fill
    // shared_frame_directory_lock.unlock();
    share_lock.unlock();
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
  std::unique_lock<std::shared_mutex> unique_lock(frame_directory_lock);

  // unique_frame_directory_lock.lock();
  frame_directory[hash]++; //a frame has been filled
  // unique_frame_directory_lock.unlock();
  unique_lock.unlock();
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
      std::unique_lock<std::shared_mutex> unique_lock(frame_directory_lock);
      frame_directory[hash]--;
      unique_lock.unlock();

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
  std::string data_path = buildDatPath(frame->table_name,frame->first_rid_page,
          frame->first_rid_page_range,frame->column);
  FILE* fp = fopen((data_path).c_str(),"w");
  if (!fp) {
    throw std::invalid_argument("Write Back : Couldn't open file " + data_path);
  }
  if (frame->page != nullptr) {
    fwrite(&(frame->page->num_rows), sizeof(int), 1, fp);
    fwrite(frame->page->data, sizeof(int), frame->page->num_rows, fp);

//    buffer = std::to_string(frame->page->num_rows) + " ";
//    fwrite(buffer.c_str(), buffer.size(), 1, textFilePtr);

//    int* ptr = frame->page->data;
//    for(int i = 0; i < frame->page->num_rows; i++){
//    	buffer = std::to_string(ptr[i]) + " ";
////    	fwrite(buffer.c_str(), buffer.size(), 1, textFilePtr);
//    }

  }
  fclose(fp);

  if(frame->page != nullptr){
    delete frame->page;
  }
}

void BufferPool::write_back_all (){
  Frame* current_frame = head;
  while(current_frame != nullptr){ //iterate through entire bufferpool
    if(current_frame != nullptr && (current_frame->dirty && current_frame->valid)){
      write_back(current_frame);
    } else if(current_frame->page != nullptr && !current_frame->dirty && current_frame->valid){
      delete current_frame->page;
    }

    current_frame->valid = false;
    current_frame = current_frame->next;
  }
  return;
}

Frame* BufferPool::pin (const RID& rid, const int& column) {
  Frame* found = nullptr;
  found = search(rid, column);
  if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
    found = load(rid, column);
  }
  (found->pin)++;
  return found;
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


Frame::Frame () {
}

Frame::~Frame () {
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
  bool current_flag = rhs.valid;
  valid = current_flag; //whether the frame contains data
  int current_pin = rhs.pin;
  pin = current_pin; //how many transactions have pinned the page
  current_flag = rhs.dirty;
  dirty = current_flag; //whether the page was modified
}
