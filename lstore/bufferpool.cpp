#include <vector>
#include "page.h"
#include "config.h"
#include "bufferpool.h"
#include <cstring>
#include <cmath>
#include <stdexcept> // Throwing errors


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
}

int BufferPool::get (const RID& rid, const int& column) {
    Frame* found = search(rid, column);
    if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
      found = load(rid, column);
    }
    update_ages(found, hash_vector[(rid.first_rid_page / 4096) % 4)];
    return *(found->page->data + rid.offset * size_of(int)); //return the value we want
}

void BufferPool::set (const RID& rid, const int& column, int value){
    pin(rid, column);
    Frame* found = search(rid, column);
    if(found == nullptr || !found->valid){ //if not already in the bufferpool, load into bufferpool
      found = load(rid, column);
    }
    update_ages(found, hash_vector[(rid.first_rid_page / 4096) % 4)];
    *(found->page->data + rid.offset * size_of(int)) = value;
    found->dirty = true; //the page has been modified
    unpin(rid, column);
    return;
}

Frame* BufferPool::search(const RID& rid, const int& column){
  int hash = (rid.first_rid_page / 4096) % 4; //perform hash on rid
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
    if(just_accesed->next != nullptr){ //if just accessed was not tail
      just_accesed->next->prev = just_accessed->prev;
    }
    just_accessed->next = range_begin; //just_accessed becomes the new range beginning
    if(range_begin != nullptr){ //if range_begin was not head
      just_accessed->prev = range_begin->prev;
    } else {
      just_accessed->prev = nullptr;
    }
    range_begin->prev = just_accessed;
    range_begin = just_accessed;
  }
  return;
}

// Called by get and set
   // There should be an option to not actually read file from storage, which we use it when we make a new file and write things in.
   // Check the availability of the pool. There should be some identifier.
   // Find the file, get the specific part and load into a memory. For now, I'm thinking about saving per table.
Frame* BufferPool::load (const RID& rid, const int& column){ //return the frame that the page was loaded into
	dbMetadataIn.seekg(std::ios::beg);

	std::string tableName = rid.table_name;

	int numberOfFiles;
	dbMetadataIn.read((char*)&numberOfFiles,sizeof(int));

	for(int i = 0; i<numberOfFiles;i++){ //?? We want to load a single page, what is the reason for for loop?
		std::string filename(std::to_string(i));
		filename+=".dat";

		std::ifstream ifs(filename, std::ofstream::out | std::ofstream::binary);

		char nameBuf[257];
		ifs.read((char*)&nameBuf,257);

		if(strcmp(tableName.c_str(),nameBuf)){
			double firstBaseId;
			double firstTailId;
			ifs.read((char*)&firstBaseId,sizeof(double));
			ifs.read((char*)&firstTailId,sizeof(double));

			int numCols;
			int numEntries;
			ifs.seekg(NUMBER_OF_COLUMNS_OFFSET,std::ifstream::beg);
			ifs.read((char*)&numCols,sizeof(int));
			ifs.read((char*)&numEntries,sizeof(int));


			if((rid.id > 0 && std::isnan(firstBaseId))
				|| (rid.id < 0 && std::isnan(firstTailId))){
				i += numCols - 1;
			}

			if((rid.id > 0 && rid.id >= firstBaseId)
					|| (rid.id < 0 && rid.id <= firstTailId)){

				Page* p = new Page();

				for(int i = 0; i < numEntries;i++){
					int nextEntry;
					ifs.read((char*)&nextEntry,sizeof(int));
					p->write(nextEntry);
				}

        Frame* frame = insert_into_frame(rid, column, p); //insert the page into a frame in the bufferpool
        frame->dirty = false; //frame has not yet been modified

				break;
			}

			i += numCols - 1;
		}
	}
  return frame;
}

Frame* BufferPool::insert_into_frame(const RID& rid, const int& column, Page* page){ //return the frame that the page was placed into
  Frame* frame;
  int hash = (rid.first_rid_page / 4096) % 4; //determine correct hash range
  if(frame_directory[hash] == bufferpool_size / NUM_BUFFERPOOL_HASH_PARTITIONS)){ //if hash range is full
    frame = evict(rid);
  } else{ //find empty frame to fill
    Frame* range_begin = hash_vector[hash]; //beginning of hash range
    Frame* range_end = (hash == hash_vector.size()) ? tail : hash_vector[hash + 1]; //end of hash range

    Frame* current_frame = range_begin; //iterate through range
    while(current_frame != range_end){
      if(!current_frame->valid){ //frame is empty
        frame = current_frame;
        break;
      }
      current_frame = current_frame->next;
    }
    if(current_frame == range_end){
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
  pin(rid, column);
  Page* page;
  *(page->data + rid.offset * size_of(int)) = value;
  Frame* frame = insert_into_frame(rid, column, page); //insert the page into a frame in the bufferpool
  update_ages(frame, hash_vector[(rid.first_rid_page / 4096) % 4)];
  frame->dirty = true; //make sure data will be written back to disk
  unpin(rid, column);
  return;
}

Frame* BufferPool::evict(const RID& rid){ //return the frame that was evicted
  int hash = (rid.first_rid_page / 4096) % 4; //determine correct hash range
  Frame* range_begin = hash_vector[hash]; //beginning of hash range
  Frame* range_end = (hash == hash_vector.size()) ? tail : hash_vector[hash + 1]; //end of hash range

  Frame* current_frame = range_end; //iterate through range
  while(true){ //search until a page with no pins is found
    if(current_frame->pin == 0){ //if not pinned, we can evict
      if(current_frame->dirty && current_frame->valid){ //if dirty and valid write back to disk
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
  fp = fopen (frame->table_name + std::to_string(frame->first_rid_page_range) + "_" + std::to_string(frame->first_rid_page) + "_" + std::to_string(frame->column) + ".dat", "wr");
  fwrite(frame->page->data, sizeof(int), PAGE_SIZE*sizeof(int), fp);
  // Write in num_rows also into page
  fclose(fp);
  frame->valid = false; //the frame is now empty
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
    throw std::invalid_argument("Attempt to unpin record that was not already pinned");
  }
  return;
}

Frame::Frame () {}

Frame::~Frame () {}
