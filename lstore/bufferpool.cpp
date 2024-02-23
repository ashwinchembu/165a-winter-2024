#include <vector>
#include "page.h"
#include "config.h"
#include "bufferpool.h"
#include <cstring>
#include <cmath>


BufferPool::BufferPool (const int& num_pages) : bufferpool_size(num_pages){
  head = new Frame;
  head->age = 0;
  hash_vector.push_back(head); //head will be the first hash range beginning

  Frame* old_frame = head;

  for(int i = 1; i < BUFFER_POOL_SIZE; i++){ //each frame is initialized with an age
    Frame* new_frame = new Frame;
    old_frame->next = new_frame;
    new_frame->prev = old_frame;
    new_frame->age = i;
    old_frame = new_frame;
  }
}

BufferPool::~BufferPool () {
    evict_all();
}

int BufferPool::get (const RID& rid, const int& column) {
    bool found = false;
    int index_of_found = -1;

    for(int i = 0; i < BUFFER_POOL_SIZE; i++){ //scan through bufferpool to find desired page
      if(buffer[i].valid && rid.first_rid_page == buffer[i].first_rid_page && column == buffer[i].column){ //if it is valid and the value that we want
        found = true;
        index_of_found = i;
        break;
      }
    }
    if(!found){ //if not already in the bufferpool, load into bufferpool
      index_of_found = load(rid, column);
    }
    update_ages(index_of_found);
    return *(buffer[index_of_found].page->data + rid.offset); //return the value we want
}

void BufferPool::set (const RID& rid, const int& column, int value){ //return the index of where you placed it
    // Given desired RID and desired column
    // Check if it is already in the buffer pool or not.
    // If not, call load to retrieve from disk and return the value
    // Update all ages
    //set dirty bit 1
}

void BufferPool::update_ages(const int& index_of_found){
  int age_of_found = buffer[index_of_found].age;
  for(int i = 0; i < BUFFER_POOL_SIZE; i++){ //age increment for all frames with age less than frame that was just accessed
      if(buffer[i].age < age_of_found){
          buffer[i].age++;
      }
  }
  buffer[index_of_found].age = 0; //page just accessed has age of 0
  return;
}

// Called by get
   // There should be an option to not actually read file from storage, which we use it when we make a new file and write things in.
   // Check the availability of the pool. There should be some identifier.
   // Find the file, get the specific part and load into a memory. For now, I'm thinking about saving per table.
   // If bufferpool is full, call evict
   // Set the valid bit of the frame to 1;
   // Set the dirty bit of the frame to 0;
void BufferPool::load (const RID& rid, const int& column){ //return the index of where you placed it
	dbMetadataIn.seekg(std::ios::beg);

	std::string tableName = rid.table_name;

	int numberOfFiles;
	dbMetadataIn.read((char*)&numberOfFiles,sizeof(int));

	for(int i = 0; i<numberOfFiles;i++){
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

				Frame frame;

				Page* p = new Page();

				for(int i = 0; i < numEntries;i++){
					int nextEntry;
					ifs.read((char*)&nextEntry,sizeof(int));
					p->write(nextEntry);
				}

				frame.page = p;

				buffer.push_back(frame);

				break;
			}

			i += numCols - 1;
		}
	}
}

void BufferPool::insert_new_page(const RID& rid, const int& column, int value) {
	// Make a new page
	// Fit into Frame using information in the RID class passed.
};

void BufferPool::evict (){
    // Called by load with which area to have a file evicted.
    // Evict a page using LRU that has no pin. Write back to disk if it is dirty.
	/* Saving from Frame to file */
	Frame decoy;
	// Use fastformat library instead of to_string
	fp = fopen (decoy.table_name + std::to_string(decoy.first_rid_page_range) + "_" + std::to_string(decoy.first_rid_page) + "_" + std::to_string(decoy.column) + ".dat", "wr");
	fwrite(decoy.page->data, sizeof(int), PAGE_SIZE*sizeof(int), fp);
	// Write in num_rows also into page
	fclose(fp);
	decoy.valid = false;

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

Frame::Frame () {}

Frame::~Frame () {}
