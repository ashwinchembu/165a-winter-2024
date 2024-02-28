#include <vector>
#include <map>
#include <string>
#include "RID.h"
#include "index.h"
#include "page.h"
#include <cstring>
#include "table.h"
#include <cstdio>
#include <memory>
#include "bufferpool.h"
#include "config.h"
#include <set>
#include <map>


#include "../DllConfig.h"

#include "../Toolkit.h"

std::vector<int>recordBuffer;
int sizeOfRecords;
int recordBufferIndex;

COMPILER_SYMBOL void clearRecordBuffer(){
	recordBuffer.clear();

	sizeOfRecords = 0;
	recordBufferIndex = 0;
}

COMPILER_SYMBOL int getRecordSize(){
	return sizeOfRecords;
}

COMPILER_SYMBOL int numberOfRecordsInBuffer(){
	if(recordBuffer.size() == 0){
		return 0;
	}

	return recordBuffer.size() / sizeOfRecords;
}

COMPILER_SYMBOL int getRecordBufferElement(const int i){
	return recordBuffer[i];
}

COMPILER_SYMBOL void fillRecordBuffer(int* obj){
	std::vector<Record>* records = (std::vector<Record>*)obj;

	sizeOfRecords = (*records)[0].columns.size() + 2;

	recordBuffer = std::vector<int>(sizeOfRecords * records->size());

	for(size_t i = 0; i < records->size(); i++){

		for(int j = 0; j < sizeOfRecords;j++){

			recordBuffer[i*sizeOfRecords + j] =

					j == 0 ? (*records)[i].rid :
					j == 1 ? (*records)[i].key :
					(*records)[i].columns[j - 2];
		}
	}



	delete records;
}



COMPILER_SYMBOL int* Record_constructor(const int rid_in, const int key_in, int* columns_in){
	std::vector<int>* cols = (std::vector<int>*)columns_in;
	return (int*)(new Record(rid_in,key_in,*cols));
}

COMPILER_SYMBOL void Record_destructor(int*obj){
	delete ((Record*)(obj));
}

COMPILER_SYMBOL int Record_rid(int*obj){
	return ((Record*)(obj))->rid;
}

COMPILER_SYMBOL int Record_key(int*obj){
	return ((Record*)(obj))->key;
}

COMPILER_SYMBOL int* Record_columns(int* obj){
	Record* ref = (Record*)obj;

	return (int*)(&(ref->columns));
}

COMPILER_SYMBOL void Table_destructor(int* obj){
	delete ((Table*)obj);
}

COMPILER_SYMBOL char* Table_name(int* obj){
	char* buf = new char[256];
	Table* ref = (Table*)obj;

	strcpy(buf,ref->name.c_str());

	return buf;
}

COMPILER_SYMBOL int Table_key(int* obj){
	return ((Table*)obj)->key;
}

COMPILER_SYMBOL int* Table_page_directory(int* obj){
	Table* ref = (Table*)obj;

	return(int*)(&( ref->page_directory));
}

COMPILER_SYMBOL int* Table_page_range(int* obj){
	Table* ref = (Table*)obj;
	return(int*)(&( ref->page_range));
}

COMPILER_SYMBOL int* Table_index(int* obj){
	Table* ref = (Table*)obj;
	return(int*)(&( ref->index));
}

COMPILER_SYMBOL int Table_num_update(int* obj){
	return ((Table*)obj)->num_update;
}

COMPILER_SYMBOL int Table_num_insert(int* obj){
	return ((Table*)obj)->num_insert;
}

COMPILER_SYMBOL int* Table_constructor(char* name_in, const int num_columns_in, const int key_in){
	return (int*)new Table({name_in},num_columns_in,key_in);
}

COMPILER_SYMBOL int* Table_insert(int* obj,int* columns){
	std::vector<int>* cols = (std::vector<int>*)columns;

	Table* ref = (Table*)obj;

	return (int*)new RID((ref->insert(*cols)));

}

COMPILER_SYMBOL int* Table_update(int* obj,int* rid, int* columns){
	std::vector<int>* cols = (std::vector<int>*)columns;

	Table* ref = (Table*)obj;

	RID* r = (RID*) rid;

	return (int*)new RID(ref->update(*r,*cols));
}

COMPILER_SYMBOL int Table_merge(int* obj){
	return ((Table*)obj)->merge();
}

COMPILER_SYMBOL int Table_num_columns(int* obj){
	return ((Table*)obj)->num_columns;
}

Table::Table(const std::string& name, const int& num_columns, const int& key): name(name), key(key), num_columns(num_columns) {
    index = new Index();
    index->setTable(this);
};

Table::~Table() {
	for (size_t i = 0; i <page_range.size(); i++) {
		if (page_range[i].unique()) {
			page_range[i].reset();
		}
	}
}

/***
 *
 * Insert a record into appropriate base page.
 *
 * @param Record record A record to insert
 * @return const std::vector<int>& columns the values of the record
 *
 */
RID Table::insert(const std::vector<int>& columns) {
    num_insert++;
    int rid_id = num_insert;
    RID record;
	record.table_name = name;
	record.id = rid_id;

    if (page_range.size() == 0 || !(page_range.back().get()->base_has_capacity())) {

    	std::shared_ptr<PageRange>newPageRange{new PageRange(record, columns)};
        page_range.push_back(newPageRange); // Make a base page with given record
    } else { // If there are base page already, just insert it normally.
		record.first_rid_page_range = (page_range.back().get())->pages[0].first_rid_page_range;
        (page_range.back().get())->insert(record, columns);
    }

    page_directory.insert({rid_id, record});
    return record;
}

/***
 *
 * Given a RID to the original base page, column number, and new value, it will update by creating new entry on tail page.
 *
 * @param RID rid Rid that pointing to the base page.
 * @param std::vector<int>& columns the new values of the record
 * @return RID of the new row upon successful update
 *
 */
RID Table::update(RID& rid, const std::vector<int>& columns) {
    num_update++;
	if (num_update >= MAX_TABLE_UPDATES){
		merge();
	}
	const int rid_id = num_update * -1;
	size_t i = 0;
	for (; i < page_range.size(); i++) {
		if ((page_range[i].get())->pages[0].first_rid_page_range == rid.first_rid_page_range) {
			break;
		}
	}
	RID new_rid(rid_id);
	new_rid.table_name = name;
	new_rid.first_rid_page_range = rid.first_rid_page_range;

	(page_range[i].get())->update(rid, new_rid, columns, page_directory);
	page_range_update[i]++;
	if (page_range_update[i] >= MAX_PAGE_RANGE_UPDATES){
		// Make a deep copy of page_range[i]
		std::shared_ptr<PageRange> deep_copy = std::make_shared<PageRange>(*(page_range[i].get()));

		// use bufferpool to get all the pages within a page range
//		auto pool_size = deep_copy->pages.size()*num_columns*2; // change to actual - temp
		//BufferPool* mergeBufferPool = new BufferPool(pool_size);
		//std::cout << "size: " << deep_copy->pages.size() << std::endl;
		std::vector<Frame*> insert_to_queue;
		for (int i = deep_copy->pages.size() - 1; i > 0; i--) {
			RID rid = deep_copy->pages[i];
			// load all of the pages in pagerange into bufferpool
			for (int to_load_tail_page_col = 0; to_load_tail_page_col < num_columns + NUM_METADATA_COLUMNS; to_load_tail_page_col++){
				Frame* new_frame = buffer_pool.get_page(rid, to_load_tail_page_col);
				insert_to_queue.push_back(new_frame);
			}
		}
		merge_queue.push(insert_to_queue);
	}

	page_directory.insert({rid_id, new_rid});
    return new_rid;
}

int Table::write(FILE* fp) {
    fwrite(&key, sizeof(int), 1, fp);
		fwrite(&num_columns, sizeof(int), 1, fp);
    fwrite(&num_update, sizeof(int), 1, fp);
    fwrite(&num_insert, sizeof(int), 1, fp);
    char nameBuffer[128];
    strcpy(nameBuffer,name.c_str());
    fwrite(nameBuffer,128,1,fp);
    for(std::map<int, RID>::iterator iter=page_directory.begin(); iter!=page_directory.end(); iter++){
        fwrite(&(iter->first), sizeof(int), 1, fp);
        iter->second.write(fp);
    }

	int num_page_range = page_range.size();
	fwrite(&(num_page_range), sizeof(int), 1, fp);
	for (int i = 0; i < num_page_range; i++) {
		page_range[i].get()->write(fp);
	}

	return 0;
}


int Table::read(FILE* fp) {
    size_t e = fread(&key, sizeof(int), 1, fp);
		e = e + fread(&num_columns, sizeof(int), 1, fp);
    e = e + fread(&num_update, sizeof(int), 1, fp);
    e = e + fread(&num_insert, sizeof(int), 1, fp);
    char nameBuffer[128];
    e = e + fread(nameBuffer,128,1,fp);
    name = std::string(nameBuffer);
		int num_element = num_insert + num_update;
		RID value;
		int key;
		for(int i = 0; i < num_element; i++){
			e = e + fread(&key, sizeof(int), 1, fp);
			value.read(fp);
			value.table_name = name;
			page_directory.insert({key, value});

		}
	page_range.clear();
	int num_page_range = 0;
	e = e + fread(&(num_page_range), sizeof(int), 1, fp);
	for (int i = 0; i < num_page_range; i++) {
		std::shared_ptr<PageRange>newPageRange{new PageRange()};
		newPageRange.get()->read(fp);
		page_range.push_back(newPageRange);
	}

	delete index;
	index = new Index();
	index->setTable(this);
	return e;
}

/***
 *
 * Merge few version of records and create new base page.
 *
 * Possible param : number of versions to merge
 *
 */
int Table::merge() {
	if (!merge_queue.size()){
		//std::cout << "size is 0" << std::endl;
		return 0;
	}
	//std::cout << "after if statement" << std::endl;
	/*
	updating at page range level

	load the a copy of all base pages of the selected range into memory
	iterate over tail page and get most up to date for record for every record -> consolidated base page
		read it until TPS < tail ID
	page directory is updated to point to the new pages

	*/
	std::cout << "entered merge" << std::endl;
	std::vector<Frame*> to_merge = merge_queue.front();

	merge_queue.pop();
	auto pool_size = to_merge.size()*PAGE_SIZE*2*sizeof(int); // change to actual - temp
	BufferPool* mergeBufferPool = new BufferPool(pool_size);
	mergeBufferPool->set_path("./ECS165/Merge");

// 	Frame* cur_frame = mergeBufferPool->head; //create number of frames according to bufferpool size
//   	for(int i = 0; i < (to_merge.size() - 1); i++){
// 		*(cur_frame) = *(to_merge[i]);
// 		cur_frame = cur_frame->next;
//   }

	*(mergeBufferPool->head) = *(to_merge[0]);
//	std::cout << "size of to merge: " << to_merge.size() << std::endl;
	for (int i = 0; i < to_merge.size(); i++) {
		//for(int j = 0; j < num_columns; )
		RID new_rid(i,
			to_merge[i]->first_rid_page_range,
			to_merge[i]->first_rid_page,
			0,
			name
		);
		Frame* frame = mergeBufferPool->insert_into_frame(new_rid, to_merge[i]->column, to_merge[i]->page);
	//	std::cout << "Merge size: " << to_merge.size() << " RID: " << new_rid.id << " " << to_merge[i]->first_rid_page << " frame: " << frame->first_rid_page << std::endl;

		std::cout << "put into frame" << frame->first_rid_page << std::endl;
	//	std::cout << "value in page" << *(to_merge[i]->page->data) << std::endl;

		frame->dirty = true;
	}
	//set last frame
	//*(mergeBufferPool->tail) = *(to_merge[to_merge.size() - 1]);


	//set last frame
//	*(mergeBufferPool->tail) = *(to_merge[to_merge.size() - 1]);
	Frame* current_frame = mergeBufferPool->head;
	while(current_frame != nullptr){ //iterate through entire bufferpool
	if(current_frame->page != nullptr){
		std::cout << "frame in linked list is" << current_frame << std::endl;
		std::cout << "this should be equal to ^ " << mergeBufferPool->hash_vector[0] << std::endl;
		std::cout << "the value in here is " << current_frame->first_rid_page << std::endl;
		std::cout << "this should be equal to ^ " << to_merge[0]->first_rid_page << std::endl;
	}
	current_frame = current_frame->next;
}

	std::map<int, std::pair<int, std::vector<int>>> latest_update; //<latest base RID: <tailRID, values>>
	std::set<int> visited_rids;

//	int tail_rid_last = 0;
	//load copy of all base pages in each page range
	for (int i = to_merge.size() - 1; i >= 0; i--) {
		Frame* currentFrame = to_merge[i];
		RID page_rid = page_directory.find(currentFrame->first_rid_page)->second;

		//determine that we dont visit same logical set twice
		auto pos = visited_rids.find(page_rid.id);

		if (pos != visited_rids.end()){
			continue;
		}
		else{
			visited_rids.insert(page_rid.id);
		}
		//determine frame holds tail page
		if (page_rid.id < 0){
			//holds tail page
			// if (page_rid > last_update_rid) {
			// 	continue;
			// }

			if (currentFrame->page){
				//valid page
				currentFrame = mergeBufferPool->search(page_rid, RID_COLUMN);
				Page currentPage = *(currentFrame->page);
				for (int tail_iterator = (currentPage.num_rows-1)*sizeof(int); tail_iterator >= 0; tail_iterator -= sizeof(int) ){
					RID currentRID(*(tail_iterator + currentPage.data),
						to_merge[i]->first_rid_page_range, to_merge[i]->first_rid_page, tail_iterator, name);

					int baseRID = mergeBufferPool->get(currentRID, BASE_RID_COLUMN);
					if (latest_update.find(baseRID) == latest_update.end()){
						if (latest_update[baseRID].first > currentRID.id){
							latest_update[baseRID].first = currentRID.id;
							std::vector<int> merge_vals;
							for (int j = 0; j < num_columns; j++) { //indirection place stuff
								int value = mergeBufferPool->get(currentRID, j);
								merge_vals.push_back(value);
							}
							latest_update[baseRID].second = merge_vals;
							//std::cout << latest_update.size() << std::endl;
						}
					}
					// if (currentRID < tail_rid_last) {
					// 	tail_rid_last = currentRID;
					// }
				}
			}
		}
	}
	//std::cout << "kdljflkadklfdsjfkjds " << latest_update.size() << std::endl;
	for (const auto& pair : latest_update) {
		if (pair.first == 0) {
			continue;
		}
		std::cout << "kdljflkadklfdsjfkjds " << pair.first << std::endl;
		RID latest_base_rid = page_directory.find(pair.first)->second;
		std::cout << "kdljflkadklfdsjfkjds" << std::endl;
		const std::vector<int>& values = pair.second.second;
		std::cout << "kdljflkadklfdsjfkjds" << std::endl;

		for (int col = 0; col < num_columns; col++){
			//mergeBufferPool->set (latest_base_rid, col, values[col], false);
			std::cout << "latest base_rid: " << latest_base_rid.id << " col :" << col << std::endl;
			mergeBufferPool->get(latest_base_rid, col);
			std::cout << "get :)" << std::endl;
			mergeBufferPool->set (latest_base_rid, col, values[col], false);
			//mergeBufferPool->set(latest_base_rid, col, 0, false);
		}
		// mergeBufferPool->set (latest_base_rid, TPS, tail_rid_last, false);
	}

	// mergeBufferPool->write_back_all();
	// delete mergeBufferPool;

    return -1;
}

void Table::PrintData() {
	std::cout << "--Page Directory--" << std::endl;
		for(auto& e: page_directory){
			std::cout << "Key: " << e.first << ", Value.id: " << e.second.id << std::endl;
		}
}
