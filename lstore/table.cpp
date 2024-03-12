#include <mutex>
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
#include <sys/stat.h>
#include "bufferpool.h"
#include "config.h"
#include <set>
#include <map>
#include "lock_manager_entry.h"


#include "../DllConfig.h"
#include "../Toolkit.h"

Table::Table(const std::string& name, const int& num_columns, const int& key): name(name), key(key), num_columns(num_columns) {
	index = new Index();
	index->setTable(this);
	page_directory_unique = std::unique_lock<std::shared_mutex>(page_directory_lock, std::defer_lock);
	page_directory_shared = std::shared_lock<std::shared_mutex>(page_directory_lock, std::defer_lock);
	page_range_unique = std::unique_lock<std::shared_mutex>(page_range_lock, std::defer_lock);
	page_range_shared = std::shared_lock<std::shared_mutex>(page_range_lock, std::defer_lock);
};

Table::~Table() {
	for (size_t i = 0; i <page_range.size(); i++) {
		page_range[i].reset();
	}
}

Table::Table (const Table& rhs) {
	name = rhs.name;
	key = rhs.key;
	merge_queue = rhs.merge_queue;
	page_range_update = rhs.page_range_update;
	index = rhs.index;
	num_columns = rhs.num_columns;
	int num_update_now = rhs.num_update;
	num_update = num_update_now;
	int num_insert_now = rhs.num_insert;
	num_insert = num_insert_now;
	page_directory_unique = std::unique_lock<std::shared_mutex>(page_directory_lock, std::defer_lock);
	page_directory_shared = std::shared_lock<std::shared_mutex>(page_directory_lock, std::defer_lock);
	page_range_unique = std::unique_lock<std::shared_mutex>(page_range_lock, std::defer_lock);
	page_range_shared = std::shared_lock<std::shared_mutex>(page_range_lock, std::defer_lock);
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
	// insert_lock2.lock();
	// num_insert++; // Should not need mutex here. num_insert is std::atomic and rid solely depend on that.
	// int rid_id = num_insert;
	// buffer_pool.lock_manager.find(name)->second.insert({rid_id, new LockManagerEntry});
	// insert_lock2.unlock();
	RID record(0);
	// record.table_name = name;
	// record.id = rid_id;
	// // std::lock(insert_lock, page_directory_shared);
	// // std::lock_guard insert_lk(insert_lock);
	// insert_lock.lock();
	// {
	// 	page_range_shared.lock();
	// 	if (page_range.size() == 0 || !(page_range.back().get()->base_has_capacity())) {
	// 		page_range_shared.unlock();
	// 		std::shared_ptr<PageRange>newPageRange{new PageRange(record, columns)};
	// 		page_range_unique.lock();
	// 		page_range.push_back(newPageRange); // Make a base page with given record
	// 		page_range_unique.unlock();
	// 		insert_lock.unlock();
	// 	} else { // If there are base page already, just insert it normally.
 //
	// 		record.first_rid_page_range = (page_range.back().get())->pages[0].first_rid_page_range;
	// 		PageRange* prange = (page_range.back().get());
	// 		page_range_shared.unlock();
	// 		insert_lock.unlock();
 //
	// 		if (prange->insert(record, columns)) {
	// 			return RID(0);
	// 		}
	// 	}
	// }
 //
 //
	// page_directory_unique.lock();
	// page_directory.insert({rid_id, record});
	// page_directory_unique.unlock();
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
	update_lock.lock();
	num_update++;
	if (num_update >= MAX_TABLE_UPDATES){
		merge();
	}
	const int rid_id = num_update * -1;
	buffer_pool.lock_manager.find(name)->second.insert({rid_id, new LockManagerEntry});
	update_lock.unlock();
	size_t i = 0;

	page_range_shared.lock();
	for (; i < page_range.size(); i++) {
		if ((page_range[i].get())->pages[0].first_rid_page_range == rid.first_rid_page_range) {
			break;
		}
	}
	page_range_shared.unlock();
	RID new_rid(rid_id);
	new_rid.table_name = name;
	new_rid.first_rid_page_range = rid.first_rid_page_range;

	page_range_shared.lock();
	PageRange* prange = (page_range[i].get());
	page_range_shared.unlock();
	if (prange->update(rid, new_rid, columns, page_directory, &page_range_shared)) {
		return RID(0);
	}
	page_range_update[i]++;
	if (page_range_update[i] >= MAX_PAGE_RANGE_UPDATES){
		// Make a deep copy of page_range[i]
		std::shared_ptr<PageRange> deep_copy = std::make_shared<PageRange>(*(page_range[i].get()));
		std::vector<Frame*> insert_to_queue;
		for (int i = deep_copy->pages.size() - 1; i >= 0; i--) {
			RID rid = deep_copy->pages[i];
			// load all of the pages in pagerange into bufferpool
			for (int to_load_tail_page_col = 0; to_load_tail_page_col < num_columns + NUM_METADATA_COLUMNS; to_load_tail_page_col++){
				Frame* new_frame = buffer_pool.get_page(rid, to_load_tail_page_col);
				insert_to_queue.push_back(new_frame);
			}
		}
		merge_queue.push(insert_to_queue);
	}
	page_directory_unique.lock();
	page_directory.insert({rid_id, new_rid});
	page_directory_unique.unlock();
	return new_rid;
}

int Table::write(FILE* fp) {
	fwrite(&key, sizeof(int), 1, fp);
	fwrite(&num_columns, sizeof(int), 1, fp);
	int curr_val = num_update;
	fwrite(&curr_val, sizeof(int), 1, fp);
	curr_val = num_insert;
	fwrite(&curr_val, sizeof(int), 1, fp);
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
		return 0;
	}
	//std::cout << "after if statement" << std::endl;
	/*
	 *	updating at page range level
	 *
	 *	load the a copy of all base pages of the selected range into memory
	 *	iterate over tail page and get most up to date for record for every record -> consolidated base page
	 *		read it until TPS < tail ID
	 *	page directory is updated to point to the new pages
	 *
	 */

	std::cout << "entered merge" << std::endl;
	std::vector<Frame*> to_merge = merge_queue.front();

	merge_queue.pop();
	auto pool_size = to_merge.size() * 5; // change to actual - temp
	BufferPool* mergeBufferPool = new BufferPool(pool_size);
	mergeBufferPool->set_path("./ECS165/Merge");
	struct stat checkDir;
	if(stat(mergeBufferPool->path.c_str(),&checkDir)!=0 || !S_ISDIR(checkDir.st_mode)){
		mkdir(mergeBufferPool->path.c_str(),0777);
	}

	for (size_t i = 0; i < to_merge.size(); i++) {
		RID new_rid(i, to_merge[i]->first_rid_page_range, to_merge[i]->first_rid_page, 0,	name);
		Frame* frame = mergeBufferPool->insert_into_frame(new_rid, to_merge[i]->column, to_merge[i]->page);
		frame->dirty = true;
	}

	int TPS = 0;
	Frame* first_frame = to_merge[0];
	RID last_tail_rid(0, first_frame->first_rid_page_range, first_frame->first_rid_page, 0 ,name);
	int latest_tail_id = mergeBufferPool->get(last_tail_rid, TPS);

	std::map<int, std::pair<int, std::vector<int>>> latest_update; //<latest base RID: <tailRID, values>>
	std::set<int> visited_rids;

	// int tail_rid_last = 0;
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

					if (currentRID.id > latest_tail_id) {
						continue;
					}

					if (currentRID.id < TPS) {
						TPS = currentRID.id;
					}

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
		RID latest_base_rid = page_directory.find(pair.first)->second;
		const std::vector<int>& values = pair.second.second;

		int tail_id = latest_update.at(pair.first).first;
		mergeBufferPool->set (latest_base_rid, INDIRECTION_COLUMN, tail_id, false);

		for (int col = 0; col < num_columns; col++){
			//mergeBufferPool->set (latest_base_rid, col, values[col], false);
			mergeBufferPool->get(latest_base_rid, col);
			mergeBufferPool->set (latest_base_rid, col, values[col], false);
			//mergeBufferPool->set(latest_base_rid, col, 0, false);
		}
		// mergeBufferPool->set (latest_base_rid, TPS, tail_rid_last, false);
	}

	// 	std::cout << ":)" << std::endl;
	mergeBufferPool->write_back_all();
	delete mergeBufferPool;

	return -1;
}

/*
 * checks if a rid is referenced by another rid over a column.
 */
bool Table::ridIsJoined(RID rid, int col){
	if(referencesOut.find(col)!=referencesOut.end()){
		return false;
	}

	std::vector<RIDJoin> joins = referencesOut.find(col)->second;

	for(RIDJoin& j : joins){
		if(j.ridSrc.id == rid.id){
			return true;
		}
	}

	return false;
}

/*
 * Returns the relationship between the argument rid
 * and another rid over a column.
 */
RIDJoin Table::getJoin(RID rid, int col){
	std::vector<RIDJoin> joins = referencesOut.find(col)->second;

	for(RIDJoin& j : joins){
		if(j.ridSrc.id == rid.id){
			return j;
		}
	}
	return RIDJoin();
}

void Table::PrintData() {
	std::cout << "--Page Directory--" << std::endl;
	for(auto& e: page_directory){
		std::cout << "Key: " << e.first << ", Value.id: " << e.second.id << std::endl;
	}
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
	return(int*)(ref->index);
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
