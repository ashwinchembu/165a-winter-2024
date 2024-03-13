#include "table.h"

#include <vector>
#include <map>
#include <string>
#include <cstring>
#include <cstdio>
#include <mutex>
#include <algorithm>
#include <memory>
#include <sys/stat.h>
#include <set>
#include <map>

#include "../DllConfig.h"
#include "bufferpool.h"
#include "config.h"
#include "index.h"
#include "page.h"
#include "RID.h"
#include "../Toolkit.h"

Table::Table(const std::string& name, const int& num_columns, const int& key): name(name), key(key), num_columns(num_columns) {
	index = new Index();
	index->setTable(this);

	this->num_columns = num_columns;
	this->key = key;

	if(buffer_pool.tableVersions.find(name)!=buffer_pool.tableVersions.end()){
		buffer_pool.tableVersions.erase(name);
	}

	buffer_pool.tableVersions.insert({this->name,0});
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
	std::lock_guard<std::mutex>merge_lock(*mutex_insert);

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

int Table::write(FILE* fp) {
	fwrite(&tableVersion,sizeof(int),1,fp);

	fwrite(&TPS,sizeof(int),1,fp);

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
	fread(&tableVersion,sizeof(int),1,fp);

	fread(&TPS,sizeof(int),1,fp);

	size_t e = fread(&key, sizeof(int), 1, fp);
	e = e + fread(&num_columns, sizeof(int), 1, fp);
	e = e + fread(&num_update, sizeof(int), 1, fp);
	e = e + fread(&num_insert, sizeof(int), 1, fp);
	char nameBuffer[128];
	e = e + fread(nameBuffer,128,1,fp);
	name = std::string(nameBuffer);

	if(buffer_pool.tableVersions.find(name)!=buffer_pool.tableVersions.end()){
		buffer_pool.tableVersions.erase(name);
	}

	buffer_pool.tableVersions.insert({this->name,tableVersion});

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

RID Table::update(RID& rid, const std::vector<int>& columns){
	num_update++;

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

	page_directory.insert({rid_id, new_rid});

	if(num_update % MAX_TABLE_UPDATES == 0){
		merge();
	}

	return new_rid;
}

int Table::merge(){
	std::cout<<"merge";
	std::lock_guard<std::mutex>merge_lock(*mutex_insert);

	std::vector<RID>basePages;

	for(std::shared_ptr<PageRange>& pr: page_range){
		for(RID& page: ((pr.get())->pages)){
			if(page.id > 0){
				basePages.push_back(page);
			}
		}
	}

	BufferPool* mergeBufferPool = new BufferPool(basePages.size() * 16);
	mergeBufferPool->tableVersions.insert({name,tableVersion+1});

	mergeBufferPool->set_path(buffer_pool.path);
	mergeBufferPool->textPath=buffer_pool.textPath;

	struct stat checkDir;

	if(stat(mergeBufferPool->path.c_str(),&checkDir) != 0 || !S_ISDIR(checkDir.st_mode)){
		mkdir(mergeBufferPool->path.c_str(),0777);
	}

	std::vector<int>visitedRids;

	for(RID& baseRid : basePages){
		if(std::find(visitedRids.begin(),visitedRids.end(),baseRid.id)!=visitedRids.end()){
			continue;
		}

		for(int field = NUM_METADATA_COLUMNS; field < NUM_METADATA_COLUMNS + num_columns;field++){


			Frame* pageOfField = buffer_pool.get_page(baseRid, field);

			buffer_pool.pin(baseRid,field);

			Page* copyOfFieldPage = new Page();

			copyOfFieldPage->deep_copy(pageOfField->page);

			buffer_pool.unpin(baseRid,field);

			Frame* frame = mergeBufferPool->insert_into_frame(baseRid,field,copyOfFieldPage);
			mergeBufferPool->pin(baseRid, field);

			frame->dirty = true;

			mergeBufferPool->unpin(baseRid, field);

			mergeBufferPool->update_ages(frame, mergeBufferPool->hash_vector[mergeBufferPool->hash_fun(baseRid.first_rid_page)]);
		}

		visitedRids.push_back(baseRid.id);
	}

	int firstRid = TPS - 1;

	int lastRid = abs(TPS) + MAX_COMMITTED_TAILS >= num_update?
			num_update * -1 : TPS - MAX_COMMITTED_TAILS;



//	buffer_pool.write_back_all();
	mergeBufferPool->write_back_all();


	for(int _tailRid = firstRid; _tailRid >=lastRid; _tailRid--){
		RID tailRid = this->page_directory.find(_tailRid)->second;
		RID baseRid = this->page_directory.find(buffer_pool.get(tailRid, BASE_RID_COLUMN))->second;


//		RID latest = this->page_directory.find(buffer_pool.get(baseRid, BASE_RID_COLUMN))->second;

		for(int c = 0; c < this->num_columns;c++){
			int val = buffer_pool.get(tailRid,NUM_METADATA_COLUMNS + c);

			mergeBufferPool->set(baseRid, NUM_METADATA_COLUMNS + c, val, false);

		}
	}



	buffer_pool.write_back_all();
	mergeBufferPool->write_back_all();
	delete mergeBufferPool;

	buffer_pool.tableVersions.erase(name);
	buffer_pool.tableVersions.insert({name,tableVersion + 1});
//
	TPS = lastRid;
	tableVersion++;

	return 0;
}

void Table::PrintLineage(){
	for(auto& rid :page_directory){

		if(rid.first > 0){
			std::cout<<"-----"<<rid.first<<std::endl;

			int nextId = buffer_pool.get(rid.second,INDIRECTION_COLUMN);

			while(nextId < 0){
				RID nextRid = page_directory.find(nextId)->second;

				std::cout<<"    "<<nextRid.id<<std::endl;

				nextId = buffer_pool.get(nextRid,INDIRECTION_COLUMN);

				if(nextId > 0){
					nextRid = page_directory.find(nextId)->second;
					std::cout<<"    "<<nextRid.id<<std::endl;
				}
			}
		}
	}
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



COMPILER_SYMBOL void Table_print_lineage(int* obj){
	((Table*)obj)->PrintLineage();
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



///////////////////////////////////////////////////////////////////////////////////////////////////////////
