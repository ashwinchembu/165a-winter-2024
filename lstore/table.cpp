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

COMPILER_SYMBOL int getRecordBufferElement(int i){
	return recordBuffer[i];
}

COMPILER_SYMBOL void fillRecordBuffer(int* obj){
	std::vector<Record>* records = (std::vector<Record>*)obj;

	if(records->size()==0){
		delete records;

		return;
	}

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



COMPILER_SYMBOL int* Record_constructor(int rid_in, int key_in, int* columns_in){
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

COMPILER_SYMBOL int* Table_constructor(char* name_in,int num_columns_in, int key_in){
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

Table::Table(std::string name, int num_columns, int key): name(name), key(key), num_columns(num_columns) {
    index = new Index();
    index->setTable(this);
};

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
    if (page_range.size() == 0 || !(page_range.back()->base_has_capacity())) {

    	std::shared_ptr<PageRange>newPageRange{new PageRange(rid_id, columns)};

        page_range.push_back(newPageRange); // Make a base page with given record
        // return the RID for index or something

        record = (page_range.back().get())->page_range[0].first;
    } else { // If there are base page already, just insert it normally.
        record = (page_range.back().get())->insert(rid_id, columns);
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
RID Table::update(RID rid, const std::vector<int>& columns) {
    num_update++;
    int rid_id = num_update * -1;
    size_t i = 0;
    for (; i < page_range.size(); i++) {

        if ((page_range[i].get())->page_range[0].first.id > rid.id) {
            break;
        }
    }
    i--;
		RID* new_rid = new RID((page_range[i].get())->update(rid, rid_id, columns));
	//	auto iter = page_directory.find(rid.id);
		//*(iter->second.pointers[0]) = rid_id;
		page_directory.insert({rid_id, *new_rid});
    return *new_rid;
}

/***
 *
 * Merge few version of records and create new base page.
 *
 * Possible param : number of versions to merge
 *
 */
int Table::merge() {
    return -1;
}
