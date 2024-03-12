#include <thread>
#include <vector>
#include <iostream>
#include <cstdlib>
#include <cmath>
// #include <parallel/algorithm>
#include "bufferpool.h"
#include "config.h"
#include "page.h"
#include "table.h"
#include "../DllConfig.h"

PageRange::PageRange (RID& new_rid, const std::vector<int>& columns) {
    num_column = columns.size();
    new_rid.offset = 0;

    new_rid.first_rid_page_range = new_rid.id;
    new_rid.first_rid_page = new_rid.id;
    // Update using
    buffer_pool.insert_new_page(new_rid, INDIRECTION_COLUMN, new_rid.id);
    buffer_pool.insert_new_page(new_rid, RID_COLUMN, new_rid.id);
    buffer_pool.insert_new_page(new_rid, TIMESTAMP_COLUMN, 0);
    buffer_pool.insert_new_page(new_rid, SCHEMA_ENCODING_COLUMN, 0);
    buffer_pool.insert_new_page(new_rid, BASE_RID_COLUMN, new_rid.id);
    buffer_pool.insert_new_page(new_rid, TPS, 0);
    for (int i = 0; i < num_column; i++) {
        buffer_pool.insert_new_page(new_rid, NUM_METADATA_COLUMNS + i, columns[i]);
    }
    page_lock.lock();
    pages.push_back(new_rid);
    page_lock.unlock();
    num_column = num_column + NUM_METADATA_COLUMNS;
}

PageRange::PageRange (const PageRange& rhs) {
    pages = rhs.pages;

    int current_num = rhs.num_slot_left;
    num_slot_left = current_num;

    current_num = rhs.num_slot_used_base;
    num_slot_used_base = current_num;

    current_num = rhs.num_slot_used_tail;
    num_slot_used_tail = current_num;

    current_num = rhs.base_last;
    base_last = current_num;

    current_num = rhs.tail_last;
    tail_last = current_num;

    current_num = rhs.num_column;
    tail_last = current_num;

    bool current_bool = rhs.base_last_wasfull;
    base_last_wasfull = current_bool;

    current_bool = rhs.tail_last_wasfull;
    tail_last_wasfull = current_bool;
}


/***
 *
 * Return if there are more space to insert record or not
 *
 * @return True if there are space for one more record left, False if not
 *
 */
bool PageRange::base_has_capacity () const {
    return (base_last < LOGICAL_PAGE) || (base_last <= LOGICAL_PAGE && num_slot_used_base < PAGE_SIZE);
    // Lazy evaluation
}

PageRange::~PageRange(){
}

/***
 *
 * Insert a record at the end of base pages
 *
 * @param int new_rid RID of the new record
 * @param vector<int> columns Vector of values to insert
 * @return return RID of new record upon successful insertion.
 *
 */
int PageRange::insert(RID& new_rid, const std::vector<int>& columns) {
    // Lock to protect variable in Page range.
    mutex_insert.lock();
    if (base_last_wasfull) {
        // Update status of the page range
        base_last_wasfull = false;
        num_slot_used_base = 1;
        tail_last++;
        base_last++;
        mutex_insert.unlock();

        // Update information in the rid class
        new_rid.offset = 0;
        new_rid.first_rid_page = new_rid.id;
        // Lock the rid of record that we are inserting
        buffer_pool.unique_lock_manager_lock.lock();
        if (!(buffer_pool.lock_manager.find(new_rid.table_name)->second.find(new_rid.id)->second->unique_lock->try_lock())) {
            return 1;
        }
        buffer_pool.unique_lock_manager_lock.unlock();
        // Write in the metadatas
        buffer_pool.insert_new_page(new_rid, INDIRECTION_COLUMN, new_rid.id);
        buffer_pool.insert_new_page(new_rid, RID_COLUMN, new_rid.id);
        buffer_pool.insert_new_page(new_rid, TIMESTAMP_COLUMN, 0);
        buffer_pool.insert_new_page(new_rid, SCHEMA_ENCODING_COLUMN, 0);
        buffer_pool.insert_new_page(new_rid, BASE_RID_COLUMN, new_rid.id);
        buffer_pool.insert_new_page(new_rid, TPS, 0);
        // Write in the data
        for (int i = NUM_METADATA_COLUMNS; i < num_column; i++) {
            buffer_pool.insert_new_page(new_rid, i, columns[i - NUM_METADATA_COLUMNS]);
        }
        // Unlock the rid of the record once we are done inserting
        buffer_pool.unique_lock_manager_lock.lock();
        buffer_pool.lock_manager.find(new_rid.table_name)->second.find(new_rid.id)->second->unique_lock->unlock();
        buffer_pool.unique_lock_manager_lock.unlock();
        // Protecting pages vector from multiple thread writing simultaneously
        page_lock.lock();
        // Insert the first rid of the logical page into appropriate place
        pages.insert(pages.begin() + base_last, new_rid);
        page_lock.unlock();
    } else {
        // Update status of the page range
        base_last_wasfull = (num_slot_used_base == PAGE_SIZE);
        num_slot_used_base++;
        mutex_insert.unlock();
        // Update information in the rid class
        new_rid.offset = num_slot_used_base - 1;
        new_rid.first_rid_page = pages[base_last].id;

        // Lock the rid of record that we are inserting
        buffer_pool.unique_lock_manager_lock.lock();
        if (!(buffer_pool.lock_manager.find(new_rid.table_name)->second.find(new_rid.id)->second->unique_lock->try_lock())) {
            return 1;
        }
        buffer_pool.unique_lock_manager_lock.unlock();
        // Write in the metadatas
        buffer_pool.set(new_rid, INDIRECTION_COLUMN, new_rid.id, true);
        buffer_pool.set(new_rid, RID_COLUMN, new_rid.id, true);
        buffer_pool.set(new_rid, TIMESTAMP_COLUMN, 0, true);
        buffer_pool.set(new_rid, SCHEMA_ENCODING_COLUMN, 0, true);
        buffer_pool.set(new_rid, BASE_RID_COLUMN, new_rid.id, true);
        buffer_pool.set(new_rid, TPS, 0, true);
        // Write in the data
        for (int i = NUM_METADATA_COLUMNS; i < num_column; i++) {
            buffer_pool.set(new_rid, i, columns[i - NUM_METADATA_COLUMNS], true);
        }
        // Unlock the rid of the record once we are done inserting
        buffer_pool.unique_lock_manager_lock.lock();
        buffer_pool.lock_manager.find(new_rid.table_name)->second.find(new_rid.id)->second->unique_lock->unlock();
        buffer_pool.unique_lock_manager_lock.unlock();
    }
    return 0;
}

/***
 *
 * Update a record
 *
 * @param RID rid RID of the base record that we are updating.
 * @param int rid_new RID of the new record
 * @param vector<int> columns Vector of values to insert
 * @return return RID of updated record upon successful insertion.
 *
 */
int PageRange::update(RID& rid, RID& rid_new, const std::vector<int>& columns, const std::map<int, RID>& page_directory, std::shared_lock<std::shared_mutex>* lock) {
    // Protecting page directory from thread writing while another thread is reading
    lock->lock();
    RID latest_rid = page_directory.find(buffer_pool.get(rid, INDIRECTION_COLUMN))->second;
    lock->unlock();

    // Declared here so that we can use after the if statement
    int schema_encoding = 0;

    // Lock to protect the variable in page range
    mutex_update.lock();

    // Create new tail pages if there are no space left or tail page does not exist.
    // Otherwise just insert the update in the last tail page
    if (tail_last_wasfull) {
        // Update the variable of the page range
        tail_last_wasfull = false;
        tail_last++;
        num_slot_used_tail = 1;
        mutex_update.unlock();

        // Update the information in the new rid class
        rid_new.offset = 0;
        rid_new.first_rid_page = rid_new.id;

        // Lock the rid of record that we are inserting
        buffer_pool.unique_lock_manager_lock.lock();
        if (!(buffer_pool.lock_manager.find(rid_new.table_name)->second.find(rid_new.id)->second->unique_lock->try_lock())) {
            return 1;
        }
        buffer_pool.unique_lock_manager_lock.unlock();

        // Write in the metadata except for schema encoding column
        buffer_pool.insert_new_page(rid_new, INDIRECTION_COLUMN, rid.id);
        buffer_pool.insert_new_page(rid_new, RID_COLUMN, rid_new.id);
        buffer_pool.insert_new_page(rid_new, TIMESTAMP_COLUMN, 0);
        buffer_pool.insert_new_page(rid_new, BASE_RID_COLUMN, rid.id);
        buffer_pool.insert_new_page(rid_new, TPS, 0);

        // Write in the data
        for (int i = NUM_METADATA_COLUMNS; i < num_column; i++) {
            if (std::isnan(columns[i - NUM_METADATA_COLUMNS]) || columns[i-NUM_METADATA_COLUMNS] < NONE) { // Wrapper changes None to smallest integer possible
                // If there are no update, we write the value from latest update
                buffer_pool.insert_new_page(rid_new, i, buffer_pool.get(latest_rid, i));
            } else {
                // If there are update, we write the new value and update the schema encoding.
                buffer_pool.insert_new_page(rid_new, i, columns[i - NUM_METADATA_COLUMNS]);
                schema_encoding = schema_encoding | (0b1 << (num_column - (i - NUM_METADATA_COLUMNS) - 1));
            }
        }

        // Write in the schema encoding once we know which one is updated.
        buffer_pool.insert_new_page(rid_new, SCHEMA_ENCODING_COLUMN, schema_encoding);

        // Unlock the lock for the new record. If we reach here then we were able to lock it before.
        buffer_pool.unique_lock_manager_lock.lock();
        buffer_pool.lock_manager.find(rid_new.table_name)->second.find(rid_new.id)->second->unique_lock->unlock();
        buffer_pool.unique_lock_manager_lock.unlock();
        // Setting the new RID to be representation of the page if the page was newly created
        page_lock.lock();
        pages.push_back(rid_new);
        page_lock.unlock();
    } else {
        // Update the variable of the page range
        num_slot_used_tail++;
        tail_last_wasfull = (num_slot_used_tail == PAGE_SIZE);
        mutex_update.unlock();

        // Update the information in the new rid class
        rid_new.first_rid_page = pages.back().first_rid_page;
        rid_new.offset = num_slot_used_tail - 1;

        // Lock the rid of record that we are inserting
        buffer_pool.unique_lock_manager_lock.lock();
        if (!(buffer_pool.lock_manager.find(rid_new.table_name)->second.find(rid_new.id)->second->unique_lock->try_lock())) {
            return 1;
        }
        buffer_pool.unique_lock_manager_lock.unlock();

        // Write in the metadata except for schema encoding column
        buffer_pool.set(rid_new, INDIRECTION_COLUMN, rid.id, true);
        buffer_pool.set(rid_new, RID_COLUMN, rid_new.id, true);
        buffer_pool.set(rid_new, TIMESTAMP_COLUMN, 0, true);
        buffer_pool.set(rid_new, BASE_RID_COLUMN, rid.id, true);
        buffer_pool.set(rid_new, TPS, 0, true);

        // Write in the data
        for (int i = NUM_METADATA_COLUMNS; i < num_column; i++) {
            if (std::isnan(columns[i - NUM_METADATA_COLUMNS]) || columns[i-NUM_METADATA_COLUMNS] < NONE) { // Wrapper changes None to smallest integer possible
                // If there are no update, we write the value from latest update
                buffer_pool.set(rid_new, i, buffer_pool.get(latest_rid, i), true);
            } else {
                // If there are update, we write the new value and update the schema encoding.
                buffer_pool.set(rid_new, i, columns[i - NUM_METADATA_COLUMNS], true);
                schema_encoding = schema_encoding | (0b1 << (num_column - (i - NUM_METADATA_COLUMNS) - 1));
            }
        }

        // Write in the schema encoding once we know which one is updated.
        buffer_pool.set(rid_new, SCHEMA_ENCODING_COLUMN, schema_encoding, true);

        // Unlock the lock for the new record. If we reach here then we were able to lock it before.
        buffer_pool.unique_lock_manager_lock.lock();
        buffer_pool.lock_manager.find(rid_new.table_name)->second.find(rid_new.id)->second->unique_lock->unlock();
        buffer_pool.unique_lock_manager_lock.unlock();
    }

    // Updating indirection column and schema encoding column for the base page
    buffer_pool.pin(latest_rid, SCHEMA_ENCODING_COLUMN, 'X');
    buffer_pool.set(latest_rid, INDIRECTION_COLUMN, rid_new.id, false);
    buffer_pool.unpin(latest_rid, SCHEMA_ENCODING_COLUMN, 'X');
    int base_schema = buffer_pool.get(rid, SCHEMA_ENCODING_COLUMN);
    buffer_pool.pin(rid, SCHEMA_ENCODING_COLUMN, 'X');
    buffer_pool.set(rid, SCHEMA_ENCODING_COLUMN, base_schema | schema_encoding, false);
    buffer_pool.unpin(rid, SCHEMA_ENCODING_COLUMN, 'X');
    return 0;
}



int PageRange::write(FILE* fp) {
    fwrite(&num_slot_left, sizeof(int), 1, fp);
    fwrite(&num_slot_used_base, sizeof(int), 1, fp);
    fwrite(&num_slot_used_tail, sizeof(int), 1, fp);
    fwrite(&base_last, sizeof(int), 1, fp);
    fwrite(&tail_last, sizeof(int), 1, fp);
    for (int i = 0; i <tail_last; i++) {
        pages[i].write(fp);
    }
    base_last_wasfull = (num_slot_used_base == PAGE_SIZE);
    tail_last_wasfull = (num_slot_used_tail == PAGE_SIZE);
    fwrite(&num_column, sizeof(int), 1, fp);
    return 0;
}


int PageRange::read(FILE* fp) {
    size_t e = fread(&num_slot_left, sizeof(int), 1, fp);
    e = e + fread(&num_slot_used_base, sizeof(int), 1, fp);
    e = e + fread(&num_slot_used_tail, sizeof(int), 1, fp);
    e = e + fread(&base_last, sizeof(int), 1, fp);
    e = e + fread(&tail_last, sizeof(int), 1, fp);
    pages.resize(tail_last);
    for (int i = 0; i <tail_last; i++) {
        pages[i].read(fp);
    }
    base_last_wasfull = (num_slot_used_base == PAGE_SIZE);
    tail_last_wasfull = (num_slot_used_tail == PAGE_SIZE);
    e = e + fread(&num_column, sizeof(int), 1, fp);
    return e;
}

Page::Page() {
    data = new int[PAGE_SIZE * 4];
}

Page::Page (const Page& rhs) {
    int current_num_rows = rhs.num_rows;
    num_rows = current_num_rows;
    data = rhs.data;
}

void Page::DeepCopy (const Page& rhs) {
    int current_num_rows = rhs.num_rows;
    num_rows = current_num_rows;
    for (int i = 0; i < current_num_rows; i++) {
        data[i] = rhs.data[i];
    }
}


Page::~Page() {
    // std::cout << "Destructor Page Called by" << std::this_thread::get_id() << std::endl;
    delete[] data;
}

/***
 *
 * Return if the page has capacity left or not.
 *
 * @return True if page has capacity left, False if not
 *
 */
const bool Page::has_capacity() const {
    return(num_rows < PAGE_SIZE);
}

/***
 *
 * Write value into page
 *
 * @param int value Value to write into
 * @warning This function is not thread safe
 * @DEPRECATED
 *
 */
int Page::write(const int& value) {
    int* insert = nullptr;
    insert = data + num_rows;
    *insert = value;
    num_rows++;
    return 0;
}

/***
 *
 * Print out content of Page as one long string into cout.
 *
 * @param ostream os Standard io
 * @param Page& p Page to print out
 * @return Standard io
 *
 */
std::ostream& operator<<(std::ostream& os, const Page& p)
{
    for (int i = 0; i < PAGE_SIZE; i++) {
        os << *(p.data + i) << " ";
    }
    return os;
}

//COMPILER_SYMBOL int Page_PAGE_SIZE(int* obj){
//	return  ((Page*)obj)->PAGE_SIZE;
//}
//
//COMPILER_SYMBOL int Page_NUM_SLOTS(int* obj){
//	return  ((Page*)obj)->NUM_SLOTS;
//}
//
//COMPILER_SYMBOL int Page_num_rows(int* obj){
//	return  ((Page*)obj)->num_rows;
//}
//
//// COMPILER_SYMBOL int* Page_availability(int* obj){
//// 	return  ((Page*)obj)->availability;
//// }
//
//COMPILER_SYMBOL int* Page_constructor(){
//	return (int*)(new Page());
//}
//
//COMPILER_SYMBOL void Page_destructor(int* obj){
//	Page* ref = ((Page*)obj);
//	delete ref;
//}
//
//COMPILER_SYMBOL bool Page_has_capacity(int* obj){
//	return  ((Page*)obj)->has_capacity();
//}
//
//COMPILER_SYMBOL int* Page_write(int* obj, int value){
//	return  ((Page*)obj)->write(value);
//}
//
//COMPILER_SYMBOL int* Page_data(int* obj){
//	return  ((Page*)obj)->data;
//}
//
//COMPILER_SYMBOL int PageRange_PAGE_SIZE(int* obj){
//	return ((PageRange*)obj)->PAGE_SIZE;
//}
//
//COMPILER_SYMBOL int PageRange_NUM_SLOTS(int* obj){
//	return ((PageRange*)obj)->NUM_SLOTS;
//}
//
//COMPILER_SYMBOL int PageRange_num_slot_left(int* obj){
//	return ((PageRange*)obj)->num_slot_left;
//}
//
//COMPILER_SYMBOL int PageRange_base_last(int* obj){
//	return ((PageRange*)obj)->base_last;
//}
//
//COMPILER_SYMBOL int PageRange_tail_last(int* obj){
//	return ((PageRange*)obj)->tail_last;
//}
//
//COMPILER_SYMBOL int PageRange_num_column(int* obj){
//	return ((PageRange*)obj)->num_column;
//}
//
//COMPILER_SYMBOL int* PageRange_constructor(const int new_rid, int* columns){
//	std::vector<int>* cols = (std::vector<int>*)columns;
//	return (int*)(new PageRange(new_rid,*cols));
//}
//
//COMPILER_SYMBOL void PageRange_destructor(int* obj){
//	delete ((PageRange*)obj);
//}
//
//COMPILER_SYMBOL int* PageRange_page_range(int* obj){
//	PageRange* ref = (PageRange*)obj;
//
//	return (int*)(&(ref->page_range));
//}
//
//COMPILER_SYMBOL int* PageRange_insert(int* obj, const int new_rid, int*columns){
//	PageRange* ref = (PageRange*)obj;
//	std::vector<int>* cols = (std::vector<int>*)columns;
//	return (int*)(new RID(ref->insert(new_rid,*cols)));
//}
//
//COMPILER_SYMBOL int* PageRange_update(int* obj, int* rid, const int rid_new, int*columns){
//	PageRange* ref = (PageRange*)obj;
//	std::vector<int>* cols = (std::vector<int>*)columns;
//	RID* r = (RID*)rid;
//
//	return (int*)(new RID(ref->update(*r,rid_new,*cols)));
//}
//
//COMPILER_SYMBOL bool PageRange_base_has_capacity(int* obj){
//	return ((PageRange*)obj)->base_has_capacity();
//}
