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

PageRange::PageRange (RID& new_rid, const std::vector<int>& columns) {
    new_rid.offset = 0;
    num_column = columns.size();

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
    pages.push_back(new_rid);
    num_column = num_column + NUM_METADATA_COLUMNS;
    base_last = 0;
    num_slot_used_base++;
}

// PageRange::PageRange(const PageRange& other) {
//     this->page_range = other.page_range;
// }
//
// PageRange::~PageRange () {
//     for (size_t i = 0; i < page_range.size(); i++) {
//         delete page_range[i].second;
//     }
// }

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
    // std::cout << "PageRange destructor" << std::endl;
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
    // Get first rid of the page and offset
    // Find if the last base page has capacity for new record
    if (base_last_wasfull) {
        new_rid.offset = 0;
        new_rid.first_rid_page = new_rid.id;
        base_last_wasfull = false;
        tail_last++;
        base_last++; // Assuming that they will call after check if there are space left or not.
        buffer_pool.insert_new_page(new_rid, INDIRECTION_COLUMN, new_rid.id);
        buffer_pool.insert_new_page(new_rid, RID_COLUMN, new_rid.id);
        buffer_pool.insert_new_page(new_rid, TIMESTAMP_COLUMN, 0);
        buffer_pool.insert_new_page(new_rid, SCHEMA_ENCODING_COLUMN, 0);
        buffer_pool.insert_new_page(new_rid, BASE_RID_COLUMN, new_rid.id);
        buffer_pool.insert_new_page(new_rid, TPS, 0);
        for (int i = NUM_METADATA_COLUMNS; i < num_column; i++) {
            buffer_pool.insert_new_page(new_rid, i, columns[i - NUM_METADATA_COLUMNS]);
        }
        pages.insert(pages.begin() + base_last, new_rid);
        num_slot_used_base = 1;
    } else {
        new_rid.offset = num_slot_used_base;
        new_rid.first_rid_page = pages[base_last].id;
        buffer_pool.set(new_rid, INDIRECTION_COLUMN, new_rid.id);
        buffer_pool.set(new_rid, RID_COLUMN, new_rid.id);
        buffer_pool.set(new_rid, TIMESTAMP_COLUMN, 0);
        buffer_pool.set(new_rid, SCHEMA_ENCODING_COLUMN, 0);
        buffer_pool.set(new_rid, BASE_RID_COLUMN, new_rid.id);
        buffer_pool.set(new_rid, TPS, 0);
        for (int i = NUM_METADATA_COLUMNS; i < num_column; i++) {
            buffer_pool.set(new_rid, i, columns[i - NUM_METADATA_COLUMNS]);
        }
        num_slot_used_base++;
    }
    base_last_wasfull = (num_slot_used_base == PAGE_SIZE);
    // Inserted.
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
int PageRange::update(RID& rid, RID& rid_new, const std::vector<int>& columns, const std::map<int, RID>& page_directory) {
    // Get the latest update of the record. Accessing the indirection column.

    buffer_pool.pin(rid, INDIRECTION_COLUMN);
    RID latest_rid = page_directory.find(buffer_pool.get(rid, INDIRECTION_COLUMN))->second;
    buffer_pool.set(rid, INDIRECTION_COLUMN, rid_new.id);
    buffer_pool.unpin(rid, INDIRECTION_COLUMN);
    // Create new tail pages if there are no space left or tail page does not exist.
    int schema_encoding = 0;
    // If tail_last and base_last is equal, that means there are no tail page created.

    if (tail_last_wasfull) {
        rid_new.offset = 0;
        rid_new.first_rid_page = rid_new.id;
        base_last_wasfull = false;
        tail_last++;
        buffer_pool.insert_new_page(rid_new, INDIRECTION_COLUMN, rid.id);
        buffer_pool.insert_new_page(rid_new, RID_COLUMN, rid_new.id);
        buffer_pool.insert_new_page(rid_new, TIMESTAMP_COLUMN, 0);
        buffer_pool.insert_new_page(rid_new, BASE_RID_COLUMN, rid.id);
        buffer_pool.insert_new_page(rid_new, TPS, 0);
        for (int i = NUM_METADATA_COLUMNS; i < num_column; i++) {
            if (std::isnan(columns[i - NUM_METADATA_COLUMNS]) || columns[i-NUM_METADATA_COLUMNS] < -2147480000) { // Wrapper changes None to smallest integer possible
                // If there are no update, we write the value from latest update
                buffer_pool.insert_new_page(rid_new, i, buffer_pool.get(latest_rid, i));
            } else {
                // If there are update, we write the new value and update the schema encoding.
                buffer_pool.insert_new_page(rid_new, i, columns[i - NUM_METADATA_COLUMNS]);
                schema_encoding = schema_encoding | (0b1 << (num_column - (i - NUM_METADATA_COLUMNS) - 1));
            }
        }
        buffer_pool.insert_new_page(rid_new, SCHEMA_ENCODING_COLUMN, schema_encoding);
        num_slot_used_tail = 1;
        pages.push_back(rid_new);
    } else {
        rid_new.first_rid_page = pages.back().first_rid_page;
        rid_new.offset = num_slot_used_tail;
        buffer_pool.set(rid_new, INDIRECTION_COLUMN, rid.id);
        buffer_pool.set(rid_new, RID_COLUMN, rid_new.id);
        buffer_pool.set(rid_new, TIMESTAMP_COLUMN, 0);
        buffer_pool.set(rid_new, BASE_RID_COLUMN, rid.id);
        buffer_pool.set(rid_new, TPS, 0);
        for (int i = NUM_METADATA_COLUMNS; i < num_column; i++) {
            buffer_pool.set(rid_new, i, columns[i - NUM_METADATA_COLUMNS]);

            if (std::isnan(columns[i - NUM_METADATA_COLUMNS]) || columns[i-NUM_METADATA_COLUMNS] < -2147480000) { // Wrapper changes None to smallest integer possible
                // If there are no update, we write the value from latest update
                buffer_pool.set(rid_new, i, buffer_pool.get(latest_rid, i));
            } else {
                // If there are update, we write the new value and update the schema encoding.
                buffer_pool.set(rid_new, i, columns[i - NUM_METADATA_COLUMNS]);
                schema_encoding = schema_encoding | (0b1 << (num_column - (i - NUM_METADATA_COLUMNS) - 1));
            }
        }
        buffer_pool.set(rid_new, SCHEMA_ENCODING_COLUMN, schema_encoding);
        num_slot_used_tail++;
    }

    // Updating indirection column and schema encoding column for the base page
    buffer_pool.pin(rid, SCHEMA_ENCODING_COLUMN);
    buffer_pool.set(rid, SCHEMA_ENCODING_COLUMN, buffer_pool.get(rid, SCHEMA_ENCODING_COLUMN) | schema_encoding);
    buffer_pool.unpin(rid, SCHEMA_ENCODING_COLUMN);
    tail_last_wasfull = (num_slot_used_tail == PAGE_SIZE);
    // Setting the new RID to be representation of the page if the page was newly created
    return 0;
}



int PageRange::write(FILE* fp) {
    fwrite(&num_slot_left, sizeof(int), 1, fp);
    fwrite(&num_slot_used_base, sizeof(int), 1, fp);
    fwrite(&num_slot_used_tail, sizeof(int), 1, fp);
    fwrite(&base_last, sizeof(int), 1, fp);
    fwrite(&tail_last, sizeof(int), 1, fp);
    // Will break. Look for alternative of string.
    // fwrite(pages.data(), sizeof(pages[0]), tail_last, fp);
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
    // Will break. Look for alternative of string.
    // fread(pages.data(), sizeof(pages[0]), tail_last, fp);
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
    // for (int i = 0; i < PAGE_SIZE; i++) {
    //     availability[i] = 0;
    // }
}

Page::~Page() {
    // std::cout << "Page destructor In" << std::endl;
    delete data;
    // std::cout << "Page destructor Out" << std::endl;
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
 *
 */
int Page::write(const int& value) {
    int* insert = nullptr;
    // num_rows++;
    // for (int location = 0; location < PAGE_SIZE; location++) {
    //     if (availability[location] == 0) {
    //         //insert on location
    //         int offset = location * sizeof(int); // Bytes from top of the page
    //         insert = data + offset;
    //         *insert = value;
    //         availability[location] = 1;
    //         break;
    //     }
    // }
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
