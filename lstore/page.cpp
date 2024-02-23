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

COMPILER_SYMBOL int Page_PAGE_SIZE(int* obj){
	return  ((Page*)obj)->PAGE_SIZE;
}

COMPILER_SYMBOL int Page_NUM_SLOTS(int* obj){
	return  ((Page*)obj)->NUM_SLOTS;
}

COMPILER_SYMBOL int Page_num_rows(int* obj){
	return  ((Page*)obj)->num_rows;
}

// COMPILER_SYMBOL int* Page_availability(int* obj){
// 	return  ((Page*)obj)->availability;
// }

COMPILER_SYMBOL int* Page_constructor(){
	return (int*)(new Page());
}

COMPILER_SYMBOL void Page_destructor(int* obj){
	Page* ref = ((Page*)obj);
	delete ref;
}

COMPILER_SYMBOL bool Page_has_capacity(int* obj){
	return  ((Page*)obj)->has_capacity();
}

COMPILER_SYMBOL int* Page_write(int* obj, int value){
	return  ((Page*)obj)->write(value);
}

COMPILER_SYMBOL int* Page_data(int* obj){
	return  ((Page*)obj)->data;
}

COMPILER_SYMBOL int PageRange_PAGE_SIZE(int* obj){
	return ((PageRange*)obj)->PAGE_SIZE;
}

COMPILER_SYMBOL int PageRange_NUM_SLOTS(int* obj){
	return ((PageRange*)obj)->NUM_SLOTS;
}

COMPILER_SYMBOL int PageRange_num_slot_left(int* obj){
	return ((PageRange*)obj)->num_slot_left;
}

COMPILER_SYMBOL int PageRange_base_last(int* obj){
	return ((PageRange*)obj)->base_last;
}

COMPILER_SYMBOL int PageRange_tail_last(int* obj){
	return ((PageRange*)obj)->tail_last;
}

COMPILER_SYMBOL int PageRange_num_column(int* obj){
	return ((PageRange*)obj)->num_column;
}

COMPILER_SYMBOL int* PageRange_constructor(const int new_rid, int* columns){
	std::vector<int>* cols = (std::vector<int>*)columns;
	return (int*)(new PageRange(new_rid,*cols));
}

COMPILER_SYMBOL void PageRange_destructor(int* obj){
	delete ((PageRange*)obj);
}

COMPILER_SYMBOL int* PageRange_page_range(int* obj){
	PageRange* ref = (PageRange*)obj;

	return (int*)(&(ref->page_range));
}

COMPILER_SYMBOL int* PageRange_insert(int* obj, const int new_rid, int*columns){
	PageRange* ref = (PageRange*)obj;
	std::vector<int>* cols = (std::vector<int>*)columns;
	return (int*)(new RID(ref->insert(new_rid,*cols)));
}

COMPILER_SYMBOL int* PageRange_update(int* obj, int* rid, const int rid_new, int*columns){
	PageRange* ref = (PageRange*)obj;
	std::vector<int>* cols = (std::vector<int>*)columns;
	RID* r = (RID*)rid;

	return (int*)(new RID(ref->update(*r,rid_new,*cols)));
}

COMPILER_SYMBOL bool PageRange_base_has_capacity(int* obj){
	return ((PageRange*)obj)->base_has_capacity();
}

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
    // new_rid.schema_encoding = 0; // Comment out for future usage : cascading abort
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
    bool newpage = false;
    // Get first rid of the page and offset
    // Find if the last base page has capacity for new record
    if (base_last_wasfull) {
        new_rid.offset = 0;
        new_rid.first_rid_page = new_rid.id;
        base_last_wasfull = false;
        newpage = true;
        tail_last++;
        base_last++; // Assuming that they will call after check if there are space left or not.
        buffer_pool.insert_new_page(new_rid, INDIRECTION_COLUMN, new_rid.id);
        buffer_pool.insert_new_page(new_rid, RID_COLUMN, new_rid.id);
        buffer_pool.insert_new_page(new_rid, TIMESTAMP_COLUMN, 0);
        buffer_pool.insert_new_page(new_rid, SCHEMA_ENCODING_COLUMN, 0);
        // new_rid.schema_encoding = 0; // Comment out for future usage : cascading abort
        buffer_pool.insert_new_page(new_rid, BASE_RID_COLUMN, new_rid.id);
        buffer_pool.insert_new_page(new_rid, TPS, 0);
        for (int i = 0; i < num_column; i++) {
            buffer_pool.insert_new_page(new_rid, NUM_METADATA_COLUMNS + i, columns[i]);
        }
        pages.push_back(new_rid);
        num_slot_used_base = 1;
    } else {
        new_rid.offset = num_slot_used_base;
        new_rid.first_rid_page = pages[base_last].id;
        buffer_pool.set(new_rid, INDIRECTION_COLUMN, new_rid.id);
        buffer_pool.set(new_rid, RID_COLUMN, new_rid.id);
        buffer_pool.set(new_rid, TIMESTAMP_COLUMN, 0);
        buffer_pool.set(new_rid, SCHEMA_ENCODING_COLUMN, 0);
        // new_rid.schema_encoding = 0; // Comment out for future usage : cascading abort
        buffer_pool.set(new_rid, BASE_RID_COLUMN, new_rid.id);
        buffer_pool.set(new_rid, TPS, 0);
        for (int i = 0; i < num_column; i++) {
            buffer_pool.set(new_rid, NUM_METADATA_COLUMNS + i, columns[i]);
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
    // Look for page available
    // Because the base record is monotonically increasing, we can use for loop and find the base page we need
    int page_of_rid = 0;
    for (; page_of_rid <= base_last; page_of_rid++) {
        if (pages[page_of_rid].first_rid_page == rid.first_rid_page) {
            break;
        }
    }
    page_of_rid--; // Logical page number of the base record

    // Find offset for the base record
    int offset = rid.offset;
    // Get the latest update of the record. Accessing the indirection column.

    /// @TODO Bufferpool::load();
    /// @TODO Bufferpool::pin(page_range[page_of_rid * num_column].first, 0);
    //int latest_rid = buffer_pool.get(rid, INDIRECTION_COLUMN);
    RID latest_rid = page_directory.find(buffer_pool.get(rid, INDIRECTION_COLUMN))->second;

    // Create new tail pages if there are no space left or tail page does not exist.
    bool new_tail = false;
    int schema_encoding = 0;
    // If tail_last and base_last is equal, that means there are no tail page created.
    if (base_last_wasfull || tail_last == base_last) {
        base_last_wasfull = false;
        tail_last++;

        buffer_pool.insert_new_page(rid_new, INDIRECTION_COLUMN, rid_new.id);
        buffer_pool.insert_new_page(rid_new, RID_COLUMN, rid_new.id);
        buffer_pool.insert_new_page(rid_new, TIMESTAMP_COLUMN, 0);
        // rid_new.schema_encoding = 0; // Comment out for future usage : cascading abort
        buffer_pool.insert_new_page(rid_new, BASE_RID_COLUMN, rid_new.id);
        buffer_pool.insert_new_page(rid_new, TPS, 0);
        for (int i = 0; i < num_column; i++) {
            if (std::isnan(columns[i - NUM_METADATA_COLUMNS]) || columns[i-NUM_METADATA_COLUMNS] < -2147480000) { // Wrapper changes None to smallest integer possible
                // If there are no update, we write the value from latest update
                buffer_pool.insert_new_page(rid_new, NUM_METADATA_COLUMNS + i, buffer_pool.get(latest_rid, NUM_METADATA_COLUMNS + i));
            } else {
                // If there are update, we write the new value and update the schema encoding.
                buffer_pool.insert_new_page(rid_new, NUM_METADATA_COLUMNS + i, columns[i - NUM_METADATA_COLUMNS]);
                schema_encoding = schema_encoding | (0b1 << (num_column - i - 1));
            }
        }
        buffer_pool.insert_new_page(rid_new, SCHEMA_ENCODING_COLUMN, 0);
    } else {

        buffer_pool.set(rid_new, INDIRECTION_COLUMN, rid_new.id);
        buffer_pool.set(rid_new, RID_COLUMN, rid_new.id);
        buffer_pool.set(rid_new, TIMESTAMP_COLUMN, 0);
        // rid_new.schema_encoding = 0; // Comment out for future usage : cascading abort
        buffer_pool.set(rid_new, BASE_RID_COLUMN, rid_new.id);
        buffer_pool.set(rid_new, TPS, 0);
        for (int i = 0; i < num_column; i++) {
            buffer_pool.set(rid_new, NUM_METADATA_COLUMNS + i, columns[i]);

            if (std::isnan(columns[i - NUM_METADATA_COLUMNS]) || columns[i-NUM_METADATA_COLUMNS] < -2147480000) { // Wrapper changes None to smallest integer possible
                // If there are no update, we write the value from latest update
                buffer_pool.set(rid_new, NUM_METADATA_COLUMNS + i, buffer_pool.get(latest_rid, NUM_METADATA_COLUMNS + i));
            } else {
                // If there are update, we write the new value and update the schema encoding.
                buffer_pool.set(rid_new, NUM_METADATA_COLUMNS + i, columns[i - NUM_METADATA_COLUMNS]);
                schema_encoding = schema_encoding | (0b1 << (num_column - i - 1));
            }
        }
        buffer_pool.set(rid_new, SCHEMA_ENCODING_COLUMN, 0);
    }

    // Find the logical page number for the latest update.
    int latest_offset = 0;
    int latest_page = page_of_rid;
    if (latest_rid < 0) { // Are there any updates we should be aware of?
        latest_page = base_last + 1;
        for (; latest_page <= tail_last; latest_page++) { //Look for the logical page number of the latest update
            if (new_tail && latest_page == tail_last) {
                break; // If there was a new page, we should not go check there. (Full of garbage value)
            } else if (page_range[latest_page * num_column].first.id < latest_rid) {
                break; // Found the page number.
            }
        }
        latest_page--; // Logical page number of the latest update

        // Find the offset for the latest record. Linear search.

        /// @TODO Bufferpool::load();
        /// @TODO Bufferpool::pin(page_range[latest_page * num_column].first, 1);
        while (latest_offset < NUM_SLOTS && latest_rid != (*((page_range[latest_page * num_column + RID_COLUMN].second)->data + latest_offset*sizeof(int)))) {
            latest_offset++;
        }
        /// @TODO Bufferpool::unpin(page_range[latest_page * num_column].first, 1);
        // Multi thread version
        // int* end = ((page_range[latest_page * num_column + 1].second)->data + PAGE_SIZE*sizeof(int));
        // int* itr = 	__gnu_parallel::find(((page_range[latest_page * num_column + 1].second)->data), end, latest_rid);
        // latest_offset = PAGE_SIZE - ((end - itr) / sizeof(int));
    } else {
        // If there are no updates, offset is simple to calculate
        latest_offset = (latest_rid - page_range[latest_page * num_column].first.id);
    }

    rid_new.offset = page_range[tail_last*num_column+RID_COLUMN].second->num_rows;

    // Writing metadata to page
    /// @TODO Bufferpool::load();
    /// @TODO Bufferpool::pin(page_range[latest_page * num_column].first, 0);
    page_range[tail_last*num_column].second->write(*((page_range[latest_page * num_column + INDIRECTION_COLUMN].second)->data + latest_offset*sizeof(int))); // Indirection column
    /// @TODO Bufferpool::unpin(page_range[latest_page * num_column].first, 0);
    /// @TODO Bufferpool::unpin(rid_new, 0);

    page_range[tail_last*num_column+RID_COLUMN].second->write(rid_new.id); // RID column
    rid_new.first_rid_page = *(page_range[tail_last*num_column+RID_COLUMN].second->data);
    /// @TODO Bufferpool::unpin(rid_new, 1);

    page_range[tail_last*num_column+TIMESTAMP_COLUMN].second->write(0); // Timestamp
    /// @TODO Bufferpool::unpin(rid_new, 2);
    page_range[tail_last*num_column + BASE_RID_COLUMN].second->write(rid.id);
    page_range[tail_last*num_column + TPS].second->write(0);

    // Writing data to page and also update schema encoding if necessary.
    for (int i = NUM_METADATA_COLUMNS; i < num_column; i++) {
        if (std::isnan(columns[i - NUM_METADATA_COLUMNS]) || columns[i-NUM_METADATA_COLUMNS] < -2147480000) { // Wrapper changes None to smallest integer possible
            // If there are no update, we write the value from latest update
            /// @TODO Bufferpool::load();
            /// @TODO Bufferpool::pin(page_range[latest_page * num_column].first, i);
            page_range[tail_last*num_column+i].second->write(*((page_range[latest_page * num_column + i].second)->data + latest_offset*sizeof(int)));
            /// @TODO Bufferpool::unpin(page_range[latest_page * num_column].first, i);
        } else {
            // If there are update, we write the new value and update the schema encoding.
            page_range[tail_last*num_column+i].second->write(columns[i - NUM_METADATA_COLUMNS]);
            schema_encoding = schema_encoding | (0b1 << (num_column - i - 1));
        }
        /// @TODO Bufferpool::unpin(rid_new, i);
    }

    // Write the schema encoding of where updated
    page_range[tail_last*num_column+SCHEMA_ENCODING_COLUMN].second->write(schema_encoding); // schema encoding
    // rid_new.schema_encoding = schema_encoding; // Comment out for future usage : cascading abort
    /// @TODO Bufferpool::unpin(rid_new, 3);

    // Updating indirection column and schema encoding column for the base page
    *((page_range[page_of_rid * num_column].second)->data + offset*sizeof(int)) = rid_new.id;
    /// @TODO Bufferpool::unpin(page_range[page_of_rid * num_column].first, 0);

    /// @TODO Bufferpool::load();
    /// @TODO Bufferpool::pin(page_range[page_of_rid * num_column].first, 3);
    // rid.schema_encoding = (*((page_range[page_of_rid * num_column + SCHEMA_ENCODING_COLUMN].second)->data + offset*sizeof(int)) | schema_encoding); // Comment out for future usage : cascading abort
    // *((page_range[page_of_rid * num_column + SCHEMA_ENCODING_COLUMN].second)->data + offset*sizeof(int)) = rid.schema_encoding;

    *((page_range[page_of_rid * num_column + SCHEMA_ENCODING_COLUMN].second)->data + offset*sizeof(int)) = (*((page_range[page_of_rid * num_column + SCHEMA_ENCODING_COLUMN].second)->data + offset*sizeof(int)) | schema_encoding);
    /// @TODO Bufferpool::unpin(page_range[page_of_rid * num_column].first, 3);

    // Setting the new RID to be representation of the page if the page was newly created
    if (new_tail) {
        for (int i = 0; i < num_column; i++) {
            page_range[tail_last*num_column + i].first = rid_new;
        }
    }
    return 0;
}

Page::Page() {
    data = new int[PAGE_SIZE];
    // for (int i = 0; i < NUM_SLOTS; i++) {
    //     availability[i] = 0;
    // }
}

Page::~Page() {
    delete data;
}

/***
 *
 * Return if the page has capacity left or not.
 *
 * @return True if page has capacity left, False if not
 *
 */
const bool Page::has_capacity() const {
    return(num_rows < NUM_SLOTS);
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
    // for (int location = 0; location < NUM_SLOTS; location++) {
    //     if (availability[location] == 0) {
    //         //insert on location
    //         int offset = location * sizeof(int); // Bytes from top of the page
    //         insert = data + offset;
    //         *insert = value;
    //         availability[location] = 1;
    //         break;
    //     }
    // }
    insert = data + num_rows *sizeof(int);
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
    for (int i = 0; i < p.NUM_SLOTS; i++) {
        os << *(p.data + i*sizeof(int)) << " ";
    }
    return os;
}
