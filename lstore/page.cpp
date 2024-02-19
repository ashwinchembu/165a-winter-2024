#include <vector>
#include <iostream>
#include <cstdlib>
#include <cmath>
// #include <parallel/algorithm>
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

PageRange::PageRange (const int& new_rid, const std::vector<int>& columns) {
    num_column = columns.size();
    for (int i = 0; i < num_column + 4; i++) {
        page_range.push_back(std::make_pair(RID(), new Page()));
    }
    std::vector<int*> record_pointers(num_column + 4);
    record_pointers[0] = page_range[0].second->write(new_rid); // Indirection column
    record_pointers[1] = page_range[1].second->write(new_rid); // RID column
    record_pointers[2] = page_range[2].second->write(0); // Timestamp
    record_pointers[3] = page_range[3].second->write(0); // schema encoding
    for (int i = 0; i < num_column; i++) {
        record_pointers[4 + i] = (page_range[4 + i]).second->write(columns[i]);
    }
    RID rid(record_pointers, new_rid);
    num_column = num_column + 4;
    for (int i = 0; i < num_column; i++) {
        page_range[i].first = rid;
    }
    base_last = 0;
    num_slot_left--;
}

PageRange::~PageRange () {
    for (size_t i = 0; i < page_range.size(); i++) {
        delete page_range[i].second;
    }
}

/***
 *
 * Return if there are more space to insert record or not
 *
 * @return True if there are space for one more record left, False if not
 *
 */
bool PageRange::base_has_capacity () const {
    return num_slot_left > 0;
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
RID PageRange::insert(const int& new_rid, const std::vector<int>& columns) {
    bool newpage = false;
    // Find if the last base page has capacity for new record
    if (!(page_range[base_last*num_column].second->has_capacity())) {
        tail_last++;
        newpage = true;
        base_last++; // Assuming that they will call after check if there are space left or not.
        for (int i = 0; i < num_column; i++) {
            page_range.insert(page_range.begin() + base_last*num_column, std::make_pair(RID(), new Page()));
        }
    }

    // Start inserting new record
    // Metadata columns
    std::vector<int*> record_pointers(num_column);
    record_pointers[0] = page_range[base_last*num_column].second->write(new_rid); // Indirection column
    record_pointers[1] = page_range[base_last*num_column + 1].second->write(new_rid); // RID column
    record_pointers[2] = page_range[base_last*num_column + 2].second->write(0); // Timestamp
    record_pointers[3] = page_range[base_last*num_column + 3].second->write(0); // schema encoding

    // Inserting data columns
    for (int i = 4; i < num_column; i++) {
        record_pointers[i] = page_range[base_last*num_column+i].second->write(columns[i - 4]);
    }

    // Creation of the new RID
    RID rid(record_pointers, new_rid);

    // Setting the new RID to be representation of the page if the page was newly created
    if (newpage){
        for (int i = 0; i < num_column; i++) {
        page_range[base_last*num_column + i].first = rid;
        }
    }
    // Inserted.
    num_slot_left--;

    return rid;
    // Collect pointers, and make RID class, return it.
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
RID PageRange::update(const RID& rid, const int& rid_new, const std::vector<int>& columns) {
    // Look for page available
    // Because the base record is monotonically increasing, we can use for loop and find the base page we need
    int page_of_rid = 0;
    for (; page_of_rid <= base_last; page_of_rid++) {
        if (page_range[page_of_rid * num_column].first.id > rid.id) {
            break;
        }
    }
    page_of_rid--; // Logical page number of the base record

    // Find offset for the base record
    int offset = rid.id - page_range[page_of_rid * num_column].first.id;
    // Get the latest update of the record. Accessing the indirection column.
    int latest_rid = (*((page_range[page_of_rid * num_column].second)->data + offset*sizeof(int)));

    // Create new tail pages if there are no space left or tail page does not exist.
    bool new_tail = false;
    // If tail_last and base_last is equal, that means there are no tail page created.
    if (!(page_range[tail_last * num_column].second->has_capacity()) || tail_last == base_last) {
        tail_last++;
        for (int i = 0; i < num_column; i++) {
            page_range.push_back(std::make_pair(RID(), new Page()));
        }
        new_tail = true;
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
        while (latest_offset < NUM_SLOTS && latest_rid != (*((page_range[latest_page * num_column + 1].second)->data + latest_offset*sizeof(int)))) {
            latest_offset++;
        }
        // Multi thread version
        // int* end = ((page_range[latest_page * num_column + 1].second)->data + PAGE_SIZE*sizeof(int));
        // int* itr = 	__gnu_parallel::find(((page_range[latest_page * num_column + 1].second)->data), end, latest_rid);
        // latest_offset = PAGE_SIZE - ((end - itr) / sizeof(int));
    } else {
        // If there are no updates, offset is simple to calculate
        latest_offset = (latest_rid - page_range[latest_page * num_column].first.id);
    }

    int schema_encoding = 0;
    std::vector<int*> new_record(num_column);

    // Writing metadata to page
    new_record[0] = page_range[tail_last*num_column].second->write(*((page_range[latest_page * num_column].second)->data + latest_offset*sizeof(int))); // Indirection column
    new_record[1] = page_range[tail_last*num_column+1].second->write(rid_new); // RID column
    new_record[2] = page_range[tail_last*num_column+2].second->write(0); // Timestamp

    // Writing data to page and also update schema encoding if necessary.
    for (int i = 4; i < num_column; i++) {
        if (std::isnan(columns[i - 4]) || columns[i-4] < -2147480000) { // Wrapper changes None to smallest integer possible
            // If there are no update, we write the value from latest update
            new_record[i] = page_range[tail_last*num_column+i].second->write(*((page_range[latest_page * num_column + i].second)->data + latest_offset*sizeof(int)));
        } else {
            // If there are update, we write the new value and update the schema encoding.
            new_record[i] = page_range[tail_last*num_column+i].second->write(columns[i - 4]);
            schema_encoding = schema_encoding | (0b1 << (num_column - i - 1));
        }
    }

    // Write the schema encoding of where updated
    new_record[3] = page_range[tail_last*num_column+3].second->write(schema_encoding); // schema encoding

    // Updating indirection column and schema encoding column for the base page
    *((page_range[page_of_rid * num_column].second)->data + offset*sizeof(int)) = rid_new;
    *((page_range[page_of_rid * num_column + 3].second)->data + offset*sizeof(int)) = (*((page_range[page_of_rid * num_column + 3].second)->data + offset*sizeof(int)) | schema_encoding);
    RID new_rid(new_record, rid_new);

    // Setting the new RID to be representation of the page if the page was newly created
    if (new_tail) {
        for (int i = 0; i < num_column; i++) {
            page_range[tail_last*num_column + i].first = new_rid;
        }
    }
    return new_rid;
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
int* Page::write(const int& value) {
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
    return insert;
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
