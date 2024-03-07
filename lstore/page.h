#ifndef PAGEH
#define PAGEH
/// This is for clang
#include <vector>
#include <iostream>
#include <cstdlib>
#include <mutex>
#include "RID.h"
#include "table.h"
#include "config.h"
#include "../Toolkit.h"

class Page {
public:
    int num_rows = 0;
    Page ();
    virtual ~Page ();
    const bool has_capacity() const;
    int write(const int& value);
    int* data = nullptr; // Data location(pointer)
    friend std::ostream& operator<<(std::ostream& os, const Page& p);

    void deep_copy(Page* other){
		delete[] data;

		data= new int[PAGE_SIZE * 4];

		num_rows = other->num_rows;

		memcpy(data,other->data,num_rows * sizeof(int));
	}
};

class PageRange {
public:
    Toolkit::BasicSharedPtr<std::mutex>mutex_insert = Toolkit::BasicSharedPtr<std::mutex>(new std::mutex());
    Toolkit::BasicSharedPtr<std::mutex>mutex_update = Toolkit::BasicSharedPtr<std::mutex>(new std::mutex());;

    const int NUM_SLOTS = 4096*LOGICAL_PAGE;
    int num_slot_left = NUM_SLOTS;
    int num_slot_used_base = 0;
    int num_slot_used_tail = 0;
    int base_last = 0;
    bool base_last_wasfull = false;
    int tail_last = 0;
    bool tail_last_wasfull = true;
    int num_column = 0;
    std::vector<RID> pages;
    PageRange () {} // Never use this outside of loading saved data
    PageRange (RID& new_rid, const std::vector<int>& columns);
    ~PageRange();
    int insert(RID& new_rid, const std::vector<int>& columns);
    int update(RID& rid, RID& rid_new, const std::vector<int>& columns, const std::map<int, RID>& page_directory);
    bool base_has_capacity () const;
    int write(FILE* fp);
    int read(FILE* fp);
};

#endif
