#ifndef PAGEH
#define PAGEH
/// This is for clang
#include <vector>
#include <iostream>
#include <cstdlib>
#include "RID.h"
#include "table.h"
#include "config.h"

class Page {
public:
    /// @TODO Move this to config file
    const int NUM_SLOTS = PAGE_SIZE/sizeof(int); // bytes
    int num_rows = 0;
    // int availability[NUM_SLOTS] = {0}; // 0 is empty, 1 is occupied, 2 is deleted.
    Page ();
    virtual ~Page ();
    const bool has_capacity() const;
    int write(const int& value);
    int* data = nullptr; // Data location(pointer)
    friend std::ostream& operator<<(std::ostream& os, const Page& p);
};

class PageRange {
public:
    /* data */
    /// @TODO Move this to config file
    // const int PAGE_SIZE = 4096;
    // const int LOGICAL_PAGE = 8;
    const int NUM_SLOTS = 4096*LOGICAL_PAGE;
    int num_slot_left = NUM_SLOTS;
    int num_slot_used_base = 0;
    int num_slot_used_tail = 0;
    int base_last = 0;
    bool base_last_wasfull = false;
    int tail_last = 0;
    bool tail_last_wasfull = false;
    int num_column = 0;

    PageRange (RID& new_rid, const std::vector<int>& columns);
    // PageRange(const PageRange& other);
    ~PageRange();
    std::vector<RID> pages;
    int insert(RID& new_rid, const std::vector<int>& columns);
    int update(RID& rid, RID& rid_new, const std::vector<int>& columns);
    bool base_has_capacity () const;
};

#endif
