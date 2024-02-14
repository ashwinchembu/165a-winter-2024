#ifndef PAGEH
#define PAGEH
/// This is for clang
#include <vector>
#include <iostream>
#include <cstdlib>
#include "RID.h"
#include "table.h"

class Page {
public:
    /// @TODO Move this to config file
    constexpr static int PAGE_SIZE = 2048 * 1/2; // bytes
    constexpr static int NUM_SLOTS = PAGE_SIZE/sizeof(int); // bytes
    int num_rows = 0;
    int availability[NUM_SLOTS] = {0}; // 0 is empty, 1 is occupied, 2 is deleted.
    Page ();
    virtual ~Page ();
    bool has_capacity();
    int* write(int value);
    int* data = nullptr; // Data location(pointer)
    friend std::ostream& operator<<(std::ostream& os, const Page& p);
};

class PageRange {
public:
    /* data */
    /// @TODO Move this to config file
    //1024, 2048, *2, *4, *8, *16
    const int PAGE_SIZE = 2048 * 1/2;
    const int NUM_SLOTS = 8192;
    int num_slot_left = NUM_SLOTS;
    int base_last = 0;
    int tail_last = 0;
    int num_column = 0;

    PageRange (int new_rid, std::vector<int> columns);
    ~PageRange();
    std::vector<std::pair<RID, Page*> > page_range;
    RID insert(int new_rid, std::vector<int> columns);
    RID update(RID rid, int rid_new, const std::vector<int>& columns);
    bool base_has_capacity ();
    bool base_has_capacity_for (int size);
};

#endif
