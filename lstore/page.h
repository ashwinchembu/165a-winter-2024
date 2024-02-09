#ifndef PAGEH
#define PAGEH
/// This is for clang
#include <vector>
#include <iostream>
#include <cstdlib>
#include "RID.h"
#include "table.h"

class Page {
private:
    /* data */
    /// @TODO Move this to config file
    constexpr static int PAGE_SIZE = 4096; // bytes
    constexpr static int NUM_SLOTS = PAGE_SIZE/sizeof(int); // bytes
    int num_rows = 0;
    int* data = nullptr; // Data location(pointer)
    int availability[NUM_SLOTS] = {0}; // 0 is empty, 1 is occupied, 2 is deleted.

public:
    Page ();
    virtual ~Page ();
    bool has_capacity();
    int* write(int value);
    friend std::ostream& operator<<(std::ostream& os, const Page& p);

};

class PageRange {
private:
    /* data */
    /// @TODO Move this to config file
    const int PAGE_SIZE = 4096;
    const int NUM_SLOTS = 16384;
    int num_slot_left = 16384; // Do we need this?
    int base_last = 0;
    int num_column = 0;
public:
    PageRange (int new_rid, std::vector<int> columns);
    virtual ~PageRange ();
    std::vector<std::pair<RID, Page*>> page_range;
    RID insert(int new_rid, std::vector<int> columns);
    RID update(RID rid, int rid_new, const std::vector<int> columns);
    bool base_has_capacity ();
    bool base_has_capacity_for (int size);
};

#endif
