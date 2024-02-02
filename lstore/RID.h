#ifndef RIDH
#define RIDH

// RID will hold page range, page number, slot number

class RID {
public:
    int page_range;
    int page;
    int slot;
    int id;
    RID (int pr, int p, int s, int i) : page_range(pr), page(p), slot(s), id(i) {};
};

#endif
