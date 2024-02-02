#ifndef RIDH
#define RIDH

// RID will hold page range, page number, slot number for each column in the record

class RID {
public:
    std::vector<int> page_range;
    std::vector<int> page;
    std::vector<int> slot;
    int id;
    RID (int pr, int p, int s, int i) : page_range(pr), page(p), slot(s), id(i) {};
};

#endif
