#ifndef INDEXH
#define INDEXH
#include <

class Index {
private:
    /* data */
    Table table;
    std::vector</* Index data type */> indices(table.num_columns);

public:
    Index (Table t) : table(t) {};
    virtual ~Index ();
    // Return pointer to array of RIDs
    std::vector<RID> locate(std::string column, int value);
    std::vector<RID> locate_range(int begin, int end, std::string column);
    void create_index(int column_number);
    void drop_index(int column_number);
};

#endif
