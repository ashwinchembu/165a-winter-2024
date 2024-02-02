#ifndef INDEXH
#define INDEXH

class Index {
private:
    /* data */
    Table table;
    std::vector<std::map<int, std::vector<RID>>> indices(table.num_columns);
    //each index is a map, each element in the map corresponds to a key and a vector of rids
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
