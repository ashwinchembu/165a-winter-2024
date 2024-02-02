#ifndef DBH
#define DBH

class Database{
  public:
    Database(){};
    ~Database(){};
    //void open(path); for next Milestone
    //void close(); for next Milestone
    Table create_table(std::string name, int num_columns, int key_index);
    void drop_table(std::string name);
    Table get_table(std::string name);
  private:
    std::map<std::string, Table> tables;
};

#endif
