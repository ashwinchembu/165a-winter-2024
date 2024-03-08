#ifndef LOG_H
#define LOG_H
#include "config.h"
#include <vector>
#include <map>
#include "../Toolkit.h"
#include "transaction.h"

class LogEntry { //represents one transaction
public:
  LogEntry (const std::vector<QueryOperation>& queries) : queries(queries){};
  virtual ~LogEntry ();
  std::vector<QueryOperation> queries;
  //int num_attempts = 0; //after three tries, abort to avoid loops
};

class Log{
public:
  Log ();
  virtual ~Log ();
  std::map<int, LogEntry> entries;
  int num_transactions = 0;
};


extern Log log;

#endif
