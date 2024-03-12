#ifndef LOG_H
#define LOG_H
#include "config.h"
#include <vector>
#include <atomic>
#include <map>
#include "../Toolkit.h"
#include "transaction.h"


class LogEntry { //represents one transaction
public:
  LogEntry (const std::vector<QueryOperation>& queries) : queries(queries){}
  virtual ~LogEntry () {}
  std::vector<QueryOperation> queries;
  //int num_attempts = 0; //after three tries, abort to avoid loops
};

class Log{
public:
  Log () {
    lk = std::unique_lock<std::mutex>(db_log_lock, std::defer_lock);
  }
  virtual ~Log () {};
  std::map<int, LogEntry> entries;
  std::atomic_int num_transactions = 0;
  std::mutex db_log_lock;
  std::unique_lock<std::mutex> lk;
};


extern Log db_log;

#endif
