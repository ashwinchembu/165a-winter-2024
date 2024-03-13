#ifndef LOCKMANAGERENTRY_H
#define LOCKMANAGERENTRY_H
#include <mutex>
#include <shared_mutex>

class LockManagerEntry{
  public:
    LockManagerEntry();
    virtual ~LockManagerEntry();
    std::shared_mutex* mutex;
};

#endif
