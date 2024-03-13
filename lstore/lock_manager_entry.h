#ifndef LOCKMANAGERENTRY_H
#define LOCKMANAGERENTRY_H
#include <mutex>
#include <shared_mutex>

class LockManagerEntry{
  public:
    LockManagerEntry();
    virtual ~LockManagerEntry();
    std::shared_mutex* mutex;
    std::shared_lock<std::shared_mutex>* shared_lock;
    std::unique_lock<std::shared_mutex>* unique_lock;


};

#endif
