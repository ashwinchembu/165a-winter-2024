#ifndef LOCKMANAGER_H
#define LOCKMANAGER_H
#include <mutex>
#include <thread>
#include <shared_mutex>

class LockManagerEntry{
  public:
    LockManagerEntry();
    virtual ~LockManagerEntry();
    std::shared_mutex* mutex;
  //  std::shared_lock<std::shared_mutex>* shared_lock;
  //  std::unique_lock<std::shared_mutex>* unique_lock;
};

class LockManager{
  public:
    LockManager();
    virtual ~LockManager();
    std::unordered_map<int, LockManagerEntry*> locks; //<primary_key, LockManagerEntry>
    std::unordered_multimap<std::thread::id, std::shared_lock<std::shared_mutex>*> active_shared_locks;
    std::unordered_multimap<std::thread::id, std::unique_lock<std::shared_mutex>*> active_unique_locks;
};

#endif
