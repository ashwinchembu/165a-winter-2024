#ifndef LOCKMANAGER_H
#define LOCKMANAGER_H
#include <mutex>
#include <thread>
#include <unordered_map>
#include <shared_mutex>

class LockManagerEntry{
  public:
    LockManagerEntry();
    virtual ~LockManagerEntry();
    std::shared_mutex* mutex;
};

class LockManager{
  public:
    std::shared_mutex active_unique_lock;
    std::shared_mutex active_shared_lock;
    LockManager();
    LockManager(const LockManager& rhs);
    LockManager& operator=(const LockManager& rhs);
    virtual ~LockManager();
    std::unordered_map<int, LockManagerEntry*> locks; //<primary_key, LockManagerEntry>
    std::unordered_multimap<std::thread::id, std::shared_lock<std::shared_mutex>*> active_shared_locks;
    std::unordered_multimap<std::thread::id, std::unique_lock<std::shared_mutex>*> active_unique_locks;
};

#endif
