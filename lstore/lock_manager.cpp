#include "lock_manager.h"
#include <unordered_map>

LockManagerEntry::LockManagerEntry(){
  mutex = new std::shared_mutex;
  // shared_lock = new std::shared_lock<std::shared_mutex>(*mutex, std::defer_lock);
  // unique_lock = new std::unique_lock<std::shared_mutex>(*mutex, std::defer_lock);
}

LockManagerEntry::~LockManagerEntry(){
  delete mutex;
  // delete shared_lock;
  // delete unique_lock;
}

LockManager::LockManager() {}

LockManager::~LockManager() {
  for(std::unordered_multimap<int, LockManagerEntry*>::iterator iter=locks.begin(); iter!=locks.end(); iter++){
    delete iter->second;
  }
}
