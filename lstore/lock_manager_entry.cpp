#include "lock_manager_entry.h"

LockManagerEntry::LockManagerEntry(){
  mutex = new std::shared_mutex;
}

LockManagerEntry::~LockManagerEntry(){
  delete mutex;
}
