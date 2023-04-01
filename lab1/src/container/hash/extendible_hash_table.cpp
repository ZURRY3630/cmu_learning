//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  size_t index = IndexOf(key);
  /* if (index > dir_.size() - 1) {
    return false;
  } */
  auto bucket = dir_[index];
  /* auto list = bucket->GetItems();
  bool ret = false;
  for (std::pair<K, V> &p : list) {
    if (key == p.first) {
      value = p.second;
      ret = true;
    }
  } */
  // bool ret = bucket->Find(key,value);

  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  bool ret = false;
  size_t index = IndexOf(key);

  /* if (index > dir_.size() - 1) {
    return false;
  } */
  auto bucket = dir_[index];
  auto list = bucket->GetItems();

  if (bucket == nullptr || list.empty()) {
    return false;
  }

  ret = bucket->Remove(key);
  // ret = true;
  return ret;
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  // UNREACHABLE("not implemented");
  std::scoped_lock<std::mutex> lock(latch_);
  // auto index = IndexOf(key);

  /*if (index < 0) {
    return;
  }*/

  // auto list = bucket->GetItems();
  // std::cout << dir_.size() << std::endl;
  while (dir_[IndexOf(key)]->IsFull()) {  // 若要插入的桶已满，则桶深度和全局深度加1，并线将dir数量翻倍
    auto index = IndexOf(key);
    auto bucket = dir_[index];
    if (bucket->GetDepth() == GetGlobalDepthInternal()) {
      size_t length = dir_.size();
      for (size_t i = 0; i < length; i++) {
        dir_.push_back(dir_[i]);
      }
      global_depth_++;
    }

    auto bucket1 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    auto bucket2 = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    int mask = 1 << bucket->GetDepth();

    for (std::pair<K, V> &item : bucket->GetItems()) {
      if (std::hash<K>{}(item.first) & mask) {
        bucket1->Insert(item.first, item.second);
      } else {
        bucket2->Insert(item.first, item.second);
      }
    }
    num_buckets_++;
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == bucket) {
        if ((i & mask) != 0U) {
          dir_[i] = bucket1;
        } else {
          dir_[i] = bucket2;
        }
      }
    }
  }
  auto index = IndexOf(key);
  auto bucket = dir_[index];
  for (std::pair<K, V> &p : bucket->GetItems()) {
    if (p.first == key) {
      p.second = value;
      return;
    }
  }
  bucket->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, &value](const std::pair<K, V> &p) {
    if (p.first == key) {
      value = p.second;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, this](const std::pair<K, V> &p) {
    if (p.first == key) {
      list_.remove(p);
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (list_.size() == size_) {
    return false;
  }
  // bool ok = false;

  /* if (!ok) {
    std::pair<K, V> pair;
    pair.first = key;
    pair.second = value;
    list_.push_back(pair);
    ok = true;
  } */
  list_.template emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
