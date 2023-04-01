//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  /*throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager_instance.cpp`.");*/
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);
  bool all_pinned = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() <= 0) {
      all_pinned = false;
      break;
    }
  }

  if (all_pinned) {  // 如果所有frame都被pin，则直接返回 null
    return nullptr;
  }
  frame_id_t frame_id;
  // Page *frame = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    auto vic = replacer_->Evict(&frame_id);
    // frame = &pages_[frame_id];

    auto evict_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evict_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();
    page_table_->Remove(pages_[frame_id].GetPageId());
    if (!vic) {
      return nullptr;
    }  // 如果没有frame可以驱逐，则返回null
  }

  *page_id = AllocatePage();  // new page
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  page_table_->Insert(*page_id, frame_id);
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::lock_guard<std::mutex> guard(latch_);

  bool all_pinned = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPinCount() <= 0) {
      all_pinned = false;
      break;
    }
  }

  if (all_pinned) {  // 如果所有frame都被pin，则直接返回 null
    return nullptr;
  }
  frame_id_t frame_id;

  auto ret = page_table_->Find(page_id, frame_id);
  // Page *frame = nullptr;
  if (ret) {
    // frame = &pages_[frame_id];
    pages_[frame_id].pin_count_++;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    return &pages_[frame_id];
  }
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    auto vic = replacer_->Evict(&frame_id);

    auto evict_id = pages_[frame_id].GetPageId();
    if (pages_[frame_id].IsDirty()) {
      disk_manager_->WritePage(evict_id, pages_[frame_id].GetData());
      pages_[frame_id].is_dirty_ = false;
    }
    pages_[frame_id].ResetMemory();
    page_table_->Remove(pages_[frame_id].GetPageId());
    if (!vic) {
      return nullptr;
    }  // 如果没有frame可以驱逐，则返回null
  }

  // auto frame = &pages_[frame_id];

  pages_[frame_id].page_id_ = page_id;  // other page
  ++pages_[frame_id].pin_count_;
  // frame->ResetMemory();
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  page_table_->Insert(page_id, frame_id);
  disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
  // *page_id = new_page_id;
  return &pages_[frame_id];
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  auto ret = page_table_->Find(page_id, frame_id);
  if (!ret) {
    return false;
  }

  // auto page = &pages_[frame_id];

  if (pages_[frame_id].pin_count_ <= 0) {
    return false;
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  --pages_[frame_id].pin_count_;

  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  // std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  auto ret = page_table_->Find(page_id, frame_id);
  if (!ret) {
    return false;
  }
  // auto frame = &pages_[frame_id];
  // frame->is_dirty_ = false;
  disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::lock_guard<std::mutex> guard(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    // auto frame = &pages_[i];
    /* if (frame->page_id_ != INVALID_PAGE_ID && frame->IsDirty()) {
      disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
      frame->is_dirty_ = false;
    } */
    FlushPgImp(pages_[i].GetPageId());
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t frame_id;
  auto ret = page_table_->Find(page_id, frame_id);
  if (!ret) {
    return true;
  }
  // auto page = &pages_[frame_id];
  if (pages_[frame_id].GetPinCount() > 0) {
    return false;
  }

  replacer_->Remove(frame_id);
  // replacer_->SetEvictable(frame_id, false);
  pages_[frame_id].ResetMemory();
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].is_dirty_ = false;
  page_table_->Remove(page_id);
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
