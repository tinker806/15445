//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  latch_.lock();
  auto iter = page_table_.find(page_id);
  if (iter != page_table_.end()) {
    frame_id_t frameId = iter->second;
    replacer_->Pin(frameId);
    pages_[frameId].pin_count_++;
    latch_.unlock();
    return &pages_[frameId];
  }
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  Page *new_frame = nullptr;
  frame_id_t new_frame_id;

  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&new_frame_id)) {
    return nullptr;
  }
  new_frame = &pages_[new_frame_id];

  // 2.     If R is dirty, write it back to the disk.
  if (new_frame->IsDirty()) {
    disk_manager_->WritePage(new_frame->GetPageId(), new_frame->GetData());
  }
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(new_frame->GetPageId());
  page_table_[page_id] = new_frame_id;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  disk_manager_->ReadPage(page_id, new_frame->data_);
  new_frame->is_dirty_ = false;
  new_frame->pin_count_ = 1;
  new_frame->page_id_ = page_id;
  replacer_->Pin(new_frame_id);
  latch_.unlock();

  return new_frame;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  latch_.lock();
  frame_id_t frame_id = page_table_[page_id];
  Page *frame = &pages_[frame_id];
  frame->is_dirty_ = is_dirty;

  if (frame->GetPinCount() <= 0) {
    latch_.unlock();
    return false;
  }

  frame->pin_count_--;
  if (frame->pin_count_ == 0){
    replacer_->Unpin(frame_id);
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = iter->second;
  auto frame = &pages_[frame_id];

  disk_manager_->WritePage(page_id, frame->GetData());
  frame->is_dirty_ = false;
  latch_.unlock();

  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  latch_.lock();
  frame_id_t frame_id;

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Victim(&frame_id)) {
    latch_.unlock();
    return nullptr;
  }

  *page_id = disk_manager_->AllocatePage();
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.


  auto frame = &pages_[frame_id];

  if (frame->page_id_ != INVALID_PAGE_ID){
    disk_manager_->WritePage(frame->page_id_, frame->GetData());
  }

  page_table_.erase(frame->page_id_);
  frame->is_dirty_ = false;
  frame->pin_count_ = 1;

  frame->page_id_ = *page_id;
  replacer_->Pin(frame_id);
  // 3.   Update P's metadata, zero out memory and add P to the page table.

  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_table_[*page_id] = frame_id;

  memset(frame->data_, 0, sizeof(frame->data_));
  latch_.unlock();
  return frame;
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  latch_.lock();
  if (page_table_.count(page_id) < 1) {
    latch_.unlock();
    return true;
  }
  // 1.   If P does not exist, return true.
  auto frame_id = page_table_[page_id];
  auto frame = &pages_[frame_id];
  if (frame->GetPinCount() > 0) {
    latch_.unlock();
    return false;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.

  disk_manager_->DeallocatePage(page_id);
  page_table_.erase(page_id);
  free_list_.push_back(frame_id);
  frame->page_id_ = INVALID_PAGE_ID;
  frame->is_dirty_ = false;
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  latch_.unlock();
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) {
    auto frame = &pages_[i];
    if (frame->page_id_ == INVALID_PAGE_ID) {
      continue;
    }

    disk_manager_->WritePage(frame->GetPageId(), frame->GetData());
    frame->is_dirty_ = false;
  }
}

}  // namespace bustub
