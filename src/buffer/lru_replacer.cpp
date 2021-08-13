//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : max_pages(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  replacer_mutex.lock();
  if (victim_queue.empty()) {
    replacer_mutex.unlock();
    return false;
  }
  *frame_id = victim_queue.front();
  frame_in_replacer.erase(*frame_id);
  victim_queue.pop_front();
  replacer_mutex.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  replacer_mutex.lock();
  if (frame_in_replacer.count(frame_id) < 1) {
    replacer_mutex.unlock();
    return;
  }

  for (auto it_list = victim_queue.begin(); it_list != victim_queue.end(); it_list++) {
    if (*it_list == frame_id) {
      victim_queue.erase(it_list);
      frame_in_replacer.erase(frame_id);
      break;
    }
  }

  replacer_mutex.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  replacer_mutex.lock();
  if (frame_in_replacer.count(frame_id) > 0) {
    replacer_mutex.unlock();
    return;
  }

  if (victim_queue.size() < max_pages) {
    frame_in_replacer.insert(frame_id);
    victim_queue.push_back(frame_id);
  }
  replacer_mutex.unlock();
}

size_t LRUReplacer::Size() {
  replacer_mutex.lock();
  auto size = victim_queue.size();
  replacer_mutex.unlock();
  return size;
}

}  // namespace bustub
