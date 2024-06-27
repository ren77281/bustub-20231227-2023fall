//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"
#include <mutex>

namespace bustub {

LRUKNode::LRUKNode(long long current_timestamp_, size_t k, frame_id_t fid) : k_(k), k_instance_(INF), fid_(fid) {
  history_.push_front(current_timestamp_);
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id_ptr) -> bool {
  std::lock_guard<std::mutex> guard(this->latch_);

  if (this->node_evict_.size() == 0) {
    return false; 
  } 
  auto it = this->node_evict_.begin();
  std::shared_ptr<LRUKNode> evict_node_ptr = *it;
  if (frame_id_ptr) {
    *frame_id_ptr = evict_node_ptr->fid_;
  }
  this->node_evict_.erase(it);
  this->node_store_.erase(*frame_id_ptr);
  this->curr_size_--;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(this->latch_);

  if (static_cast<size_t>(frame_id) > this->replacer_size_) {
    throw Exception("frame id is invalid \n");
  }
  
  auto now = std::chrono::high_resolution_clock::now();
  this->current_timestamp_ = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();

  auto it = this->node_store_.find(frame_id);
  if (it != this->node_store_.end()) {
    std::shared_ptr<LRUKNode> node_ptr = it->second;
    // 如果该页是可驱逐的
    // 由于set不会根据元素的变化维护自身结构，所以需要将元素先拿出，修改后再插入
    if (node_ptr->is_evictable_) {
      this->node_evict_.erase(node_ptr);
    }
    // 维护页的访问记录
    std::list<long long> &history = node_ptr->history_;
    history.push_front(this->current_timestamp_);
    if (history.size() > k_) {
      history.pop_back();
    }
    if (history.size() == k_) {
      node_ptr->k_instance_ = history.front() - history.back();
    }
    if (node_ptr->is_evictable_) {
      this->node_evict_.insert(node_ptr);
    }
  } else {
    auto new_node = std::make_shared<LRUKNode>(current_timestamp_, k_, frame_id);
    node_store_[frame_id] = new_node;
    curr_size_++;
    if (curr_size_ > replacer_size_) {
        frame_id_t evict_fid;
        Evict(&evict_fid);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(this->latch_);
  
  if (static_cast<size_t>(frame_id) > this->replacer_size_) {
    throw Exception("frame id is invalid \n");
  }

  auto it = this->node_store_.find(frame_id);
  if (it != this->node_store_.end()) {
    std::shared_ptr<LRUKNode> node_ptr = it->second;
    if (set_evictable) {  // 将不可驱逐设置为可驱逐
      if (this->node_evict_.count(node_ptr) == 0) {
        node_ptr->is_evictable_ = true;
        this->node_evict_.insert(node_ptr);
      }
    } else {  // 将可驱逐设置为不可驱逐
      if (this->node_evict_.count(node_ptr)) {
        node_ptr->is_evictable_ = false;
        this->node_evict_.erase(node_ptr);
      }
    }
  } else {
    throw Exception("frame id is invalid\n");
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(this->latch_);
  
  if (static_cast<size_t>(frame_id) > this->replacer_size_) {
    throw Exception("frame id is invalid \n");
  }

  auto it = this->node_store_.find(frame_id);
  if (it != this->node_store_.end()) {
    std::shared_ptr<LRUKNode> node_ptr = it->second;
    if (this->node_evict_.count(node_ptr)) {
      this->node_evict_.erase(node_ptr);
    } else {
      throw Exception("frame id is non-evictable\n");
    }
    this->node_store_.erase(it);
    this->curr_size_--;
  } else {
    throw Exception("frame id is invalid\n");
  }
}

auto LRUKReplacer::Size() -> size_t { 
  std::lock_guard<std::mutex> guard(this->latch_);
  
  return this->node_evict_.size(); 
}

}  // namespace bustub
