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

#include <mutex>

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(int64_t current_timestamp_, size_t k, frame_id_t fid)
    : k_(k), earliest_time_(current_timestamp_), k_distance_(NEGINF), fid_(fid) {
  history_.push_front(current_timestamp_);
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id_ptr) -> bool {
  std::lock_guard<std::mutex> guard(this->latch_);
  if (this->node_evict_.empty() || frame_id_ptr == nullptr) {
    return false;
  }
  auto it = this->node_evict_.begin();
  const std::shared_ptr<LRUKNode> &evict_node_ptr = *it;
  *frame_id_ptr = evict_node_ptr->fid_;
  this->node_evict_.erase(it);
  this->node_store_.erase(*frame_id_ptr);
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> guard(this->latch_);
  if (static_cast<size_t>(frame_id) > this->replacer_size_) {
    throw Exception("frame id is invalid \n");
  }
  this->current_timestamp_ += 1;
  auto it = this->node_store_.find(frame_id);
  if (it != this->node_store_.end()) {  // 存在该帧的访问记录
    std::shared_ptr<LRUKNode> node_ptr = it->second;
    /** 如果该帧是可驱逐的，由于set不会根据元素的变化维护自身结构，所以需要将元素先拿出，修改后再插入 */
    if (node_ptr->is_evictable_) {
      this->node_evict_.erase(node_ptr);
    }
    /** 维护帧的访问记录 */
    std::list<int64_t> &history = node_ptr->history_;
    history.push_front(this->current_timestamp_);
    if (history.size() > k_) {
      history.pop_back();
    }
    if (history.size() == k_) {
      node_ptr->k_distance_ = history.back();
    }
    /** 如果该帧是可驱逐的，由于set不会根据元素的变化维护自身结构，所以需要将元素先拿出，修改后再插入 */
    if (node_ptr->is_evictable_) {
      this->node_evict_.insert(node_ptr);
    }
  } else {  // 没有该帧的访问记录
    auto new_node = std::make_shared<LRUKNode>(current_timestamp_, k_, frame_id);
    this->node_store_[frame_id] = new_node;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> guard(this->latch_);
  if (static_cast<size_t>(frame_id) > this->replacer_size_) {
    throw Exception("frame id is invalid \n");
  }

  /** 判断该frame是否存在 */
  auto it = this->node_store_.find(frame_id);
  if (it == this->node_store_.end()) {
    throw Exception("frame id is invalid\n");
  }

  /** 根据set_evictable设置is_evictable_ */
  std::shared_ptr<LRUKNode> node_ptr = it->second;
  if (set_evictable) {  // 设置帧为可驱逐
    if (this->node_evict_.count(node_ptr) == 0) {
      node_ptr->is_evictable_ = true;
      this->node_evict_.insert(node_ptr);
    }
  } else {  // 设置帧为不可驱逐
    if (this->node_evict_.count(node_ptr) != 0) {
      node_ptr->is_evictable_ = false;
      this->node_evict_.erase(node_ptr);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(this->latch_);
  if (static_cast<size_t>(frame_id) > this->replacer_size_) {
    throw Exception("frame id is invalid \n");
  }

  /** 判断该frame是否存在 */
  auto it = this->node_store_.find(frame_id);
  if (it == this->node_store_.end()) {
    return;
  }

  /** 只能删除evictable的frame */
  std::shared_ptr<LRUKNode> node_ptr = it->second;
  if (this->node_evict_.count(node_ptr) == 0) {
    throw Exception("frame id is non-evictable\n");
  }
  this->node_evict_.erase(node_ptr);
  this->node_store_.erase(it);
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> guard(this->latch_);
  return this->node_evict_.size();
}

}  // namespace bustub
