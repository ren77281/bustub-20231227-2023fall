//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <cstdint>
#include <unordered_map>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

/** 需要保证max_depth有效 */
void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  if (max_depth > HTABLE_DIRECTORY_MAX_DEPTH) {
    throw Exception("invalid max_depth\n");
  }
  // std::cout << "-------- Directory Init, max_depth: " << max_depth << "--------\n";
  this->max_depth_ = max_depth;
  this->global_depth_ = 0;
  std::memset(this->local_depths_, 0, sizeof this->local_depths_);
  for (size_t i = 0; i < static_cast<uint32_t>(1 << max_depth); i++) {
    this->bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
  // std::cout << "-------- Directory GetGlobalDepthMask: " << (1 << this->global_depth_) - 1 << "--------\n";
  return (1 << this->global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= this->Size()) {
    LOG_ERROR("invalid bucket_idx\n");
    return 0;
  }
  // std::cout << "-------- Directory GetLocalDepthMask: " << (1 << this->local_depths_[bucket_idx]) - 1 <<
  // "--------\n";
  return (1 << this->local_depths_[bucket_idx]) - 1;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  uint32_t res = (hash & this->GetGlobalDepthMask());
  // std::cout << "-------- Directory HashToBucketIndex "<< hash << ' ' << res << " --------\n";
  return res;
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  if (bucket_idx >= this->Size()) {
    LOG_ERROR("invalid bucket_idx\n");
    return INVALID_PAGE_ID;
  }
  // std::cout << "-------- Directory GetBucketPageId: " << bucket_idx << "--------\n";
  return this->bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if (bucket_idx >= this->MaxSize()) {
    throw Exception("invalid bucket_idx\n");
  }
  // std::cout << "-------- Directory SetBucketPageId: " << bucket_idx << ' ' << bucket_page_id << "--------\n";
  this->bucket_page_ids_[bucket_idx] = bucket_page_id;
}

/** 假设local_depth_已经增长 */
auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= this->Size()) {
    throw Exception("invalid bucket_idx\n");
  }
  // std::cout << "-------- Directory GetSplitImageIndex: " << bucket_idx << "--------\n";
  if (this->local_depths_[bucket_idx] == 0) {
    return INVALID_PAGE_ID;
  }
  return bucket_idx ^ (1 << (this->local_depths_[bucket_idx] - 1));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return this->global_depth_; }

/** 全局深度的增加将导致local_depths_和bucket_page_ids_的增加，此时需要维护分裂元素的信息 */
void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  if (this->global_depth_ == this->max_depth_) {
    LOG_ERROR("The current size is already the maximum size\n");
    return;
  }
  // std::cout << "Directory IncrGlobalDepth, global_depth:" << this->global_depth_ << "\n";
  this->global_depth_++;
  /** Size已经增加，注意只需要遍历旧数组，以维护新数组 */
  for (size_t i = 0; i < (this->Size() / 2); i++) {
    this->local_depths_[i ^ (1 << (this->global_depth_ - 1))] = this->local_depths_[i];
    this->bucket_page_ids_[i ^ (1 << (this->global_depth_ - 1))] = this->bucket_page_ids_[i];
  }
  // std::cout << "Directory IncrGlobalDepth Done\n";
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  for (uint32_t i = (this->Size() / 2); i < this->Size(); i++) {
    this->local_depths_[i] = 0;
    this->bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
  this->global_depth_--;
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  for (uint32_t i = 0; i < this->Size(); i++) {
    if (static_cast<uint32_t>(this->local_depths_[i]) == this->global_depth_) {
      return false;
    }
  }
  return true;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return (1 << this->global_depth_); }

auto ExtendibleHTableDirectoryPage::MaxSize() const -> uint32_t { return (1 << this->max_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  if (bucket_idx >= this->Size()) {
    LOG_ERROR("invalid bucket_idx:%d", bucket_idx);
    return -1;
  }
  return static_cast<uint32_t>(this->local_depths_[bucket_idx]);
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  // std::cout << "--------- Directory SetLocalDepth::" << "bucket_idx:" << bucket_idx << " local_depth:" <<
  // static_cast<uint32_t>(local_depth) << " ---------\n";
  if (bucket_idx >= this->Size()) {
    LOG_ERROR("invalid bucket_idx\n");
    return;
  }
  this->local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx >= this->Size()) {
    LOG_ERROR("invalid bucket_idx\n");
    return;
  }
  if (this->local_depths_[bucket_idx] == this->global_depth_) {
    LOG_ERROR("LD==GD, Unable to continue adding\n");
    return;
  }
  this->local_depths_[bucket_idx]++;
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
  if (bucket_idx >= this->Size()) {
    LOG_ERROR("invalid bucket_idx\n");
    return;
  }
  if (this->local_depths_[bucket_idx] == 0) {
    LOG_ERROR("local_depths_ is 0\n");
    return;
  }
  this->local_depths_[bucket_idx]--;
}

}  // namespace bustub
