//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  // std::cout << "-------- Header Init, max_depth: " << max_depth << "--------\n";
  if (max_depth > HTABLE_HEADER_MAX_DEPTH) {
    throw Exception("invalid max_depth\n");
  }
  this->max_depth_ = max_depth;
  for (size_t i = 0; i < static_cast<uint32_t>(1 << max_depth); i++) {
    this->directory_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t {
  if (this->max_depth_ == 0) {
    return 0;
  }
  uint32_t res = hash >> (sizeof(uint32_t) * 8 - this->max_depth_);
  // std::cout << "-------- Header HashToDirectoryIndex: " << res << " --------\n";
  return res;
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
  // std::cout << "-------- Header GetDirectoryPageId: " << directory_idx << " --------\n";
  if (directory_idx >= static_cast<uint32_t>(1 << this->max_depth_)) {
    LOG_ERROR("directory_idx is invalid\n");
    return INVALID_PAGE_ID;
  }
  return this->directory_page_ids_[directory_idx];
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  // std::cout << "-------- Header SetDirectoryPageId: " << directory_idx << ' ' << directory_page_id << " --------\n";
  if (directory_idx >= static_cast<uint32_t>(1 << this->max_depth_)) {
    LOG_ERROR("directory_idx is invalid\n");
    return;
  }
  this->directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return (1 << this->max_depth_); }

}  // namespace bustub
