//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

/** 将所有元素初始化成默认值 */
template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  // std::cout << "-------- Bucket Init, max_size:" << max_size << "--------\n";
  this->size_ = 0;
  this->max_size_ = max_size;
  for (size_t i = 0; i < max_size; i++) {
    this->array_[i] = std::make_pair(K(), V());
  }
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  // std::cout << "-------- Bucket Lookup --------\n";
  for (size_t i = 0; i < static_cast<size_t>(this->Size()); i++) {
    if (cmp(this->array_[i].first, key) == 0) {
      value = this->array_[i].second;
      return true;
    }
  }
  // std::cout << "-------- Bucket Lookup Done --------\n";
  return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  // std::cout << "-------- Bucket Insert --------\n";
  if (this->size_ == this->max_size_) {
    LOG_DEBUG("bucket已满，无法继续插入数据");
    return false;
  }
  for (size_t i = 0; i < static_cast<size_t>(this->Size()); i++) {
    if (cmp(key, this->array_[i].first) == 0) {
      return false;
    }
  }
  this->array_[this->size_++] = {key, value};
  // std::cout << "-------- Bucket Insert Done, size:" << this->size_ << "--------\n";
  return true;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  // std::cout << "-------- Bucket Remove --------\n";
  if (this->Size() == 0) {
    LOG_ERROR("bucket已经为空（调用层应该进行相关判断）");
    return false;
  }
  for (size_t i = 0; i < this->size_; i++) {
    if (cmp(key, this->array_[i].first) == 0) {
      for (size_t j = i; j + 1 < this->size_; j++) {
        this->array_[j] = this->array_[j + 1];
      }
      this->size_--;
      return true;
    }
  }
  // std::cout << "-------- Bucket Remove Done --------\n";
  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  if (bucket_idx >= this->Size()) {
    LOG_ERROR("invalid bucket_idx");
    return;
  }
  // std::cout << "-------- Bucket RemoveAt --------\n";
  this->array_[bucket_idx] = std::move(this->array_[this->size_ - 1]);
  this->size_--;
  // std::cout << "-------- Bucket RemoveAt Done --------\n";
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  if (bucket_idx >= this->Size()) {
    LOG_ERROR("invalid bucket_idx");
    return {};
  }
  return {this->array_[bucket_idx].first};
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  if (bucket_idx >= this->Size()) {
    LOG_ERROR("invalid bucket_idx");
    return {};
  }
  return {this->array_[bucket_idx].second};
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  if (bucket_idx >= this->Size()) {
    throw Exception("invalid bucket_idx\n");
  }
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return this->size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return this->size_ == this->max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return this->size_ == 0;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
