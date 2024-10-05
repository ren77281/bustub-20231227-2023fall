//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/hash_table_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/** 创建hash table时，需要保证有且仅有一张header_page */
template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  this->index_name_ = name;
  if (this->bpm_ == nullptr) {
    throw Exception("DiskExtendibleHashTable::bpm_ is nullptr\n");
  }
  BasicPageGuard header_page_guard = this->bpm_->NewPageGuarded(&(this->header_page_id_));
  if (!header_page_guard) {
    throw Exception("DiskExtendibleHashTable::NewPageGuard failure\n");
  }
  auto header_page_ptr = (header_page_guard.AsMut<ExtendibleHTableHeaderPage>());
  header_page_ptr->Init(this->header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash = this->Hash(key);
  /** 获取唯一的header_page并加读锁 */
  ReadPageGuard header_page_rguard = bpm_->FetchPageRead(this->header_page_id_);
  if (!header_page_rguard) {
    throw Exception("DiskExtendibleHashTable:: Fetch header_page failure!\n");
  }
  auto header_page_rptr = header_page_rguard.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_page_idx = header_page_rptr->HashToDirectoryIndex(hash);
  /** 非法页下标 */
  if (directory_page_idx >= header_page_rptr->MaxSize()) {
    LOG_DEBUG("DiskExtendibleHashTable::GetValue::非法页下标: %d", directory_page_idx);
    return false;
  }
  uint32_t directory_page_id = header_page_rptr->GetDirectoryPageId(directory_page_idx);
  /** 读取header_page完成，解读锁 */
  header_page_rguard.Drop();
  /** 读取directory_page，上读锁 */
  ReadPageGuard directory_page_rguard = this->bpm_->FetchPageRead(directory_page_id);
  /** 无效页号（不存在的页） */
  if (!directory_page_rguard) {
    return false;
  }
  auto directory_page_rptr = directory_page_rguard.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_page_idx = directory_page_rptr->HashToBucketIndex(hash);
  /** 非法桶下标 */
  if (bucket_page_idx >= directory_page_rptr->Size()) {
    LOG_DEBUG("DiskExtendibleHashTable::GetValue::非法桶下标: %d", directory_page_idx);
    return false;
  }
  uint32_t bucket_page_id = directory_page_rptr->GetBucketPageId(bucket_page_idx);
  /** 读取directory_page完成，解读锁 */
  directory_page_rguard.Drop();

  /** 读取bucket_page，上读锁 */
  ReadPageGuard bucket_page_rguard = this->bpm_->FetchPageRead(bucket_page_id);
  /** 无效桶号（不存在的桶） */
  if (!bucket_page_rguard) {
    LOG_DEBUG("DiskExtendibleHashTable::GetValue::无效桶号: %d", bucket_page_id);
    return false;
  }
  auto bucket_page_rptr = bucket_page_rguard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  if (!bucket_page_rptr->Lookup(key, value, cmp_)) {
    return false;
  }
  result->push_back(std::move(value));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  std::vector<V> result;
  if (this->GetValue(key, &result)) {
    return false;
  }
  uint32_t hash = this->Hash(key);
  /** 获取唯一的header_page并加写锁，因为可能修改header_page */
  WritePageGuard header_page_wguard = this->bpm_->FetchPageWrite(this->header_page_id_);
  if (!header_page_wguard) {
    throw Exception("DiskExtendibleHashTable:: Fetch header_page failure!\n");
  }
  auto header_page_rptr = header_page_wguard.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_page_idx = header_page_rptr->HashToDirectoryIndex(hash);
  /** 非法页下标 */
  if (directory_page_idx >= header_page_rptr->MaxSize()) {
    LOG_DEBUG("DiskExtendibleHashTable::Insert:: 非法页下标 :%d", directory_page_idx);
    return false;
  }
  uint32_t directory_page_id = header_page_rptr->GetDirectoryPageId(directory_page_idx);
  /** 读取directory_page，上写锁，因为可能修改directory_page */
  WritePageGuard directory_page_wguard = this->bpm_->FetchPageWrite(directory_page_id);
  /** 无效页号，先获取header page的写锁，再调用InsertToNewDirectory */
  if (!directory_page_wguard) {
    /** 获取header_page的写指针（设置脏位） */
    auto header_page_wptr = header_page_wguard.AsMut<ExtendibleHTableHeaderPage>();
    return this->InsertToNewDirectory(header_page_wptr, directory_page_idx, hash, key, value);
  }
  /** 若页号有效，读取header_page完成，解写锁 */
  header_page_wguard.Drop();
  /** 获取页的读指针（不设置脏位） */
  auto directory_page_rptr = directory_page_wguard.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_page_idx = directory_page_rptr->HashToBucketIndex(hash);
  /** 非法桶下标 */
  if (bucket_page_idx >= directory_page_rptr->Size()) {
    LOG_DEBUG("DiskExtendibleHashTable::Insert:: 非法桶下标 :%d", bucket_page_idx);
    return false;
  }
  uint32_t bucket_page_id = directory_page_rptr->GetBucketPageId(bucket_page_idx);
  /** 获取桶的写锁，准备写入k-v */
  WritePageGuard bucket_page_wguard = this->bpm_->FetchPageWrite(bucket_page_id);
  /** 无效桶号，先获取directory_page的写锁，再调用InsertToNewBucket */
  if (!bucket_page_wguard) {
    LOG_DEBUG("DiskExtendibleHashTable::Insert:: 无效桶号 :%d", bucket_page_id);
    /** 获取directory_page的写指针（设置脏位） */
    auto directory_page_wptr = directory_page_wguard.AsMut<ExtendibleHTableDirectoryPage>();
    /** 这里要注意，调用InsertToNewBucket时，因为guard仍然处于生命周期，所以依然有写锁 */
    return this->InsertToNewBucket(directory_page_wptr, bucket_page_idx, key, value);
  }
  /** 若桶号有效，应该继续持有directory_page的写锁，因为可能的分裂需要修改directory_page */
  auto bucket_page_wptr = bucket_page_wguard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  /** 判断桶是否为满，满了需要分裂 */
  if (bucket_page_wptr->IsFull()) {
    /** 获取页的写指针（设置脏位） */
    auto directory_page_wptr = directory_page_wguard.AsMut<ExtendibleHTableDirectoryPage>();
    /** 如果global_depth_和local_depth相同，那么增加local_depth的同时也需要增加global_depth_ */
    if (directory_page_wptr->GetLocalDepth(bucket_page_idx) == directory_page_wptr->GetGlobalDepth()) {
      /** 页满无法分裂，Insert失败 */
      if (directory_page_wptr->Size() == directory_page_wptr->MaxSize()) {
        LOG_DEBUG("页满无法分裂，Insert失败");
        return false;
      }
      directory_page_wptr->IncrGlobalDepth();
      LOG_DEBUG("local_depth增加导致global_depth增加，GD:%d", directory_page_wptr->GetGlobalDepth());
    }
    /** 增加local_depth */
    directory_page_wptr->IncrLocalDepth(bucket_page_idx);
    uint32_t split_bucket_page_idx = directory_page_wptr->GetSplitImageIndex(bucket_page_idx);
    if (split_bucket_page_idx >= directory_page_wptr->Size()) {
      LOG_ERROR("分裂桶的idx不合法，directory的global没有增加？GD:%d, LD:%d", directory_page_wptr->GetGlobalDepth(),
                directory_page_wptr->GetLocalDepth(bucket_page_idx));
      return false;
    }
    /** 获取新页，修改directory_page的bucket_page_ids_与local_depths_ */
    page_id_t split_bucket_page_id = INVALID_PAGE_ID;
    BasicPageGuard split_bucket_page_guard = this->bpm_->NewPageGuarded(&split_bucket_page_id);
    if (!split_bucket_page_guard) {
      LOG_ERROR("无法获取新page");
      return false;
    }
    this->UpdateDirectoryMapping(directory_page_wptr, split_bucket_page_idx, split_bucket_page_id,
                                 directory_page_wptr->GetLocalDepth(bucket_page_idx));
    /** 初始化新页，准备rehash */
    WritePageGuard split_bucket_page_wguard = split_bucket_page_guard.UpgradeWrite();
    auto split_bucket_page_wptr = split_bucket_page_wguard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    split_bucket_page_wptr->Init(this->bucket_max_size_);
    /** rehash */
    for (size_t i = 0; i < bucket_page_wptr->Size(); i++) {
      K bucket_key = bucket_page_wptr->KeyAt(i);
      V bucket_value = bucket_page_wptr->ValueAt(i);
      uint32_t idx = directory_page_wptr->HashToBucketIndex(this->Hash(bucket_key));
      if (idx == split_bucket_page_idx) {
        bucket_page_wptr->RemoveAt(i);
        split_bucket_page_wptr->Insert(bucket_key, bucket_value, this->cmp_);
        /** 因为最后的元素被替换到i位置，这里需要重新遍历i的位置 */
        i--;
      } else if (idx != bucket_page_idx) {
        LOG_ERROR("存在意外k-v, split_idx:%d, bucket_idx:%d, idx:%d", split_bucket_page_idx, bucket_page_idx, idx);
      }
    }
    /** 分裂完成，继续调用insert */
    /** 调用自己前，先解锁 */
    bucket_page_wguard.Drop();
    split_bucket_page_wguard.Drop();
    directory_page_wguard.Drop();
    return this->Insert(key, value, transaction);
  }
  return bucket_page_wptr->Insert(key, value, this->cmp_);
}

/** 子函数不涉及header_page的锁资源管理 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  /** 先获取新页以存储新的directory_page */
  page_id_t new_directory_page_id = INVALID_PAGE_ID;
  BasicPageGuard new_directory_page_guard = this->bpm_->NewPageGuarded(&new_directory_page_id);
  if (!new_directory_page_guard) {
    LOG_ERROR("无法获取page");
    return false;
  }
  /** 在header_page中维护新directory_page信息 */
  header->SetDirectoryPageId(directory_idx, new_directory_page_id);
  /** 给新页上写锁 */
  WritePageGuard new_directory_page_wguard = new_directory_page_guard.UpgradeWrite();
  auto new_directory_page_wptr = new_directory_page_wguard.AsMut<ExtendibleHTableDirectoryPage>();
  new_directory_page_wptr->Init(this->directory_max_depth_);
  /** 获取k-v所在桶的idx */
  uint32_t bucket_page_idx = new_directory_page_wptr->HashToBucketIndex(this->Hash(key));
  /** 调用InsertToNewBucket即可 */
  return this->InsertToNewBucket(new_directory_page_wptr, bucket_page_idx, key, value);
}

/** 子函数不涉及directory_page的锁资源管理 */
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  /** 先获取新页以存储新的bucket_page */
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  BasicPageGuard new_bucket_page_guard = this->bpm_->NewPageGuarded(&new_bucket_page_id);
  if (!new_bucket_page_guard) {
    return false;
  }
  /** 在directory_page中维护新bucket_page信息 */
  directory->SetBucketPageId(bucket_idx, new_bucket_page_id);
  /** 给新页上写锁 */
  WritePageGuard new_bucket_page_wguard = new_bucket_page_guard.UpgradeWrite();
  auto new_bucket_page_wptr = new_bucket_page_wguard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket_page_wptr->Init(this->bucket_max_size_);
  return new_bucket_page_wptr->Insert(key, value, this->cmp_);
}

/** 该子函数需要在分裂桶时，用来维护directory_page */
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth) {
  for (uint32_t i = new_bucket_idx; i < directory->Size(); i += (1 << new_local_depth)) {
    directory->SetLocalDepth(i, new_local_depth);
    directory->SetBucketPageId(i, new_bucket_page_id);
  }
  uint32_t old_bucket_idx = directory->GetSplitImageIndex(new_bucket_idx);
  for (uint32_t i = old_bucket_idx; i < directory->Size(); i += (1 << new_local_depth)) {
    directory->SetLocalDepth(i, new_local_depth);
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  /** 获取唯一的header_page，并加读锁(Remove操作不会修改header_page) */
  ReadPageGuard header_page_rguard = this->bpm_->FetchPageRead(this->header_page_id_);
  if (!header_page_rguard) {
    LOG_ERROR("获取header_page_rguard失败");
    return false;
  }
  uint32_t hash = this->Hash(key);
  auto header_page_rptr = header_page_rguard.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_page_idx = header_page_rptr->HashToDirectoryIndex(hash);
  if (directory_page_idx >= header_page_rptr->MaxSize()) {
    LOG_DEBUG("DiskExtendibleHashTable::Remove::非法页下标: %d", directory_page_idx);
    return false;
  }
  uint32_t directory_page_id = header_page_rptr->GetDirectoryPageId(directory_page_idx);
  /** 读取header_page完成，解读锁 */
  header_page_rguard.Drop();
  /** 读取directory_page，可能涉及到合并操作，先上写锁 */
  WritePageGuard directory_page_wguard = this->bpm_->FetchPageWrite(directory_page_id);
  if (!directory_page_wguard) {
    LOG_ERROR("获取directory_page_guard失败");
    return false;
  }
  auto directory_page_wptr = directory_page_wguard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_page_idx = directory_page_wptr->HashToBucketIndex(hash);
  if (bucket_page_idx >= directory_page_wptr->Size()) {
    LOG_DEBUG("DiskExtendibleHashTable::Remove::非法桶下标: %d", bucket_page_idx);
    return false;
  }
  uint32_t bucket_page_id = directory_page_wptr->GetBucketPageId(bucket_page_idx);
  WritePageGuard bucket_page_wguard = this->bpm_->FetchPageWrite(bucket_page_id);
  if (!bucket_page_wguard) {
    LOG_DEBUG("获取bucket_page_guard失败, bucket_page_id: %d", bucket_page_id);
    return false;
  }
  auto bucket_page_wptr = bucket_page_wguard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket_page_wptr->IsEmpty()) {
    return false;
  }
  if (!bucket_page_wptr->Remove(key, this->cmp_)) {
    LOG_DEBUG("不存在的数据，删除失败");
    return false;
  }
  /** 删除数据完成，释放写锁资源 */
  bucket_page_wguard.Drop();
  while (true) {
    /** 不断合并的过程中，idx不会变，但是存储数据的page总是在变，所以需要不断获取page资源，这里获取读锁就行 */
    uint32_t curr_bucket_page_id = directory_page_wptr->GetBucketPageId(bucket_page_idx);
    ReadPageGuard curr_bucket_page_rguard = this->bpm_->FetchPageRead(curr_bucket_page_id);
    auto curr_bucket_page_rptr = curr_bucket_page_rguard.As<ExtendibleHTableBucketPage<K, V, KC>>();
    /** 获取split_bucket的读锁，阻塞其他线程的写操作 */
    uint32_t split_bucket_page_idx = directory_page_wptr->GetSplitImageIndex(bucket_page_idx);
    if (split_bucket_page_idx == static_cast<uint32_t>(-1)) {
      break;
    }
    uint32_t split_bucket_page_id = directory_page_wptr->GetBucketPageId(split_bucket_page_idx);
    ReadPageGuard split_bucket_page_rguard = this->bpm_->FetchPageRead(split_bucket_page_id);
    if (!split_bucket_page_rguard) {
      LOG_DEBUG("获取split_bucket_page_wguard失败, split_bucket_page_idx:%d, bucket_page_idx:%d", split_bucket_page_idx,
                bucket_page_idx);
      break;
    }
    auto split_bucket_page_rptr = split_bucket_page_rguard.As<ExtendibleHTableBucketPage<K, V, KC>>();
    /** 如果curr_bucket或者split_bucket为空，尝试进行合并 */
    if (!curr_bucket_page_rptr->IsEmpty() && !split_bucket_page_rptr->IsEmpty()) {
      break;
    }
    if (split_bucket_page_idx >= directory_page_wptr->Size()) {
      LOG_DEBUG("非法桶下标: %d", split_bucket_page_idx);
      break;
    }
    /** 如果和split_bucket的local_depth相同，说明可以合并 */
    if (directory_page_wptr->GetLocalDepth(bucket_page_idx) !=
        directory_page_wptr->GetLocalDepth(split_bucket_page_idx)) {
      LOG_DEBUG("两者depth不同, bucket_page_idx:%d, split_bucket_page_idx:%d", bucket_page_idx, split_bucket_page_idx);
      break;
    }
    /** 确定哪个是空桶，哪个非空 */
    uint32_t empty_bucket_page_idx = (curr_bucket_page_rptr->IsEmpty() ? bucket_page_idx : split_bucket_page_idx);
    uint32_t empty_bucket_page_id = directory_page_wptr->GetBucketPageId(empty_bucket_page_idx);
    uint32_t non_empty_bucket_page_idx = (!curr_bucket_page_rptr->IsEmpty() ? bucket_page_idx : split_bucket_page_idx);
    uint32_t non_empty_bucket_page_id = directory_page_wptr->GetBucketPageId(non_empty_bucket_page_idx);
    uint32_t depth = directory_page_wptr->GetLocalDepth(bucket_page_idx) - 1;
    /** 释放空桶的资源（先Drop再删除） */
    if (curr_bucket_page_rptr->IsEmpty()) {
      curr_bucket_page_rguard.Drop();
    } else {
      split_bucket_page_rguard.Drop();
    }
    this->bpm_->DeletePage(empty_bucket_page_id);
    /** 将存储了empty_bucket_id的entry修改为non_empty_bucket_page_id，同时修改local_depths_ */
    for (uint32_t idx = std::min(empty_bucket_page_idx, non_empty_bucket_page_idx); idx < directory_page_wptr->Size();
         idx += (1 << depth)) {
      directory_page_wptr->SetBucketPageId(idx, non_empty_bucket_page_id);
      directory_page_wptr->SetLocalDepth(idx, depth);
    }
  }
  /** 尽可能地减少global_depth */
  while (directory_page_wptr->CanShrink()) {
    directory_page_wptr->DecrGlobalDepth();
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
