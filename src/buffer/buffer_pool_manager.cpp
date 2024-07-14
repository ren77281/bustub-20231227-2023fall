//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <mutex>

#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
  // std::cout << "pool_size: " << pool_size << ' ' << "replacer_k: " << replacer_k << '\n';
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lk(this->latch_);
  if (page_id == nullptr) {
    return nullptr;
  }
  // std::cout << "NewPage..." << '\n';
  frame_id_t frame_id = -1;
  if (!this->free_list_.empty()) {  // 如果有空闲帧
    // std::cout << "存在空闲帧\n";
    frame_id = this->free_list_.front();
    this->free_list_.pop_front();
  } else {  // 没有空闲帧，试着通过驱逐得到空闲帧
    /** 所有的帧都无法使用 */
    if (!this->replacer_->Evict(&frame_id)) {
      // std::cout << "所有的帧都无法使用\n";
      return nullptr;
    }
    Page *evict_page_ptr = this->pages_ + frame_id;
    page_id_t evict_page_id = evict_page_ptr->GetPageId();
    if (evict_page_ptr->IsDirty()) {  // 如果是脏页，构造写入磁盘的请求
      // std::cout << "驱逐了脏页，需要刷脏\n";
      DiskRequest r;
      r.is_write_ = true;
      r.data_ = evict_page_ptr->GetData();
      r.page_id_ = evict_page_id;
      std::future f(r.callback_.get_future());
      this->disk_scheduler_->Schedule(std::move(r));
      if (!f.get()) {
        /** TODO */
        throw Exception("刷脏失败，需要回溯状态\n");
      }
    }  // 刷脏完成
    /** 将页号映射到-1，表示该页在磁盘中 */
    this->page_table_[evict_page_id] = -1;
    /** 重置page中的元数据 */
    evict_page_ptr->ResetMemory();
    evict_page_ptr->page_id_ = INVALID_PAGE_ID;
    evict_page_ptr->pin_count_ = 0;
    evict_page_ptr->is_dirty_ = false;
  }
  if (frame_id != -1) {
    *page_id = this->AllocatePage();
    this->page_table_[*page_id] = frame_id;
    this->replacer_->RecordAccess(frame_id);
    Page *new_page = this->pages_ + frame_id;
    new_page->page_id_ = *page_id;
    /** 设置帧的不可驱逐，而不可驱逐的帧存储的页应该是被pin住的*/
    this->replacer_->SetEvictable(frame_id, false);
    new_page->pin_count_ += 1;
    // std::cout << "NewPage Done:" << *page_id << "\n";
    return new_page;
  }
  return nullptr;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lk(this->latch_);
  // std::cout << "FetchPage:" << page_id << '\n';
  auto it = this->page_table_.find(page_id);
  if (it == this->page_table_.end()) {
    return nullptr;
  }
  frame_id_t frame_id = it->second;
  if (frame_id != -1) {  // 缓存池存在指定页，pin住该页再返回
    // std::cout << "缓存池存在指定页，pin住该页再返回\n";
    Page *page_ptr = this->pages_ + frame_id;
    this->replacer_->SetEvictable(frame_id, false);
    page_ptr->pin_count_ += 1;
    return page_ptr;
  }
  // std::cout << "需要从磁盘加载页\n";
  /** 如果页在磁盘中，从磁盘加载Page到BufferPool中 */
  if (!this->free_list_.empty()) {  // 如果有空闲帧
    // std::cout << "存在空闲帧\n";
    frame_id = this->free_list_.front();
    this->free_list_.pop_front();
  } else {  // 没有空闲帧，试着通过驱逐得到空闲帧
    /** 所有的帧都无法使用 */
    if (!this->replacer_->Evict(&frame_id)) {
      // std::cout << "所有的帧都无法使用\n";
      return nullptr;
    }
    Page *evict_page_ptr = this->pages_ + frame_id;
    page_id_t evict_page_id = evict_page_ptr->GetPageId();
    if (evict_page_ptr->IsDirty()) {  // 如果是脏页需要刷脏
      // std::cout << "驱逐了脏页，需要刷脏\n";
      DiskRequest r;
      r.is_write_ = true;
      r.data_ = evict_page_ptr->GetData();
      r.page_id_ = evict_page_id;
      std::future f(r.callback_.get_future());
      this->disk_scheduler_->Schedule(std::move(r));
      if (!f.get()) {
        /** TODO */
        throw Exception("刷脏失败，需要回溯状态\n");
      }
    }  // 刷脏完成
    /** 将页号映射到-1，表示该页在磁盘中 */
    this->page_table_[evict_page_id] = -1;
    /** 重置page中的元数据 */
    evict_page_ptr->ResetMemory();
    evict_page_ptr->page_id_ = INVALID_PAGE_ID;
    evict_page_ptr->pin_count_ = 0;
    evict_page_ptr->is_dirty_ = false;
  }
  if (frame_id != -1) {
    this->page_table_[page_id] = frame_id;
    this->replacer_->RecordAccess(frame_id);
    Page *new_page = this->pages_ + frame_id;
    new_page->page_id_ = page_id;
    /** 设置帧的不可驱逐，而不可驱逐的帧存储的页应该是被pin住的*/
    this->replacer_->SetEvictable(frame_id, false);
    new_page->pin_count_ += 1;

    /** 构造读请求，从磁盘获取数据 */
    DiskRequest r;
    r.is_write_ = false;
    r.data_ = new_page->data_;
    r.page_id_ = page_id;
    std::future f(r.callback_.get_future());
    this->disk_scheduler_->Schedule(std::move(r));
    if (!f.get()) {
      /** TODO */
      throw Exception("读取失败\n");
    }
    // std::cout << "FetchPage Done\n";
    return new_page;
  }
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lk(this->latch_);
  // std::cout << "UnpinPage:" << page_id << '\n';
  auto it = this->page_table_.find(page_id);
  if (it == this->page_table_.end()) {
    // std::cout << "被删除的页\n";
    return false;
  }
  frame_id_t frame_id = it->second;
  if (frame_id == -1) {
    // std::cout << "不在缓存池的页\n";
    return false;
  }
  Page *page_ptr = this->pages_ + frame_id;
  if (page_ptr->GetPinCount() == 0) {
    // std::cout << "page's pincount = 0\n";
    return false;
  }
  if (--page_ptr->pin_count_ == 0) {
    // std::cout << "-- : page's pincount = 0\n";
    this->replacer_->SetEvictable(frame_id, true);
  }
  if (is_dirty) {
    // std::cout << "is_dirty\n";
    page_ptr->is_dirty_ = true;
  }
  // std::cout << "UnpinPage Done\n";
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(this->latch_);
  // std::cout << "FlushPage: " << page_id << '\n';
  auto it = this->page_table_.find(page_id);
  if (it == this->page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = it->second;
  if (frame_id == -1) {
    // std::cout << "不在缓存池的页\n";
    return false;
  }
  Page *page_ptr = this->pages_ + frame_id;
  DiskRequest r;
  r.is_write_ = true;
  r.data_ = page_ptr->GetData();
  r.page_id_ = page_ptr->GetPageId();
  std::future<bool> f(r.callback_.get_future());
  this->disk_scheduler_->Schedule(std::move(r));
  if (!f.get()) {
    /** TODO */
    throw Exception("刷脏失败，需要回溯状态\n");
  }
  page_ptr->is_dirty_ = false;
  // std::cout << "FlushPage Done " << page_id << '\n';
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lk(this->latch_);
  // std::cout << "FlushAllPages\n";
  for (auto &elem : this->page_table_) {
    this->FlushPage(elem.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lk(this->latch_);
  // std::cout << "DeletePage: " << page_id << '\n';

  /** 无需删除在磁盘中以及被删除了的页 */
  auto it = this->page_table_.find(page_id);
  if (it == this->page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = it->second;
  if (frame_id == -1) {
    return true;
  }

  /** 不能删除被pin住的页 */
  Page *page_ptr = this->pages_ + frame_id;
  if (page_ptr->GetPinCount() != 0) {
    LOG_DEBUG("删除了被pin住的页");
    return false;
  }
  this->replacer_->Remove(frame_id);
  this->page_table_.erase(page_id);
  this->free_list_.push_back(frame_id);
  /** 重置page中的元数据 */
  page_ptr->ResetMemory();
  page_ptr->page_id_ = INVALID_PAGE_ID;
  page_ptr->pin_count_ = 0;
  page_ptr->is_dirty_ = false;
  this->DeallocatePage(page_id);
  // std::cout << "DeletePage Done\n";
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  // std::cout << "FetchPageBasic: " << page_id << '\n';
  auto page_ptr = this->FetchPage(page_id);
  if (page_ptr == nullptr) {
    /** 如果页不在BufferPool（在磁盘，被删除或者不存在，返回无效对象 */
    return {nullptr, nullptr};
  }
  // std::cout << "FetchPageBasic Done\n";
  return {this, page_ptr};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // std::cout << "FetchPageRead: " << page_id << '\n';
  BasicPageGuard bpg = this->FetchPageBasic(page_id);
  if (!bpg) {
    return {nullptr, nullptr};
  }
  // std::cout << "FetchPageRead Done" << page_id << '\n';
  return bpg.UpgradeRead();
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // std::cout << "FetchPageWrite: " << page_id << '\n';
  BasicPageGuard bpg = this->FetchPageBasic(page_id);
  if (!bpg) {
    return {nullptr, nullptr};
  }
  // std::cout << "FetchPageWrite Done" << page_id << '\n';
  return bpg.UpgradeWrite();
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page_ptr = this->NewPage(page_id);
  if (page_ptr == nullptr) {
    return {nullptr, nullptr};
  }
  // std::cout << "NewPageGuarded: " << *page_id << '\n';
  return {this, page_ptr};
}

}  // namespace bustub
