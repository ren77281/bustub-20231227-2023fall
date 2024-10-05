#include "storage/page/page_guard.h"
#include <utility>
#include "buffer/buffer_pool_manager.h"

namespace bustub {

/** 进行资源转移即可 */
BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  std::swap(this->page_, that.page_);
  std::swap(this->bpm_, that.bpm_);
  std::swap(this->is_dirty_, that.is_dirty_);
}

/** 手动放弃protect对象的所有权 */
void BasicPageGuard::Drop() {
  if (this->page_ != nullptr) {
    this->bpm_->UnpinPage(this->PageId(), this->is_dirty_);
    this->page_ = nullptr;
    this->bpm_ = nullptr;
    this->is_dirty_ = false;
  }
}

/** 与移动构造不同，移动赋值需要先放弃当前protect对象的所有权，再进行资源转移 */
auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  if (this != &that) {
    this->Drop();
    std::swap(this->page_, that.page_);
    std::swap(this->bpm_, that.bpm_);
    std::swap(this->is_dirty_, that.is_dirty_);
  }
  return *this;
}

BasicPageGuard::~BasicPageGuard() { this->Drop(); }

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (this->page_ == nullptr) {
    throw Exception("page_ is nullptr\n");
  }
  this->page_->RLatch();
  ReadPageGuard rpg(this->bpm_, this->page_);
  this->page_ = nullptr;
  this->bpm_ = nullptr;
  return rpg;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (this->page_ == nullptr) {
    throw Exception("page_ is nullptr\n");
  }
  this->page_->WLatch();
  WritePageGuard wpg(this->bpm_, this->page_);
  this->page_ = nullptr;
  this->bpm_ = nullptr;
  return wpg;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  std::swap(this->guard_.page_, that.guard_.page_);
  std::swap(this->guard_.bpm_, that.guard_.bpm_);
  std::swap(this->guard_.is_dirty_, that.guard_.is_dirty_);
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    this->Drop();
    std::swap(this->guard_.page_, that.guard_.page_);
    std::swap(this->guard_.bpm_, that.guard_.bpm_);
    std::swap(this->guard_.is_dirty_, that.guard_.is_dirty_);
  }
  return *this;
}

void ReadPageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->RUnlatch();
    this->guard_.Drop();
  }
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  std::swap(this->guard_.page_, that.guard_.page_);
  std::swap(this->guard_.bpm_, that.guard_.bpm_);
  std::swap(this->guard_.is_dirty_, that.guard_.is_dirty_);
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    this->Drop();
    std::swap(this->guard_.page_, that.guard_.page_);
    std::swap(this->guard_.bpm_, that.guard_.bpm_);
    std::swap(this->guard_.is_dirty_, that.guard_.is_dirty_);
  }
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->WUnlatch();
    /** 因为获取的Page类型是Write，所以设置脏位 */
    this->guard_.is_dirty_ = true;
    this->guard_.Drop();
  }
}

WritePageGuard::~WritePageGuard() { this->Drop(); }

}  // namespace bustub
