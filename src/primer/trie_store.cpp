#include "primer/trie_store.h"
#include "common/exception.h"

namespace bustub {

template <class T>
auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<T>> {
  this->root_lock_.lock();
  Trie trie = this->root_;
  this->root_lock_.unlock();

  const T *val_ptr = trie.Get<T>(key);
  if (val_ptr != nullptr) {
    return ValueGuard<T>(trie, *val_ptr);
  }
  return std::nullopt;
}

template <class T>
void TrieStore::Put(std::string_view key, T value) {
  std::lock_guard<std::mutex> write_lock(this->write_lock_);
  Trie trie = this->root_;

  Trie new_trie = trie.Put<T>(key, std::move(value));
  std::lock_guard<std::mutex> root_lock(this->root_lock_);
  this->root_ = new_trie;
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
}

void TrieStore::Remove(std::string_view key) {
  std::lock_guard<std::mutex> write_lock(this->write_lock_);
  Trie trie = this->root_;

  Trie new_trie = trie.Remove(key);
  std::lock_guard<std::mutex> root_lock(this->root_lock_);
  this->root_ = new_trie;
  // You will need to ensure there is only one writer at a time. Think of how you can achieve this.
  // The logic should be somehow similar to `TrieStore::Get`.
}

// Below are explicit instantiation of template functions.

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<uint32_t>>;
template void TrieStore::Put(std::string_view key, uint32_t value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<std::string>>;
template void TrieStore::Put(std::string_view key, std::string value);

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<Integer>>;
template void TrieStore::Put(std::string_view key, Integer value);

template auto TrieStore::Get(std::string_view key) -> std::optional<ValueGuard<MoveBlocked>>;
template void TrieStore::Put(std::string_view key, MoveBlocked value);

}  // namespace bustub