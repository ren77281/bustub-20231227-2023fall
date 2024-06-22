#include "primer/trie.h"
#include <memory>
#include <string_view>

namespace bustub {

// 根据key遍历Trie，如果children_无子节点或最后一个节点没有value，则说明不存在key，返回nullptr
// 否则返回value
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // 保存Trie的根节点，从根节点开始遍历
  std::shared_ptr<const TrieNode> cur_node = this->root_;
  if (cur_node != nullptr) {
    for (char i : key) {
      auto it = (cur_node->children_).find(i);
      if (it != (cur_node->children_).end()) {
        cur_node = it->second;
      } else {
        return nullptr;
      }
    }

    if (cur_node->is_value_node_) {
      const auto cur_node_ptr = dynamic_cast<const TrieNodeWithValue<T> *>(cur_node.get());
      if (cur_node_ptr != nullptr) {
        return cur_node_ptr->value_.get();
      }
    }
  }
  return nullptr;
  // throw NotImplementedException("Trie::Get is not implemented.");
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

// 根据key遍历Trie树，如果目标节点不存在则创建TrieNode再遍历
// 用val_ptr管理value
// 创建TrieNodeWithValue，修改其val_ptr，再修改父节点的map
// 此时需要修改父节点children_，所以遍历过程需要保存父节点
// 如果父节点为nullptr，说明当前节点为根节点（插入的值为空值），此时不用修改父节点，但要修改this->root_

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // 如果根节点为空，需要先创建根节点
  std::shared_ptr<TrieNode> sha_new_root;
  if (this->root_ == nullptr) {
    sha_new_root = std::make_shared<TrieNode>();
  } else {
    sha_new_root = this->root_->Clone();
  }
  std::shared_ptr<TrieNode> pre_node = nullptr;
  std::shared_ptr<TrieNode> cur_node = sha_new_root;

  for (char i : key) {
    auto it = (cur_node->children_).find(i);
    if (it != (cur_node->children_).end()) {
      std::shared_ptr<TrieNode> sha_new_node = it->second->Clone();
      cur_node->children_[i] = sha_new_node;

      pre_node = cur_node;
      cur_node = sha_new_node;
    } else {
      std::shared_ptr<TrieNode> new_node = std::make_shared<TrieNode>();
      cur_node->children_.emplace(i, new_node);

      pre_node = cur_node;
      cur_node = new_node;
    }
  }

  std::shared_ptr<T> value_ptr = std::make_shared<T>(std::move(value));

  // 根据cur_node创建新的TrieNodeWithValue，并赋予value
  std::shared_ptr<TrieNodeWithValue<T>> cur_node_with_value =
      std::make_shared<TrieNodeWithValue<T>>(cur_node->children_, value_ptr);
  cur_node_with_value->is_value_node_ = true;
  // 修改父节点的children
  // 如果没有父节点说明当前节点为根节点，插入的key为空串，则以新节点为根节点，创建Trie
  if (pre_node != nullptr) {
    pre_node->children_[key.back()] = cur_node_with_value;
  } else {
    return Trie(cur_node_with_value);
  }

  return Trie(sha_new_root);

  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
}

// 如果在遍历的过程中遇到nullptr节点，则返回*this
// 否则调用对应节点的Clone进行写时拷贝
// 倒着遍历遍历过的节点，如果最后一个节点的children_大小为0，则删除该节点
// 同时删除父节点children_对应的键值对，如果父节点的childred_大小为为0，并且is_value_node = false，删除
// 重复以上过程直到条件不满足

// 首先根据cur_node拷贝一个TrieNode，替换pre_node的map相关元素
// 什么情况下需要删除节点？如果当前节点的map为空，且没有value，则删除
// 接着遍历父节点，删除父节点中map的映射，然后对父节点进行相同的判断

auto Trie::Remove(std::string_view key) const -> Trie {
  if (this->root_ == nullptr) {
    return *this;
  }
  std::vector<std::shared_ptr<TrieNode>> record;
  std::shared_ptr<TrieNode> sha_new_root = this->root_->Clone();
  std::shared_ptr<TrieNode> cur_node = sha_new_root;
  std::shared_ptr<TrieNode> pre_node = nullptr;
  record.push_back(sha_new_root);

  for (char i : key) {
    auto it = (cur_node->children_).find(i);
    if (it != (cur_node->children_).end()) {
      std::shared_ptr<TrieNode> sha_new_node = it->second->Clone();
      record.push_back(sha_new_node);
      cur_node->children_[i] = sha_new_node;
      pre_node = cur_node;
      cur_node = sha_new_node;
    } else {
      return *this;
    }
  }
  // 如果cur_node存在value，需要Clone一个没有value的cur_node，并维护ptr_node
  if (cur_node->is_value_node_) {
    std::shared_ptr<TrieNode> cur_node_without_value = std::make_shared<TrieNode>(cur_node->children_);
    if (pre_node == nullptr) {
      return Trie(cur_node_without_value);
    }
    pre_node->children_[key.back()] = cur_node_without_value;

    record.pop_back();
    record.push_back(cur_node_without_value);
    // 试着删除不需要的子节点
    bool can_erase = false;
    for (int i = static_cast<int>(record.size()) - 1; i >= 0; --i) {
      cur_node = record[i];
      if (can_erase) {
        cur_node->children_.erase(key[i]);
      }

      if (!cur_node->is_value_node_ && cur_node->children_.empty()) {
        can_erase = true;
      } else {
        break;
      }
    }
  }
  // 如果根节点的children为空，则返回空的Trie
  if (sha_new_root->children_.empty()) {
    return Trie(nullptr);
  }
  return Trie(sha_new_root);
  // throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.
// 将模板的声明定义分离后，需要在cpp文件中显式声明模板的实例化

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub