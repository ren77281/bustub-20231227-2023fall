//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <utility>

#include "binder/table_ref/bound_join_ref.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

struct HashJoinKey {
  /** 根据哪些列进行Join？ */
  std::vector<Value> join_attrs_;
  auto operator==(const HashJoinKey &other) const -> bool {
    if (this->join_attrs_.size() != other.join_attrs_.size()) {
      return false;
    }
    for (size_t i = 0; i < this->join_attrs_.size(); i++) {
      if (this->join_attrs_[i].CompareEquals(other.join_attrs_[i]) == CmpBool::CmpFalse) {
        return false;
      }
    }
    return true;
  }
};

}  // namespace bustub

namespace std {
/** 实现HashJoinKey的hash方法 */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.join_attrs_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

class SimpleHashJoinHashTable {
 public:
  explicit SimpleHashJoinHashTable(const HashJoinPlanNode *plan) : plan_(plan) {}
  /**
   * 将tuple插入到hash table中
   * @param tuple the value to be inserted
   */
  void InsertTuple(const Tuple &tuple, JoinType join_type) {
    /** 我们需要提取出left_key，再插入 */
    switch (join_type) {
      case JoinType::INNER:
      case JoinType::LEFT:
        this->ht_[this->GetRightJoinKey(tuple)].push_back(tuple);
        break;
      case JoinType::INVALID:
      case JoinType::RIGHT:
      case JoinType::OUTER:
        break;
    }
  }

  /** @return The Join Attributions of left tuple */
  auto GetLeftJoinKey(const Tuple &tuple) const -> HashJoinKey {
    std::vector<Value> join_attrs;
    for (const auto &expr : this->plan_->left_key_expressions_) {
      join_attrs.push_back(expr->Evaluate(&tuple, this->plan_->GetLeftPlan()->OutputSchema()));
    }
    return {join_attrs};
  }

  /** @return The Join Attributions of right tuple */
  auto GetRightJoinKey(const Tuple &tuple) const -> HashJoinKey {
    std::vector<Value> join_attrs;
    for (const auto &expr : this->plan_->right_key_expressions_) {
      join_attrs.push_back(expr->Evaluate(&tuple, this->plan_->GetRightPlan()->OutputSchema()));
    }
    return {join_attrs};
  }

  /**
   * 通过right_key在hash table中获取具有相同hash value的left_tuple
   * 由于可能存在多个匹配的tuple，所以返回std::vector<Tuple>
   * TODO:该算法与左右表强耦合，默认用右表构建hash，左表查询
   */
  auto GetTuple(const Tuple &left_tuple) const -> std::vector<Tuple> {
    std::vector<Tuple> results;
    /** 提取left_tuple的key */
    HashJoinKey left_key = this->GetLeftJoinKey(left_tuple);
    auto it = this->ht_.find(left_key);
    if (it != this->ht_.end()) {
      /** 可能存在多个匹配的right_tuple，需要依次判断 */
      for (const auto &right_tuple : it->second) {
        /** 我们需要从left_tuple提取出left_key */
        HashJoinKey right_key = this->GetRightJoinKey(right_tuple);
        /** 只有两个key相同才保存结果 */
        if (left_key == right_key) {
          results.push_back(right_tuple);
        }
      }
    }
    return results;
  }

 private:
  /** 用来存储hash结果 */
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_;
  /** 需要根据执行计划提取左表的Join Attrs，进行GetTuple */
  const HashJoinPlanNode *plan_;
};
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_executor,
                   std::unique_ptr<AbstractExecutor> &&right_executor);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  SimpleHashJoinHashTable aht_;
  /** 保存连接的结果 */
  std::vector<Tuple> results_;
  std::vector<Tuple>::const_iterator it_;
};

}  // namespace bustub
