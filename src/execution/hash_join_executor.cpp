//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/rid.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_executor,
                                   std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      aht_(plan) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  this->left_executor_->Init();
  this->right_executor_->Init();
  /** Build:根据右表构建hash table */
  Tuple right_tuple_tmp{};
  RID rid{};
  while (this->right_executor_->Next(&right_tuple_tmp, &rid)) {
    this->aht_.InsertTuple(right_tuple_tmp, this->plan_->GetJoinType());
  }
  /** Probe:根据左表的tuple_key查询hash table，连接相同key的tuple */
  Tuple left_tuple{};
  Tuple right_tuple{};
  Schema left_schema = left_executor_->GetOutputSchema();
  Schema right_schema = right_executor_->GetOutputSchema();
  uint32_t left_col_num = this->left_executor_->GetOutputSchema().GetColumnCount();
  uint32_t right_col_num = this->right_executor_->GetOutputSchema().GetColumnCount();
  while (this->left_executor_->Next(&left_tuple, &rid)) {
    std::vector<Tuple> right_tuples = this->aht_.GetTuple(left_tuple);
    /** left_tuple没有匹配的right_tuple，且Join类型为left join */
    if (right_tuples.empty() && this->plan_->GetJoinType() == JoinType::LEFT) {
      /** 左连接并且left tuple无匹配right tuple */
      std::vector<Value> values;
      for (uint32_t idx = 0; idx < left_col_num; idx++) {
        values.push_back(Value{left_tuple.GetValue(&left_schema, idx)});
      }
      for (uint32_t idx = 0; idx < right_col_num; idx++) {
        values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(idx).GetType()));
      }
      this->results_.emplace_back(values, &this->GetOutputSchema());
      continue;
    }
    /** 遍历匹配的tuple，将左右两条tuple Join */
    for (const auto &right_tuple : right_tuples) {
      std::vector<Value> values;
      for (uint32_t idx = 0; idx < left_col_num; idx++) {
        values.push_back(Value{left_tuple.GetValue(&left_schema, idx)});
      }
      for (uint32_t idx = 0; idx < right_col_num; idx++) {
        values.push_back(Value{right_tuple.GetValue(&right_schema, idx)});
      }
      /** 保存Join结果 */
      this->results_.emplace_back(values, &this->GetOutputSchema());
    }
  }
  this->it_ = results_.cbegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->it_ == this->results_.end()) {
    return false;
  }
  *tuple = *this->it_;
  ++this->it_;
  return true;
}

}  // namespace bustub
