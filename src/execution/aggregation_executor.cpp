//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      aht_{this->plan_->GetAggregateTypes()},
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  this->child_executor_->Init();
  Tuple children_tuple{};
  RID rid{};
  /** 获取tuple并保存聚合结果 */
  while (this->child_executor_->Next(&children_tuple, &rid)) {
    this->aht_.InsertCombine(this->MakeAggregateKey(&children_tuple), this->MakeAggregateValue(&children_tuple));
  }
  this->aht_iterator_ = this->aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (zero_flag_) {
    return false;
  }
  if (aht_.Empty() && this->plan_->GetGroupBys().empty()) {
    this->zero_flag_ = true;
    AggregateValue empty_values = this->aht_.GenerateInitialAggregateValue();
    *tuple = Tuple(empty_values.aggregates_, &this->GetOutputSchema());
    return true;
  }
  if (this->aht_iterator_ == this->aht_.End()) {
    return false;
  }
  /** 利用迭代器遍历hash table，构造聚合结果 */
  std::vector<Value> values;
  for (const Value &group_by_value : this->aht_iterator_.Key().group_bys_) {
    values.push_back(group_by_value);
  }
  for (const Value &agg_value : this->aht_iterator_.Val().aggregates_) {
    values.push_back(agg_value);
  }
  ++this->aht_iterator_;
  *tuple = Tuple(values, &this->GetOutputSchema());
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
