//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)),
      it_(this->results_.end()) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  std::vector<Tuple> right_table;
  Tuple left_tuple{};
  Tuple right_tuple_tmp{};
  RID rid;
  /** 先保存右表 */
  while (this->right_executor_->Next(&right_tuple_tmp, &rid)) {
    right_table.push_back(right_tuple_tmp);
  }
  /** 先进行NLJ，保存结果 */
  Schema left_schema = left_executor_->GetOutputSchema();
  Schema right_schema = right_executor_->GetOutputSchema();
  uint32_t left_col_num = this->left_executor_->GetOutputSchema().GetColumnCount();
  uint32_t right_col_num = this->right_executor_->GetOutputSchema().GetColumnCount();
  while (this->left_executor_->Next(&left_tuple, &rid)) {
    this->right_executor_->Init();
    /** left_tuple是否被连接过 */
    bool is_joined = false;
    for (const auto &right_tuple : right_table) {
      Value is_equal = this->plan_->predicate_->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      // std::cout << right_tuple.ToString(&right_schema) << '\n';
      /** 如果可以连接，则保存结果 */
      if (!is_equal.IsNull() && is_equal.GetAs<bool>()) {
        std::vector<Value> values;
        is_joined = true;
        for (uint32_t idx = 0; idx < left_col_num; idx++) {
          values.push_back(Value{left_tuple.GetValue(&left_schema, idx)});
        }
        for (uint32_t idx = 0; idx < right_col_num; idx++) {
          values.push_back(Value{right_tuple.GetValue(&right_schema, idx)});
        }
        /** 保存Join 结果 */
        // std::cout << "保存一条结果:" << results_.size() << '\n';
        this->results_.emplace_back(values, &this->GetOutputSchema());
      }
    }
    /** 如果无法连接，且当前连接类型为左连接，则需要连接空数据 */
    if (!is_joined && this->plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      // std::cout << "因为无法连接，保存一条结果:" << results_.size() << '\n';
      for (uint32_t idx = 0; idx < left_col_num; idx++) {
        values.push_back(Value{left_tuple.GetValue(&left_schema, idx)});
      }
      for (uint32_t idx = 0; idx < right_col_num; idx++) {
        values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(idx).GetType()));
      }
      this->results_.emplace_back(values, &this->GetOutputSchema());
    }
  }
  this->it_ = this->results_.begin();

  //   this->left_executor_->Init();
  //   this->right_executor_->Init();
  //   std::vector<Tuple> right_table;
  //   Tuple left_tuple{}, right_tuple{};
  //   RID rid;
  //   /** 先进行NLJ，保存结果 */
  //   Schema left_schema = left_executor_->GetOutputSchema();
  //   Schema right_schema = right_executor_->GetOutputSchema();
  //   uint32_t left_col_num = this->left_executor_->GetOutputSchema().GetColumnCount();
  //   uint32_t right_col_num = this->right_executor_->GetOutputSchema().GetColumnCount();
  //   while (this->left_executor_->Next(&left_tuple, &rid)) {
  //     this->right_executor_->Init();
  //     /** left_tuple是否被连接过 */
  //     bool is_joined = false;
  //     int cnt = 0;
  //     while (this->right_executor_->Next(&right_tuple, &rid)) {
  //       cnt++;
  //       // std::cout << cnt << "\n";
  //       Value is_equal = this->plan_->predicate_->EvaluateJoin(&left_tuple, left_schema,
  //                                             &right_tuple, right_schema);
  //       std::cout << cnt << ' ' << right_tuple.ToString(&right_schema) << '\n';
  //       if (cnt > 200) {
  //         int i = 0;
  //         std::cout << i << '\n';
  //       }
  //       /** 如果可以连接，则保存结果 */
  //       if (!is_equal.IsNull() && is_equal.GetAs<bool>()) {
  //         std::vector<Value> values;
  //         is_joined = true;
  //         for (uint32_t idx = 0; idx < left_col_num; idx++) {
  //           values.push_back(Value{left_tuple.GetValue(&left_schema, idx)});
  //         }
  //         for (uint32_t idx = 0; idx < right_col_num; idx++) {
  //           values.push_back(Value{right_tuple.GetValue(&right_schema, idx)});
  //         }
  //         this->results_.push_back(Tuple{values, &this->GetOutputSchema()});
  //       }
  //     }
  //     if (cnt >= 40) {
  //       std::cout << "匹配次数:" << cnt << '\n';
  //     }
  //     /** 如果无法连接，且当前连接类型为左连接，则需要连接空数据 */
  //     if (!is_joined && this->plan_->GetJoinType() == JoinType::LEFT) {
  //       std::vector<Value> values;
  //       for (uint32_t idx = 0; idx < left_col_num; idx++) {
  //         values.push_back(Value{left_tuple.GetValue(&left_schema, idx)});
  //       }
  //       for (uint32_t idx = 0; idx < right_col_num; idx++) {
  //         values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(idx).GetType()));
  //       }
  //       this->results_.push_back(Tuple{values, &this->GetOutputSchema()});
  //     }
  //   }
  //   this->it_ = this->results_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->it_ == this->results_.end()) {
    return false;
  }
  *tuple = *this->it_;
  ++this->it_;
  return true;
}

}  // namespace bustub
