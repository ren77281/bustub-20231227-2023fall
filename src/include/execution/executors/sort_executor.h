//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "binder/bound_order_by.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"

namespace bustub {

struct TupleCompare {
  TupleCompare(std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys, Schema schema)
      : order_bys_(std::move(order_bys)), schema_(std::move(schema)) {}

  /** 比较器需要重载() */
  auto operator()(const Tuple &left, const Tuple &right) const -> bool {
    for (const auto &order_by : order_bys_) {
      Value left_value = order_by.second->Evaluate(&left, this->schema_);
      Value right_value = order_by.second->Evaluate(&right, this->schema_);
      if (left_value.CompareEquals(right_value) == CmpBool::CmpTrue) {
        continue;
      }
      switch (order_by.first) {
        case OrderByType::ASC:
        case OrderByType::DEFAULT:
          return left_value.CompareLessThan(right_value) == CmpBool::CmpTrue;
        case OrderByType::DESC:
          return left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue;
        case OrderByType::INVALID:
          throw Exception("非法比较\n");
      }
    }
    return true;
  }
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
  Schema schema_;
};

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** 保存排序结果 */
  std::vector<Tuple> results_;
  std::vector<Tuple>::iterator it_;
};

}  // namespace bustub
