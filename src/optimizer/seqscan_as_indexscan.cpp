#include <memory>
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"
#include "storage/index/index.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan_tmp = plan->CloneWithChildren(std::move(children));
  AbstractPlanNodeRef optimized_plan = std::move(optimized_plan_tmp);
  /** 只有类型为SeqScan才需要进行优化 */
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    /** 先将顺序扫描与filter投影进行合并 */
    AbstractPlanNodeRef merged_plan = OptimizeMergeFilterScan(optimized_plan);
    auto seq_scan_plan = dynamic_cast<const SeqScanPlanNode *>(merged_plan.get());
    if (seq_scan_plan->filter_predicate_ == nullptr) {
      return optimized_plan;
    }
    auto filter = dynamic_cast<const ComparisonExpression *>(seq_scan_plan->filter_predicate_.get());
    if (filter != nullptr && filter->comp_type_ == ComparisonType::Equal) {
      auto left_const_expr =
          dynamic_cast<ConstantValueExpression *>(seq_scan_plan->filter_predicate_->GetChildAt(0).get());
      auto right_const_expr =
          dynamic_cast<ConstantValueExpression *>(seq_scan_plan->filter_predicate_->GetChildAt(1).get());
      auto left_column_expr =
          dynamic_cast<ColumnValueExpression *>(seq_scan_plan->filter_predicate_->GetChildAt(0).get());
      auto right_column_expr =
          dynamic_cast<ColumnValueExpression *>(seq_scan_plan->filter_predicate_->GetChildAt(0).get());
      auto const_expr = left_const_expr != nullptr ? left_const_expr : right_const_expr;
      auto column_expr = left_column_expr != nullptr ? left_column_expr : right_column_expr;
      if (const_expr != nullptr && column_expr != nullptr) {
        uint32_t col_idx = column_expr->GetColIdx();
        std::vector<IndexInfo *> indexes = this->catalog_.GetTableIndexes(seq_scan_plan->table_name_);
        for (const auto &index_info : indexes) {
          std::vector<uint32_t> key_attrs = index_info->index_->GetKeyAttrs();
          for (const auto &attr : key_attrs) {
            if (attr == col_idx) {
              auto res = std::make_shared<IndexScanPlanNode>(seq_scan_plan->output_schema_,
                                                             seq_scan_plan->GetTableOid(), index_info->index_oid_,
                                                             seq_scan_plan->filter_predicate_, const_expr);
              return res;
            }
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
