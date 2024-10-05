#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

/**
 * 辅助函数，用来递归分解表达式
 * 如果表达式类型都为逻辑/等值比较，则返回true，否则返回false
 */
auto DecomposeExpression(const AbstractExpression *expr_ptr, std::vector<AbstractExpressionRef> &left_key_expressions,
                         std::vector<AbstractExpressionRef> &right_key_expressions) -> bool {
  /** 先尝试转换成逻辑/比较表达式 */
  auto logic_expr = dynamic_cast<const LogicExpression *>(expr_ptr);
  auto comp_expr = dynamic_cast<const ComparisonExpression *>(expr_ptr);
  if (comp_expr != nullptr) {
    if (comp_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }
    /** TODO: 是否有必要获取原生指针再转换，还是能直接进行智能指针的转换？ */
    /** 这里需要确保等值比较的是两个列值，而不是常量，算术表达式 */
    auto left_col_expr = dynamic_cast<ColumnValueExpression *>(comp_expr->GetChildAt(0).get());
    auto right_col_expr = dynamic_cast<ColumnValueExpression *>(comp_expr->GetChildAt(1).get());
    /** 这里需要判断两个列值表达式的顺序是否正确 */
    if (left_col_expr != nullptr && left_col_expr->GetTupleIdx() == 1) {
      std::swap(left_col_expr, right_col_expr);
    }
    if (left_col_expr != nullptr && right_col_expr != nullptr) {
      left_key_expressions.push_back(std::make_shared<ColumnValueExpression>(std::move(*left_col_expr)));
      right_key_expressions.push_back(std::make_shared<ColumnValueExpression>(std::move(*right_col_expr)));
      return true;
    }
    return false;
  }
  /** 如果是逻辑表达式，分别递归左右表达式即可 */
  if (logic_expr != nullptr) {
    return DecomposeExpression(logic_expr->GetChildAt(0).get(), left_key_expressions, right_key_expressions) &&
           DecomposeExpression(logic_expr->GetChildAt(1).get(), left_key_expressions, right_key_expressions);
  }
  /** 不是逻辑/比较，直接返回false */
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  std::unique_ptr<AbstractPlanNode> optimized_plan_tmp = plan->CloneWithChildren(std::move(children));
  AbstractPlanNodeRef optimized_plan = std::move(optimized_plan_tmp);
  /** 只有类型为NLJ才需要优化 */
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    AbstractPlanNodeRef merged_plan = OptimizeMergeFilterNLJ(optimized_plan);
    auto nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode *>(merged_plan.get());
    std::vector<AbstractExpressionRef> left_key_expressions{};
    std::vector<AbstractExpressionRef> right_key_expressions{};
    if (DecomposeExpression(nlj_plan->predicate_.get(), left_key_expressions, right_key_expressions)) {
      return std::make_shared<HashJoinPlanNode>(nlj_plan->output_schema_, nlj_plan->GetLeftPlan(),
                                                nlj_plan->GetRightPlan(), left_key_expressions, right_key_expressions,
                                                nlj_plan->GetJoinType());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
