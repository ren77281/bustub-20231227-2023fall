#include "execution/plans/abstract_plan.h"
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan_tmp = plan->CloneWithChildren(std::move(children));
  AbstractPlanNodeRef optimized_plan = std::move(optimized_plan_tmp);
  /** 当前执行计划为limit且子执行计划为sort，则优化 */
  if (optimized_plan->GetType() == PlanType::Limit) {
    if (optimized_plan->GetChildren().size() == 1 && optimized_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
      auto limit_plan = dynamic_cast<const LimitPlanNode *>(optimized_plan.get());
      auto sort_plan = dynamic_cast<const SortPlanNode *>(limit_plan->GetChildPlan().get());
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan->GetChildPlan(),
                                            sort_plan->GetOrderBy(), limit_plan->GetLimit());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
