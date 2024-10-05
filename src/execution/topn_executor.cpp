#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      topn_compare_(this->plan_->GetOrderBy(), *this->plan_->output_schema_) {}

void TopNExecutor::Init() {
  this->child_executor_->Init();
  std::priority_queue<Tuple, std::vector<Tuple>, TupleCompare> result_tmp(this->topn_compare_);
  Tuple tuple{};
  RID rid{};
  while (this->child_executor_->Next(&tuple, &rid)) {
    if (result_tmp.size() == this->plan_->n_) {
      if (topn_compare_(tuple, result_tmp.top())) {
        result_tmp.pop();
        result_tmp.push(tuple);
      }
    } else {
      result_tmp.push(tuple);
    }
  }
  /** 因为priority_queue无法用迭代器遍历，所以这里需要保存到vector */
  while (!result_tmp.empty()) {
    this->results_.push_back(result_tmp.top());
    result_tmp.pop();
  }
  /** 找最小数，建大堆，此时获取的是降序数列，需要反转 */
  reverse(this->results_.begin(), this->results_.end());
  this->it_ = this->results_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->it_ == this->results_.end()) {
    return false;
  }
  *tuple = *this->it_;
  ++this->it_;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t { return this->results_.size(); };

}  // namespace bustub
