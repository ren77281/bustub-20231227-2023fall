#include "execution/executors/sort_executor.h"
#include "buffer/lru_k_replacer.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  while (this->child_executor_->Next(&tuple, &rid)) {
    this->results_.push_back(tuple);
  }
  std::sort(this->results_.begin(), this->results_.end(),
            TupleCompare(this->plan_->GetOrderBy(), this->GetOutputSchema()));
  this->it_ = this->results_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->it_ == this->results_.end()) {
    return false;
  }
  *tuple = *this->it_;
  ++this->it_;
  return true;
}

}  // namespace bustub
