//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/config.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      it_(this->GetExecutorContext()->GetCatalog()->GetTable(plan->GetTableOid())->table_->MakeIterator()) {}

void SeqScanExecutor::Init() { this->results_it_ = this->results_.begin(); }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  /** 多次调用SqlScan算子的逻辑 */
  if (!is_first_) {
    if (this->results_it_ == this->results_.end()) {
      return false;
    }
    *tuple = this->results_it_->first;
    *rid = this->results_it_->second;
    ++this->results_it_;
    return true;
  }
  /** 第一次调用SeqScan算子的逻辑 */
  TransactionManager *txn_mgr = this->GetExecutorContext()->GetTransactionManager();
  Transaction *txn = this->GetExecutorContext()->GetTransaction();
  TableInfo *table_info = this->GetExecutorContext()->GetCatalog()->GetTable(this->plan_->GetTableOid());
  while (!this->it_.IsEnd()) {
    RID cur_rid = this->it_.GetRID();
    std::optional<Tuple> rebuild_tuple =
        FindFirstUndoLog(cur_rid, table_info, txn_mgr, txn, this->plan_->filter_predicate_);
    if (rebuild_tuple.has_value()) {
      *rid = cur_rid;
      *tuple = *rebuild_tuple;
      this->results_.emplace_back(*tuple, *rid);
      ++this->it_;
      return true;
    }
    ++this->it_;
  }
  this->is_first_ = false;
  return false;
}

}  // namespace bustub
