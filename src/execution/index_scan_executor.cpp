//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  IndexInfo *index_info = this->GetExecutorContext()->GetCatalog()->GetIndex(this->plan_->index_oid_);
  auto hash_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());
  Tuple key_tuple{{this->plan_->pred_key_->val_}, &index_info->key_schema_};
  hash_index->ScanKey(key_tuple, &this->results_, nullptr);
  this->it_ = this->results_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Transaction *txn = this->GetExecutorContext()->GetTransaction();
  TransactionManager *txn_mgr = this->GetExecutorContext()->GetTransactionManager();
  TableInfo *table_info = this->GetExecutorContext()->GetCatalog()->GetTable(this->plan_->table_oid_);
  /** 遍历results，根据tuple的版本链重建tuple */
  while (this->it_ != this->results_.end()) {
    RID result_rid = *this->it_;
    ++this->it_;
    std::optional<Tuple> rebuild_tuple = FindFirstUndoLog(result_rid, table_info, txn_mgr, txn, std::nullopt);
    if (rebuild_tuple.has_value()) {
      *tuple = *rebuild_tuple;
      *rid = result_rid;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
