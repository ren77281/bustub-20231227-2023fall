//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <memory>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/delete_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() { this->child_executor_->Init(); }

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (this->is_executed_) {
    return false;
  }
  int32_t cnt = 0;
  Tuple children_tuple{};
  TableInfo *table_info = this->GetExecutorContext()->GetCatalog()->GetTable(this->plan_->GetTableOid());
  Transaction *txn = this->GetExecutorContext()->GetTransaction();
  TransactionManager *txn_mgr = this->GetExecutorContext()->GetTransactionManager();
  while (this->child_executor_->Next(&children_tuple, rid)) {
    DeleteTuple(*rid, table_info, txn_mgr, txn);
    cnt++;
  }
  std::vector<Value> value_cnt;
  value_cnt.emplace_back(TypeId::INTEGER, cnt);
  *tuple = Tuple{value_cnt, &this->GetOutputSchema()};
  this->is_executed_ = true;
  return true;
}

}  // namespace bustub
