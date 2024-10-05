//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <ctime>
#include <memory>
#include <thread>

#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executor_context.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { this->child_executor_->Init(); }

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (this->is_executed_) {
    return false;
  }
  int32_t cnt = 0;
  Tuple children_tuple{};
  Transaction *txn = this->GetExecutorContext()->GetTransaction();
  TransactionManager *txn_mgr = this->GetExecutorContext()->GetTransactionManager();
  TableInfo *table_info = this->GetExecutorContext()->GetCatalog()->GetTable(this->plan_->GetTableOid());
  std::vector<IndexInfo *> indexes = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  while (this->child_executor_->Next(&children_tuple, rid)) {
    InsertTuple(indexes, children_tuple, table_info, txn_mgr, txn);
    cnt++;
  }  // end of while (this->child_executor_->Next(&children_tuple, rid))
  std::vector<Value> value_cnt;
  value_cnt.emplace_back(TypeId::INTEGER, cnt);
  *tuple = Tuple(value_cnt, &this->GetOutputSchema());
  this->is_executed_ = true;
  return true;
}

}  // namespace bustub
