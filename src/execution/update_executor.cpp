//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "catalog/catalog.h"
#include "common/config.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executor_context.h"
#include "execution/executors/update_executor.h"
#include "storage/table/tuple.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  this->child_executor_->Init();
  RID rid;
  Tuple children_tuple{};
  while (child_executor_->Next(&children_tuple, &rid)) {
    this->rids_.emplace_back(children_tuple, rid);
  }
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (this->is_executed_) {
    return false;
  }
  int32_t cnt = 0;
  Tuple children_tuple{};
  /** 如果是主键更新，需要先删除当前tuple，再插入，这里保存需要插入的tuple */
  std::vector<Tuple> to_insert_tuple;
  Transaction *txn = this->GetExecutorContext()->GetTransaction();
  TransactionManager *txn_mgr = this->GetExecutorContext()->GetTransactionManager();
  TableInfo *table_info = this->GetExecutorContext()->GetCatalog()->GetTable(this->plan_->GetTableOid());
  std::vector<IndexInfo *> indexes = this->GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info->name_);
  for (auto [old_tuple, modified_rid] : this->rids_) {
    TupleMeta meta = table_info->table_->GetTupleMeta(modified_rid);
    /** 保存VersionUndoLink */
    std::optional<VersionUndoLink> original_version_link = txn_mgr->GetVersionLink(modified_rid);
    if (!original_version_link.has_value()) {
      txn->SetTainted();
      throw Exception("获取VerionLink失败，存在非法rid\n");
    }
    /** 保存old key tuple */
    std::optional<Tuple> old_key_tuple;
    for (const auto &index_info : indexes) {
      if (index_info->is_primary_key_) {
        old_key_tuple =
            old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        break;
      }
    }
    UndoLink first_link = original_version_link->prev_;
    /** 如果是自我修改 */
    if (meta.ts_ == txn->GetTransactionId()) {
      if (first_link.IsValid()) {
        /** 先保存该undo log的下标与undo log */
        size_t undo_log_idx = first_link.prev_log_idx_;
        UndoLog old_undo_log = txn->GetUndoLog(undo_log_idx);
        /** 构建undo log */
        Tuple new_tuple{};
        UndoLog undo_log = ConstructUndoLog(old_tuple, table_info, this->plan_->target_expressions_, old_undo_log.ts_,
                                            std::nullopt, old_undo_log, new_tuple);
        std::optional<Tuple> new_key_tuple;
        if (old_key_tuple.has_value()) {
          for (const auto &index_info : indexes) {
            if (index_info->is_primary_key_) {
              new_key_tuple = new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                                     index_info->index_->GetKeyAttrs());
              break;
            }
          }
          /** 如果是主键更新 */
          if (!IsTupleContentEqual(*old_key_tuple, *new_key_tuple)) {
            /** 先delete再insert */
            DeleteTuple(modified_rid, table_info, txn_mgr, txn);
            to_insert_tuple.push_back(new_tuple);
            cnt++;
            continue;
          }
        }
        /** 更新txn的undo logs即可，不用修改版本链 */
        txn->ModifyUndoLog(undo_log_idx, undo_log);
        /** 只需要更新tuple，无需更新其meta */
        table_info->table_->UpdateTupleInPlace(meta, new_tuple, modified_rid);
      } else {
        /** 只需要更新base tuple */
        table_info->table_->UpdateTupleInPlace(
            meta, GetNewTuple(old_tuple, table_info->schema_, this->plan_->target_expressions_), modified_rid);
      }
      cnt++;
      continue;
    }
    /** 不能修改其他事务正在操作，或是来自未来的tuple */
    if ((original_version_link->in_progress_ && meta.ts_ != txn->GetTransactionId()) ||
        (meta.ts_ < TXN_START_ID && meta.ts_ > txn->GetReadTs())) {
      txn->SetTainted();
      throw ExecutionException("不能修改其他事务正在操作的tuple\n");
    }
    /** 获取tuple lock */
    if (!GetTupleLock(txn_mgr, modified_rid, original_version_link)) {
      txn->SetTainted();
      throw ExecutionException("获取tuple lock失败");
    }
    Tuple new_tuple{};
    /** 由于之前获取的tuple可能被修改过，在持有tuple lock时需要再获取一次 */
    auto [meta_again, tuple_again] = table_info->table_->GetTuple(modified_rid);
    /** 构建undo log */
    UndoLog undo_log = ConstructUndoLog(tuple_again, table_info, this->plan_->target_expressions_, meta_again.ts_,
                                        first_link, std::nullopt, new_tuple);
    std::optional<Tuple> new_key_tuple;
    for (const auto &index_info : indexes) {
      if (index_info->is_primary_key_) {
        new_key_tuple =
            new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        break;
      }
    }
    /** 维护txn的write set与undo logs */
    txn->AppendWriteSet(this->plan_->GetTableOid(), modified_rid);
    txn->AppendUndoLog(undo_log);
    /** 更新VersionLink */
    ConstructAndUpdateVersionLink(txn->GetTransactionId(), txn->GetUndoLogNum() - 1, txn_mgr, modified_rid);
    /** 更新base tuple */
    table_info->table_->UpdateTupleInPlace({txn->GetTransactionId(), false}, new_tuple, modified_rid);
    cnt++;
    /** 如果是主键更新 */
    if (old_key_tuple.has_value() && !IsTupleContentEqual(*old_key_tuple, *new_key_tuple)) {
      /** 先delete再insert */
      DeleteTuple(modified_rid, table_info, txn_mgr, txn);
      to_insert_tuple.push_back(new_tuple);
    }
  }
  /** 如果是主键更新，同一最后插入tuple */
  for (const auto &new_tuple : to_insert_tuple) {
    InsertTuple(indexes, new_tuple, table_info, txn_mgr, txn);
  }
  std::vector<Value> value_cnt;
  value_cnt.emplace_back(TypeId::INTEGER, cnt);
  *tuple = Tuple(value_cnt, &this->GetOutputSchema());
  this->is_executed_ = true;
  return true;
}

}  // namespace bustub
