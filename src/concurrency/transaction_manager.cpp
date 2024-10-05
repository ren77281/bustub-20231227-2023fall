//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ptr = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));
  txn_ptr->read_ts_ = this->last_commit_ts_.load();
  running_txns_.AddTxn(txn_ptr->read_ts_);
  return txn_ptr;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);
  auto commit_ts = this->last_commit_ts_.load() + 1;
  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }
  /** 遍历write_set_更新tuple ts */
  for (const auto &[table_oid, rids] : txn->GetWriteSets()) {
    TableInfo *table_info = this->catalog_->GetTable(table_oid);
    for (const auto &rid : rids) {
      TupleMeta meta = table_info->table_->GetTupleMeta(rid);
      table_info->table_->UpdateTupleMeta({commit_ts, meta.is_deleted_}, rid);
      auto version_link = this->GetVersionLink(rid);
      this->UpdateVersionLink(rid, VersionUndoLink{UndoLink{version_link->prev_}, false}, nullptr);
    }
  }
  txn->state_ = TransactionState::COMMITTED;
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->commit_ts_ = commit_ts;
  this->last_commit_ts_++;
  /** 维护watermark */
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }
  // TODO(fall2023): Implement the abort logic!
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  txn_id_t min_read_ts = this->GetWatermark();
  /** 遍历所有表中，tuple的版本链，为不可见版本打上is_delete标志 */
  std::vector<std::string> tables = this->catalog_->GetTableNames();
  for (const auto &table_name : tables) {
    TableInfo *table_info = this->catalog_->GetTable(table_name);
    TableIterator it = table_info->table_->MakeIterator();
    /** 找到min_read_ts能读取的undo log，将该undo log之后的undo log删除 */
    while (!it.IsEnd()) {
      RID rid = it.GetRID();
      bool can_delete = it.GetTuple().first.ts_ <= min_read_ts;
      std::optional<UndoLink> undo_link = this->GetUndoLink(rid);
      while (undo_link.has_value() && undo_link->IsValid()) {
        auto undo_log = this->GetUndoLogOptional(*undo_link);
        /** 由于GC的存在，可能有悬空指针 */
        if (!undo_log.has_value()) {
          break;
        }
        /** 找到了min_read_ts能读取的undo log，将其之后的undo log删除 */
        if (can_delete) {
          std::shared_ptr<Transaction> txn = this->txn_map_[undo_link->prev_txn_];
          if (txn == nullptr) {
            throw Exception("版本链中存在非法事务!\n");
          }
          undo_log->is_deleted_ = true;
          txn->ModifyUndoLog(undo_link->prev_log_idx_, *undo_log);
        }
        if (!can_delete && undo_log->ts_ <= min_read_ts) {
          can_delete = true;
        }
        undo_link = undo_log->prev_version_;
      }  // end of while (undo_link.has_value() && undo_link->IsValid())
      ++it;
    }  // end of while (!it.IsEnd())
  }    // end of for (const auto &table_name : tables)
  /** 遍历txn_map_，看是否能移除该事务 */
  std::vector<txn_id_t> to_delete;
  for (const auto &[txn_id, txn] : this->txn_map_) {
    if (txn->state_ != TransactionState::COMMITTED) {
      continue;
    }
    bool can_delete = true;
    for (size_t i = 0; i < txn->GetUndoLogNum(); i++) {
      if (!txn->GetUndoLog(i).is_deleted_) {
        can_delete = false;
        break;
      }
    }
    if (!can_delete) {
      continue;
    }
    to_delete.push_back(txn_id);
  }
  for (const auto &delete_txn_id : to_delete) {
    this->txn_map_.erase(delete_txn_id);
  }
}

}  // namespace bustub
