#include "execution/execution_common.h"
#include <optional>
#include <sstream>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/expressions/abstract_expression.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  /** 先将原值填入values数组，后续将根据undo log修改values数组 */
  std::vector<Value> values;
  values.resize(schema->GetColumnCount());
  for (size_t idx = 0; idx < values.size(); idx++) {
    values[idx] = base_tuple.GetValue(schema, idx);
  }
  /** 遍历所有的undo log */
  for (const auto &undo_log : undo_logs) {
    std::vector<Column> modified_cols;
    std::vector<size_t> modified_col_idxs;
    /** 这里的idx指的是base_tuple的列下标 */
    for (size_t idx = 0; idx < undo_log.modified_fields_.size(); idx++) {
      if (undo_log.modified_fields_[idx]) {
        modified_cols.push_back(schema->GetColumn(idx));
        modified_col_idxs.push_back(idx);
      }
    }
    Schema modified_schema{modified_cols};
    /** 这里的idx指的是undo log中tuple的列下标 */
    for (size_t idx = 0; idx < modified_cols.size(); idx++) {
      /** 对values数组应用修改 */
      values[modified_col_idxs[idx]] = Value{undo_log.tuple_.GetValue(&modified_schema, idx)};
    }
  }
  if ((undo_logs.empty() && !base_meta.is_deleted_) || (!undo_logs.empty() && !undo_logs.back().is_deleted_)) {
    return std::make_optional(Tuple{values, schema});
  }
  return std::nullopt;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  fmt::println(stderr, "debug_hook: {}", info);

  TableIterator it(table_heap->MakeIterator());
  while (!it.IsEnd()) {
    auto [meta, base_tuple] = it.GetTuple();
    RID rid = base_tuple.GetRid();
    std::optional<UndoLink> undo_link = txn_mgr->GetUndoLink(rid);
    std::stringstream row_ss;
    row_ss << "RID=" << rid.GetPageId() << '/' << rid.GetSlotNum() << " ts=";
    if (meta.ts_ > TXN_START_ID) {
      row_ss << "txn" << (meta.ts_ ^ TXN_START_ID) << ' ';
    } else {
      row_ss << meta.ts_ << ' ';
    }
    if (meta.is_deleted_) {
      row_ss << "<del marker> ";
    }
    row_ss << "tuple=" << base_tuple.ToString(&table_info->schema_);
    fmt::println(stderr, row_ss.str().c_str());
    std::vector<UndoLog> undo_logs;
    while (undo_link.has_value() && undo_link->IsValid()) {
      auto undo_log = txn_mgr->GetUndoLogOptional(*undo_link);
      if (!undo_log.has_value()) {
        break;
      }
      std::stringstream tuple_ss;
      tuple_ss << "  txn" << (undo_link->prev_txn_ ^ TXN_START_ID) << ' ';
      undo_logs.push_back(*undo_log);
      auto tuple = ReconstructTuple(&table_info->schema_, base_tuple, meta, undo_logs);
      if (tuple.has_value()) {
        tuple_ss << tuple->ToString(&table_info->schema_) << ' ';
      } else {
        tuple_ss << " <del> ";
      }
      tuple_ss << "ts=" << undo_log->ts_;
      fmt::println(stderr, tuple_ss.str().c_str());
      undo_link = undo_log->prev_version_;
    }
    ++it;
  }
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

auto ConstructUndoLog(const Tuple &old_tuple, TableInfo *table_info,
                      const std::vector<AbstractExpressionRef> &expressions, timestamp_t ts,
                      std::optional<UndoLink> link, std::optional<UndoLog> old_undo_log, Tuple &new_tuple) -> UndoLog {
  std::vector<Column> old_undo_log_cols;
  if (old_undo_log.has_value()) {
    for (size_t i = 0; i < old_undo_log->modified_fields_.size(); i++) {
      if (old_undo_log->modified_fields_[i]) {
        old_undo_log_cols.push_back(table_info->schema_.GetColumn(i));
      }
    }
  }
  Schema old_undo_log_schema{old_undo_log_cols};
  UndoLog new_undo_log;
  new_undo_log.is_deleted_ = old_undo_log.has_value() ? old_undo_log->is_deleted_ : false;
  new_undo_log.modified_fields_.resize(table_info->schema_.GetColumnCount(), false);
  new_undo_log.ts_ = ts;
  std::vector<Value> new_undo_log_vals;
  std::vector<Value> new_tuple_vals;
  new_tuple_vals.reserve(expressions.size());
  std::vector<Column> new_undo_log_cols;
  size_t old_modified_idx = 0;
  for (size_t i = 0; i < expressions.size(); i++) {
    std::optional<Value> undo_log_val;
    std::optional<Column> undo_log_col;
    const auto &expr = expressions[i];
    Value new_val = expr->Evaluate(&old_tuple, table_info->schema_);
    Value old_val = old_tuple.GetValue(&table_info->schema_, i);
    new_tuple_vals.push_back(new_val);
    if (!new_val.CompareExactlyEquals(old_val)) {
      undo_log_val = old_val;
      undo_log_col = table_info->schema_.GetColumn(i);
    }
    if (old_undo_log.has_value() && old_undo_log->modified_fields_[i]) {
      undo_log_val = old_undo_log->tuple_.GetValue(&old_undo_log_schema, old_modified_idx);
      undo_log_col = old_undo_log_schema.GetColumn(old_modified_idx++);
    }
    if (undo_log_val.has_value()) {
      new_undo_log_vals.push_back(*undo_log_val);
      new_undo_log_cols.push_back(*undo_log_col);
      new_undo_log.modified_fields_[i] = true;
    }
  }
  /** 构建undo log的增量 */
  Schema new_undo_log_schema{new_undo_log_cols};
  new_undo_log.tuple_ = Tuple{new_undo_log_vals, &new_undo_log_schema};
  new_undo_log.prev_version_ = link.has_value() ? *link : old_undo_log->prev_version_;
  /** 输出new_tuple */
  new_tuple = Tuple{new_tuple_vals, &table_info->schema_};
  return new_undo_log;
}

auto GetNewTuple(const Tuple &old_tuple, const Schema &schema, const std::vector<AbstractExpressionRef> &expressions)
    -> Tuple {
  std::vector<Value> values;
  values.reserve(expressions.size());
  for (const auto &expr : expressions) {
    values.push_back(expr->Evaluate(&old_tuple, schema));
  }
  return Tuple{values, &schema};
}

void ConstructAndUpdateVersionLink(txn_id_t txn_id, size_t undo_log_idx, TransactionManager *txn_mgr, RID rid) {
  UndoLink undo_link;
  undo_link.prev_txn_ = txn_id;
  undo_link.prev_log_idx_ = undo_log_idx;
  txn_mgr->UpdateVersionLink(rid, VersionUndoLink{undo_link, true}, nullptr);
}

auto GetDeleteUndoLog(const Schema &schema, const UndoLink &link, timestamp_t ts, const Tuple &tuple) -> UndoLog {
  UndoLog undo_log;
  undo_log.is_deleted_ = false;
  undo_log.modified_fields_.resize(schema.GetColumnCount(), true);
  undo_log.prev_version_ = link;
  undo_log.ts_ = ts;
  undo_log.tuple_ = tuple;
  return undo_log;
}

auto FindFirstUndoLog(const RID &rid, const TableInfo *table_info, TransactionManager *txn_mgr, Transaction *txn,
                      std::optional<AbstractExpressionRef> filter) -> std::optional<Tuple> {
  auto [meta, base_tuple] = table_info->table_->GetTuple(rid);
  std::optional<UndoLink> undo_link = txn_mgr->GetUndoLink(rid);
  std::vector<UndoLog> undo_logs;
  bool can_read = false;
  timestamp_t tuple_ts = meta.ts_;
  timestamp_t read_ts = txn->GetReadTs();
  if ((read_ts >= tuple_ts || tuple_ts == txn->GetTransactionId())) {
    can_read = true;
  }
  while (!can_read && undo_link.has_value() && undo_link->IsValid()) {
    /** 先保存undo_link指向的undo_log */
    auto undo_log = txn_mgr->GetUndoLogOptional(*undo_link);
    /** 由于GC的存在，可能有悬空指针 */
    if (!undo_log.has_value()) {
      break;
    }
    undo_logs.push_back(*undo_log);
    /** 将read ts与tuple ts进行比较，直到遇到一个能够读取 */
    if (read_ts >= undo_log->ts_) {
      can_read = true;
      break;
    }
    /** 更新undo_link */
    undo_link = undo_log->prev_version_;
  } /** end of while (undo_link.has_value() && undo_link->IsValid()) */
  if (can_read) {
    std::optional<Tuple> tuple_optional = ReconstructTuple(&table_info->schema_, base_tuple, meta, undo_logs);
    if (tuple_optional.has_value()) {
      /** filter条件的判断 */
      if (filter.has_value() && *filter != nullptr) {
        Value is_equal = (*filter)->Evaluate(&(*tuple_optional), table_info->schema_);
        if (!is_equal.IsNull() && !is_equal.GetAs<bool>()) {
          return std::nullopt;
        }
      }
      return tuple_optional;
    }
  }
  return std::nullopt;
}

auto IsTupleDelete(const TableInfo *table_info, TransactionManager *txn_mgr, const RID &rid) -> bool {
  TupleMeta meta = table_info->table_->GetTupleMeta(rid);
  if (!meta.is_deleted_) {
    return false;
  }
  std::optional<UndoLink> undo_link = txn_mgr->GetUndoLink(rid);
  while (undo_link.has_value() && undo_link->IsValid()) {
    auto undo_log = txn_mgr->GetUndoLogOptional(*undo_link);
    if (!undo_log.has_value()) {
      break;
    }
    if (!undo_log->is_deleted_) {
      return false;
    }
    undo_link = undo_log->prev_version_;
  }
  return true;
}

auto GetTupleLock(TransactionManager *txn_mgr, RID rid, std::optional<VersionUndoLink> original_version_link) -> bool {
  if (!original_version_link.has_value()) {
    return txn_mgr->UpdateVersionLink(
        rid, VersionUndoLink{UndoLink{}, true},
        [](std::optional<VersionUndoLink> cur_version_link) -> bool { return !cur_version_link.has_value(); });
  }
  return txn_mgr->UpdateVersionLink(rid, VersionUndoLink{original_version_link->prev_, true},
                                    [original_version_link](std::optional<VersionUndoLink> cur_version_link) -> bool {
                                      if (original_version_link.has_value() && cur_version_link.has_value()) {
                                        return *original_version_link == *cur_version_link;
                                      }
                                      return (!original_version_link.has_value() && !cur_version_link.has_value());
                                    });
}

void DeleteTuple(const RID &modified_rid, TableInfo *table_info, TransactionManager *txn_mgr, Transaction *txn) {
  auto [meta, delete_tuple] = table_info->table_->GetTuple(modified_rid);
  /** 保存VersionUndoLink */
  std::optional<VersionUndoLink> original_version_link = txn_mgr->GetVersionLink(modified_rid);
  if (!original_version_link.has_value()) {
    txn->SetTainted();
    throw Exception("获取VerionLink失败，存在非法rid\n");
  }
  UndoLink first_link = original_version_link->prev_;
  /** 如果是自我修改 */
  if (meta.ts_ == txn->GetTransactionId()) {
    if (!meta.is_deleted_) {
      if (first_link.IsValid()) {
        /** 先保存该undo log的下标与undo log */
        size_t undo_log_idx = first_link.prev_log_idx_;
        UndoLog old_undo_log = txn->GetUndoLog(undo_log_idx);
        /** 构建undo log */
        auto before_tuple = ReconstructTuple(&table_info->schema_, delete_tuple, meta, {old_undo_log});
        /** 如果无法构建undo log，说明前一个undo log被delete，此时只需要更新base tuple */
        if (!before_tuple.has_value()) {
          meta.is_deleted_ = true;
          table_info->table_->UpdateTupleMeta(meta, modified_rid);
          return;
        }
        UndoLog undo_log =
            GetDeleteUndoLog(table_info->schema_, old_undo_log.prev_version_, old_undo_log.ts_, *before_tuple);
        /** 更新txn的undo logs即可，不用修改版本链 */
        txn->ModifyUndoLog(undo_log_idx, undo_log);
      }
      /** 被当前事务insert的tuple，直接更新base tuple即可 */
      meta.is_deleted_ = true;
      table_info->table_->UpdateTupleMeta(meta, modified_rid);
    }
    return;
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
  /** 由于之前获取的tuple可能被修改过，在持有tuple lock时需要再获取一次 */
  auto [meta_again, tuple_again] = table_info->table_->GetTuple(modified_rid);
  /** 构建undo log */
  UndoLog undo_log = GetDeleteUndoLog(table_info->schema_, first_link, meta_again.ts_, tuple_again);
  /** 更新事务的undo log与write set */
  txn->AppendUndoLog(undo_log);
  txn->AppendWriteSet(table_info->oid_, modified_rid);
  /** 更新VersionLink */
  ConstructAndUpdateVersionLink(txn->GetTransactionId(), txn->GetUndoLogNum() - 1, txn_mgr, modified_rid);
  /** 更新base tuple的is_delete标记 */
  meta.ts_ = txn->GetTransactionId();
  meta.is_deleted_ = true;
  table_info->table_->UpdateTupleMeta(meta, modified_rid);
}

void InsertTuple(const std::vector<IndexInfo *> &indexes, Tuple children_tuple, TableInfo *table_info,
                 TransactionManager *txn_mgr, Transaction *txn) {
  /** 检查主键的唯一性冲突 */
  std::vector<RID> rids;
  for (const auto &index_info : indexes) {
    if (index_info->is_primary_key_) {
      Tuple insert_key =
          children_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->ScanKey(insert_key, &rids, txn);
      break;
    }
  }
  /** 存在唯一性冲突 */
  if (!rids.empty()) {
    RID modified_rid = rids[0];
    TupleMeta meta = table_info->table_->GetTupleMeta(modified_rid);
    std::optional<VersionUndoLink> original_version_link = txn_mgr->GetVersionLink(modified_rid);
    /** 自我修改 */
    if (meta.ts_ == txn->GetTransactionId()) {
      /** 只有tuple被删除时，才能自我修改 */
      if (!meta.is_deleted_) {
        txn->SetTainted();
        // std::cerr << "唯一键冲突（正在操作的事务是自己，但是tuple未删除）\n";
        throw ExecutionException("唯一键冲突（自我修改时，但是tuple未删除）");
      }
      /** 自我修改只需要update table heap */
      table_info->table_->UpdateTupleInPlace({txn->GetTransactionId(), false}, children_tuple, modified_rid);
      return;
    }
    TupleMeta meta_again = table_info->table_->GetTupleMeta(modified_rid);
    /** 其他事务正在操作，来自未来，或是没有被删除的tuple：都不能操作 */
    if ((original_version_link->in_progress_ && meta_again.ts_ != txn->GetTransactionId()) ||
        (meta_again.ts_ < TXN_START_ID && meta_again.ts_ > txn->GetReadTs()) || !meta_again.is_deleted_) {
      txn->SetTainted();
      throw ExecutionException("不能修改其他事务正在操作的tuple\n");
    }
    /** 获取tuple lock */
    if (!GetTupleLock(txn_mgr, modified_rid, original_version_link)) {
      txn->SetTainted();
      throw ExecutionException("唯一键冲突（check失败）");
    }
    /** 构建undo log */
    UndoLog undo_log;
    undo_log.is_deleted_ = true;
    undo_log.ts_ = meta_again.ts_;
    undo_log.modified_fields_.resize(table_info->schema_.GetColumnCount(), false);
    undo_log.prev_version_ = original_version_link->prev_;
    undo_log.tuple_ = table_info->table_->GetTuple(modified_rid).second;
    /** 维护txn的write set和undo logs */
    txn->AppendWriteSet(table_info->oid_, modified_rid);
    txn->AppendUndoLog(undo_log);
    /** 更新VersionLink */
    ConstructAndUpdateVersionLink(txn->GetTransactionId(), txn->GetUndoLogNum() - 1, txn_mgr, modified_rid);
    /** 只需要update table heap */
    table_info->table_->UpdateTupleInPlace({txn->GetTransactionId(), false}, children_tuple, modified_rid);
  } else { /** 不存在冲突，进行正常的insert逻辑 */
    /** 将tuple插入table hea */
    auto new_rid = table_info->table_->InsertTuple(TupleMeta{txn->GetTransactionId(), false}, children_tuple);
    if (!new_rid.has_value()) {
      txn->SetTainted();
      throw ExecutionException("Insert返回的rid无效");
    }
    /** 维护VersionLink，获取tuple lock */
    if (!GetTupleLock(txn_mgr, *new_rid, std::nullopt)) {
      txn->SetTainted();
      throw ExecutionException("唯一键冲突（不存在冲突时check失败）");
    }
    /** 维护事务的write set */
    txn->AppendWriteSet(table_info->oid_, *new_rid);
    /** 维护索引 */
    /** TODO:这里用传入的indexes是否有问题？ */
    for (const IndexInfo *index_info : indexes) {
      Tuple key_tuple =
          children_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      if (!index_info->index_->InsertEntry(key_tuple, *new_rid, txn)) {
        txn_mgr->UpdateVersionLink(*new_rid, VersionUndoLink{UndoLink{}, false});
        txn->SetTainted();
        throw ExecutionException("唯一键冲突（插入索引时失败）");
      }
    }
  }
}

}  // namespace bustub
