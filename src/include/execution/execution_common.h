#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "storage/table/tuple.h"

namespace bustub {

/** 回溯版本链，重建tuple */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

auto ConstructUndoLog(const Tuple &old_tuple, TableInfo *table_info,
                      const std::vector<AbstractExpressionRef> &expressions, timestamp_t ts,
                      std::optional<UndoLink> link, std::optional<UndoLog> old_undo_log, Tuple &new_tuple) -> UndoLog;

auto GetNewTuple(const Tuple &old_tuple, const Schema &schema, const std::vector<AbstractExpressionRef> &expressions)
    -> Tuple;

void ConstructAndUpdateVersionLink(txn_id_t txn_id, size_t undo_log_idx, TransactionManager *txn_mgr, RID rid);

auto GetDeleteUndoLog(const Schema &schema, const UndoLink &link, timestamp_t ts, const Tuple &tuple) -> UndoLog;

/** 用于scan时，遍历版本链并重建tuple，根据返回值判断是否重建成功 */
auto FindFirstUndoLog(const RID &rid, const TableInfo *table_info, TransactionManager *txn_mgr, Transaction *txn,
                      std::optional<AbstractExpressionRef> filter) -> std::optional<Tuple>;

/** 检测tuple是否被完全删除 */
auto IsTupleDelete(const TableInfo *table_info, TransactionManager *txn_mgr, const RID &rid) -> bool;

/** 获取tuple lock */
auto GetTupleLock(TransactionManager *txn_mgr, RID rid, std::optional<VersionUndoLink> original_version_link) -> bool;

void DeleteTuple(const RID &modified_rid, TableInfo *table_info, TransactionManager *txn_mgr, Transaction *txn);

void InsertTuple(const std::vector<IndexInfo *> &indexes, Tuple children_tuple, TableInfo *table_info,
                 TransactionManager *txn_mgr, Transaction *txn);

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
