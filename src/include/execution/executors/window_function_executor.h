//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * The WindowFunctionExecutor executor executes a window function for columns using window function.
 *
 * Window function is different from normal aggregation as it outputs one row for each inputing rows,
 * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
 * normal selected columns and placeholder columns for window functions.
 *
 * For example, if we have a query like:
 *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
 *      FROM table;
 *
 * The WindowFunctionPlanNode contains following structure:
 *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
 *    window_functions_: {
 *      3: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.2}
 *        order_by: std::vector<AbstractExpressionRef>{0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.3}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *      4: {
 *        partition_by: std::vector<AbstractExpressionRef>{0.1}
 *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
 *        functions: std::vector<AbstractExpressionRef>{0.4}
 *        window_func_type: WindowFunctionType::SumAggregate
 *      }
 *    }
 *
 * Your executor should use child executor and exprs in columns to produce selected columns except for window
 * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
 * generate window function columns results. Directly use placeholders for window function columns in columns is
 * not allowed, as it contains invalid column id.
 *
 * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
 * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
 * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
 *
 */
/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
class WindowFunctionHashTable {
 public:
  WindowFunctionHashTable() = default;
  explicit WindowFunctionHashTable(const WindowFunctionType &agg_types) : agg_types_{agg_types} {}

  auto GenerateInitialAggregateValue() -> Value {
    std::vector<Value> values{};
    switch (this->agg_types_) {
      case WindowFunctionType::CountStarAggregate:
        return ValueFactory::GetIntegerValue(0);
      case WindowFunctionType::Rank:
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        // Others starts at null.
        return ValueFactory::GetNullValueByType(TypeId::INTEGER);
    }
    return {};
  }

  void CombineAggregateValues(Value &result, const Value &input) {
    /** null值不参与运算 */
    if (!input.IsNull()) {
      switch (this->agg_types_) {
        case WindowFunctionType::CountStarAggregate:
          result = result.Add(ValueFactory::GetIntegerValue(1));
          break;
        case WindowFunctionType::CountAggregate:
          if (result.IsNull()) {
            result = ValueFactory::GetIntegerValue(1);
          } else {
            result = result.Add(ValueFactory::GetIntegerValue(1));
          }
          break;
        case WindowFunctionType::Rank:
        case WindowFunctionType::SumAggregate:
          if (result.IsNull()) {
            result = input;
          } else {
            result = result.Add(input);
          }
          break;
        case WindowFunctionType::MinAggregate:
          if (result.IsNull()) {
            result = input;
          } else {
            result = result.Min(input);
          }
          break;
        case WindowFunctionType::MaxAggregate:
          if (result.IsNull()) {
            result = input;
          } else {
            result = result.Max(input);
          }
          break;
      }
    }
  }

  void InsertCombine(const AggregateKey &win_key, const Value &win_val) {
    if (ht_.count(win_key) == 0) {
      ht_.insert({win_key, GenerateInitialAggregateValue()});
    }
    CombineAggregateValues(ht_[win_key], win_val);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

  /** @return The Value of the Key */
  auto Get(const AggregateKey &key) -> const Value & { return ht_[key]; }

  auto Count(const AggregateKey &key) const -> uint32_t { return ht_.count(key); }

  /** @return hash table is empty? */
  auto Empty() -> bool { return ht_.empty(); }

  /** The hash table is just a map from Window keys to Window values */
  std::unordered_map<AggregateKey, Value> ht_{};
  /** The types of aggregations that we have */
  WindowFunctionType agg_types_;
};

class WindowFunctionExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new WindowFunctionExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The window aggregation plan to be executed
   */
  WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the window aggregation */
  void Init() override;

  /**
   * Yield the next tuple from the window aggregation.
   * @param[out] tuple The next tuple produced by the window aggregation
   * @param[out] rid The next tuple RID produced by the window aggregation
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the window aggregation plan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** @return The tuple as an AggregateKey */
  auto MakeAggregateKey(const Tuple *tuple, const std::vector<AbstractExpressionRef> &partition_by,
                        const Schema &old_schema) -> AggregateKey {
    std::vector<Value> keys;
    keys.reserve(partition_by.size());
    for (const auto &expr : partition_by) {
      keys.emplace_back(expr->Evaluate(tuple, old_schema));
    }
    return {keys};
  }

  auto MakeOrderKey(const Tuple *tuple, const std::vector<std::pair<OrderByType, AbstractExpressionRef>> &order_by,
                    const Schema &old_schema) -> AggregateKey {
    std::vector<Value> keys;
    keys.reserve(order_by.size());
    for (const auto &[_, expr] : order_by) {
      keys.emplace_back(expr->Evaluate(tuple, old_schema));
    }
    return {keys};
  }

  struct PairHash {
    template <class T1, class T2>
    auto operator()(const std::pair<T1, T2> &p) const -> size_t {
      size_t curr_hash = 0;
      Value l = ValueFactory::GetIntegerValue(p.first);
      Value r = ValueFactory::GetIntegerValue(p.second);
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&l));
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&r));
      return curr_hash;
    }
  };

 private:
  /** The window aggregation plan node to be executed */
  const WindowFunctionPlanNode *plan_;

  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> results_;
  std::vector<Tuple>::reverse_iterator it_;
  /** 窗口函数的聚合表达式（获取列值） */
  std::unordered_map<uint32_t, AbstractExpressionRef> functions_;
  /** 窗口函数所在col idx */
  std::vector<uint32_t> window_func_indexs_;
  /** 窗口函数的类型 */
  std::unordered_map<uint32_t, WindowFunctionType> agg_types_;
  /** 保存聚合结果 */
  std::unordered_map<std::pair<size_t, size_t>, Value, PairHash> agg_results_;
  /** 保存分组情况 */
  std::unordered_map<uint32_t, std::vector<AbstractExpressionRef>> partions_;
  /** 保存排序情况 */
  std::unordered_map<uint32_t, std::vector<std::pair<OrderByType, AbstractExpressionRef>>> order_bys_;
};
}  // namespace bustub
