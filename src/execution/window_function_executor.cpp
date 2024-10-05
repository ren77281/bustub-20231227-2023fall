#include "execution/executors/window_function_executor.h"
#include <memory>
#include "catalog/schema.h"
#include "execution/executors/sort_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  this->child_executor_->Init();
  Tuple tuple{};
  RID rid{};
  /** 先保存临时表 */
  std::vector<Tuple> tmp_table;
  while (this->child_executor_->Next(&tuple, &rid)) {
    tmp_table.push_back(tuple);
  }
  /** 保存当前的列值表达式 */
  const std::vector<AbstractExpressionRef> &columns = this->plan_->columns_;
  /** 保存原来的Schema */
  const Schema &old_schema = this->child_executor_->GetOutputSchema();
  /** 获取窗口函数所在col idx，相应的function以及类型，并判断是否存在order-by */
  bool is_order_by = false;
  for (const auto &window_function : this->plan_->window_functions_) {
    this->window_func_indexs_.push_back(window_function.first);
    this->agg_types_.insert({window_function.first, window_function.second.type_});
    this->partions_.insert({window_function.first, window_function.second.partition_by_});
    this->functions_.insert({window_function.first, window_function.second.function_});
    this->order_bys_.insert({window_function.first, window_function.second.order_by_});
    /** 如果存在order-by，先排序 */
    if (!is_order_by && !window_function.second.order_by_.empty()) {
      is_order_by = true;
      std::sort(tmp_table.begin(), tmp_table.end(), TupleCompare(window_function.second.order_by_, old_schema));
    }
  }
  /** 遍历窗口函数列，构造agg_results_tmp */
  std::unordered_map<uint32_t, WindowFunctionHashTable> agg_results_tmp;
  for (const auto &func_idx : this->window_func_indexs_) {
    WindowFunctionHashTable ht(this->agg_types_[func_idx]);
    agg_results_tmp.insert({func_idx, ht});
  }
  /** 用于Rank */
  uint32_t same = 1;
  for (const auto &func_idx : this->window_func_indexs_) {
    auto &ht = agg_results_tmp[func_idx];
    /** 特判Rank */
    if (this->agg_types_[func_idx] == WindowFunctionType::Rank) {
      for (size_t i = 0; i < tmp_table.size(); i++) {
        auto win_key = this->MakeAggregateKey(&tmp_table[i], this->partions_[func_idx], old_schema);
        auto order_key = this->MakeOrderKey(&tmp_table[i], this->order_bys_[func_idx], old_schema);
        /** 第一行不用与上一行进行判断 */
        if (i == 0) {
          ht.InsertCombine(win_key, this->functions_[func_idx]->Evaluate(&tmp_table[i], old_schema));
          this->agg_results_.insert({{i, func_idx}, ht.Get(win_key)});
        } else {
          auto last_win_key = this->MakeAggregateKey(&tmp_table[i - 1], this->partions_[func_idx], old_schema);
          auto last_order_key = this->MakeOrderKey(&tmp_table[i - 1], this->order_bys_[func_idx], old_schema);
          /** 当前行等于上一行 */
          if (last_order_key == order_key) {
            this->agg_results_.insert({{i, func_idx}, ht.Get(last_win_key)});
            same++;
            continue;
          }
          /** 当前行不等于上一行且属于一个分组 */
          if (last_win_key == win_key) {
            ht.InsertCombine(win_key, ValueFactory::GetIntegerValue(same));
          } else {
            ht.InsertCombine(win_key, this->functions_[func_idx]->Evaluate(&tmp_table[i], old_schema));
          }
          this->agg_results_.insert({{i, func_idx}, ht.Get(win_key)});
        }
        same = 1;
      }
    } else { /** 非Rank类型的窗口函数 */
      for (size_t i = 0; i < tmp_table.size(); i++) {
        auto win_key = this->MakeAggregateKey(&tmp_table[i], this->partions_[func_idx], old_schema);
        ht.InsertCombine(win_key, this->functions_[func_idx]->Evaluate(&tmp_table[i], old_schema));
        this->agg_results_.insert({{i, func_idx}, ht.Get(win_key)});
      }
    }
  }
  /** 构造最终结果 */
  size_t col_num = columns.size();
  int32_t sz = tmp_table.size();
  std::vector<Value> prev_values;
  for (int32_t i = sz - 1; i >= 0; i--) {
    /** 根据当前Schema，填入Value */
    std::vector<Value> values;
    for (size_t j = 0; j < col_num; j++) {
      /** 当前列是窗口函数列 */
      if (this->plan_->window_functions_.count(j) != 0) {
        if (i != sz - 1) {
          auto win_key = this->MakeAggregateKey(&tmp_table[i], this->partions_[j], old_schema);
          auto prev_win_key = this->MakeAggregateKey(&tmp_table[i + 1], this->partions_[j], old_schema);
          if (!is_order_by && win_key == prev_win_key) {
            values.push_back(prev_values[j]);
            continue;
          }
        }
        /** TODO:关于partition和order-by可以进行优化 */
        values.push_back(this->agg_results_[{i, j}]);
      } else {
        values.push_back(columns[j]->Evaluate(&tmp_table[i], old_schema));
      }
    }
    this->results_.emplace_back(values, &this->GetOutputSchema());
    prev_values = values;
  }
  this->it_ = this->results_.rbegin();
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (this->it_ == this->results_.rend()) {
    return false;
  }
  *tuple = *this->it_;
  ++this->it_;
  return true;
}

}  // namespace bustub
