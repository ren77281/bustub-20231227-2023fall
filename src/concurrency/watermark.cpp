#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  if (++this->current_reads_[read_ts] == 1) {
    this->min_read_ts_.insert(read_ts);
    this->watermark_ = *this->min_read_ts_.begin();
  }
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  if (--this->current_reads_[read_ts] == 0) {
    this->min_read_ts_.erase(read_ts);
    this->current_reads_.erase(read_ts);
    this->watermark_ = *this->min_read_ts_.begin();
  }
}

}  // namespace bustub
