/*
 * Copyright 2019 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "RadixJoinHashTable.h"
#include "Execute.h"
#include "JoinHashTable.h"

std::shared_ptr<RadixJoinHashTable> RadixJoinHashTable::getInstance(
    const std::shared_ptr<Analyzer::BinOper> qual_bin_oper,
    const std::vector<InputTableInfo>& query_infos,
    const Data_Namespace::MemoryLevel memory_level,
    const HashType preferred_hash_type,
    const int device_count,
    ColumnCacheMap& column_cache,
    Executor* executor) {
  auto join_hash_table =
      std::shared_ptr<RadixJoinHashTable>(new RadixJoinHashTable(qual_bin_oper,
                                                                 query_infos,
                                                                 memory_level,
                                                                 preferred_hash_type,
                                                                 device_count,
                                                                 column_cache,
                                                                 executor));
  join_hash_table->reify(device_count);

  return join_hash_table;
}

RadixJoinHashTable::RadixJoinHashTable(
    const std::shared_ptr<Analyzer::BinOper> qual_bin_oper,
    const std::vector<InputTableInfo>& query_infos,
    const Data_Namespace::MemoryLevel memory_level,
    const HashType preferred_hash_type,
    const int device_count,
    ColumnCacheMap& column_cache,
    Executor* executor)
    : qual_bin_oper_(qual_bin_oper)
    , query_infos_(query_infos)
    , memory_level_(memory_level)
    , layout_(preferred_hash_type)
    , column_cache_(column_cache)
    , executor_(executor) {
  auto inner_outer_pairs_ = normalize_column_pairs(
      qual_bin_oper.get(), *executor->getCatalog(), executor->getTemporaryTables());
  CHECK(!inner_outer_pairs_.empty());
}

int64_t RadixJoinHashTable::getJoinHashBuffer(const ExecutorDeviceType device_type,
                                              const int device_id,
                                              const int partition_id) const noexcept {
  auto it = part_tables_.find(partition_id);
  if (it != part_tables_.end())
    return it->second->getJoinHashBuffer(device_type, device_id);
  return 0;
}

size_t RadixJoinHashTable::getJoinHashBufferSize(const ExecutorDeviceType device_type,
                                                 const int device_id,
                                                 const int partition_id) const noexcept {
  auto it = part_tables_.find(partition_id);
  if (it != part_tables_.end())
    return it->second->getJoinHashBuffer(device_type, device_id);
  return 0;
}

std::string RadixJoinHashTable::toString(const ExecutorDeviceType device_type,
                                         const int device_id,
                                         bool raw) const noexcept {
  std::stringstream ss;
  for (auto& pr : part_tables_) {
    ss << "P" << pr.first << " { " << pr.second->toString(device_type, device_id, raw)
       << " } ";
  }
  return ss.str();
}

std::set<DecodedJoinHashBufferEntry> RadixJoinHashTable::decodeJoinHashBuffer(
    const ExecutorDeviceType device_type,
    const int device_id) const noexcept {
  std::set<DecodedJoinHashBufferEntry> res;
  for (auto& pr : part_tables_) {
    auto part_res = pr.second->decodeJoinHashBuffer(device_type, device_id);
    res.insert(part_res.begin(), part_res.end());
  }
  return res;
}

llvm::Value* RadixJoinHashTable::codegenSlot(const CompilationOptions& co,
                                             const size_t index) {
  // All tables are the same for now, so pick any and use it to generate code.
  CHECK(!part_tables_.empty());
  return part_tables_.begin()->second->codegenSlot(co, index);
}

HashJoinMatchingSet RadixJoinHashTable::codegenMatchingSet(const CompilationOptions& co,
                                                           const size_t index) {
  // All tables are the same for now, so pick any and use it to generate code.
  CHECK(!part_tables_.empty());
  return part_tables_.begin()->second->codegenMatchingSet(co, index);
}

int RadixJoinHashTable::getInnerTableId() const noexcept {
  CHECK(!inner_outer_pairs_.empty());
  const auto first_inner_col = inner_outer_pairs_.front().first;
  return first_inner_col->get_table_id();
}

int RadixJoinHashTable::getInnerTableRteIdx() const noexcept {
  CHECK(!inner_outer_pairs_.empty());
  const auto first_inner_col = inner_outer_pairs_.front().first;
  return first_inner_col->get_rte_idx();
}

JoinHashTableInterface::HashType RadixJoinHashTable::getHashType() const noexcept {
  return layout_;
}

size_t RadixJoinHashTable::offsetBufferOff(const int partition_id) const noexcept {
  auto it = part_tables_.find(partition_id);
  if (it != part_tables_.end())
    return it->second->offsetBufferOff();
  return 0;
}

size_t RadixJoinHashTable::countBufferOff(const int partition_id) const noexcept {
  auto it = part_tables_.find(partition_id);
  if (it != part_tables_.end())
    return it->second->countBufferOff();
  return 0;
}

size_t RadixJoinHashTable::payloadBufferOff(const int partition_id) const noexcept {
  auto it = part_tables_.find(partition_id);
  if (it != part_tables_.end())
    return it->second->payloadBufferOff();
  return 0;
}

void RadixJoinHashTable::reify(const int device_count) {
  // Currently all base hash tables share the same layout
  layout_ = HashType::OneToMany;

  // TODO: partitioning
  // TODO: check for paritions cache
  // TODO: check for hash table cache
  // TODO: build sub-tables
}