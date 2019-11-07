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

#include "Partitioning.h"
#include "Execute.h"
#include "JoinHashTable.h"
#include "TablePartitioner.h"

namespace {

// Structure to describe input table replacement with
// a temporary table (partitioned version).
struct InputTableMap {
  // Id of table to replace.
  int table_id;
  int scan_idx;
  // Old col id -> new col id.
  std::unordered_map<int, int> col_map;
  // New table.
  TemporaryTable table;
};

using InputTableMaps = std::unordered_map<InputDescriptor, InputTableMap>;

InputTableMap performTablePartitioning(
    const std::vector<const Analyzer::ColumnVar*>& key_cols,
    const RelAlgExecutionUnit& ra_exe_unit,
    const CompilationOptions& co,
    const ExecutionOptions& eo,
    const std::vector<InputTableInfo>& query_infos,
    ColumnCacheMap& column_cache,
    Executor* executor,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner) {
  auto table_id = key_cols.front()->get_table_id();
  auto scan_idx = key_cols.front()->get_rte_idx();

  if (table_id < 0) {
    throw std::runtime_error(
        "Hash join failed: result set partitioning is not yet supported");
  }

  // Collect keys columns for partitioning.
  std::vector<InputColDescriptor> key;
  std::vector<InputColDescriptor> payload;
  std::unordered_map<int, int> col_map;
  for (auto& key_col : key_cols) {
    CHECK_EQ(key_col->get_table_id(), table_id);
    CHECK_EQ(key_col->get_rte_idx(), scan_idx);
    col_map[key_col->get_column_id()] = key.size();
    key.emplace_back(key_col->get_column_id(), table_id, scan_idx);
  }

  // Collect payload columns for partitioning.
  for (auto& col : ra_exe_unit.input_col_descs) {
    if (col->getScanDesc().getTableId() == table_id &&
        col->getScanDesc().getNestLevel() == scan_idx &&
        !col_map.count(col->getColId())) {
      col_map[col->getColId()] = key.size() + payload.size();
      payload.emplace_back(col->getColId(), table_id, scan_idx);
    }
  }

  // Select fragments to process.
  const auto& table_info = get_inner_query_info(table_id, query_infos);

  PartitioningOptions po;
  // TODO: pass this value through the option?
  po.mask_bits = 3;
  po.scale_bits = 0;
  TablePartitioner partitioner(ra_exe_unit,
                               key,
                               payload,
                               table_info,
                               column_cache,
                               po,
                               executor,
                               row_set_mem_owner);
  auto tmp_table = partitioner.runPartitioning();

  // Fix-up execution unit and query infos to use partitions
  // instead of original table.
  // replaceInputTable(ra_exe_unit, query_infos, table_id, scan_idx,
  // std::move(partitions));

  // TODO: register and cache partitions somewhere for
  // re-usage.
  return {table_id, scan_idx, col_map, std::move(tmp_table)};
}

RelAlgExecutionUnit performPartitioningForQualifier(
    const std::shared_ptr<Analyzer::BinOper>& qual_bin_oper,
    const RelAlgExecutionUnit& ra_exe_unit,
    const CompilationOptions& co,
    const ExecutionOptions& eo,
    const std::vector<InputTableInfo>& query_infos,
    ColumnCacheMap& column_cache,
    Executor* executor,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner) {
  std::vector<InnerOuter> inner_outer_pairs;
  try {
    inner_outer_pairs = normalize_column_pairs(
        qual_bin_oper.get(), *executor->getCatalog(), executor->getTemporaryTables());
  } catch (HashJoinFail&) {
    return ra_exe_unit;
  }

  std::vector<const Analyzer::ColumnVar*> inner_key;
  std::vector<const Analyzer::ColumnVar*> outer_key;
  for (auto& pr : inner_outer_pairs) {
    inner_key.push_back(pr.first);
    auto outer_col = dynamic_cast<const Analyzer::ColumnVar*>(pr.second);
    if (!outer_col) {
      throw std::runtime_error(
          "Hash join failed: cannot use radix hash join for given expression");
    }
    outer_key.push_back(outer_col);
  }

  performTablePartitioning(inner_key,
                           ra_exe_unit,
                           co,
                           eo,
                           query_infos,
                           column_cache,
                           executor,
                           row_set_mem_owner);
  performTablePartitioning(outer_key,
                           ra_exe_unit,
                           co,
                           eo,
                           query_infos,
                           column_cache,
                           executor,
                           row_set_mem_owner);

  return ra_exe_unit;
}

}  // namespace

RelAlgExecutionUnit performTablesPartitioning(
    const RelAlgExecutionUnit& ra_exe_unit,
    const CompilationOptions& co,
    const ExecutionOptions& eo,
    ColumnCacheMap& column_cache,
    Executor* executor,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner) {
  if (!g_force_radix_join)
    return ra_exe_unit;

  if (ra_exe_unit.join_quals.empty())
    return ra_exe_unit;

  if (ra_exe_unit.join_quals.size() > 1)
    throw std::runtime_error(
        "Hash join failed: radix hash join is not yet supported for deep joins");

  if (co.device_type_ == ExecutorDeviceType::GPU)
    throw std::runtime_error(
        "Hash join failed: radix hash join for GPU is not yet supported");

  auto query_infos = get_table_infos(ra_exe_unit, executor);
  auto& join_condition = ra_exe_unit.join_quals.front();
  for (const auto& join_qual : join_condition.quals) {
    auto qual_bin_oper = std::dynamic_pointer_cast<Analyzer::BinOper>(join_qual);
    if (!qual_bin_oper || !IS_EQUIVALENCE(qual_bin_oper->get_optype()))
      continue;

    return performPartitioningForQualifier(qual_bin_oper,
                                           ra_exe_unit,
                                           co,
                                           eo,
                                           query_infos,
                                           column_cache,
                                           executor,
                                           row_set_mem_owner);
  }

  return ra_exe_unit;
}