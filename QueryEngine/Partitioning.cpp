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

void performTablePartitioning(const std::vector<const Analyzer::ColumnVar*>& key_cols,
                              RelAlgExecutionUnit& ra_exe_unit,
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
  std::unordered_set<int64_t> key_col_ids;
  std::vector<InputColDescriptor> key;
  std::vector<InputColDescriptor> payload;
  for (auto& key_col : key_cols) {
    CHECK_EQ(key_col->get_table_id(), table_id);
    CHECK_EQ(key_col->get_rte_idx(), scan_idx);
    key_col_ids.insert(key_col->get_column_id());
    key.emplace_back(key_col->get_column_id(), table_id, scan_idx);
  }

  // Collect payload columns for partitioning.
  for (auto& col : ra_exe_unit.input_col_descs) {
    if (col->getScanDesc().getTableId() == table_id &&
        col->getScanDesc().getNestLevel() && !key_col_ids.count(col->getColId())) {
      payload.emplace_back(col->getColId(), table_id, scan_idx);
    }
  }

  // Select fragments to process.
  const auto& table_info = get_inner_query_info(table_id, query_infos);

  PartitioningOptions po;
  TablePartitioner partitioner(
      ra_exe_unit, key, payload, table_info, po, executor, row_set_mem_owner);
  auto tmp_table = partitioner.runPartitioning();

  // Fix-up execution unit and query infos to use partitions
  // instead of original table.
  // replaceInputTable(ra_exe_unit, query_infos, table_id, scan_idx,
  // std::move(partitions));

  // TODO: register and cache partitions somewhere for
  // re-usage.
}

void performPartitioningForQualifier(
    const std::shared_ptr<Analyzer::BinOper>& qual_bin_oper,
    RelAlgExecutionUnit& ra_exe_unit,
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
    return;
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
}

}  // namespace

void performTablesPartitioning(RelAlgExecutionUnit& ra_exe_unit,
                               const CompilationOptions& co,
                               const ExecutionOptions& eo,
                               const std::vector<InputTableInfo>& query_infos,
                               ColumnCacheMap& column_cache,
                               Executor* executor,
                               std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner) {
  if (!g_force_radix_join)
    return;

  if (ra_exe_unit.join_quals.empty())
    return;

  if (ra_exe_unit.join_quals.size() > 1)
    throw std::runtime_error(
        "Hash join failed: radix hash join is not yet supported for deep joins");

  if (co.device_type_ == ExecutorDeviceType::GPU)
    throw std::runtime_error(
        "Hash join failed: radix hash join for GPU is not yet supported");

  auto& join_condition = ra_exe_unit.join_quals.front();
  for (const auto& join_qual : join_condition.quals) {
    auto qual_bin_oper = std::dynamic_pointer_cast<Analyzer::BinOper>(join_qual);
    if (!qual_bin_oper || !IS_EQUIVALENCE(qual_bin_oper->get_optype()))
      continue;

    performPartitioningForQualifier(qual_bin_oper,
                                    ra_exe_unit,
                                    co,
                                    eo,
                                    query_infos,
                                    column_cache,
                                    executor,
                                    row_set_mem_owner);
  }
}