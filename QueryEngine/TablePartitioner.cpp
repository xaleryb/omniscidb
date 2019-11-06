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

#include "TablePartitioner.h"

TablePartitioner::TablePartitioner(const RelAlgExecutionUnit& ra_exe_unit,
                                   std::vector<InputColDescriptor> key_cols,
                                   std::vector<InputColDescriptor> payload_cols,
                                   const InputTableInfo& info,
                                   PartitioningOptions po,
                                   Executor* executor,
                                   std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner)
    : key_cols_(std::move(key_cols))
    , payload_cols_(std::move(payload_cols))
    , info_(info)
    , po_(std::move(po))
    , executor_(executor)
    , ra_exe_unit_(ra_exe_unit)
    , row_set_mem_owner_(row_set_mem_owner) {}

TemporaryTable TablePartitioner::runPartitioning() {
  // This vector holds write positions (in number of elements, not bytes)
  // in partitions for each partitioned fragment.
  // [fragment idx][partition id] -> offset in partition buffer.
  std::vector<std::vector<size_t>> partition_offsets;
  computePartitionSizesAndOffsets(partition_offsets);

  // Prepare some aux structures for memory descriptors and result sets.
  // ColSlotContext consumes Expr plain pointers and we use col_vars
  // to own memory. It can be released after ColSlotContext creation.
  std::vector<std::shared_ptr<Analyzer::ColumnVar>> col_vars;
  std::vector<Analyzer::Expr*> slots;
  std::vector<TargetInfo> targets;
  for (auto& col : key_cols_) {
    auto col_var = createColVar(col);
    slots.push_back(col_var.get());
    col_vars.emplace_back(std::move(col_var));
    targets.emplace_back(
        TargetInfo{false, kMIN, getType(col), SQLTypeInfo(), false, false});
  }
  for (auto& col : payload_cols_) {
    auto col_var = createColVar(col);
    slots.push_back(col_var.get());
    col_vars.emplace_back(std::move(col_var));
    targets.emplace_back(
        TargetInfo{false, kMIN, getType(col), SQLTypeInfo(), false, false});
  }
  std::vector<ssize_t> dummy;
  ColSlotContext slot_ctx(slots, dummy);
  slot_ctx.setAllSlotsPaddedSizeToLogicalSize();

  // Now we can allocate parition's buffers.
  std::vector<ResultSetPtr> partitions;
  // [partition id][column idx] -> column buffer.
  std::vector<std::vector<int8_t*>> col_bufs;
  col_bufs.resize(partition_sizes_.size());
  for (size_t frag_id = 0; frag_id < partition_sizes_.size(); ++frag_id) {
    QueryMemoryDescriptor mem_desc(QueryDescriptionType::Projection,
                                   executor_,
                                   slot_ctx,
                                   partition_sizes_[frag_id],
                                   true);
    auto rs = std::make_shared<ResultSet>(
        targets, ExecutorDeviceType::CPU, mem_desc, row_set_mem_owner_, executor_);
    rs->setCachedRowCount(partition_sizes_[frag_id]);
    col_bufs[frag_id].resize(targets.size(), nullptr);

    // Init storage for non-empty partitions only.
    if (partition_sizes_[frag_id]) {
      auto* storage = rs->allocateStorage();
      for (size_t col_idx = 0; col_idx < targets.size(); ++col_idx) {
        size_t col_offs = mem_desc.getColOffInBytes(col_idx);
        int8_t* col_buf = storage->getUnderlyingBuffer() + col_offs;
        col_bufs[frag_id][col_idx] = col_buf;
      }
    }
    partitions.push_back(rs);
  }

  for (size_t i = 0; i < info_.info.fragments.size(); ++i) {
    // run partitioning function
  }

  return TemporaryTable(partitions);
}

size_t TablePartitioner::getPartitionsCount() const {
  return 1 << po_.mask_bits;
}

void TablePartitioner::computePartitionSizesAndOffsets(
    std::vector<std::vector<size_t>>& partition_offsets) {
  auto pcnt = getPartitionsCount();
  std::vector<std::vector<int64_t>> histograms(info_.info.fragments.size());

  // Run histogram collection.
  for (size_t i = 0; i < info_.info.fragments.size(); ++i) {
    // run histogram collection function.
  }

  // Count partition sizes and offsets (in number of tuples).
  for (size_t i = 0; i < (histograms.size() - 1); ++i) {
    for (size_t j = 0; j < pcnt; ++j) {
      partition_offsets[i + 1][j] = partition_offsets[i][j] + histograms[i][j];
    }
  }

  for (size_t j = 0; j < pcnt; ++j) {
    partition_sizes_[j] = partition_offsets.back()[j] + histograms.back()[j];
  }
}

SQLTypeInfo TablePartitioner::getType(const InputColDescriptor& col) {
  auto& cat = *executor_->getCatalog();
  auto table_id = col.getScanDesc().getTableId();

  if (table_id < 0)
    throw std::runtime_error(
        "Failed to patition table: virtual tables are not supported");

  auto* desc = get_column_descriptor(col.getColId(), table_id, cat);
  CHECK(desc);

  return desc->columnType;
}

std::shared_ptr<Analyzer::ColumnVar> TablePartitioner::createColVar(
    const InputColDescriptor& col) {
  auto& cat = *executor_->getCatalog();
  auto table_id = col.getScanDesc().getTableId();

  if (table_id < 0)
    throw std::runtime_error(
        "Failed to patition table: virtual tables are not supported");

  auto* desc = get_column_descriptor(col.getColId(), table_id, cat);
  CHECK(desc);

  return std::make_shared<Analyzer::ColumnVar>(
      desc->columnType, table_id, col.getColId(), col.getScanDesc().getNestLevel());
}
