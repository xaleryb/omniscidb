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

#include "ColumnFetcher.h"

TablePartitioner::TablePartitioner(const RelAlgExecutionUnit& ra_exe_unit,
                                   std::vector<InputColDescriptor> key_cols,
                                   std::vector<InputColDescriptor> payload_cols,
                                   const InputTableInfo& info,
                                   ColumnCacheMap& column_cache,
                                   PartitioningOptions po,
                                   Executor* executor,
                                   std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner)
    : key_cols_(std::move(key_cols))
    , payload_cols_(std::move(payload_cols))
    , info_(info)
    , column_cache_(column_cache)
    , po_(std::move(po))
    , executor_(executor)
    , ra_exe_unit_(ra_exe_unit)
    , row_set_mem_owner_(row_set_mem_owner) {}

void TablePartitioner::fetchFragment(const Fragmenter_Namespace::FragmentInfo& frag,
                                     size_t frag_num,
                                     std::vector<const Analyzer::ColumnVar*>& vars,
                                     std::vector<const int8_t*>& output) {
  std::vector<std::shared_ptr<Chunk_NS::Chunk>> chunks_owner;
  const int8_t* col_frag = nullptr;
  size_t elem_count = 0;
  for (auto var : vars) {
    std::tie(col_frag, elem_count) =
        ColumnFetcher::getOneColumnFragment(executor_,
                                            *(var),
                                            frag,
                                            Data_Namespace::CPU_LEVEL,
                                            0,
                                            chunks_owner,
                                            column_cache_);
    if (col_frag == nullptr) {
      continue;
    }
    CHECK_NE(elem_count, size_t(0));
    output.push_back(col_frag);
  }
}

void TablePartitioner::fetchFragments(
    std::vector<const Analyzer::ColumnVar*>& key_vars,
    std::vector<const Analyzer::ColumnVar*>& payload_vars) {
  // get all fragments for columns separately
  // so far only one key column is considered
  std::vector<std::shared_ptr<Chunk_NS::Chunk>> chunks_owner;
  std::vector<const int8_t*> empty;
  key_data_.assign(info_.info.fragments.size(), empty);
  payload_data_.assign(info_.info.fragments.size(), empty);
  size_t frag_idx = 0;
  for (auto& frag : info_.info.fragments) {
    fetchFragment(frag, frag_idx, key_vars, key_data_[frag_idx]);
    fetchFragment(frag, frag_idx, payload_vars, payload_data_[frag_idx]);
    frag_idx++;
  }
}

TemporaryTable TablePartitioner::runPartitioning() {
  // This vector holds write positions (in number of elements, not bytes)
  // in partitions for each partitioned fragment.
  // [fragment idx][partition id] -> offset in partition buffer.
  std::vector<std::vector<size_t>> partition_offsets;
  computePartitionSizesAndOffsets(partition_offsets);

  std::vector<const Analyzer::ColumnVar*> key_vars;
  std::vector<const Analyzer::ColumnVar*> payload_vars;
  // Prepare some aux structures for memory descriptors and result sets.
  // ColSlotContext consumes Expr plain pointers and we use col_vars
  // to own memory. It can be released after ColSlotContext creation.
  std::vector<std::shared_ptr<Analyzer::ColumnVar>> col_vars;
  std::vector<Analyzer::Expr*> slots;
  std::vector<TargetInfo> targets;
  for (auto& col : key_cols_) {
    auto col_var = createColVar(col);
    slots.push_back(col_var.get());
    // fill info for obtaining key columns
    key_vars.push_back(col_var.get());
    key_sizes_.push_back(col_var->get_type_info().get_size());
    targets.emplace_back(
        TargetInfo{false, kMIN, col_var->get_type_info(), SQLTypeInfo(), false, false});
    col_vars.emplace_back(std::move(col_var));
  }
  for (auto& col : payload_cols_) {
    auto col_var = createColVar(col);
    slots.push_back(col_var.get());
    // fill info for obtaining payload columns
    payload_vars.push_back(col_var.get());
    payload_sizes_.push_back(col_var->get_type_info().get_size());
    targets.emplace_back(
        TargetInfo{false, kMIN, col_var->get_type_info(), SQLTypeInfo(), false, false});
    col_vars.emplace_back(std::move(col_var));
  }
  std::vector<ssize_t> dummy;
  ColSlotContext slot_ctx(slots, dummy);
  slot_ctx.setAllSlotsPaddedSizeToLogicalSize();

  // fetch column data - keys and payloads
  fetchFragments(key_vars, payload_vars);

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
    doPartition(i, col_bufs);
  }

  return TemporaryTable(partitions);
}

size_t TablePartitioner::getPartitionsCount() const {
  return 1 << po_.mask_bits;
}

#define HASH_BIT_MODULO(K, MASK, NBITS) (((K)&MASK) >> NBITS)

uint32_t TablePartitioner::getHashValue(const int8_t* key,
                                        int size,
                                        int mask,
                                        int shift) {
  uint64_t value = 0;
  // FIXME: only one key column!
  switch (size) {
    case 2:
      value = *((int16_t*)key);
      break;
    case 4:
      value = *((int32_t*)key);
      break;
    case 8:
      value = *((int64_t*)key);
      break;
    default:
      CHECK(false);
  }
  return HASH_BIT_MODULO(value, mask, shift);
}

void TablePartitioner::collectHistogram(int frag_idx, std::vector<size_t>& histogram) {
  uint64_t i;
  const uint32_t fanOut = getPartitionsCount();
  // FIXME: only one key column for now!
  auto fragment = key_data_[frag_idx].at(0);
  auto fragment_size = info_.info.fragments[frag_idx].getNumTuples();
  // FIXME: only one key column for now!
  auto key_size = key_sizes_.at(0);
  for (i = 0; i < fragment_size; ++i) {
    uint32_t idx = getHashValue(&(fragment[i * key_size]), key_size, fanOut - 1, 0);
    histogram[idx]++;
  }

  /* compute local prefix sum on hist */
#if 0
  uint64_t sum = 0;
  for (i = 0; i < fanOut; i++) {
    sum += histogram[i];
    histogram[i] = sum;
  }
#endif
}

void TablePartitioner::computePartitionSizesAndOffsets(
    std::vector<std::vector<size_t>>& partition_offsets) {
  auto pcnt = getPartitionsCount();
  std::vector<std::vector<size_t>> histograms(info_.info.fragments.size());

  // Run histogram collection.
  for (size_t i = 0; i < info_.info.fragments.size(); ++i) {
    // run histogram collection function.
    histograms[i].assign(pcnt, 0);
    collectHistogram(i, histograms[i]);
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

void TablePartitioner::doPartition(int frag_idx,
                                   std::vector<std::vector<int8_t*>>& col_bufs) {
  const uint32_t fanOut = getPartitionsCount();
  std::vector<size_t> positions(payload_cols_.size() + key_cols_.size());
  // FIXME: only one key column for now!
  auto key_size = key_sizes_.at(0);
  auto keys = key_data_.at(frag_idx).at(0);
  auto fragment_size = info_.info.fragments.at(frag_idx).getNumTuples();
  for (uint64_t i = 0; i < fragment_size; ++i) {
    uint32_t idx = getHashValue(&(keys[i * key_size]), key_size, fanOut - 1, 0);
    // fill partition - first key than payload(s) if needed
    // FIXME: only one key column for now!
    memcpy(&(col_bufs[idx][0][positions[0]]), &(keys[i * key_size]), key_size);
    positions[0] += key_size;
    if (payload_cols_.size() > 0) {
      // payload start pos in col_bufs inner vector
      int payload_num = key_cols_.size();
      int payload_idx = 0;
      for (auto payload : payload_data_[frag_idx]) {
        auto payload_size = payload_sizes_.at(payload_idx);
        auto pos = positions[payload_num + payload_idx];
        memcpy(&(col_bufs[idx][payload_num++][pos]),
               &(payload[i * payload_size]),
               payload_size);
        positions[payload_num + payload_idx] += payload_size;
      }
    }
  }
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
