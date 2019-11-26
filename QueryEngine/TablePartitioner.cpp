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
#include "MurmurHash1Inl.h"

#include "Utils/Threading.h"

TablePartitioner::TablePartitioner(const RelAlgExecutionUnit& ra_exe_unit,
                                   const std::vector<InputColDescriptor>& key_cols,
                                   const std::vector<InputColDescriptor>& payload_cols,
                                   const InputTableInfo& info,
                                   ColumnCacheMap& column_cache,
                                   PartitioningOptions po,
                                   Executor* executor,
                                   std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner)
    : key_count_(key_cols.size())
    , info_(info)
    , column_cache_(column_cache)
    , po_(std::move(po))
    , executor_(executor)
    , row_set_mem_owner_(row_set_mem_owner) {
  for (auto& col : key_cols)
    input_cols_.emplace_back(col);
  for (auto& col : payload_cols)
    input_cols_.emplace_back(col);
}

void TablePartitioner::fetchFragment(
    const Fragmenter_Namespace::FragmentInfo& frag,
    const std::vector<std::shared_ptr<Analyzer::ColumnVar>>& col_vars,
    const size_t expected_size,
    std::vector<int8_t*>& output) {
  const int8_t* col_frag = nullptr;
  size_t elem_count = 0;
  for (auto var : col_vars) {
    std::tie(col_frag, elem_count) =
        ColumnFetcher::getOneColumnFragment(executor_,
                                            *var,
                                            frag,
                                            Data_Namespace::CPU_LEVEL,
                                            0,
                                            chunks_owner_,
                                            column_cache_);
    output.push_back(const_cast<int8_t*>(col_frag));
    CHECK_EQ(elem_count, expected_size);
  }
}

void TablePartitioner::fetchFragments(
    const std::vector<std::shared_ptr<Analyzer::ColumnVar>>& col_vars) {
  std::vector<int8_t*> empty;
  input_bufs_.assign(info_.info.fragments.size(), empty);
  size_t frag_idx = 0;
  for (auto& frag : info_.info.fragments) {
    fetchFragment(frag, col_vars, frag.getNumTuples(), input_bufs_[frag_idx]);
    input_sizes_.push_back(frag.getNumTuples());
    frag_idx++;
  }
}

TemporaryTable TablePartitioner::runPartitioning() {
  // Prepare some aux structures for memory descriptors and result sets.
  // ColSlotContext consumes Expr plain pointers and we use col_vars
  // to own the memory.
  std::vector<std::shared_ptr<Analyzer::ColumnVar>> col_vars;
  std::vector<Analyzer::Expr*> slots;
  std::vector<TargetInfo> targets;
  for (auto& col : input_cols_) {
    auto col_var = createColVar(col);
    slots.push_back(col_var.get());
    elem_sizes_.push_back(col_var->get_type_info().get_size());
    targets.emplace_back(
        TargetInfo{false, kMIN, col_var->get_type_info(), SQLTypeInfo(), false, false});
    col_vars.emplace_back(std::move(col_var));
  }

  // Fix-up number of passes.
  size_t pass_count = g_radix_pass_num;
  if (!pass_count)
    pass_count = 1;
  else if (pass_count > po_.mask_bits)
    pass_count = po_.mask_bits;

  std::vector<ResultSetPtr> partitions;
  for (size_t pass_no = 0; pass_no < pass_count; ++pass_no) {
    bool last_pass = (pass_no == (pass_count - 1));

    // Compute partitioning options for the current pass.
    // We start partitioning using higher hash/value bits
    // and then add lower bits.
    PartitioningOptions pass_opts(po_);
    pass_opts.mask_bits = po_.mask_bits * (pass_no + 1) / pass_count;
    pass_opts.scale_bits = po_.scale_bits + po_.mask_bits - pass_opts.mask_bits;

    // This is a holder of result sets from the previous pass
    // to keep data alive during this one.
    std::vector<ResultSetPtr> prev_partitions;
    prev_partitions.swap(partitions);

    // On the first pass we should fetch input table. For all
    // subsequent passes we just use output as an input.
    if (pass_no == 0) {
      fetchFragments(col_vars);
    } else {
      input_bufs_ = std::move(output_bufs_);
      input_sizes_ = std::move(output_sizes_);
      output_bufs_.clear();
      output_sizes_.clear();
    }

    // This vector holds write positions (in number of elements, not bytes)
    // in partitions for each partitioned fragment.
    // [fragment idx][partition id] -> offset in partition buffer.
    std::vector<std::vector<size_t>> partition_offsets;
    computePartitionSizesAndOffsets(pass_opts, partition_offsets);

    // Now we create result sets to hold partitioning output.
    std::vector<ssize_t> dummy;
    ColSlotContext slot_ctx(slots, dummy);
    slot_ctx.setAllSlotsPaddedSizeToLogicalSize();
    output_bufs_.resize(output_sizes_.size());
    for (size_t frag_id = 0; frag_id < output_sizes_.size(); ++frag_id) {
      QueryMemoryDescriptor mem_desc(QueryDescriptionType::Projection,
                                     executor_,
                                     slot_ctx,
                                     output_sizes_[frag_id],
                                     true);
      auto rs = std::make_shared<ResultSet>(
          targets, ExecutorDeviceType::CPU, mem_desc, row_set_mem_owner_, executor_);
      rs->setCachedRowCount(output_sizes_[frag_id]);
      output_bufs_[frag_id].resize(targets.size(), nullptr);

      // For the last pass we allocated buffers owned by RowSetMemoryOwner
      // to enable zero-copy fetch for the resulting table. All other
      // ResultSets allocate buffers by themselves.
      int8_t* buff;
      if (last_pass) {
        buff = static_cast<int8_t*>(
            checked_malloc(mem_desc.getBufferSizeBytes(ExecutorDeviceType::CPU)));
        row_set_mem_owner_->addColBuffer(buff);
        rs->allocateStorage(buff, {});
      } else {
        auto* storage = rs->allocateStorage();
        buff = storage->getUnderlyingBuffer();
      }
      for (size_t col_idx = 0; col_idx < targets.size(); ++col_idx) {
        output_bufs_[frag_id][col_idx] = buff + mem_desc.getColOffInBytes(col_idx);
      }
      partitions.push_back(rs);
    }

    std::vector<std::future<void>> partitioning_threads;
    partitioning_threads.reserve(input_bufs_.size());
    for (size_t i = 0; i < input_bufs_.size(); ++i) {
      // run partitioning function
      if (g_enable_multi_thread_partitioning)
        partitioning_threads.emplace_back(
            utils::async([this, i, &pass_opts, &partition_offsets] {
              doPartition(pass_opts, i, partition_offsets);
            }));
      else
        partitioning_threads.emplace_back(
            std::async(std::launch::deferred, [this, i, &pass_opts, &partition_offsets] {
              doPartition(pass_opts, i, partition_offsets);
            }));
    }
    for (auto& thread : partitioning_threads)
      thread.get();

// TODO: remove debug prints
#if PARTITIONING_DEBUG_PRINT
    std::cerr << "Partitioning results after pass #" << pass_no << std::endl;
    std::cerr << "Pass options: type=" << pass_opts.kind
              << " mask=" << pass_opts.mask_bits << " scale=" << pass_opts.scale_bits
              << std::endl;
    for (size_t pid = 0; pid < partitions.size(); ++pid) {
      std::cerr << "========== PARTITION " << pid << " ==========" << std::endl;
      if (!partitions[pid]) {
        std::cerr << "(empty)" << std::endl;
        continue;
      }
      for (size_t rid = 0; rid < partitions[pid]->entryCount(); ++rid) {
        auto row = partitions[pid]->getRowAt(rid);
        for (auto& val : row) {
          auto scalar_r = boost::get<ScalarTargetValue>(&val);
          CHECK(scalar_r);
          std::cerr << *scalar_r << " ";
        }
        std::cerr << std::endl;
      }
      std::cerr << "=================================" << std::endl;
    }
#endif
  }

  return TemporaryTable(partitions, true);
}

uint32_t TablePartitioner::getPartitionNo(const PartitioningOptions& pass_opts,
                                          const std::vector<int8_t*>& bufs,
                                          size_t idx) {
  uint64_t value = 0;
  // FIXME: only one key column!
  if (pass_opts.kind == PartitioningOptions::HASH) {
    // FIXME: use GenericKeyHandler and a single Mumrmur call
    // to get the same hashing as for baseline hash table.
    for (size_t i = 0; i < key_count_; ++i) {
      size_t key_size = elem_sizes_[i];
      int8_t* key_p = bufs[i] + idx * key_size;
      if (pass_opts.scale_bits + pass_opts.scale_bits > 32) {
        value = MurmurHash64AImpl(key_p, key_size, value);
      } else {
        value = MurmurHash1Impl(key_p, key_size, value);
      }
    }
  } else {
    // For partitioning by value only the first key component
    // is used.
    CHECK(pass_opts.kind == PartitioningOptions::VALUE);
    size_t key_size = elem_sizes_[0];
    int8_t* key_p = bufs[0] + idx * key_size;
    switch (key_size) {
      case 2:
        value = *((int16_t*)key_p);
        break;
      case 4:
        value = *((int32_t*)key_p);
        break;
      case 8:
        value = *((int64_t*)key_p);
        break;
      default:
        CHECK(false);
    }
  }

  uint64_t mask = pass_opts.getPartitionsCount() - 1;
  return (value >> pass_opts.scale_bits) & mask;
}

void TablePartitioner::collectHistogram(const PartitioningOptions& pass_opts,
                                        int frag_idx,
                                        std::vector<size_t>& histogram) {
  auto& input = input_bufs_[frag_idx];
  auto size = input_sizes_[frag_idx];
  for (size_t i = 0; i < size; ++i) {
    uint32_t part_no = getPartitionNo(pass_opts, input, i);
    histogram[part_no]++;
  }
}

void TablePartitioner::computePartitionSizesAndOffsets(
    const PartitioningOptions& pass_opts,
    std::vector<std::vector<size_t>>& partition_offsets) {
  auto pcnt = pass_opts.getPartitionsCount();
  std::vector<std::vector<size_t>> histograms(input_bufs_.size(),
                                              std::vector<size_t>(pcnt, 0));

  // Run histogram collection.
  for (size_t i = 0; i < input_bufs_.size(); ++i) {
    collectHistogram(pass_opts, i, histograms[i]);
  }

  // Count partition sizes and offsets (in number of tuples).
  partition_offsets.assign(input_bufs_.size(), std::vector<size_t>(pcnt, 0));
  for (size_t i = 0; i < (histograms.size() - 1); ++i) {
    for (size_t j = 0; j < pcnt; ++j) {
      partition_offsets[i + 1][j] = partition_offsets[i][j] + histograms[i][j];
    }
  }

  output_sizes_.resize(pcnt, 0);
  for (size_t j = 0; j < pcnt; ++j) {
    output_sizes_[j] = partition_offsets.back()[j] + histograms.back()[j];
  }
}

void TablePartitioner::doPartition(const PartitioningOptions& pass_opts,
                                   int frag_idx,
                                   std::vector<std::vector<size_t>>& partition_offsets) {
  auto& input = input_bufs_[frag_idx];
  auto fragment_size = input_sizes_[frag_idx];
  for (size_t i = 0; i < fragment_size; ++i) {
    uint32_t part_no = getPartitionNo(pass_opts, input, i);
    auto& output = output_bufs_[part_no];
    auto pos = partition_offsets[frag_idx][part_no];
    // fill partition
    for (size_t elem_idx = 0; elem_idx < elem_sizes_.size(); ++elem_idx) {
      auto elem_size = elem_sizes_[elem_idx];
      memcpy(
          output[elem_idx] + pos * elem_size, input[elem_idx] + i * elem_size, elem_size);
    }
    partition_offsets[frag_idx][part_no]++;
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
