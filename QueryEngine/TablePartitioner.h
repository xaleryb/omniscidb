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

#ifndef QUERYENGINE_TABLEPARTITIONER_H
#define QUERYENGINE_TABLEPARTITIONER_H

#include "Descriptors/InputDescriptors.h"
#include "Execute.h"
#include "InputMetadata.h"
#include "Partitioning.h"

class TablePartitioner {
 public:
  TablePartitioner(const RelAlgExecutionUnit& ra_exe_unit,
                   const std::vector<InputColDescriptor>& key_cols,
                   const std::vector<InputColDescriptor>& payload_cols,
                   const InputTableInfo& info,
                   ColumnCacheMap& column_cache,
                   PartitioningOptions po,
                   Executor* executor,
                   std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner);

  TemporaryTable runPartitioning();

  size_t getPartitionsCount() const;

 private:
  void fetchFragment(const Fragmenter_Namespace::FragmentInfo& frag,
                     const std::vector<std::shared_ptr<Analyzer::ColumnVar>>& col_vars,
                     const size_t expected_size,
                     std::vector<int8_t*>& output);
  void fetchFragments(const std::vector<std::shared_ptr<Analyzer::ColumnVar>>& col_vars);
  void computePartitionSizesAndOffsets(
      std::vector<std::vector<size_t>>& partition_offsets);
  void collectHistogram(int frag_idx, std::vector<size_t>& histogram);
  uint32_t getHashValue(const int8_t* key, int size, int mask, int shift);
  void doPartition(int frag_idx, std::vector<std::vector<size_t>>& partition_offsets);
  std::shared_ptr<Analyzer::ColumnVar> createColVar(const InputColDescriptor& col);

  // In input and output vectors key columns always go first.
  std::vector<InputColDescriptor> input_cols_;
  std::vector<std::vector<int8_t*>> input_bufs_;
  std::vector<size_t> input_sizes_;
  std::vector<std::vector<int8_t*>> output_bufs_;
  std::vector<size_t> output_sizes_;
  std::vector<size_t> elem_sizes_;
  size_t key_count_;
  const InputTableInfo& info_;
  ColumnCacheMap& column_cache_;
  PartitioningOptions po_;
  Executor* executor_;
  std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner_;
  std::vector<std::shared_ptr<Chunk_NS::Chunk>> chunks_owner_;
};

#endif  // QUERYENGINE_TABLEPARTITIONER_H