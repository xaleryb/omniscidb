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
                   std::vector<InputColDescriptor> key_cols,
                   std::vector<InputColDescriptor> payload_cols,
                   const InputTableInfo& info,
                   ColumnCacheMap& column_cache,
                   PartitioningOptions po,
                   Executor* executor,
                   std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner);

  TemporaryTable runPartitioning();

  size_t getPartitionsCount() const;

 private:
  void fetchFragment(const Fragmenter_Namespace::FragmentInfo& frag,
                     size_t frag_num,
                     std::vector<const Analyzer::ColumnVar*>& vars,
                     std::vector<const int8_t*>& output);
  void fetchFragments(std::vector<const Analyzer::ColumnVar*>& key_vars,
                      std::vector<const Analyzer::ColumnVar*>& payload_vars);
  void computePartitionSizesAndOffsets(
      std::vector<std::vector<size_t>>& partition_offsets);
  void collectHistogram(int frag_idx, std::vector<size_t>& histogram);
  uint32_t getHashValue(const int8_t* key, int size, int mask, int shift);
  void doPartition(int frag_idx,
                   std::vector<std::vector<size_t>>& partition_offsets,
                   std::vector<std::vector<int8_t*>>& col_bufs);
  std::shared_ptr<Analyzer::ColumnVar> createColVar(const InputColDescriptor& col);

  std::vector<InputColDescriptor> key_cols_;
  std::vector<InputColDescriptor> payload_cols_;
  std::vector<std::vector<const int8_t*>> key_data_;
  std::vector<std::vector<const int8_t*>> payload_data_;
  std::vector<size_t> key_sizes_;
  std::vector<size_t> payload_sizes_;
  const InputTableInfo& info_;
  ColumnCacheMap& column_cache_;
  PartitioningOptions po_;
  Executor* executor_;
  const RelAlgExecutionUnit& ra_exe_unit_;
  std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner_;
  std::vector<std::shared_ptr<Chunk_NS::Chunk>> chunks_owner_;
  // Maps partition ID to a number of tuples in this partition.
  std::vector<size_t> partition_sizes_;
  //
};

#endif  // QUERYENGINE_TABLEPARTITIONER_H