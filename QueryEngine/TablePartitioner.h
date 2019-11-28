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
                   const CompilationOptions& co,
                   const std::vector<InputColDescriptor>& key_cols,
                   const std::vector<InputColDescriptor>& payload_cols,
                   const InputTableInfo& info,
                   ColumnCacheMap& column_cache,
                   PartitioningOptions po,
                   Executor* executor,
                   std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner);

  TemporaryTable runPartitioning();

 private:
  void fetchFragments(const std::vector<std::shared_ptr<Analyzer::ColumnVar>>& col_vars);
  PartitioningOptions getPassOpts(size_t pass_no);
  CodeCacheKey getCodeCacheKey() const;
  void generatePartitioningModule();
  void setupModule(const CodeCacheValWithModule& val);
  void computePartitionSizesAndOffsets(size_t pass_no,
                                       const PartitioningOptions& pass_opts,
                                       std::vector<size_t>& partition_offsets);
  void collectHistogram(size_t pass_no,
                        const PartitioningOptions& pass_opts,
                        int frag_idx,
                        size_t* histogram);
  uint32_t getPartitionNo(const PartitioningOptions& pass_opts,
                          int8_t** bufs,
                          size_t row_no);
  void doPartition(size_t pass_no,
                   const PartitioningOptions& pass_opts,
                   std::vector<size_t>& partition_offsets);
  void doPartition(size_t pass_no,
                   const PartitioningOptions& pass_opts,
                   int frag_idx,
                   std::vector<size_t>& partition_offsets);
  std::shared_ptr<Analyzer::ColumnVar> createColVar(const InputColDescriptor& col);

  // In input and output vectors key columns always go first.
  std::vector<InputColDescriptor> input_cols_;
  std::vector<size_t> elem_sizes_;
  // Input and output buffers are actually two-dimensional
  // arrays stored as a single vector. To get column col_idx
  // of partition/fragment part_idx we use index:
  // [part_idx * input_cols_.size() + col_idx].
  std::vector<int8_t*> input_bufs_;
  std::vector<int8_t*> output_bufs_;
  // Input and output sizes vectors hold number of rows
  // in input and output partitions/fragments.
  std::vector<size_t> input_sizes_;
  std::vector<size_t> output_sizes_;
  size_t key_count_;
  const InputTableInfo& info_;
  ColumnCacheMap& column_cache_;
  CompilationOptions co_;
  PartitioningOptions po_;
  llvm::Module* module_;
  std::vector<void*> pass_entries_;
  Executor* executor_;
  std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner_;
  std::vector<std::shared_ptr<Chunk_NS::Chunk>> chunks_owner_;
  static CodeCache code_cache_;
};

#endif  // QUERYENGINE_TABLEPARTITIONER_H