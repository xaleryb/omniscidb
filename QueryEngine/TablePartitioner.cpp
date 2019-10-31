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

TablePartitioner::TablePartitioner(std::vector<InputColDescriptor> key_cols,
                                   std::vector<InputColDescriptor> payload_cols,
                                   const Fragmenter_Namespace::TableInfo& info,
                                   PartitioningOptions po)
    : key_cols_(std::move(key_cols))
    , payload_cols_(std::move(payload_cols))
    , info_(info)
    , po_(std::move(po)) {}

InputTableInfo TablePartitioner::runPartitioning() {
  // This vector holds write positions (in number of elements, not bytes)
  // in partitions for each partitioned fragment.
  // [fragment idx][partition id] -> offset in partition buffer.
  std::vector<std::vector<size_t>> partition_offsets;
  computePartitionSizesAndOffsets(partition_offsets);

  for (size_t i = 0; i < info_.fragments.size(); ++i) {
    // run partitioning function
  }
}

int TablePartitioner::getPartitionsCount() const {
  return 1 << po_.mask_bits;
}

void TablePartitioner::computePartitionSizesAndOffsets(
    std::vector<std::vector<size_t>>& partition_offsets) {
  int pcnt = getPartitionsCount();
  std::vector<std::vector<int64_t>> histograms(info_.fragments.size());

  // Run histogram collection.
  for (size_t i = 0; i < info_.fragments.size(); ++i) {
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
