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
#include "InputMetadata.h"
#include "Partitioning.h"

class TablePartitioner {
 public:
  TablePartitioner(std::vector<InputColDescriptor> key_cols,
                   std::vector<InputColDescriptor> payload_cols,
                   const Fragmenter_Namespace::TableInfo& info,
                   PartitioningOptions po);

  InputTableInfo runPartitioning();

  int getPartitionsCount() const;

 private:
  void fetchFragments();
  void computePartitionSizesAndOffsets(
      std::vector<std::vector<size_t>>& partition_offsets);
  void collectHistogram(int frag_idx, std::vector<size_t>& histogram);

  std::vector<InputColDescriptor> key_cols_;
  std::vector<InputColDescriptor> payload_cols_;
  const Fragmenter_Namespace::TableInfo& info_;
  PartitioningOptions po_;
  // Maps partition ID to a number of tuples in this partition.
  std::vector<size_t> partition_sizes_;
  //
};

#endif  // QUERYENGINE_TABLEPARTITIONER_H