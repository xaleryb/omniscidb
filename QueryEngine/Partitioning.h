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

#ifndef QUERYENGINE_PARTITIONING_H
#define QUERYENGINE_PARTITIONING_H

#include "ColumnarResults.h"
#include "CompilationOptions.h"
#include "InputMetadata.h"
#include "RelAlgExecutionUnit.h"

#define PARTITIONING_DEBUG_PRINT 1

struct PartitioningOptions {
  enum PartitioningKind {
    // Compute hash value and use its bits to get partition ID.
    HASH,
    // Use bits from original key to get partition ID.
    // For composite keys only the first key component is used.
    VALUE
  };

  PartitioningKind kind = HASH;
  size_t mask_bits = 16;
  size_t scale_bits = 16;
};

RelAlgExecutionUnit performTablesPartitioning(
    const RelAlgExecutionUnit& ra_exe_unit,
    const CompilationOptions& co,
    const ExecutionOptions& eo,
    ColumnCacheMap& column_cache,
    Executor* executor,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
    TemporaryTables& temporary_tables,
    Analyzer::ExpressionPtrVector& target_exprs_owned);

#endif  // QUERYENGINE_PARTITIONING_H