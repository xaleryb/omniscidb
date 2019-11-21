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

// Structure to describe input table replacement with
// a temporary table (partitioned version).
struct InputTableMap {
  // Id of table to replace.
  int table_id;
  int scan_idx;
  // Old col id -> new col id.
  std::unordered_map<int, int> col_map;
  // New table.
  int new_table_id;
  TemporaryTable table;
};

using InputTableMaps = std::unordered_map<InputDescriptor, InputTableMap>;

std::vector<InputDescriptor> replaceInputDescriptors(
    const std::vector<InputDescriptor>& descs,
    const InputTableMaps& input_maps) {
  std::vector<InputDescriptor> res;
  for (auto& desc : descs) {
    auto it = input_maps.find(desc);
    if (it == input_maps.end()) {
      res.push_back(it->first);
    } else {
      res.push_back({it->second.new_table_id, desc.getNestLevel()});
    }
  }
  return res;
}

std::list<std::shared_ptr<const InputColDescriptor>> replaceInputColDescriptors(
    const std::list<std::shared_ptr<const InputColDescriptor>>& input_col_descs,
    const InputTableMaps& input_maps) {
  std::list<std::shared_ptr<const InputColDescriptor>> res;
  for (auto& col_desc : input_col_descs) {
    auto it = input_maps.find(col_desc->getScanDesc());
    if (it == input_maps.end()) {
      res.push_back(col_desc);
    } else {
      CHECK(it->second.col_map.count(col_desc->getColId()));
      res.push_back(std::make_shared<InputColDescriptor>(
          it->second.col_map.at(col_desc->getColId()),
          it->second.new_table_id,
          col_desc->getScanDesc().getNestLevel()));
    }
  }
  return res;
}

std::shared_ptr<Analyzer::Expr> replaceInputInExpr(
    Analyzer::Expr* expr,
    const Analyzer::ColumnVarMap& col_map) {
  if (!expr)
    return nullptr;
  return expr->rewrite_var_to_var(col_map);
}

std::list<std::shared_ptr<Analyzer::Expr>> replaceInputInExprList(
    const std::list<std::shared_ptr<Analyzer::Expr>>& exprs,
    const Analyzer::ColumnVarMap& col_map) {
  std::list<std::shared_ptr<Analyzer::Expr>> res;
  for (auto& expr : exprs)
    res.push_back(replaceInputInExpr(expr.get(), col_map));
  return res;
}

JoinCondition replaceInputInJoinCondition(const JoinCondition& cond,
                                          const Analyzer::ColumnVarMap& col_map) {
  return {replaceInputInExprList(cond.quals, col_map), cond.type};
}

JoinQualsPerNestingLevel replaceInputInJoinConditions(
    const JoinQualsPerNestingLevel& join_quals,
    const Analyzer::ColumnVarMap& col_map) {
  JoinQualsPerNestingLevel res;
  for (auto& cond : join_quals)
    res.push_back(replaceInputInJoinCondition(cond, col_map));
  return res;
}

std::vector<Analyzer::Expr*> replaceInputInTargetExprs(
    const std::vector<Analyzer::Expr*>& target_exprs,
    const Analyzer::ColumnVarMap& col_map,
    Analyzer::ExpressionPtrVector& target_exprs_owned) {
  std::vector<Analyzer::Expr*> res;
  for (auto& target_expr : target_exprs) {
    auto new_target_expr = replaceInputInExpr(target_expr, col_map);
    target_exprs_owned.push_back(new_target_expr);
    res.push_back(new_target_expr.get());
  }
  return res;
}

std::shared_ptr<Analyzer::Estimator> replaceInputInEstimator(
    std::shared_ptr<Analyzer::Estimator> estimator,
    const Analyzer::ColumnVarMap& col_map) {
  if (estimator) {
    auto arg = replaceInputInExprList(estimator->getArgument(), col_map);
    if (dynamic_cast<Analyzer::NDVEstimator*>(estimator.get())) {
      return std::make_shared<Analyzer::NDVEstimator>(arg);
    } else {
      CHECK(false) << "Unknown estimator.";
    }
  }
  return nullptr;
}

#if PARTITIONING_DEBUG_PRINT
template <typename T>
void dumpExprList(const T& exprs, const std::string& prefix, std::ostream& os) {
  for (auto& expr : exprs) {
    if (expr) {
      os << prefix << expr->toString();
    }
  }
}

void dumpUnit(const RelAlgExecutionUnit& ra_exe_unit, std::ostream& os) {
  os << "Input table descriptors:";
  for (auto& desc : ra_exe_unit.input_descs) {
    os << " " << desc.getTableId() << ":" << desc.getNestLevel();
  }
  os << std::endl;

  os << "Input column descriptors:";
  for (auto& col_desc : ra_exe_unit.input_col_descs) {
    os << " " << col_desc->getScanDesc().getTableId() << ":"
       << col_desc->getScanDesc().getNestLevel() << ":" << col_desc->getColId();
  }
  os << std::endl;

  os << "Simple qualifiers:";
  dumpExprList(ra_exe_unit.simple_quals, "\n  ", os);
  os << std::endl;

  os << "Qualifiers:";
  dumpExprList(ra_exe_unit.quals, "\n  ", os);
  os << std::endl;

  os << "Join qualifiers:";
  for (auto& cond : ra_exe_unit.join_quals) {
    os << "\n  Type(" << (int)cond.type << "):";
    dumpExprList(cond.quals, "  ", os);
  }
  os << std::endl;

  os << "Group by exprs:";
  dumpExprList(ra_exe_unit.groupby_exprs, "\n  ", os);
  os << std::endl;

  os << "Target exprs:";
  dumpExprList(ra_exe_unit.target_exprs, "\n  ", os);
  os << std::endl;

  os << "Estimator: " << (ra_exe_unit.estimator ? ra_exe_unit.estimator->toString() : "")
     << std::endl;
}
#endif

RelAlgExecutionUnit replaceInputInUnit(
    const RelAlgExecutionUnit& ra_exe_unit,
    const InputTableMaps& input_maps,
    Analyzer::ExpressionPtrVector& target_exprs_owned) {
#if PARTITIONING_DEBUG_PRINT
  std::cerr << "Replacing input tables in execution unit" << std::endl;
  std::cerr << "============= Column replacement map ==============" << std::endl;
  for (auto& pr : input_maps) {
    std::cerr << "Table " << pr.first.getTableId() << ":" << pr.first.getNestLevel()
              << " -> " << pr.second.new_table_id << ":" << pr.first.getNestLevel()
              << std::endl;
    for (auto& col : pr.second.col_map) {
      std::cerr << "  Column " << col.first << " -> " << col.second << std::endl;
    }
  }
  std::cerr << "===================================================" << std::endl;
  std::cerr << "============= Original execution unit =============" << std::endl;
  dumpUnit(ra_exe_unit, std::cerr);
  std::cerr << "===================================================" << std::endl;
#endif

  Analyzer::ColumnVarMap col_map;
  for (auto& pr : input_maps) {
    for (auto& cols : pr.second.col_map) {
      auto orig_tuple =
          std::make_tuple(pr.second.table_id, cols.first, pr.second.scan_idx);
      auto new_tuple =
          std::make_tuple(pr.second.new_table_id, cols.second, pr.second.scan_idx);
      col_map[orig_tuple] = new_tuple;
    }
  }

  RelAlgExecutionUnit res{
      replaceInputDescriptors(ra_exe_unit.input_descs, input_maps),
      replaceInputColDescriptors(ra_exe_unit.input_col_descs, input_maps),
      replaceInputInExprList(ra_exe_unit.simple_quals, col_map),
      replaceInputInExprList(ra_exe_unit.quals, col_map),
      replaceInputInJoinConditions(ra_exe_unit.join_quals, col_map),
      replaceInputInExprList(ra_exe_unit.groupby_exprs, col_map),
      replaceInputInTargetExprs(ra_exe_unit.target_exprs, col_map, target_exprs_owned),
      replaceInputInEstimator(ra_exe_unit.estimator, col_map),
      ra_exe_unit.sort_info,
      ra_exe_unit.scan_limit,
      ra_exe_unit.query_features,
      ra_exe_unit.use_bump_allocator};

#if PARTITIONING_DEBUG_PRINT
  std::cerr << "============= Modified execution unit =============" << std::endl;
  dumpUnit(res, std::cerr);
  std::cerr << "===================================================" << std::endl;
#endif

  return res;
}

void performTablePartitioning(const std::vector<const Analyzer::ColumnVar*>& key_cols,
                              const RelAlgExecutionUnit& ra_exe_unit,
                              const CompilationOptions& co,
                              const ExecutionOptions& eo,
                              const std::vector<InputTableInfo>& query_infos,
                              ColumnCacheMap& column_cache,
                              Executor* executor,
                              std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
                              InputTableMaps& input_maps) {
  auto table_id = key_cols.front()->get_table_id();
  auto scan_idx = key_cols.front()->get_rte_idx();

  if (table_id < 0) {
    throw std::runtime_error(
        "Hash join failed: result set partitioning is not yet supported");
  }

  // Collect keys columns for partitioning.
  std::vector<InputColDescriptor> key;
  std::vector<InputColDescriptor> payload;
  std::unordered_map<int, int> col_map;
  for (auto& key_col : key_cols) {
    CHECK_EQ(key_col->get_table_id(), table_id);
    CHECK_EQ(key_col->get_rte_idx(), scan_idx);
    col_map[key_col->get_column_id()] = key.size();
    key.emplace_back(key_col->get_column_id(), table_id, scan_idx);
  }

  // Collect payload columns for partitioning.
  for (auto& col : ra_exe_unit.input_col_descs) {
    if (col->getScanDesc().getTableId() == table_id &&
        col->getScanDesc().getNestLevel() == scan_idx &&
        !col_map.count(col->getColId())) {
      col_map[col->getColId()] = key.size() + payload.size();
      payload.emplace_back(col->getColId(), table_id, scan_idx);
    }
  }

  // Select fragments to process.
  const auto& table_info = get_inner_query_info(table_id, query_infos);

  PartitioningOptions po;
  po.mask_bits = g_radix_bits_count;
  po.scale_bits = g_radix_bits_scale;
  po.kind = g_radix_type;
  TablePartitioner partitioner(ra_exe_unit,
                               key,
                               payload,
                               table_info,
                               column_cache,
                               po,
                               executor,
                               row_set_mem_owner);
  auto tmp_table = partitioner.runPartitioning();

  // TODO: register and cache partitions somewhere for
  // re-usage.

  // For now we don't modify RelAlgNode tree after partitioning,
  // but we still need a unique ID for temporary table. Create
  // dummy scan node to get it.
  RelScan dummy_node(nullptr, {});
  input_maps.emplace(std::make_pair(InputDescriptor{table_id, scan_idx},
                                    InputTableMap{table_id,
                                                  scan_idx,
                                                  col_map,
                                                  -static_cast<int>(dummy_node.getId()),
                                                  std::move(tmp_table)}));
}

RelAlgExecutionUnit performPartitioningForQualifier(
    const std::shared_ptr<Analyzer::BinOper>& qual_bin_oper,
    const RelAlgExecutionUnit& ra_exe_unit,
    const CompilationOptions& co,
    const ExecutionOptions& eo,
    const std::vector<InputTableInfo>& query_infos,
    ColumnCacheMap& column_cache,
    Executor* executor,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
    TemporaryTables& temporary_tables,
    Analyzer::ExpressionPtrVector& target_exprs_owned) {
  std::vector<InnerOuter> inner_outer_pairs;
  try {
    inner_outer_pairs = normalize_column_pairs(
        qual_bin_oper.get(), *executor->getCatalog(), executor->getTemporaryTables());
  } catch (HashJoinFail&) {
    return ra_exe_unit;
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
    outer_key.push_back(outer_col);
  }

  InputTableMaps input_maps;
  performTablePartitioning(inner_key,
                           ra_exe_unit,
                           co,
                           eo,
                           query_infos,
                           column_cache,
                           executor,
                           row_set_mem_owner,
                           input_maps);
  performTablePartitioning(outer_key,
                           ra_exe_unit,
                           co,
                           eo,
                           query_infos,
                           column_cache,
                           executor,
                           row_set_mem_owner,
                           input_maps);

  auto res = replaceInputInUnit(ra_exe_unit, input_maps, target_exprs_owned);
  for (auto& pr : input_maps) {
    const auto it_ok =
        temporary_tables.emplace(pr.second.new_table_id, std::move(pr.second.table));
    CHECK(it_ok.second);
  }
  return res;
}

}  // namespace

RelAlgExecutionUnit performTablesPartitioning(
    const RelAlgExecutionUnit& ra_exe_unit,
    const CompilationOptions& co,
    const ExecutionOptions& eo,
    ColumnCacheMap& column_cache,
    Executor* executor,
    std::shared_ptr<RowSetMemoryOwner> row_set_mem_owner,
    TemporaryTables& temporary_tables,
    Analyzer::ExpressionPtrVector& target_exprs_owned) {
  if (!g_force_radix_join)
    return ra_exe_unit;

  if (ra_exe_unit.join_quals.empty())
    return ra_exe_unit;

  if (ra_exe_unit.join_quals.size() > 1)
    throw std::runtime_error(
        "Hash join failed: radix hash join is not yet supported for deep joins");

  if (co.device_type_ == ExecutorDeviceType::GPU)
    throw std::runtime_error(
        "Hash join failed: radix hash join for GPU is not yet supported");

  auto query_infos = get_table_infos(ra_exe_unit, executor);
  auto& join_condition = ra_exe_unit.join_quals.front();
  for (const auto& join_qual : join_condition.quals) {
    auto qual_bin_oper = std::dynamic_pointer_cast<Analyzer::BinOper>(join_qual);
    if (!qual_bin_oper || !IS_EQUIVALENCE(qual_bin_oper->get_optype()))
      continue;

    return performPartitioningForQualifier(qual_bin_oper,
                                           ra_exe_unit,
                                           co,
                                           eo,
                                           query_infos,
                                           column_cache,
                                           executor,
                                           row_set_mem_owner,
                                           temporary_tables,
                                           target_exprs_owned);
  }

  return ra_exe_unit;
}