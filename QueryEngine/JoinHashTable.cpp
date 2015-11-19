#include "JoinHashTable.h"
#include "Execute.h"
#include "HashJoinRuntime.h"
#include "RuntimeFunctions.h"

#include <glog/logging.h>
#include <thread>

namespace {

std::pair<const Analyzer::ColumnVar*, const Analyzer::ColumnVar*> get_cols(
    const std::shared_ptr<Analyzer::BinOper> qual_bin_oper) {
  const auto lhs = qual_bin_oper->get_left_operand();
  const auto rhs = qual_bin_oper->get_right_operand();
  if (lhs->get_type_info() != rhs->get_type_info()) {
    return {nullptr, nullptr};
  }
  const auto lhs_cast = dynamic_cast<const Analyzer::UOper*>(lhs);
  const auto rhs_cast = dynamic_cast<const Analyzer::UOper*>(rhs);
  if (static_cast<bool>(lhs_cast) != static_cast<bool>(rhs_cast) || (lhs_cast && lhs_cast->get_optype() != kCAST) ||
      (rhs_cast && rhs_cast->get_optype() != kCAST)) {
    return {nullptr, nullptr};
  }
  const auto lhs_col = lhs_cast ? dynamic_cast<const Analyzer::ColumnVar*>(lhs_cast->get_operand())
                                : dynamic_cast<const Analyzer::ColumnVar*>(lhs);
  const auto rhs_col = rhs_cast ? dynamic_cast<const Analyzer::ColumnVar*>(rhs_cast->get_operand())
                                : dynamic_cast<const Analyzer::ColumnVar*>(rhs);
  if (!lhs_col || !rhs_col) {
    return {nullptr, nullptr};
  }
  const Analyzer::ColumnVar* inner_col{nullptr};
  const Analyzer::ColumnVar* outer_col{nullptr};
  if (lhs_col->get_rte_idx() == 0 && rhs_col->get_rte_idx() == 1) {
    inner_col = rhs_col;
    outer_col = lhs_col;
  } else {
    CHECK((lhs_col->get_rte_idx() == 1 && rhs_col->get_rte_idx() == 0));
    inner_col = lhs_col;
    outer_col = rhs_col;
  }
  const auto& ti = inner_col->get_type_info();
  if (!(ti.is_integer() || (ti.is_string() && ti.get_compression() == kENCODING_DICT))) {
    return {nullptr, nullptr};
  }
  return {inner_col, outer_col};
}

}  // namespace

std::shared_ptr<JoinHashTable> JoinHashTable::getInstance(
    const std::shared_ptr<Analyzer::BinOper> qual_bin_oper,
    const Catalog_Namespace::Catalog& cat,
    const std::vector<Fragmenter_Namespace::QueryInfo>& query_infos,
    const Data_Namespace::MemoryLevel memory_level,
    const int device_count,
    const Executor* executor) {
  CHECK_EQ(kEQ, qual_bin_oper->get_optype());
  const auto cols = get_cols(qual_bin_oper);
  const auto inner_col = cols.first;
  if (!inner_col) {
    return nullptr;
  }
  const auto& ti = inner_col->get_type_info();
  auto col_range = getExpressionRange(ti.is_string() ? cols.second : inner_col, query_infos, nullptr);
  if (ti.is_string()) {
    // The nullable info must be the same as the source column.
    const auto source_col_range = getExpressionRange(inner_col, query_infos, nullptr);
    col_range.has_nulls = source_col_range.has_nulls;
    col_range.int_max = std::max(source_col_range.int_max, col_range.int_max);
    col_range.int_min = std::min(source_col_range.int_min, col_range.int_min);
  }
  auto join_hash_table = std::shared_ptr<JoinHashTable>(
      new JoinHashTable(qual_bin_oper, inner_col, cat, query_infos, memory_level, col_range, executor));
  const int err = join_hash_table->reify(device_count);
  if (err) {
    return nullptr;
  }
  return join_hash_table;
}

int JoinHashTable::reify(const int device_count) {
  CHECK_LT(0, device_count);
  const auto cols = get_cols(qual_bin_oper_);
  const auto inner_col = cols.first;
  CHECK(inner_col);
  const auto& query_info = query_infos_[inner_col->get_rte_idx()];
  if (query_info.fragments.size() != 1) {  // we don't support multiple fragment inner tables (yet)
    return -1;
  }
  const auto& fragment = query_info.fragments.front();
  auto chunk_meta_it = fragment.chunkMetadataMap.find(inner_col->get_column_id());
  CHECK(chunk_meta_it != fragment.chunkMetadataMap.end());
  ChunkKey chunk_key{
      cat_.get_currentDB().dbId, inner_col->get_table_id(), inner_col->get_column_id(), fragment.fragmentId};
  const auto cd = cat_.getMetadataForColumn(inner_col->get_table_id(), inner_col->get_column_id());
  CHECK(!(cd->isVirtualCol));
  const auto& ti = inner_col->get_type_info();
  // Since we don't have the string dictionary payloads on the GPU, we'll build
  // the join hash table on the CPU and transfer it to the GPU.
  const auto effective_memory_level = ti.is_string() ? Data_Namespace::CPU_LEVEL : memory_level_;
#ifdef HAVE_CUDA
  gpu_hash_table_buff_.resize(device_count);
#endif
  std::vector<int> errors(device_count);
  std::vector<std::thread> init_threads;
  for (int device_id = 0; device_id < device_count; ++device_id) {
    const auto chunk = Chunk_NS::Chunk::getChunk(cd,
                                                 &cat_.get_dataMgr(),
                                                 chunk_key,
                                                 effective_memory_level,
                                                 effective_memory_level == Data_Namespace::CPU_LEVEL ? 0 : device_id,
                                                 chunk_meta_it->second.numBytes,
                                                 chunk_meta_it->second.numElements);
    init_threads.emplace_back([&errors, &chunk_meta_it, &cols, chunk, effective_memory_level, device_id, this] {
      try {
        errors[device_id] =
            initHashTableForDevice(chunk, chunk_meta_it->second.numElements, cols, effective_memory_level, device_id);
      } catch (...) {
        errors[device_id] = -1;
      }
    });
  }
  for (auto& init_thread : init_threads) {
    init_thread.join();
  }
  for (const int err : errors) {
    if (err) {
      return err;
    }
  }
  return 0;
}

int JoinHashTable::initHashTableForDevice(const std::shared_ptr<Chunk_NS::Chunk> chunk,
                                          const size_t num_elements,
                                          const std::pair<const Analyzer::ColumnVar*, const Analyzer::ColumnVar*>& cols,
                                          const Data_Namespace::MemoryLevel effective_memory_level,
                                          const int device_id) {
  CHECK(chunk);
  const auto inner_col = cols.first;
  CHECK(inner_col);
  const auto& ti = inner_col->get_type_info();
  auto ab = chunk->get_buffer();
  CHECK(ab->getMemoryPtr());
  const auto col_buff = reinterpret_cast<int8_t*>(ab->getMemoryPtr());
  const int32_t hash_entry_count = col_range_.int_max - col_range_.int_min + 1 + (col_range_.has_nulls ? 1 : 0);
#ifdef HAVE_CUDA
  // Even if we join on dictionary encoded strings, the memory on the GPU is still needed
  // once the join hash table has been built on the CPU.
  if (memory_level_ == Data_Namespace::GPU_LEVEL) {
    auto& data_mgr = cat_.get_dataMgr();
    gpu_hash_table_buff_[device_id] = alloc_gpu_mem(&data_mgr, hash_entry_count * sizeof(int32_t), device_id);
  }
#else
  CHECK_EQ(Data_Namespace::CPU_LEVEL, effective_memory_level);
#endif
  int err = 0;
  const int32_t hash_join_invalid_val{-1};
  if (effective_memory_level == Data_Namespace::CPU_LEVEL) {
    {
      std::lock_guard<std::mutex> cpu_hash_table_buff_lock(cpu_hash_table_buff_mutex_);
      if (cpu_hash_table_buff_.empty()) {
        cpu_hash_table_buff_.resize(hash_entry_count);
        const StringDictionary* sd_inner{nullptr};
        const StringDictionary* sd_outer{nullptr};
        if (ti.is_string()) {
          CHECK_EQ(kENCODING_DICT, ti.get_compression());
          sd_inner = executor_->getStringDictionary(inner_col->get_comp_param(), executor_->row_set_mem_owner_);
          CHECK(sd_inner);
          sd_outer = executor_->getStringDictionary(cols.second->get_comp_param(), executor_->row_set_mem_owner_);
          CHECK(sd_outer);
        }
        int thread_count = cpu_threads();
        std::vector<std::thread> init_cpu_buff_threads;
        for (int thread_idx = 0; thread_idx < thread_count; ++thread_idx) {
          init_cpu_buff_threads.emplace_back([this, hash_entry_count, thread_idx, thread_count] {
            init_hash_join_buff(
                &cpu_hash_table_buff_[0], hash_entry_count, hash_join_invalid_val, thread_idx, thread_count);
          });
        }
        for (auto& t : init_cpu_buff_threads) {
          t.join();
        }
        init_cpu_buff_threads.clear();
        for (int thread_idx = 0; thread_idx < thread_count; ++thread_idx) {
          init_cpu_buff_threads.emplace_back([this,
                                              hash_join_invalid_val,
                                              hash_entry_count,
                                              col_buff,
                                              num_elements,
                                              sd_inner,
                                              sd_outer,
                                              thread_idx,
                                              thread_count,
                                              &ti,
                                              &err] {
            int partial_err = fill_hash_join_buff(&cpu_hash_table_buff_[0],
                                                  hash_join_invalid_val,
                                                  col_buff,
                                                  num_elements,
                                                  ti.get_size(),
                                                  col_range_.int_min,
                                                  inline_int_null_val(ti),
                                                  col_range_.int_max + 1,
                                                  sd_inner,
                                                  sd_outer,
                                                  thread_idx,
                                                  thread_count);
            __sync_val_compare_and_swap(&err, 0, partial_err);
          });
        }
        for (auto& t : init_cpu_buff_threads) {
          t.join();
        }
      }
    }
    // Transfer the hash table on the GPU if we've only built it on CPU
    // but the query runs on GPU (join on dictionary encoded columns).
    // Don't transfer the buffer if there was an error since we'll bail anyway.
    if (memory_level_ == Data_Namespace::GPU_LEVEL && !err) {
#ifdef HAVE_CUDA
      CHECK(ti.is_string());
      auto& data_mgr = cat_.get_dataMgr();
      copy_to_gpu(&data_mgr,
                  gpu_hash_table_buff_[device_id],
                  &cpu_hash_table_buff_[0],
                  cpu_hash_table_buff_.size() * sizeof(cpu_hash_table_buff_[0]),
                  device_id);
#else
      CHECK(false);
#endif
    }
  } else {
#ifdef HAVE_CUDA
    CHECK_EQ(Data_Namespace::GPU_LEVEL, effective_memory_level);
    auto& data_mgr = cat_.get_dataMgr();
    auto dev_err_buff = alloc_gpu_mem(&data_mgr, sizeof(int), device_id);
    copy_to_gpu(&data_mgr, dev_err_buff, &err, sizeof(err), device_id);
    init_hash_join_buff_on_device(reinterpret_cast<int32_t*>(gpu_hash_table_buff_[device_id]),
                                  hash_entry_count,
                                  hash_join_invalid_val,
                                  executor_->blockSize(),
                                  executor_->gridSize());
    fill_hash_join_buff_on_device(reinterpret_cast<int32_t*>(gpu_hash_table_buff_[device_id]),
                                  hash_join_invalid_val,
                                  reinterpret_cast<int*>(dev_err_buff),
                                  col_buff,
                                  num_elements,
                                  ti.get_size(),
                                  col_range_.int_min,
                                  inline_int_null_val(ti),
                                  col_range_.int_max + 1,
                                  executor_->blockSize(),
                                  executor_->gridSize());
    copy_from_gpu(&data_mgr, &err, dev_err_buff, sizeof(err), device_id);
#else
    CHECK(false);
#endif
  }
  return err;
}

llvm::Value* JoinHashTable::codegenSlot(Executor* executor, const bool hoist_literals) {
  CHECK(executor->plan_state_->join_info_.join_impl_type_ == Executor::JoinImplType::HashOneToOne);
  const auto cols = get_cols(qual_bin_oper_);
  auto key_col = cols.second;
  CHECK(key_col);
  auto val_col = cols.first;
  CHECK(val_col);
  const auto key_lvs = executor->codegen(key_col, true, hoist_literals);
  CHECK_EQ(size_t(1), key_lvs.size());
  CHECK(executor->plan_state_->join_info_.join_hash_table_);
  auto& hash_ptr = executor->cgen_state_->row_func_->getArgumentList().back();
  std::vector<llvm::Value*> hash_join_idx_args{&hash_ptr,
                                               executor->toDoublePrecision(key_lvs.front()),
                                               executor->ll_int(col_range_.int_min),
                                               executor->ll_int(col_range_.int_max)};
  if (col_range_.has_nulls) {
    hash_join_idx_args.push_back(executor->ll_int(inline_int_null_val(key_col->get_type_info())));
  }
  std::string fname{"hash_join_idx"};
  if (col_range_.has_nulls) {
    fname += "_nullable";
  }
  const auto slot_lv = executor->cgen_state_->emitCall(fname, hash_join_idx_args);
  const auto it_ok = executor->cgen_state_->scan_idx_to_hash_pos_.emplace(val_col->get_rte_idx(), slot_lv);
  CHECK(it_ok.second);
  const auto slot_valid_lv =
      executor->cgen_state_->ir_builder_.CreateICmp(llvm::ICmpInst::ICMP_SGE, slot_lv, executor->ll_int(int64_t(0)));
  return slot_valid_lv;
}
