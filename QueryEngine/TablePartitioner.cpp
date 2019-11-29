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
#include "TablePartitionerCgen.h"
#include "Utils/Threading.h"

CodeCache TablePartitioner::code_cache_(20);

TablePartitioner::TablePartitioner(const RelAlgExecutionUnit& ra_exe_unit,
                                   const CompilationOptions& co,
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
    , co_(co)
    , po_(std::move(po))
    , module_(nullptr)
    , executor_(executor)
    , row_set_mem_owner_(row_set_mem_owner) {
  for (auto& col : key_cols)
    input_cols_.emplace_back(col);
  for (auto& col : payload_cols)
    input_cols_.emplace_back(col);
  // Fix-up number of passes.
  if (po_.passes > po_.mask_bits)
    po_.passes = po_.mask_bits;
}

void TablePartitioner::fetchFragments(
    const std::vector<std::shared_ptr<Analyzer::ColumnVar>>& col_vars) {
  input_bufs_.clear();
  for (auto& frag : info_.info.fragments) {
    for (auto var : col_vars) {
      const int8_t* buf = nullptr;
      size_t elem_count = 0;
      std::tie(buf, elem_count) =
          ColumnFetcher::getOneColumnFragment(executor_,
                                              *var,
                                              frag,
                                              Data_Namespace::CPU_LEVEL,
                                              0,
                                              chunks_owner_,
                                              column_cache_);
      CHECK_EQ(elem_count, frag.getNumTuples());
      input_bufs_.push_back(const_cast<int8_t*>(buf));
    }
    input_sizes_.push_back(frag.getNumTuples());
  }
}

PartitioningOptions TablePartitioner::getPassOpts(size_t pass_no) {
  // Compute partitioning options for the current pass.
  // We start partitioning using higher hash/value bits
  // and then add lower bits.
  PartitioningOptions res(po_);
  res.mask_bits = po_.mask_bits * (pass_no + 1) / po_.passes;
  res.scale_bits = po_.scale_bits + po_.mask_bits - res.mask_bits;
  return res;
}

CodeCacheKey TablePartitioner::getCodeCacheKey() const {
  std::stringstream ss;
  ss << po_.kind << " " << po_.mask_bits << " " << po_.scale_bits << " " << po_.passes;
  for (auto sz : elem_sizes_)
    ss << " " << sz;
  return CodeCacheKey({ss.str()});
}

void TablePartitioner::generatePartitioningModule() {
  CodeCacheKey cache_key = getCodeCacheKey();
  auto it = code_cache_.find(cache_key);
  if (it != code_cache_.cend()) {
    setupModule(it->second);
  } else {
    PartitioningCgen cgen;
    for (size_t pass_no = 0; pass_no < po_.passes; ++pass_no)
      cgen.genParitioningPass(elem_sizes_, key_count_, getPassOpts(pass_no));
    auto cache_val = cgen.compile(co_);
    setupModule(cache_val);
    code_cache_.put(cache_key, std::move(cache_val));
  }
}

void TablePartitioner::setupModule(const CodeCacheValWithModule& val) {
  module_ = val.second;
  pass_entries_ = std::get<0>(val.first.front());
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

  // Generate module if JIT is enabled.
  if (g_enable_jit_partitioning)
    generatePartitioningModule();

  std::vector<ResultSetPtr> partitions;
  for (size_t pass_no = 0; pass_no < po_.passes; ++pass_no) {
    bool last_pass = (pass_no == (po_.passes - 1));

    PartitioningOptions pass_opts = getPassOpts(pass_no);

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

    // partition_offsets vector holds write positions (in number of elements, not bytes)
    // in partitions for each input fragment. We use one-dimensional vector for more
    // efficient allocation, initialization and to enable vectorization.
    std::vector<size_t> partition_offsets(input_sizes_.size() *
                                          pass_opts.getPartitionsCount());
    computePartitionSizesAndOffsets(pass_no, pass_opts, partition_offsets);

    // Now we create result sets to hold partitioning output.
    std::vector<ssize_t> dummy;
    ColSlotContext slot_ctx(slots, dummy);
    slot_ctx.setAllSlotsPaddedSizeToLogicalSize();
    output_bufs_.clear();
    output_bufs_.reserve(output_sizes_.size() * elem_sizes_.size());
    for (size_t frag_id = 0; frag_id < output_sizes_.size(); ++frag_id) {
      QueryMemoryDescriptor mem_desc(QueryDescriptionType::Projection,
                                     executor_,
                                     slot_ctx,
                                     output_sizes_[frag_id],
                                     true);
      auto rs = std::make_shared<ResultSet>(
          targets, ExecutorDeviceType::CPU, mem_desc, row_set_mem_owner_, executor_);
      rs->setCachedRowCount(output_sizes_[frag_id]);

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
        output_bufs_.push_back(buff + mem_desc.getColOffInBytes(col_idx));
      }
      partitions.push_back(rs);
    }

    std::vector<std::future<void>> partitioning_threads;
    partitioning_threads.reserve(input_sizes_.size());
    for (size_t i = 0; i < input_sizes_.size(); ++i) {
      // run partitioning function
      if (g_enable_multi_thread_partitioning)
        partitioning_threads.emplace_back(
            utils::async([this, i, pass_no, &pass_opts, &partition_offsets] {
              doPartition(pass_no, pass_opts, i, partition_offsets);
            }));
      else
        partitioning_threads.emplace_back(std::async(
            std::launch::deferred, [this, i, pass_no, &pass_opts, &partition_offsets] {
              doPartition(pass_no, pass_opts, i, partition_offsets);
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
                                          int8_t** bufs,
                                          size_t row_no) {
  uint64_t value = 0;
  // FIXME: only one key column!
  if (pass_opts.kind == PartitioningOptions::HASH) {
    // FIXME: use GenericKeyHandler and a single Mumrmur call
    // to get the same hashing as for baseline hash table.
    for (size_t i = 0; i < key_count_; ++i) {
      size_t key_size = elem_sizes_[i];
      int8_t* key_p = bufs[i] + row_no * key_size;
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
    const int8_t* key_p = bufs[0] + row_no * key_size;
    switch (key_size) {
      case 2:
        value = *((const int16_t*)key_p);
        break;
      case 4:
        value = *((const int32_t*)key_p);
        break;
      case 8:
        value = *((const int64_t*)key_p);
        break;
      default:
        CHECK(false);
    }
  }

  uint64_t mask = pass_opts.getPartitionsCount() - 1;
  return (value >> pass_opts.scale_bits) & mask;
}

void TablePartitioner::collectHistogram(size_t pass_no,
                                        const PartitioningOptions& pass_opts,
                                        int frag_idx,
                                        size_t* histogram) {
  int8_t** input = input_bufs_.data() + frag_idx * elem_sizes_.size();
  size_t rows = input_sizes_[frag_idx];
  if (g_enable_jit_partitioning) {
    auto hist_fn = (void (*)(int8_t**, size_t, size_t*))pass_entries_[pass_no * 2];
    (*hist_fn)(input, rows, histogram);
  } else {
    for (size_t i = 0; i < rows; ++i) {
      uint32_t part_no = getPartitionNo(pass_opts, input, i);
      histogram[part_no]++;
    }
  }
}

void TablePartitioner::computePartitionSizesAndOffsets(
    size_t pass_no,
    const PartitioningOptions& pass_opts,
    std::vector<size_t>& partition_offsets) {
  auto pcnt = pass_opts.getPartitionsCount();
  std::vector<size_t> histograms(partition_offsets.size());

  std::vector<std::future<void>> working_threads;
  working_threads.reserve(input_sizes_.size());
  for (size_t i = 0, offs = 0; i < input_sizes_.size(); ++i, offs += pcnt) {
    // run partitioning function
    if (g_enable_multi_thread_partitioning)
      working_threads.emplace_back(utils::async(
          [this, pass_no, i, &pass_opts, histogram = histograms.data() + offs] {
            collectHistogram(pass_no, pass_opts, i, histogram);
          }));
    else
      working_threads.emplace_back(std::async(
          std::launch::deferred,
          [this, pass_no, i, &pass_opts, histogram = histograms.data() + offs] {
            collectHistogram(pass_no, pass_opts, i, histogram);
          }));
  }
  for (auto& thread : working_threads)
    thread.get();

  // Count partition sizes and offsets (in number of tuples).
  size_t idx = 0;
  for (; idx < (histograms.size() - pcnt); ++idx)
    partition_offsets[idx + pcnt] = partition_offsets[idx] + histograms[idx];

  output_sizes_.assign(pcnt, 0);
  for (size_t i = 0; idx < histograms.size(); ++idx, ++i) {
    output_sizes_[i] = partition_offsets[idx] + histograms[idx];
  }
}

#define CACHE_LINE_SIZE 64

#include <immintrin.h>

void TablePartitioner::nonTempStore(int8_t* dst, const int8_t* src, size_t size) {
  if (size != 64) {
    memcpy(dst, src, size);
    return;
  }

#ifdef __AVX512F__
  __m512i* d1 = (__m512i*)dst;
  __m512i s1 = *((__m512i*)src);

  _mm512_stream_si512(d1, s1);
#elif defined(__AVX__)
  register __m256i* d1 = (__m256i*)dst;
  register __m256i s1 = *((__m256i*)src);
  register __m256i* d2 = d1 + 1;
  register __m256i s2 = *(((__m256i*)src) + 1);

  _mm256_stream_si256(d1, s1);
  _mm256_stream_si256(d2, s2);

#elif defined(__SSE2__)

  __m128i* d1 = (__m128i*)dst;
  __m128i* d2 = d1 + 1;
  __m128i* d3 = d1 + 2;
  __m128i* d4 = d1 + 3;
  __m128i s1 = *(__m128i*)src;
  __m128i s2 = *((__m128i*)src + 1);
  __m128i s3 = *((__m128i*)src + 2);
  __m128i s4 = *((__m128i*)src + 3);

  _mm_stream_si128(d1, s1);
  _mm_stream_si128(d2, s2);
  _mm_stream_si128(d3, s3);
  _mm_stream_si128(d4, s4);

#else
  // Regular memcpy
  memcpy(dst, src, size);
#endif
}

// If enabled, use software-write-combined-buffer technique to fill partitions
void TablePartitioner::copyWithSWCB(int8_t* swcb_buf,
                                    size_t swcb_buf_size,
                                    int8_t* real_dst,
                                    const int8_t* src,
                                    size_t elem_size) {
  auto swcb_limit = CACHE_LINE_SIZE / elem_size - 1;
  auto pos = swcb_buf_size & swcb_limit;
  memcpy(swcb_buf + pos * elem_size, src, elem_size);
  // drop buffer in a partition when it is full
  if (pos == swcb_limit) {
    nonTempStore(real_dst - swcb_limit * elem_size, swcb_buf, CACHE_LINE_SIZE);
  }
}

void TablePartitioner::remainderCopyWithSWCB(int8_t* swcb_buf,
                                             size_t swcb_buf_size,
                                             int8_t* real_dst,
                                             size_t elem_size) {
  auto swcb_limit = CACHE_LINE_SIZE / elem_size - 1;
  auto rem_size = swcb_buf_size & swcb_limit;
  real_dst -= rem_size * elem_size;
  for (uint32_t j = 0; j < rem_size; ++j) {
    memcpy(real_dst, swcb_buf, elem_size);
    real_dst += elem_size;
    swcb_buf += elem_size;
  }
}

void TablePartitioner::initSWCBuffers(int frag_idx,
                                      const uint32_t part_count,
                                      std::vector<std::vector<int8_t*>>& swcb_bufs,
                                      std::vector<std::vector<size_t>>& swcb_sizes,
                                      std::vector<std::vector<size_t>>& unswcb_elts,
                                      std::vector<std::vector<bool>>& can_done_swcb) {
  // Fill initial information about swcb buffers and allocate them
  swcb_bufs.resize(part_count);
  swcb_sizes.resize(part_count);
  unswcb_elts.resize(part_count);
  can_done_swcb.resize(part_count);
  for (uint32_t i = 0; i < part_count; ++i) {
    // swcb_sizes[i] = partition_offsets[frag_idx][i];
    unswcb_elts[i].resize(elem_sizes_.size(), 0);
    can_done_swcb[i].resize(elem_sizes_.size(), 0);
    swcb_sizes[i].resize(elem_sizes_.size(), 0);
    for (uint32_t j = 0; j < elem_sizes_.size(); j++) {
      auto alloc_buf = static_cast<int8_t*>(aligned_alloc(
          CACHE_LINE_SIZE, elem_sizes_[j] * (CACHE_LINE_SIZE / elem_sizes_[j])));
      if (!alloc_buf)
        throw std::runtime_error("Not enough memory for software-combined buffer");
      swcb_bufs[i].push_back(alloc_buf);
    }
  }
}

void TablePartitioner::finalizeSWCBuffers(int frag_idx,
                                          const uint32_t part_count,
                                          std::vector<size_t>& partition_offsets,
                                          std::vector<std::vector<int8_t*>>& swcb_bufs,
                                          std::vector<std::vector<size_t>>& swcb_sizes,
                                          std::vector<std::vector<size_t>>& unswcb_elts) {
  for (uint32_t part_no = 0; part_no < part_count; ++part_no) {
    auto offsets = partition_offsets.data() + frag_idx * part_count;
    int8_t** output = output_bufs_.data() + part_no * elem_sizes_.size();
    for (uint32_t j = 0; j < elem_sizes_.size(); j++) {
      int size = swcb_sizes[part_no][j];
      if (size > 0) {
        auto pos = offsets[part_no] + unswcb_elts[part_no][j];
        auto elem_size = elem_sizes_.at(j);
        remainderCopyWithSWCB(&(swcb_bufs[part_no][j][0]),
                              size,
                              output[j] + (pos + size) * elem_size,
                              elem_size);
      }
    }
  }
  for (uint32_t i = 0; i < part_count; ++i) {
    for (uint32_t j = 0; j < swcb_bufs[i].size(); j++) {
      free(swcb_bufs[i][j]);
    }
  }
}

bool TablePartitioner::canStartSWCB(size_t offset_addr) {
  // TODO: we may choose alignment depending on vector size
  return (offset_addr & (CACHE_LINE_SIZE - 1)) == 0;
}

void TablePartitioner::doPartition(size_t pass_no,
                                   const PartitioningOptions& pass_opts,
                                   int frag_idx,
                                   std::vector<size_t>& partition_offsets) {
  int8_t** input = input_bufs_.data() + frag_idx * elem_sizes_.size();
  auto part_count = pass_opts.getPartitionsCount();
  size_t rows = input_sizes_[frag_idx];
  size_t* offsets = partition_offsets.data() + frag_idx * part_count;
  if (g_enable_jit_partitioning) {
    auto hist_fn =
        (void (*)(int8_t**, size_t, int8_t**, size_t*))pass_entries_[pass_no * 2 + 1];
    (*hist_fn)(input, rows, output_bufs_.data(), offsets);
  } else {
    // Software combine buffers
    std::vector<std::vector<int8_t*>> swcb_bufs;
    std::vector<std::vector<size_t>> swcb_sizes;
    std::vector<std::vector<size_t>> unswcb_elts;
    std::vector<std::vector<bool>> can_done_swcb;
    if (g_radix_use_swcb) {
      initSWCBuffers(
          frag_idx, part_count, swcb_bufs, swcb_sizes, unswcb_elts, can_done_swcb);
    }
    for (size_t i = 0; i < rows; ++i) {
      uint32_t part_no = getPartitionNo(pass_opts, input, i);
      int8_t** output = output_bufs_.data() + part_no * elem_sizes_.size();
      auto pos = offsets[part_no];
      // fill partition
      for (size_t elem_idx = 0; elem_idx < elem_sizes_.size(); ++elem_idx) {
        auto elem_size = elem_sizes_[elem_idx];
        auto input_elem_pos = input[elem_idx] + i * elem_size;
        if (g_radix_use_swcb) {
          auto init_off = pos + unswcb_elts[part_no][elem_idx];
          auto size = swcb_sizes[part_no][elem_idx];
          can_done_swcb[part_no][elem_idx] =
              can_done_swcb[part_no][elem_idx]
                  ? can_done_swcb[part_no][elem_idx]
                  : canStartSWCB((int64_t)(output[elem_idx] + init_off * elem_size));
          if (can_done_swcb[part_no][elem_idx]) {
            copyWithSWCB(&(swcb_bufs[part_no][elem_idx][0]),
                         size,
                         output[elem_idx] + (size + init_off) * elem_size,
                         input_elem_pos,
                         elem_size);
          } else {
            memcpy(output[elem_idx] + init_off * elem_size, input_elem_pos, elem_size);
          }
        } else
          // Regular store, no swcb
          memcpy(output[elem_idx] + pos * elem_size, input_elem_pos, elem_size);
        if (g_radix_use_swcb) {
          if (!can_done_swcb[part_no][elem_idx])
            unswcb_elts[part_no][elem_idx]++;
          else
            swcb_sizes[part_no][elem_idx]++;
        }
      }
      if (!g_radix_use_swcb)
        // Regular store, no swcb
        offsets[part_no] = pos + 1;
    }
    if (g_radix_use_swcb) {
      finalizeSWCBuffers(
          frag_idx, part_count, partition_offsets, swcb_bufs, swcb_sizes, unswcb_elts);
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
