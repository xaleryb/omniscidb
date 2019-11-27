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

#include "TablePartitionerCgen.h"
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/IR/LegacyPassManager.h>
#include <llvm/Support/SourceMgr.h>
#include <llvm/Support/raw_os_ostream.h>
#include <llvm/Transforms/IPO.h>
#include <llvm/Transforms/IPO/AlwaysInliner.h>
#include <llvm/Transforms/InstCombine/InstCombine.h>
#include <llvm/Transforms/Instrumentation.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/Cloning.h>
#include "CodeGenerator.h"
#include "IRCodegenUtils.h"
#include "LLVMGlobalContext.h"
#include "Shared/mapdpath.h"

#if LLVM_VERSION_MAJOR >= 7
#include <llvm/Transforms/Scalar/InstSimplifyPass.h>
#include <llvm/Transforms/Utils.h>
#endif

namespace {

llvm::Module* read_part_rt_module(llvm::LLVMContext& context) {
  llvm::SMDiagnostic err;

  auto buffer_or_error = llvm::MemoryBuffer::getFile(
      mapd_root_abs_path() + "/QueryEngine/TablePartitionerRuntime.bc");
  CHECK(!buffer_or_error.getError());
  llvm::MemoryBuffer* buffer = buffer_or_error.get().get();

  auto owner = llvm::parseBitcodeFile(buffer->getMemBufferRef(), context);
  CHECK(!owner.takeError());
  auto module = owner.get().release();
  CHECK(module);

  for (llvm::Function& f : *module) {
    if (!f.isDeclaration()) {
      f.setLinkage(llvm::GlobalValue::InternalLinkage);
    }
  }

  return module;
}

std::unique_ptr<llvm::Module> g_part_rt_module(
    read_part_rt_module(getGlobalLLVMContext()));

}  // namespace

PartitioningCgen::PartitioningCgen()
    : context_(getGlobalLLVMContext()), ir_builder_(context_) {
  module_ = llvm::CloneModule(
#if LLVM_VERSION_MAJOR >= 7
      *g_part_rt_module.get()
#else
      g_part_rt_module.get()
#endif
  );
}

void PartitioningCgen::genCommonForLoop(
    const std::vector<size_t>& elem_sizes,
    size_t key_count,
    const PartitioningOptions& pass_opts,
    llvm::Function* fn,
    std::function<void(const std::vector<llvm::Value*>&, llvm::Value*)> proc_fn) {
  size_t key_component_size = 0;
  for (size_t i = 0; i < key_count; ++i)
    key_component_size = std::max(key_component_size, elem_sizes[i]);
  if (key_component_size < 4)
    key_component_size = 4;
  CHECK(key_component_size == 4 || key_component_size == 8);
  auto key_elem_ty = llvm::Type::getIntNTy(context_, key_component_size * 8);
  auto int64_ty = llvm::Type::getInt64Ty(context_);
  auto int_ty = llvm::Type::getIntNTy(context_, sizeof(int) * 8);

  // Extract input args.
  auto input = getInputArg(fn);
  auto rows = getRowsArg(fn);

  // Create main basic blocks.
  auto entry_bb = llvm::BasicBlock::Create(context_, "entry", fn);
  auto body_bb = llvm::BasicBlock::Create(context_, "for_loop", fn);
  auto exit_bb = llvm::BasicBlock::Create(context_, "exit", fn);

  // Fill entry blocks. Compute some loop invariants and check for empty input.
  std::vector<llvm::Value*> col_bufs;
  llvm::Value* key_p = nullptr;
  ir_builder_.SetInsertPoint(entry_bb);
  for (size_t col = 0; col < elem_sizes.size(); ++col) {
    auto col_buf_p = ir_builder_.CreateGEP(input, llvm::ConstantInt::get(int64_ty, col));
    auto col_buf = ir_builder_.CreateLoad(col_buf_p);
    auto col_buf_ty =
        llvm::Type::getIntNTy(context_, elem_sizes[col] * 8)->getPointerTo();
    col_bufs.push_back(
        ir_builder_.CreatePointerCast(col_buf, col_buf_ty, "col" + std::to_string(col)));
  }
  if (pass_opts.kind == PartitioningOptions::HASH && key_count > 1) {
    key_p = ir_builder_.CreateAlloca(
        key_elem_ty, llvm::ConstantInt::get(int64_ty, key_count), "key");
  }
  auto non_empty =
      ir_builder_.CreateICmpUGT(rows, llvm::ConstantInt::get(rows->getType(), 0));
  ir_builder_.CreateCondBr(non_empty, body_bb, exit_bb);

  // Create phi node for the current pos.
  ir_builder_.SetInsertPoint(body_bb);
  auto pos = ir_builder_.CreatePHI(rows->getType(), 2, "pos");
  pos->addIncoming(llvm::ConstantInt::get(pos->getType(), 0), entry_bb);

  // Load input elements and build a key for hash computation.
  std::vector<llvm::Value*> input_elems;
  for (size_t col = 0; col < elem_sizes.size(); ++col) {
    auto elem_p = ir_builder_.CreateGEP(col_bufs[col], pos);
    auto elem = ir_builder_.CreateLoad(elem_p, "elem" + std::to_string(col));
    if (pass_opts.kind == PartitioningOptions::HASH && key_count > 1) {
      auto key_elem =
          ir_builder_.CreateSExt(elem, key_elem_ty, "key" + std::to_string(col));
      ir_builder_.CreateStore(
          key_elem, ir_builder_.CreateGEP(key_p, llvm::ConstantInt::get(int64_ty, col)));
    }
    input_elems.push_back(elem);
  }

  // Compute partition number.
  llvm::Value* hash_val = nullptr;
  if (pass_opts.kind == PartitioningOptions::HASH) {
    auto hash_fn = getHashFunction(key_component_size, key_count, pass_opts);
    std::vector<llvm::Value*> args;
    if (hash_fn->arg_size() == 1) {
      args.push_back(ir_builder_.CreateSExt(input_elems[0], key_elem_ty, "key"));
    } else {
      CHECK_EQ(hash_fn->arg_size(), 2);
      args.push_back(key_p);
      args.push_back(llvm::ConstantInt::get(int_ty, key_component_size * key_count));
    }
    hash_val = ir_builder_.CreateCall(hash_fn, args, "hash_val");
  } else {
    CHECK(pass_opts.kind == PartitioningOptions::VALUE);
    hash_val = ir_builder_.CreateSExt(input_elems[0], key_elem_ty);
  }
  size_t mask = pass_opts.getPartitionsCount() - 1;
  auto part_no = ir_builder_.CreateSExt(
      ir_builder_.CreateAnd(ir_builder_.CreateLShr(hash_val, pass_opts.scale_bits), mask),
      int64_ty,
      "part_no");

  proc_fn(input_elems, part_no);

  // Inc current pos and check if loop is over.
  auto inc_pos =
      ir_builder_.CreateAdd(pos, llvm::ConstantInt::get(pos->getType(), 1), "next_pos");
  pos->addIncoming(inc_pos, body_bb);
  auto finished_loop = ir_builder_.CreateICmpUGE(inc_pos, rows);
  ir_builder_.CreateCondBr(finished_loop, exit_bb, body_bb);

  // Generate return.
  ir_builder_.SetInsertPoint(exit_bb);
  ir_builder_.CreateRetVoid();

  verify_function_ir(fn);
  entries_.push_back(fn);
}

void PartitioningCgen::genHistogramFn(const std::vector<size_t>& elem_sizes,
                                      size_t key_count,
                                      const PartitioningOptions& pass_opts) {
  auto hist_fn = PartitioningCgen::createHistogramFunc(pass_opts);

  auto proc_fn = [this, hist_fn](const std::vector<llvm::Value*>& input_elems,
                                 llvm::Value* part_no) {
    auto output = getOutputArg(hist_fn);
    auto counter_p = ir_builder_.CreateGEP(output, part_no, "counter_p");
    auto counter = ir_builder_.CreateLoad(counter_p, "counter");
    auto inc_counter = ir_builder_.CreateAdd(
        counter, llvm::ConstantInt::get(counter->getType(), 1), "inc_counter");
    ir_builder_.CreateStore(inc_counter, counter_p);
  };

  genCommonForLoop(elem_sizes, key_count, pass_opts, hist_fn, proc_fn);
}

void PartitioningCgen::genPartitioningFn(const std::vector<size_t>& elem_sizes,
                                         size_t key_count,
                                         const PartitioningOptions& pass_opts) {
  auto part_fn = PartitioningCgen::createPartitioningFunc(pass_opts);

  auto proc_fn = [this, part_fn, &elem_sizes](
                     const std::vector<llvm::Value*>& input_elems, llvm::Value* part_no) {
    auto output = getOutputArg(part_fn);
    auto offsets = getOffsetsArg(part_fn);
    auto int64_ty = llvm::Type::getInt64Ty(context_);

    // Store data.
    auto part_output = ir_builder_.CreateGEP(
        output,
        ir_builder_.CreateMul(
            part_no, llvm::ConstantInt::get(part_no->getType(), elem_sizes.size())));
    auto offset_p = ir_builder_.CreateGEP(offsets, part_no);
    auto offset = ir_builder_.CreateLoad(offset_p, "offset");
    for (size_t col = 0; col < elem_sizes.size(); ++col) {
      auto col_output_p =
          ir_builder_.CreateGEP(part_output, llvm::ConstantInt::get(int64_ty, col));
      auto col_output_ty =
          llvm::Type::getIntNTy(context_, elem_sizes[col] * 8)->getPointerTo();
      auto col_output = ir_builder_.CreatePointerCast(
          ir_builder_.CreateLoad(col_output_p), col_output_ty);
      auto out_pos = ir_builder_.CreateGEP(col_output, offset);
      ir_builder_.CreateStore(input_elems[col], out_pos);
    }
    // Store new offset.
    auto inc_offset =
        ir_builder_.CreateAdd(offset, llvm::ConstantInt::get(offset->getType(), 1));
    ir_builder_.CreateStore(inc_offset, offset_p);
  };

  genCommonForLoop(elem_sizes, key_count, pass_opts, part_fn, proc_fn);
}

void PartitioningCgen::genParitioningPass(const std::vector<size_t>& elem_sizes,
                                          size_t key_count,
                                          const PartitioningOptions& pass_opts) {
  genHistogramFn(elem_sizes, key_count, pass_opts);
  genPartitioningFn(elem_sizes, key_count, pass_opts);
}

CodeCacheValWithModule PartitioningCgen::compile(const CompilationOptions& co) {
  CHECK(!entries_.empty());
  auto live_funcs = CodeGenerator::markDeadRuntimeFuncs(*module_, entries_, {});
  auto execution_engine =
      CodeGenerator::generateNativeCPUCode(entries_[0], live_funcs, co);
  // Module ownership is taken by code generator.
  auto module = module_.release();
  std::vector<void*> native_entries;
  for (auto entry : entries_) {
    auto fn_ptr = execution_engine->getPointerToFunction(entry);
    CHECK(fn_ptr);
    native_entries.push_back(fn_ptr);
  }
  CodeCacheValWithModule res;
  res.first.emplace_back(std::move(native_entries), std::move(execution_engine), nullptr);
  res.second = module;
  return res;
}

std::string PartitioningCgen::getHistogramFunctionName(
    const PartitioningOptions& pass_opts) const {
  std::stringstream ss;
  ss << "histogram_collection_pass_" << pass_opts.mask_bits << "_"
     << pass_opts.scale_bits;
  return ss.str();
}

std::string PartitioningCgen::getPartitioningFunctionName(
    const PartitioningOptions& pass_opts) const {
  std::stringstream ss;
  ss << "partitioning_pass_" << pass_opts.mask_bits << "_" << pass_opts.scale_bits;
  return ss.str();
}

llvm::Function* PartitioningCgen::createHistogramFunc(
    const PartitioningOptions& pass_opts) {
  // We want to create a function with following signature:
  //   histogram_collection_pass_N_S(int8_t **input, size_t rows, int64_t *output);
  std::string fn_name = getHistogramFunctionName(pass_opts);
  std::vector<llvm::Type*> arg_types;

  arg_types.push_back(llvm::Type::getInt8PtrTy(context_)->getPointerTo());
  arg_types.push_back(llvm::Type::getInt64Ty(context_));
  arg_types.push_back(llvm::Type::getInt64PtrTy(context_));

  // generate the function
  auto fn_type =
      llvm::FunctionType::get(llvm::Type::getVoidTy(context_), arg_types, false);
  auto fn = llvm::Function::Create(
      fn_type, llvm::Function::ExternalLinkage, fn_name, module_.get());

  auto arg_it = fn->arg_begin();
  arg_it->setName("input");
  ++arg_it;
  arg_it->setName("rows");
  ++arg_it;
  arg_it->setName("output");

  return fn;
}

llvm::Function* PartitioningCgen::createPartitioningFunc(
    const PartitioningOptions& pass_opts) {
  // We want to create a function with following signature:
  //   partitioning_pass_N_S(int8_t **input, size_t rows, int8_t **output, int64_t
  //   **offsets);
  std::string fn_name = getPartitioningFunctionName(pass_opts);
  std::vector<llvm::Type*> arg_types;

  arg_types.push_back(llvm::Type::getInt8PtrTy(context_)->getPointerTo());
  arg_types.push_back(llvm::Type::getInt64Ty(context_));
  arg_types.push_back(llvm::Type::getInt8PtrTy(context_)->getPointerTo());
  arg_types.push_back(llvm::Type::getInt64PtrTy(context_));

  // generate the function
  auto fn_type =
      llvm::FunctionType::get(llvm::Type::getVoidTy(context_), arg_types, false);
  auto fn = llvm::Function::Create(
      fn_type, llvm::Function::ExternalLinkage, fn_name, module_.get());

  auto arg_it = fn->arg_begin();
  arg_it->setName("input");
  ++arg_it;
  arg_it->setName("rows");
  ++arg_it;
  arg_it->setName("output");
  ++arg_it;
  arg_it->setName("offsets");

  return fn;
}

llvm::Function* PartitioningCgen::getHashFunction(int key_component_size,
                                                  size_t key_count,
                                                  const PartitioningOptions& pass_opts) {
  llvm::Function* res;
  bool long_hash = ((pass_opts.scale_bits + pass_opts.scale_bits) > 32);
  if (key_count == 1) {
    if (key_component_size == 4)
      res = module_->getFunction(long_hash ? "MurmurHash64_4" : "MurmurHash32_4");
    else {
      CHECK_EQ(key_component_size, 8);
      res = module_->getFunction(long_hash ? "MurmurHash64_8" : "MurmurHash32_8");
    }
  } else {
    res = module_->getFunction(long_hash ? "MurmurHash64" : "MurmurHash32");
  }
  CHECK(res) << "Cannot find hash function in table partitioner runtime module";
  return res;
}

llvm::Value* PartitioningCgen::getInputArg(llvm::Function* fn) {
  return fn->arg_begin() + 0;
}

llvm::Value* PartitioningCgen::getRowsArg(llvm::Function* fn) {
  return fn->arg_begin() + 1;
}

llvm::Value* PartitioningCgen::getOutputArg(llvm::Function* fn) {
  return fn->arg_begin() + 2;
}

llvm::Value* PartitioningCgen::getOffsetsArg(llvm::Function* fn) {
  return fn->arg_begin() + 3;
}
