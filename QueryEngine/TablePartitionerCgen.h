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

#ifndef QUERYENGINE_TABLEPARTITIONERCGEN_H
#define QUERYENGINE_TABLEPARTITIONERCGEN_H

#include <llvm/IR/IRBuilder.h>
#include "CodeCache.h"
#include "Partitioning.h"

class PartitioningCgen {
 public:
  PartitioningCgen();

  void genParitioningPass(const std::vector<size_t>& elem_sizes,
                          size_t key_count,
                          const PartitioningOptions& pass_opts);
  CodeCacheValWithModule compile(const CompilationOptions& co);

  std::string getHistogramFunctionName(const PartitioningOptions& pass_opts) const;
  std::string getPartitioningFunctionName(const PartitioningOptions& pass_opts) const;

 private:
  void genCommonForLoop(
      const std::vector<size_t>& elem_sizes,
      size_t key_count,
      const PartitioningOptions& pass_opts,
      llvm::Function* fn,
      std::function<void(const std::vector<llvm::Value*>&, llvm::Value*)> proc_fn);
  void genHistogramFn(const std::vector<size_t>& elem_sizes,
                      size_t key_count,
                      const PartitioningOptions& pass_opts);
  void genPartitioningFn(const std::vector<size_t>& elem_sizes,
                         size_t key_count,
                         const PartitioningOptions& pass_opts);
  llvm::Function* createHistogramFunc(const PartitioningOptions& pass_opts);
  llvm::Function* createPartitioningFunc(const PartitioningOptions& pass_opts);
  llvm::Function* getHashFunction(int key_component_size,
                                  size_t key_count,
                                  const PartitioningOptions& pass_opts);

  llvm::Value* getInputArg(llvm::Function* fn);
  llvm::Value* getRowsArg(llvm::Function* fn);
  llvm::Value* getOutputArg(llvm::Function* fn);
  llvm::Value* getOffsetsArg(llvm::Function* fn);

  llvm::LLVMContext& context_;
  llvm::IRBuilder<> ir_builder_;
  std::unique_ptr<llvm::Module> module_;
  std::vector<llvm::Function*> entries_;
};

#endif  // QUERYENGINE_TABLEPARTITIONER_H