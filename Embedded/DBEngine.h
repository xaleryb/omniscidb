/*
 * Copyright 2020 OmniSci, Inc.
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

#pragma once

#include "DBETypes.h"
#include <arrow/table.h>

namespace EmbeddedDatabase {

class Cursor {
 public:
  size_t getColCount();
  size_t getRowCount();
  Row getNextRow();
  ColumnType getColType(uint32_t col_num);
  std::shared_ptr<arrow::RecordBatch> getArrowRecordBatch();

 protected:
  Cursor() {}
  Cursor(const Cursor&) = delete;
  Cursor& operator=(const Cursor&) = delete;
};

class DBEngine {
 public:
  void reset();
  void executeDDL(const std::string& query);
  Cursor* executeDML(const std::string& query);
  static DBEngine* create(const std::string &path, int calcite_port,  bool enable_columnar_output);
  std::vector<std::string> getTables();
  std::vector<ColumnDetails> getTableDetails(const std::string& table_name);
  void createArrowTable(const std::string&, std::shared_ptr<arrow::Table> &table);
  
 protected:
  DBEngine() {}
  DBEngine(const DBEngine&) = delete;
  DBEngine& operator=(const DBEngine&) = delete;
};
}  // namespace EmbeddedDatabase
