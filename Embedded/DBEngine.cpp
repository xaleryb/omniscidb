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

#include "DBEngine.h"
#include "DBEManager.h"
#include "DataMgr/ForeignStorage/ArrowForeignStorage.h"
#include "Fragmenter/FragmentDefaultValues.h"
#include "QueryEngine/ArrowResultSet.h"
#include "QueryRunner/QueryRunner.h"
#include "Parser/ParserWrapper.h"
#include "Parser/parser.h"
#include "QueryEngine/Execute.h"

extern bool g_enable_union;

using QR = QueryRunner::QueryRunner;

namespace EmbeddedDatabase {


/**
 * Cursor internal implementation
 */
class CursorImpl : public Cursor {
 public:
  CursorImpl(std::shared_ptr<ResultSet> result_set,
             std::vector<std::string> col_names)
      : result_set_(result_set), col_names_(col_names) {}

  ~CursorImpl() {
    col_names_.clear();
    record_batch_.reset();
    result_set_.reset();
  }

  size_t getColCount() { return result_set_ ? result_set_->colCount() : 0; }

  size_t getRowCount() { return result_set_ ? result_set_->rowCount() : 0; }

  Row getNextRow() {
    if (result_set_) {
      auto row = result_set_->getNextRow(true, false);
      return row.empty() ? Row() : Row(row);
    }
    return Row();
  }

  ColumnType getColType(uint32_t col_num) {
    if (col_num < getColCount()) {
      SQLTypeInfo type_info = result_set_->getColType(col_num);
      return sqlToColumnType(type_info.get_type());
    }
    return ColumnType::UNKNOWN;
  }

  std::shared_ptr<arrow::RecordBatch> getArrowRecordBatch() {
    if (record_batch_) {
      return record_batch_;
    }
    auto col_count = getColCount();
    if (col_count > 0) {
      auto row_count = getRowCount();
      if (row_count > 0) {
        auto converter = std::make_unique<ArrowResultSetConverter>(
          result_set_, col_names_, -1);
        record_batch_ = converter->convertToArrow();
        return record_batch_;
      }
    }
    return nullptr;
  }

 private:
  std::shared_ptr<ResultSet> result_set_;
  std::vector<std::string> col_names_;
  std::shared_ptr<arrow::RecordBatch> record_batch_;
};


/**
 * DBEngine internal implementation
 */
class DBEngineImpl : public DBEngine {

 public:

  DBEngineImpl(const std::string& base_path, int port, const std::string& udf_filename = "") {
    manager_ = getManager(base_path, port, udf_filename);
  }

  ~DBEngineImpl() {
  }

  bool initialized() {
    return manager_->initialized();
  }

  void executeDDL(const std::string& query) {
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return;
    }
    try {
      QR::get()->runDDLStatement(query);
    } catch (std::exception const& e) {
      std::cerr << "DBE:executeDDL: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "DBE:executeDDL: Unknown exception" << std::endl;
    }
  }

  void createArrowTable(const std::string& name, std::shared_ptr<arrow::Table>& table) {
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return;
    }
    setArrowTable(name, table);
    try {
      auto session = QR::get()->getSession();
      TableDescriptor td;
      td.tableName = name;
      td.userId = session->get_currentUser().userId;
      td.storageType = "ARROW:" + name;
      td.persistenceLevel = Data_Namespace::MemoryLevel::CPU_LEVEL;
      td.isView = false;
      td.fragmenter = nullptr;
      td.fragType = Fragmenter_Namespace::FragmenterType::INSERT_ORDER;
      td.maxFragRows = DEFAULT_FRAGMENT_ROWS;
      td.maxChunkSize = DEFAULT_MAX_CHUNK_SIZE;
      td.fragPageSize = DEFAULT_PAGE_SIZE;
      td.maxRows = DEFAULT_MAX_ROWS;
      td.keyMetainfo = "[]";

      std::list<ColumnDescriptor> cols;
      std::vector<Parser::SharedDictionaryDef> dictionaries;
      auto catalog = QR::get()->getCatalog();
      // nColumns
      catalog->createTable(td, cols, dictionaries, false);
      Catalog_Namespace::SysCatalog::instance().createDBObject(
          session->get_currentUser(), td.tableName, TableDBObjectType, *catalog);
    } catch (...) {
      releaseArrowTable(name);
      throw;
    }
    releaseArrowTable(name);
  }

  std::unique_ptr<CursorImpl> executeDML(const std::string& query) {
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return std::unique_ptr<CursorImpl>();
    }
    try {
      ParserWrapper pw{query};
      if (pw.isCalcitePathPermissable()) {
        const auto execution_result =
          QR::get()->runSelectQuery(query, ExecutorDeviceType::CPU, true, true);
        auto targets = execution_result.getTargetsMeta();
        std::vector<std::string> col_names;
        for (const auto target : targets) {
          col_names.push_back(target.get_resname());
        }
        return std::make_unique<CursorImpl>(execution_result.getRows(), col_names);
      }

      auto session_info = QR::get()->getSession();
      auto query_state = QR::create_query_state(session_info, query);
      auto stdlog = STDLOG(query_state);

      SQLParser parser;
      std::list<std::unique_ptr<Parser::Stmt>> parse_trees;
      std::string last_parsed;
      CHECK_EQ(parser.parse(query, parse_trees, last_parsed), 0) << query;
      CHECK_EQ(parse_trees.size(), size_t(1));
      auto stmt = parse_trees.front().get();
      auto insert_values_stmt = dynamic_cast<InsertValuesStmt*>(stmt);
      CHECK(insert_values_stmt);
      insert_values_stmt->execute(*session_info);
    } catch (std::exception const& e) {
      std::cerr << "DBE:executeDML: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "DBE:executeDML: Unknown exception" << std::endl;
    }
    return std::unique_ptr<CursorImpl>();
  }

  std::unique_ptr<CursorImpl> executeRA(const std::string& query) {
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return std::unique_ptr<CursorImpl>();
    }
    try {
      const auto execution_result =
          QR::get()->runSelectQueryRA(query, ExecutorDeviceType::CPU, true, true);
      auto targets = execution_result.getTargetsMeta();
      std::vector<std::string> col_names;
      for (const auto target : targets) {
        col_names.push_back(target.get_resname());
      }
      return std::make_unique<CursorImpl>(execution_result.getRows(), col_names);
    } catch (std::exception const& e) {
      std::cerr << "DBE:executeRA: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "DBE:executeRA: Unknown exception" << std::endl;
    }
    return std::unique_ptr<CursorImpl>();
  }

  std::vector<std::string> getTables() {
    std::vector<std::string> table_names;
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return table_names;
    }
    auto catalog = QR::get()->getCatalog();
    if (catalog) {
      try {
        const auto tables = catalog->getAllTableMetadata();
        for (const auto td : tables) {
          if (td->shard >= 0) {
            // skip shards, they're not standalone tables
            continue;
          }
          table_names.push_back(td->tableName);
        }
      } catch (std::exception const& e) {
        std::cerr << "DBE:getTables: " << e.what() << std::endl;
      }
    } else {
      std::cerr << "DBE:getTables: catalog is NULL" << std::endl;
    }
    return table_names;
  }

  std::vector<ColumnDetails> getTableDetails(const std::string& table_name) {
    std::vector<ColumnDetails> result;
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return result;
    }

    auto catalog = QR::get()->getCatalog();
    if (catalog) {
      auto metadata = catalog->getMetadataForTable(table_name, false);
      if (metadata) {
        const auto col_descriptors = catalog->getAllColumnMetadataForTable(
          metadata->tableId, false, true, false);
        const auto deleted_cd = catalog->getDeletedColumn(metadata);
        for (const auto cd : col_descriptors) {
          if (cd == deleted_cd) {
            continue;
          }
          ColumnDetails col_details;
          col_details.col_name = cd->columnName;
          auto ct = cd->columnType;
          SQLTypes sql_type = ct.get_type();
          EncodingType sql_enc = ct.get_compression();
          col_details.col_type = sqlToColumnType(sql_type);
          col_details.encoding = sqlToColumnEncoding(sql_enc);
          col_details.nullable = !ct.get_notnull();
          col_details.is_array = (sql_type == kARRAY);
          if (IS_GEO(sql_type)) {
            col_details.precision = static_cast<int>(ct.get_subtype());
            col_details.scale = ct.get_output_srid();
          } else {
            col_details.precision = ct.get_precision();
            col_details.scale = ct.get_scale();
          }
          if (col_details.encoding == ColumnEncoding::DICT) {
            // have to get the actual size of the encoding from the dictionary
            // definition
            const int dict_id = ct.get_comp_param();
            auto dd = catalog->getMetadataForDict(dict_id, false);
            if (dd) {
              col_details.comp_param = dd->dictNBits;
            } else {
              std::cerr << "DBE:getTableDetails: Dictionary doesn't exist" << std::endl;
            }
          } else {
            col_details.comp_param = ct.get_comp_param();
            if (ct.is_date_in_days() && col_details.comp_param == 0) {
              col_details.comp_param = 32;
            }
          }
          result.push_back(col_details);
        }
      }
    }
    return result;
  }

  void createUser(const std::string& user_name, const std::string& password) {
    manager_->createUser(user_name, password);
  }

  void dropUser(const std::string& user_name) {
    manager_->dropUser(user_name);
  }

  void createDatabase(const std::string& db_name) {
    manager_->createDatabase(db_name);
  }

  void dropDatabase(const std::string& db_name) {
    manager_->dropDatabase(db_name);
  }

  bool setDatabase(std::string& db_name) {
    return manager_->setDatabase(db_name);
  }

  bool login(std::string& db_name,
             std::string& user_name,
             const std::string& password) {
    return manager_->login(db_name, user_name, password);
  }

 private:

  std::shared_ptr<DBEManager> getManager(const std::string& base_path, int port, const std::string& udf_filename) {
    std::shared_ptr<DBEManager> temp = master_manager_.lock();
    if (!temp) {
      temp.reset(new DBEManager(base_path, port, udf_filename));
      master_manager_ = temp;
    }
    return temp;
  }

  std::shared_ptr<DBEManager> manager_;
  static std::weak_ptr<DBEManager>  master_manager_;
};

std::weak_ptr<DBEManager> DBEngineImpl::master_manager_;

DBEngine* DBEngine::create(const std::string& path, int port) {
  g_enable_union = false;
  g_enable_columnar_output = true;
  return new DBEngineImpl(path, port);
}

DBEngine* DBEngine::create(const std::map<std::string, std::string>& parameters) {
  int port = DEFAULT_CALCITE_PORT;
  std::string path, udf_filename;
  g_enable_union = false;
  g_enable_columnar_output = true;
  for (const auto& [key, value]: parameters) {
    if (key == "path") {
      path = value;
    } else if (key == "port") {
      port = std::stoi(value);
    } else if (key == "enable_columnar_output") {
      g_enable_columnar_output = std::stoi(value);
    } else if (key == "enable_union") {
      g_enable_union = std::stoi(value);
    } else if (key == "enable_debug_timer") {
      g_enable_debug_timer = std::stoi(value);
    } else if (key == "enable_lazy_fetch") {
      g_enable_lazy_fetch = std::stoi(value);
    } else if (key == "udf_filename") {
      udf_filename = value;
    }
  }
  return new DBEngineImpl(path, port, udf_filename);
}

/** DBEngine downcasting methods */

inline DBEngineImpl* getImpl(DBEngine* ptr) {
  return (DBEngineImpl*)ptr;
}

inline const DBEngineImpl* getImpl(const DBEngine* ptr) {
  return (const DBEngineImpl*)ptr;
}

/** DBEngine external methods */

void DBEngine::reset() {
  DBEngineImpl* engine = getImpl(this);
  engine->reset();
}

void DBEngine::executeDDL(const std::string& query) {
  DBEngineImpl* engine = getImpl(this);
  engine->executeDDL(query);
}

std::unique_ptr<Cursor> DBEngine::executeDML(const std::string& query) {
  DBEngineImpl* engine = getImpl(this);
  return engine->executeDML(query);
}

std::unique_ptr<Cursor> DBEngine::executeRA(const std::string& query) {
  DBEngineImpl* engine = getImpl(this);
  return engine->executeRA(query);
}

void DBEngine::createArrowTable(const std::string& name,
                                std::shared_ptr<arrow::Table>& table) {
  DBEngineImpl* engine = getImpl(this);
  return engine->createArrowTable(name, table);
}

std::vector<std::string> DBEngine::getTables() {
  DBEngineImpl* engine = getImpl(this);
  return engine->getTables();
}

std::vector<ColumnDetails> DBEngine::getTableDetails(const std::string& table_name) {
  DBEngineImpl* engine = getImpl(this);
  return engine->getTableDetails(table_name);
}


void DBEngine::createUser(const std::string& user_name, const std::string& password) {
  DBEngineImpl* engine = getImpl(this);
  engine->createUser(user_name, password);
}

void DBEngine::dropUser(const std::string& user_name) {
  DBEngineImpl* engine = getImpl(this);
  engine->dropUser(user_name);
}

void DBEngine::createDatabase(const std::string& db_name) {
  DBEngineImpl* engine = getImpl(this);
  engine->createDatabase(db_name);
}

void DBEngine::dropDatabase(const std::string& db_name) {
  DBEngineImpl* engine = getImpl(this);
  engine->dropDatabase(db_name);
}

bool DBEngine::setDatabase(std::string& db_name) {
  DBEngineImpl* engine = getImpl(this);
  return engine->setDatabase(db_name);
}

bool DBEngine::login(std::string& db_name,
             std::string& user_name,
             const std::string& password) {
  DBEngineImpl* engine = getImpl(this);
  return engine->login(db_name, user_name, password);
}


/** Cursor downcasting methods */

inline CursorImpl* getImpl(Cursor* ptr) {
  return (CursorImpl*)ptr;
}

inline const CursorImpl* getImpl(const Cursor* ptr) {
  return (const CursorImpl*)ptr;
}

/** Cursor external methods */

size_t Cursor::getColCount() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getColCount();
}

size_t Cursor::getRowCount() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getRowCount();
}

Row Cursor::getNextRow() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getNextRow();
}

ColumnType Cursor::getColType(uint32_t col_num) {
  CursorImpl* cursor = getImpl(this);
  return cursor->getColType(col_num);
}

std::shared_ptr<arrow::RecordBatch> Cursor::getArrowRecordBatch() {
  CursorImpl* cursor = getImpl(this);
  return cursor->getArrowRecordBatch();
}
}  // namespace EmbeddedDatabase
