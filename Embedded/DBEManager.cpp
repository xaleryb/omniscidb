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

#include "DBEManager.h"
#include <boost/filesystem.hpp>
#include "DataMgr/ForeignStorage/ArrowForeignStorage.h"
#include "DataMgr/ForeignStorage/ForeignStorageInterface.h"
#include "QueryEngine/ExtensionFunctionsWhitelist.h"
#include "QueryEngine/TableFunctions/TableFunctionsFactory.h"
#include "QueryRunner/QueryRunner.h"

using QR = QueryRunner::QueryRunner;

namespace EmbeddedDatabase {

  DBEManager::DBEManager(const std::string& base_path, int port, const std::string& udf_filename)
  : is_temp_db_(false) {
    if (!init(base_path, port, udf_filename)) {
      std::cerr << "DBE initialization failed" << std::endl;
    }
  }

  DBEManager::~DBEManager() {
    reset();
  }

  bool DBEManager::initialized() {
    return initialized_;
  }

  bool DBEManager::init(const std::string& base_path, int port, const std::string& udf_filename) {
    std::lock_guard<std::mutex> guard(init_mutex_);

    if (initialized_) {
      std::cout << "DB engine already initialized" << std::endl;
      return true;
    }

    SystemParameters mapd_parms;
    std::string db_path = base_path;
    try {
      registerArrowForeignStorage();
      registerArrowCsvForeignStorage();
      bool is_new_db = base_path.empty() || !catalogExists(base_path);
      if (is_new_db) {
        db_path = createCatalog(base_path);
        if (db_path.empty()) {
          return false;
        }
      }
      auto data_path = db_path + + "/mapd_data";
      data_mgr_= std::make_shared<Data_Namespace::DataMgr>(data_path, mapd_parms, false, 0);
      calcite_ = std::make_shared<Calcite>(-1, port, db_path, 1024, 5000);

      ExtensionFunctionsWhitelist::add(calcite_->getExtensionFunctionWhitelist());
      if (!udf_filename.empty()) {
        ExtensionFunctionsWhitelist::addUdfs(calcite_->getUserDefinedFunctionWhitelist());
      }
      table_functions::TableFunctionsFactory::init();

      auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
      sys_cat.init(db_path, data_mgr_, {}, calcite_, is_new_db, false, {});

      logger::LogOptions log_options("DBE");
      log_options.set_base_path(db_path);
      logger::init(log_options);

      if (!sys_cat.getSqliteConnector()) {
        std::cerr << "DBE:init: SqliteConnector is null" << std::endl;
        return false;
      }

      sys_cat.getMetadataForDB(OMNISCI_DEFAULT_DB, database_);
      auto catalog = Catalog_Namespace::Catalog::get(
          db_path, database_, data_mgr_, std::vector<LeafHostInfo>(), calcite_, false);
      sys_cat.getMetadataForUser(OMNISCI_ROOT_USER, user_);
      auto session = std::make_unique<Catalog_Namespace::SessionInfo>(
          catalog, user_, ExecutorDeviceType::CPU, "");
      QR::init(session);
      initialized_ = true;
      base_path_ = db_path;
      return true;
    } catch (std::exception const& e) {
      std::cerr << "DBE:init: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "DBE:init: Unknown exception" << std::endl;
    }
    return false;
 }

  void DBEManager::reset() {
    std::lock_guard<std::mutex> guard(init_mutex_);

    if (!initialized_) {
      std::cerr << "DBE is not initialized" << std::endl;
      return;
    }

    if (calcite_) {
      calcite_->close_calcite_server();
      calcite_.reset();
    }
    QR::reset();
    ForeignStorageInterface::destroy();
    data_mgr_.reset();
    if (is_temp_db_) {
        boost::filesystem::remove_all(base_path_);
    }
    base_path_.clear();
    initialized_ = false;
  }

  void DBEManager::createUser(const std::string& user_name, const std::string& password) {
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return;
    }
    Catalog_Namespace::UserMetadata user;
    auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
    if (!sys_cat.getMetadataForUser(user_name, user)) {
      sys_cat.createUser(user_name, password, false, "", true);
    }
  }

  void DBEManager::dropUser(const std::string& user_name) {
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return;
    }
    Catalog_Namespace::UserMetadata user;
    auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
    if (!sys_cat.getMetadataForUser(user_name, user)) {
      sys_cat.dropUser(user_name);
    }
  }

  void DBEManager::createDatabase(const std::string& db_name) {
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return;
    }
    Catalog_Namespace::DBMetadata db;
    auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
    if (!sys_cat.getMetadataForDB(db_name, db)) {
      sys_cat.createDatabase(db_name, user_.userId);
    }
  }

  void DBEManager::dropDatabase(const std::string& db_name) {
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return;
    }
    Catalog_Namespace::DBMetadata db;
    auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
    if (sys_cat.getMetadataForDB(db_name, db)) {
      sys_cat.dropDatabase(db);
    }
  }

  bool DBEManager::setDatabase(std::string& db_name) {
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return false;
    }
    try {
      auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
      auto catalog = sys_cat.switchDatabase(db_name, user_.userName);
      updateSession(catalog);
      sys_cat.getMetadataForDB(db_name, database_);
      return true;
    } catch (std::exception const& e) {
      std::cerr << "DBE:setDatabase: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "DBE:setDatabase: Unknown exception" << std::endl;
    }
    return false;
  }

  bool DBEManager::login(std::string& db_name,
             std::string& user_name,
             const std::string& password) {
    if (!initialized()) {
      std::cerr << "DBE is not initialized" << std::endl;
      return false;
    }
    Catalog_Namespace::UserMetadata user_meta;
    try {
      auto& sys_cat = Catalog_Namespace::SysCatalog::instance();
      auto catalog = sys_cat.login(db_name, user_name, password, user_meta, true);
      updateSession(catalog);
      sys_cat.getMetadataForDB(db_name, database_);
      sys_cat.getMetadataForUser(user_name, user_);
      return true;
    } catch (std::exception const& e) {
      std::cerr << "DBE:login: " << e.what() << std::endl;
    } catch (...) {
      std::cerr << "DBE:login: Unknown exception" << std::endl;
    }
    return false;
  }

  void DBEManager::updateSession(std::shared_ptr<Catalog_Namespace::Catalog> catalog) {
    auto session = std::make_unique<Catalog_Namespace::SessionInfo>(
      catalog, user_, ExecutorDeviceType::CPU, "");
    QR::reset();
    QR::init(session);
  }

  bool DBEManager::catalogExists(const std::string& base_path) {
    if (!boost::filesystem::exists(base_path)) {
      return false;
    }
      for (auto& subdir : system_folders_) {
        std::string path = base_path + "/" + subdir;
        if (!boost::filesystem::exists(path)) {
          return false;
        }
      }
    return true;
  }

  void DBEManager::cleanCatalog(const std::string& base_path) {
    if (boost::filesystem::exists(base_path)) {
      for (auto& subdir : system_folders_) {
        std::string path = base_path + "/" + subdir;
        if (boost::filesystem::exists(path)) {
          boost::filesystem::remove_all(path);
        }
      }
    }
  }

  std::string DBEManager::createCatalog(const std::string& base_path) {
    std::string root_dir = base_path;
    if (base_path.empty()) {
      boost::system::error_code error;
      auto tmp_path = boost::filesystem::temp_directory_path(error);
      if (boost::system::errc::success != error.value()) {
        std::cerr << error.message() << std::endl;
        return "";
      }
      tmp_path /= "omnidbe_%%%%-%%%%-%%%%";
      auto uniq_path = boost::filesystem::unique_path(tmp_path, error);
      if (boost::system::errc::success != error.value()) {
        std::cerr << error.message() << std::endl;
        return "";
      }
      root_dir = uniq_path.string();
      is_temp_db_ = true;
    }
    if (!boost::filesystem::exists(root_dir)) {
      if (!boost::filesystem::create_directory(root_dir)) {
        std::cerr << "Cannot create database directory: " << root_dir << std::endl;
        return "";
      }
    }
    size_t absent_count = 0;
    for (auto& sub_dir : system_folders_) {
      std::string path = root_dir + "/" + sub_dir;
      if (!boost::filesystem::exists(path)) {
        if (!boost::filesystem::create_directory(path)) {
          std::cerr << "Cannot create database subdirectory: " << path << std::endl;
          return "";
        }
        ++absent_count;
      }
    }
    if ((absent_count > 0) && (absent_count < system_folders_.size())) {
      std::cerr << "Database directory structure is broken: " << root_dir << std::endl;
      return "";
    }
    return root_dir;
  }
}
