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

#include "Catalog/Catalog.h"

namespace EmbeddedDatabase {

/**
 * Embedded databases manager
 */
class DBEManager {

 public:

  DBEManager(const std::string& base_path, int port, const std::string& udf_filename = "");
  ~DBEManager();
  bool initialized();
  bool init(const std::string& base_path, int port, const std::string& udf_filename = "");
  void reset();
  void createUser(const std::string& user_name, const std::string& password);
  void dropUser(const std::string& user_name);
  void createDatabase(const std::string& db_name);
  void dropDatabase(const std::string& db_name);
  bool setDatabase(std::string& db_name);
  bool login(std::string& db_name,
             std::string& user_name,
             const std::string& password);

 private:

  void updateSession(std::shared_ptr<Catalog_Namespace::Catalog> catalog);
  bool catalogExists(const std::string& base_path);
  void cleanCatalog(const std::string& base_path);
  std::string createCatalog(const std::string& base_path);

  bool initialized_;
  std::string base_path_;
  std::shared_ptr<Data_Namespace::DataMgr> data_mgr_;
  std::shared_ptr<Calcite> calcite_;
  Catalog_Namespace::DBMetadata database_;
  Catalog_Namespace::UserMetadata user_;
  bool is_temp_db_;
  std::string udf_filename_;
  mutable std::mutex init_mutex_;

  std::vector<std::string> system_folders_ = {
    "mapd_catalogs", 
    "mapd_data", 
    "mapd_export"};
};
}