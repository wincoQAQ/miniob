/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Longda on 2021/11/8.
//

#include "sql/stmt/alter_table_stmt.h"
#include "common/log/log.h"
#include "common/lang/string.h"
#include "storage/db/db.h"
#include "event/sql_debug.h"

RC AlterTableStmt::create(Db *db, const AlterTableSqlNode &sql_node, Stmt *&stmt)
{
  stmt = nullptr;

  if (sql_node.relation_name.empty()) {
    LOG_WARN("table name is empty");
    return RC::INVALID_ARGUMENT;
  }

  auto alter_stmt = new AlterTableStmt();
  alter_stmt->table_name_ = sql_node.relation_name;
  alter_stmt->new_table_name_ = sql_node.new_relation_name;
  alter_stmt->column_defs_ = sql_node.column_defs;

  stmt = alter_stmt;
  sql_debug("create alter table statement: table name=%s", sql_node.relation_name.c_str());
  return RC::SUCCESS;
}
