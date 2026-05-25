/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "common/lang/string.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"

class Db;

/**
 * @brief 描述一个ALTER TABLE语句
 * @ingroup Statement
 */
class AlterTableStmt : public Stmt
{
public:
  AlterTableStmt() = default;
  ~AlterTableStmt() override = default;

  StmtType type() const override { return StmtType::ALTER_TABLE; }

  static RC create(Db *db, const AlterTableSqlNode &sql_node, Stmt *&stmt);

  const string &table_name() const { return table_name_; }
  const string &new_table_name() const { return new_table_name_; }
  const vector<AlterColumnDef> &column_defs() const { return column_defs_; }

private:
  string                  table_name_;      // 要改的表名
  string                  new_table_name_;  // 新表名（RENAME时使用）
  vector<AlterColumnDef> column_defs_;     // 列操作定义
};
