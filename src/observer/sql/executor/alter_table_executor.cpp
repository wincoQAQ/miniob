/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/executor/alter_table_executor.h"

#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/alter_table_stmt.h"
#include "sql/parser/parse_defs.h"
#include "storage/db/db.h"

RC AlterTableExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  ASSERT(stmt->type() == StmtType::ALTER_TABLE,
      "alter table executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  AlterTableStmt *alter_table_stmt = static_cast<AlterTableStmt *>(stmt);
  Db *db = session->get_current_db();
  RC rc = RC::SUCCESS;

  // Handle rename table (standalone operation)
  if (!alter_table_stmt->new_table_name().empty()) {
    rc = db->rename_table(alter_table_stmt->table_name().c_str(), 
                         alter_table_stmt->new_table_name().c_str());
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  // 处理每一列操作
  for (const auto &col_def : alter_table_stmt->column_defs()) {
    switch (col_def.op_type) {
      case AlterTableOperationType::ADD_COLUMN: {
        rc = db->add_column(alter_table_stmt->table_name().c_str(), 
                           col_def.column_name.c_str(), 
                           col_def.column_type, 
                           col_def.column_length, 
                           col_def.nullable);
        break;
      }
      case AlterTableOperationType::DROP_COLUMN: {
        rc = db->drop_column(alter_table_stmt->table_name().c_str(), 
                            col_def.column_name.c_str());
        break;
      }
      case AlterTableOperationType::RENAME_COLUMN: {
        rc = db->rename_column(alter_table_stmt->table_name().c_str(), 
                              col_def.column_name.c_str(), 
                              col_def.new_column_name.c_str());
        break;
      }
      case AlterTableOperationType::MODIFY_COLUMN: {
        rc = db->modify_column(alter_table_stmt->table_name().c_str(), 
                              col_def.column_name.c_str(), 
                              col_def.column_type, 
                              col_def.column_length, 
                              col_def.nullable);
        break;
      }
      case AlterTableOperationType::RENAME_TABLE: {
        // RENAME_TABLE 在上方已经处理了，这个 case 不会发生
        break;
      }
      default: {
        return RC::UNIMPLEMENTED;
      }
    }
    
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  return RC::SUCCESS;
}
