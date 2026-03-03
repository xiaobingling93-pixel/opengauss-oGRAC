/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd. All rights reserved.
 * This file is part of the oGRAC project.
 *
 * oGRAC is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * ogsql_merge_join.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/executor/ogsql_merge_join.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SQL_MERGE_JOIN_H__
#define __SQL_MERGE_JOIN_H__

#include "ogsql_join_comm.h"
#include "ogsql_mtrl.h"
#include "ogsql_sort.h"

typedef struct st_join_sort_input {
    sql_cursor_t *sql_cur;
    join_info_t *join_info;
    plan_node_t *plan_node;
    uint32 batch_size;
} join_sort_input_t;

status_t og_merge_join_execute(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node, bool32 *end);
status_t og_merge_join_fetch(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node, bool32 *end);
void og_merge_join_free(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node);

#endif