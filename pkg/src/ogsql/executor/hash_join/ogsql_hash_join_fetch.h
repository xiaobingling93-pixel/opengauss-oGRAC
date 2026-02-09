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
 * ogsql_hash_join_fetch.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/executor/ogsql_hash_join_fetch.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __OGSQL_HASH_JOIN_FETCH_H__
#define __OGSQL_HASH_JOIN_FETCH_H__

#include "ogsql_join_comm.h"

status_t og_fetch_full_join_row_cb(void *sql_cur, const char *next_row_buf, uint32 next_row_size,
    const char *curr_row_buf, uint32 curr_row_size, bool32 match_found);
status_t og_fetch_next_row_cb(void *sql_cur, const char *next_row_buf, uint32 next_row_size, const char *curr_row_buf,
                             uint32 curr_row_size, bool32 match_found);
status_t og_fetch_right_left_row_cb(void *sql_cur, const char *next_row_buf, uint32 next_row_size,
    const char *curr_row_buf, uint32 curr_row_size, bool32 match_found);
status_t og_fetch_right_semi_row_cb(void *sql_cur, const char *next_row_buf, uint32 next_row_size,
    const char *curr_row_buf, uint32 curr_row_size, bool32 match_found);
status_t og_hash_join_fetch(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node, bool32 *end);
status_t og_hash_join_fetch_left(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node, bool32 *end);
status_t og_hash_join_fetch_right_left(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node,
    bool32 *end);
status_t og_hash_join_fetch_semi(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node, bool32 *end);
status_t og_hash_join_fetch_right_semi(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node,
    bool32 *end);
status_t og_hash_join_fetch_anti_right(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node,
    bool32 *end);
status_t og_hash_join_fetch_full(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node, bool32 *end);

#endif