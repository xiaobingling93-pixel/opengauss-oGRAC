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
 * plan_assist.c
 *
 *
 * IDENTIFICATION
 *      pkg/src/ogsql/plan/plan_assist.c
 *
 * -------------------------------------------------------------------------
 */

#ifndef __SQL_PLAN_ASSIST_H__
#define __SQL_PLAN_ASSIST_H__

#include "plan_join.h"
#include "cbo_base.h"

#ifdef __cplusplus
extern "C" {
#endif

status_t og_create_qry_jtree_4_rewrite(sql_stmt_t *statement, sql_query_t *qry, sql_join_node_t **result_jnode);
void og_get_query_cbo_cost(sql_query_t *qry, cbo_cost_t *qry_cost);
void og_free_query_vmc(sql_query_t *qry);
void og_free_select_node_vmc(select_node_t *slct_node);
status_t og_check_index_4_rewrite(sql_stmt_t *statement, sql_query_t *qry, ck_type_t type, bool32 *result);
void og_set_dst_qry_field(query_field_t *src_field, query_field_t *dst_field);
status_t og_clone_qry_fields(sql_stmt_t *statement, sql_table_t *src_tbl, sql_table_t *dest_tbl);
void og_disable_func_idx_only(sql_query_t *sql_qry);

#ifdef __cplusplus
}
#endif

#endif