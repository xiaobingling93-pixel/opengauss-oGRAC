/* -------------------------------------------------------------------------
 *  This file is part of the oGRAC project.
 * Copyright (c) 2024 Huawei Technologies Co.,Ltd.
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
 * plan_scan.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/plan/plan_scan.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __PLAN_SCAN_H__
#define __PLAN_SCAN_H__

#include "ogsql_scan.h"
#include "ogsql_plan.h"
#include "ogsql_cbo_cost.h"

#ifdef __cplusplus
extern "C" {
#endif

status_t sql_create_query_scan_plan(sql_stmt_t *stmt, plan_assist_t *plan_ass, plan_node_t **plan);
bool32 sql_has_hash_join_oper(sql_join_node_t *join_node);

bool32 sql_match_cbo_cond(sql_stmt_t* stmt, sql_table_t* table, sql_query_t* query);
void cbo_check_rowid_cost(sql_stmt_t *stmt, plan_assist_t *pa, sql_table_t *table);

status_t sql_union_rowid_set(sql_stmt_t* stmt, plan_rowid_set_t* l_set, plan_rowid_set_t* r_set, plan_rowid_set_t* rowid_set, bool32 is_temp);
status_t sql_intersect_rowid_set(sql_stmt_t* stmt, plan_rowid_set_t* l_set, plan_rowid_set_t* r_set, plan_rowid_set_t* rowid_set);
status_t sql_init_plan_rowid_set(sql_stmt_t* stmt, plan_rowid_set_t** rowid_set, bool32 is_temp);
bool32 sql_rowid_expr_matched(expr_tree_t* expr, uint32 table_id);
bool32 sql_rowid_cmp_matched(plan_assist_t* pa, sql_table_t* table, var_column_t* v_col, expr_tree_t* l_expr, expr_tree_t* r_expr);
status_t sql_fetch_expr_rowids(expr_tree_t* expr, plan_rowid_set_t* rid_set);
status_t sql_try_fetch_expr_rowid(sql_stmt_t* stmt, plan_assist_t* pa, sql_table_t* table, cmp_node_t* node, plan_rowid_set_t* rid_set);
status_t sql_create_rowid_set(sql_stmt_t *stmt, plan_assist_t *pa, sql_table_t *table, cond_node_t *node,
                              plan_rowid_set_t **plan_rid_set, bool32 is_temp);
status_t sql_get_rowid_cost(sql_stmt_t *stmt, plan_assist_t *pa, cond_node_t *cond, sql_table_t *table, bool32 is_temp);                             
bool32 check_rowid_certain(plan_assist_t* pa, expr_node_t* node, uint32 table_id);
bool32 check_rowid_cmp_certain(plan_assist_t* pa, cmp_node_t* cmp, uint32 tab_id);
bool32 check_rowid_cond_certain(plan_assist_t* pa, cond_node_t* cond_node, uint32 tab_id);
bool32 check_rowid_join_for_hash(plan_assist_t* pa, uint32 tab_id);
bool32 cbo_can_rowid_scan(plan_assist_t* pa, sql_table_t* table);
bool32 sql_try_choose_rowid_scan(plan_assist_t* pa, sql_table_t* table);
bool32 sql_expr_is_certain(plan_assist_t *pa, expr_tree_t *expr, var_column_t *v_col);
#ifdef __cplusplus
}
#endif

#endif