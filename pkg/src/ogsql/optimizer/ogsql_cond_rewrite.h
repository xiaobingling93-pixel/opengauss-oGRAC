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
 * ogsql_cond_rewrite.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/optimizer/ogsql_cond_rewrite.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SQL_COND_REWRITE_H__
#define __SQL_COND_REWRITE_H__

#include "ogsql_stmt.h"
#include "ogsql_cond.h"
#include "plan_range.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum en_collect_mode {
    DLVR_COLLECT_FV,
    DLVR_COLLECT_FF,
    DLVR_COLLECT_ALL,
} collect_mode_t;

typedef struct st_dlvr_pair {
    uint32 *col_map[OG_MAX_JOIN_TABLES];
    galist_t cols;
    galist_t values;
} dlvr_pair_t;

typedef struct st_pred_pushdown_helper {
    sql_query_t *p_query;
    uint32 ssa_count;
    select_node_type_t subslct_type;
    expr_node_t *ssa_nodes[OG_MAX_SUBSELECT_EXPRS];
    bool8 is_del[OG_MAX_SUBSELECT_EXPRS];
} pred_pushdown_helper_t;

#define DLVR_MAX_IN_COUNT 5
status_t sql_predicate_push_down(sql_stmt_t *stmt, sql_query_t *query);
status_t sql_process_predicate_dlvr(sql_stmt_t *stmt, sql_query_t *query, cond_tree_t *cond);
status_t sql_process_dlvr_join_tree_on(sql_stmt_t *stmt, sql_query_t *query, sql_join_node_t *join_tree);
status_t push_down_predicate(sql_stmt_t *stmt, cond_tree_t *cond, sql_table_t *table, select_node_t *slct,
    pred_pushdown_helper_t *pred_pushdown_helper);
status_t cond_rewrite_4_chg_order(sql_stmt_t *stmt, sql_query_t *query);
status_t replace_group_expr_node(sql_stmt_t *stmt, expr_node_t **node);
bool32 get_specified_level_query(sql_query_t *curr_query, uint32 level, sql_query_t **query, sql_select_t **subslct);
status_t sql_update_query_ssa(sql_stmt_t *statement, sql_query_t *query);
bool32 sql_can_expr_node_optm_by_hash(expr_node_t *node);
status_t sql_generate_fv_cond(sql_stmt_t *stmt, expr_tree_t *left, plan_range_t *range, cond_tree_t *cond);
bool32 if_dlvr_range_equal(sql_stmt_t *stmt, plan_range_t *range1, plan_range_t *range2);
bool32 has_semi_in_cmp_node(sql_query_t *query, cmp_node_t *cmp);
uint32 sql_get_func_table_column_count(sql_stmt_t *stmt, sql_table_t *table);
bool32 check_single_col_filter_cond(const cmp_node_t *cmp1, const cmp_node_t *cmp2, const expr_tree_t *r1,
    const expr_tree_t *r2, int32 *result);
status_t expr_node_is_dlvr_value(visit_assist_t *visit_ass, expr_node_t **node);
status_t expr_tree_is_dlvr_value(sql_stmt_t *stmt, expr_tree_t *expr_tree, bool32 *is_dlvr);
status_t pre_generate_dlvr_cond(sql_stmt_t *stmt, expr_tree_t *column, cond_node_t **node);
bool32 if_dlvr_border_equal(sql_stmt_t *stmt, plan_border_t *border1, plan_border_t *border2);
status_t sql_process_oper_or_sink(sql_stmt_t *stmt, cond_node_t **cond);
bool32 sql_dlvr_inter_range(sql_stmt_t *stmt, plan_range_t *range1, plan_range_t *range2, plan_range_t *result);
bool32 sql_is_simple_expr_node(expr_node_t *node, uint32 level);
#ifdef __cplusplus
}
#endif

#endif