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
 * plan_join_merge.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/plan/plan_join_merge.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __PLAN_JOIN_MERGE_H__
#define __PLAN_JOIN_MERGE_H__

#include "ogsql_plan.h"
#include "ogsql_context.h"
#include "cbo_base.h"
#include "ogsql_join_path.h"

#ifdef __cplusplus
extern "C" {
#endif

bool32 og_check_can_merge_join(sql_join_table_t *jtbl1, sql_join_table_t *jtbl2,
    sql_join_type_t jointype, galist_t *restricts);
bool32 og_check_cmp_is_mergeable(cmp_node_t *cmp_node);
status_t og_gen_sort_inner_and_outer_merge_paths(join_assist_t *ja, sql_join_type_t jointype,
    sql_join_table_t *jtable, sql_join_table_t *jtbl1, sql_join_table_t *jtbl2,
    special_join_info_t *sjoininfo, galist_t* restricts, join_tbl_bitmap_t *param_source_rels);
status_t og_gen_unsorted_merge_paths(join_assist_t *ja, sql_join_type_t jointype,
    sql_join_table_t *jtable, sql_join_table_t *jtbl1, sql_join_table_t *jtbl2,
    special_join_info_t *sjoininfo, galist_t *restricts, join_tbl_bitmap_t *param_source_rels);
status_t og_create_merge_join_sort_item(galist_t *sort_lst, expr_tree_t *exprtr, sort_direction_t direction,
    og_type_t cmp_type);
status_t og_gen_sorted_paths(join_assist_t *ja, sql_join_table_t *jtable, sql_table_t *table);
status_t og_create_merge_join_plan(sql_stmt_t *statement, plan_assist_t *p_ast, sql_join_node_t *jnode,
    plan_node_t *out_plan);

#ifdef __cplusplus
}
#endif

#endif