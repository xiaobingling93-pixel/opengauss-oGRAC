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
 * ogsql_or2union.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/optimizer/ogsql_or2union.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SQL_OR_2_UNION_H__
#define __SQL_OR_2_UNION_H__

#include "ogsql_stmt.h"
#include "ogsql_expr.h"
#include "ogsql_cond.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct st_bitmapset {
    uint64 map_low;  // when table_id in {0, 63}
    uint64 map_high; // when table_id in {64, 127}
} bitmapset_t;

typedef enum en_rewrite_level {
    NO_OR2UNION = 0,
    LOW_OR2UNION = 1,
    HIGH_OR2UNION = 2,
} rewrite_level_t;

typedef struct st_or2union_info {
    bool32 is_merge_or_cond;  // Indicates whether OR conditions are not fully expanded, only used in Scen3
    double current_cost;      // Current cost of the query plan
    double optimal_cost;      // Optimal cost found so far
    double additional_cost;   // Scanning cost of other filter conditions except OR conditions
    double *cost_cache;       // Used searching for optimal UNION method, to optim cost calcul of intermediate values.
    uint32 or_branch_count;   // Number of OR condition branches
    uint32 set_id;
    uint32 *sets; // or conds list is[{A},{B},{C},{D}] and sets is [0,0,1,1], means or2union plan as{{A,B} union {C,D}}
    uint32 *optimal_set;
    uint32 start_id;
    uint32 iteration_count;
    knl_index_desc_t *index_desc;
    sql_table_t *target_table;
    expr_node_t *target_column;
    cond_node_t *remaining_conditions;
} or2union_info_t;

typedef struct st_or_expand_helper {
    bool32 has_nested_or_cond;
    rewrite_level_t rewrite_level;
    galist_t *or_conds;
    sql_query_t *query;
    sql_stmt_t *stmt;
    bitmapset_t table_bitmap;
    uint32 table_bit_cnt;
    uint32 opt_subselect_count;
} or_expand_helper_t;

typedef enum en_compare_result {
    LESS_RES = -1,
    EQUAL_RES = 0,
    GREATER_RES = 1
} compare_result_t;

#define OR2UNION_MAX_TABLES 32
#define OR_EXPAND_MAX_CONDS 32
#define OR2UNION_COST_THRESHOLD (double)0.5
#define SET_NO_OR2UNION_FLAG(helper) ((helper)->rewrite_level = NO_OR2UNION)
#define SET_LOW_OR2UNION_FLAG(helper) ((helper)->rewrite_level = LOW_OR2UNION)
#define SET_HIGH_OR2UNION_FLAG(helper) ((helper)->rewrite_level = HIGH_OR2UNION)
#define HAS_HIGH_OR2UNION_FLAG(helper) ((helper)->rewrite_level == HIGH_OR2UNION)
#define HAS_NO_OR2UNION_FLAG(helper) ((helper)->rewrite_level == NO_OR2UNION)
#define GET_OPT_SUBSLCT_CNT(helper) ((helper)->opt_subselect_count)
#define ALL_HAS_JOIN_COND(bitmap, mask) (((uint64)0xFFFFFFFF >> (OR2UNION_MAX_TABLES - (mask))) == (bitmap))
#define COND_HAS_EXIST_CMP(cmp_type) ((cmp_type) == CMP_TYPE_EXISTS || (cmp_type) == CMP_TYPE_NOT_EXISTS)
#define COND_HAS_IN_CMP(cmp_type) ((cmp_type) == CMP_TYPE_IN || (cmp_type) == CMP_TYPE_NOT_IN)
#define IS_BORDER_INFINITE_LEFT(type) ((type) == BORDER_INFINITE_LEFT)
#define IS_BORDER_INFINITE_RIGHT(type) ((type) == BORDER_INFINITE_RIGHT)
#define IS_ANY_BORDER_INFINITE_RIGHT(type1, type2) \
    ((type1) == BORDER_INFINITE_RIGHT || (type2) == BORDER_INFINITE_RIGHT)
#define IS_BOTH_RANGE_POINT(type1, type2) ((type1) == RANGE_POINT && (type2) == RANGE_POINT)
#define IS_CMP_TYPE_HAS_IS_NULL(type1, type2) ((type1) == CMP_TYPE_IS_NULL || (type2) == CMP_TYPE_IS_NULL)
#define IS_BORDER_BOTH_NULL(type1, type2) ((type1) == BORDER_IS_NULL && (type2) == BORDER_IS_NULL)
#define IS_ANY_BORDER_NULL(type1, type2) ((type1) == BORDER_IS_NULL || (type2) == BORDER_IS_NULL)
#define IS_BORDER_NULL(type) ((type) == BORDER_IS_NULL)
#define CHK_IF_ANY_RANGE_EMPTY(type1, type2) ((type1) == RANGE_EMPTY || (type2) == RANGE_EMPTY)
#define IS_CMP_TYPE_HAS_NULL(type) ((type) == CMP_TYPE_IS_NOT_NULL || (type) == CMP_TYPE_IS_NULL)
#define HINT_OR2UNION_KEY_CLEAR(hint_info)                                \
    do {                                                                  \
        ((hint_info)->mask[OPTIM_HINT]) &= ~(HINT_KEY_WORD_OR_EXPAND);    \
        (hint_info)->args[ID_HINT_OR_EXPAND] = NULL;                      \
    } while (0)
#define HINT_NO_OR2UNION_KEY_SET(hint_info)                               \
    do {                                                                  \
        ((hint_info)->mask[OPTIM_HINT]) |= (HINT_KEY_WORD_NO_OR_EXPAND);  \
    } while (0)
#define SET_OR2UNION_CBO_INFO(info, tbl, index, add_cost)                 \
    do {                                                                  \
        (info)->index_desc = (index);                                     \
        (info)->target_table = (tbl);                                     \
        (info)->additional_cost = (add_cost);                             \
    } while (0)
#define RANGE_SET_FULL(range)                                             \
    do {                                                                  \
        (range)->right.type = (BORDER_INFINITE_RIGHT);                    \
        (range)->left.type  = (BORDER_INFINITE_LEFT);                     \
        (range)->type       = (RANGE_FULL);                               \
    } while (0)
#define MERGE_OR_COND(node_prev, node_next, opt_node)                     \
    do {                                                                  \
        (opt_node)->left = (node_prev);                                   \
        (opt_node)->right = (node_next);                                  \
        (opt_node)->type = (COND_NODE_OR);                                \
    } while (0)
status_t og_transf_or2union_rewrite(sql_stmt_t *statement, sql_query_t *sql_qry);

#ifdef __cplusplus
}
#endif

#endif
