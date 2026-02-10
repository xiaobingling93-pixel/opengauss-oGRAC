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
 * ogsql_plan.c
 *
 *
 * IDENTIFICATION
 * src/ogsql/plan/ogsql_plan.c
 *
 * -------------------------------------------------------------------------
 */
#include "ogsql_plan.h"
#include "plan_dml.h"
#include "plan_query.h"
#include "plan_rbo.h"
#include "plan_range.h"
#include "dml_parser.h"
#include "ogsql_func.h"
#include "ogsql_table_func.h"
#include "expr_parser.h"
#include "srv_instance.h"
#include "dml_executor.h"
#include "cbo_base.h"
#include "ogsql_verifier.h"

#ifdef __cplusplus
extern "C" {
#endif

plan_assist_t *sql_get_ancestor_pa(plan_assist_t *curr_pa, uint32 temp_ancestor)
{
    uint32 anc = temp_ancestor;
    while (anc > 0 && curr_pa != NULL) {
        curr_pa = curr_pa->parent;
        anc--;
    }
    return curr_pa;
}

sql_query_t *sql_get_ancestor_query(sql_query_t *query, uint32 anc)
{
    uint32 depth = 0;
    while (depth < anc) {
        if (query == NULL || query->owner == NULL) {
            return NULL;
        }
        query = query->owner->parent;
        depth++;
    }
    return query;
}

void sql_collect_select_nodes(biqueue_t *queue, select_node_t *node)
{
    if (node->type == SELECT_NODE_QUERY) {
        biqueue_add_tail(queue, QUEUE_NODE_OF(node));
    } else {
        sql_collect_select_nodes(queue, node->left);
        sql_collect_select_nodes(queue, node->right);
    }
}

status_t visit_select_node(sql_stmt_t *stmt, select_node_t *node, query_visit_func_t visit_func)
{
    // The query node is processed separately to avoid the scenario where the node is already in queue.
    if (node->type == SELECT_NODE_QUERY) {
        return visit_func(stmt, node->query);
    }
    biqueue_t queue;
    biqueue_init(&queue);
    sql_collect_select_nodes(&queue, node);

    select_node_t *obj = NULL;
    biqueue_node_t *cur = biqueue_first(&queue);
    biqueue_node_t *end = biqueue_end(&queue);

    while (cur != end) {
        obj = OBJECT_OF(select_node_t, cur);
        if (obj != NULL && obj->query != NULL) {
            OG_RETURN_IFERR(visit_func(stmt, obj->query));
        }
        cur = BINODE_NEXT(cur);
    }
    return OG_SUCCESS;
}

#define MIN_PARAM_ROWNUM (uint32)1
#define MAX_PARAM_ROWNUM (uint32)1000

static uint32 sql_calc_param_rownum(sql_stmt_t *stmt, cmp_node_t *cmp, expr_node_t *left_node, expr_node_t *right_node)
{
    switch (cmp->type) {
        case CMP_TYPE_EQUAL:
        case CMP_TYPE_EQUAL_ANY:
            return MIN_PARAM_ROWNUM;
        case CMP_TYPE_LESS:
        case CMP_TYPE_LESS_EQUAL:
        case CMP_TYPE_LESS_ALL:
        case CMP_TYPE_LESS_ANY:
        case CMP_TYPE_LESS_EQUAL_ALL:
            if (NODE_IS_RES_ROWNUM(left_node) && NODE_IS_PARAM(right_node)) {
                return MAX_PARAM_ROWNUM;
            }
            break;
        case CMP_TYPE_GREAT:
        case CMP_TYPE_GREAT_ALL:
        case CMP_TYPE_GREAT_ANY:
        case CMP_TYPE_GREAT_EQUAL:
        case CMP_TYPE_GREAT_EQUAL_ALL:
            if (NODE_IS_RES_ROWNUM(right_node) && NODE_IS_PARAM(left_node)) {
                return MAX_PARAM_ROWNUM;
            }
            break;
        default:
            break;
    }
    return OG_INFINITE32;
}

uint32 sql_calc_rownum(sql_stmt_t *stmt, sql_query_t *query)
{
    if ((query->join_assist.outer_node_count == 0 && query->cond == NULL) ||
        (query->join_assist.outer_node_count > 0 && query->filter_cond == NULL)) {
        return OG_INFINITE32;
    }
    cond_tree_t *cond = sql_get_rownum_cond(stmt, query);
    uint32 row_num_upper = GET_MAX_ROWNUM(cond);
    if (row_num_upper != OG_INFINITE32 || cond == NULL) {
        return row_num_upper;
    }
    // handle rownum param
    cond_node_t *node = cond->root;
    if (node->type != COND_NODE_COMPARE) {
        return OG_INFINITE32;
    }
    expr_tree_t *left = node->cmp->left;
    expr_tree_t *right = node->cmp->right;
    if (left != NULL && right != NULL) {
        if ((NODE_IS_RES_ROWNUM(left->root) && NODE_IS_PARAM(right->root)) ||
            (NODE_IS_RES_ROWNUM(right->root) && NODE_IS_PARAM(left->root))) {
            return sql_calc_param_rownum(stmt, node->cmp, left->root, right->root);
        }
    }
    return OG_INFINITE32;
}

static inline uint32 get_query_cond_max_ancestor(sql_query_t *query)
{
    if (!query->cond_has_acstor_col || query->cond == NULL) {
        return 0;
    }
    cols_used_t cols_used;
    init_cols_used(&cols_used);
    sql_collect_cols_in_cond(query->cond->root, &cols_used);
    return cols_used.ancestor;
}

void sql_init_plan_assist_impl(sql_stmt_t *stmt, plan_assist_t *plan_ass, sql_query_t *query, sql_node_type_t type,
    plan_assist_t *parent)
{
    plan_ass->stmt = stmt;
    {
        plan_ass->cond = query->cond;
    }
    plan_ass->type = type;
    plan_ass->query = query;
    plan_ass->top_pa = NULL;
    plan_ass->cbo_flags = CBO_NONE_FLAG;
    plan_ass->cbo_index_ast = NONE_INDEX;
    plan_ass->col_use_flag = USE_NONE_FLAG;
    plan_ass->spec_drive_flag = DRIVE_FOR_NONE;
    plan_ass->has_parent_join = query->cond_has_acstor_col;
    plan_ass->max_ancestor = 0;
    plan_ass->no_nl_batch = OG_FALSE;
    plan_ass->resv_outer_join = OG_FALSE;
    plan_ass->hj_pos = 0;
    plan_ass->sort_items = NULL;
    plan_ass->list_expr_count = 0;
    plan_ass->plan_count = 0;
    plan_ass->table_count = query->tables.count;
    plan_ass->join_assist = &query->join_assist;
    plan_ass->join_assist->has_hash_oper = OG_FALSE;
    plan_ass->join_oper_map = NULL;
    plan_ass->parent = parent;
    plan_ass->scan_part_cnt = 1;
    plan_ass->is_final_plan = (parent == NULL) ? OG_FALSE : parent->is_final_plan;
    plan_ass->ignore_hj = (parent == NULL) ? OG_FALSE : parent->ignore_hj;
    plan_ass->is_subqry_cost = OG_FALSE;
    plan_ass->join_card_map = NULL;
    plan_ass->nlf_mtrl_cnt = 0;
    plan_ass->nlf_dupl_plan_cnt = 0;
    plan_ass->is_nl_full_opt = OG_FALSE;
    plan_ass->save_plcnt = 0;
    plan_ass->filter_node_pptr = NULL;
    plan_ass->vpeek_flag = OG_FALSE;
    plan_ass->outer_rels_list = NULL;
}

static inline void set_query_sort_plan_flag(sql_query_t *query, uint32 *flag)
{
    if (query->sort_items->count > 0) {
        if (query->order_siblings && !query->has_distinct) {
            (*flag) |= EX_QUERY_SIBL_SORT;
        } else {
            (*flag) |= EX_QUERY_SORT;
        }
    }
}

static inline void set_query_pivot_plan_flag(sql_query_t *query, uint32 *flag)
{
    if (query->pivot_items != NULL) {
        if (query->pivot_items->type == PIVOT_TYPE) {
            (*flag) |= EX_QUERY_PIVOT;
        } else if (query->pivot_items->type == UNPIVOT_TYPE) {
            (*flag) |= EX_QUERY_UNPIVOT;
        }
    }
}

uint32 get_query_plan_flag(sql_query_t *query)
{
    bool32 flag = 0;
    if (query->for_update != OG_FALSE) {
        flag |= EX_QUERY_FOR_UPDATE;
    }

    if (query->has_distinct != OG_FALSE) {
        flag |= EX_QUERY_DISTINCT;
    }

    if (query->having_cond != NULL) {
        flag |= EX_QUERY_HAVING;
    }

    set_query_sort_plan_flag(query, &flag);

    if (query->group_cubes != NULL) {
        flag |= EX_QUERY_CUBE;
    }

    if (query->aggrs->count > 0 || query->group_sets->count > 0) {
        flag |= EX_QUERY_AGGR;
    }

    if (LIMIT_CLAUSE_OCCUR(&query->limit)) {
        flag |= EX_QUERY_LIMIT;
    }

    if (query->connect_by_cond != NULL) {
        flag |= EX_QUERY_CONNECT;
    }
    if (query->filter_cond != NULL) {
        flag |= EX_QUERY_FILTER;
    }

    if (query->winsort_list->count > 0) {
        flag |= EX_QUERY_WINSORT;
    }

    set_query_pivot_plan_flag(query, &flag);

    // (query->cond != NULL && query->cond->rownum_upper == 0) == > rownum count
    if (QUERY_HAS_ROWNUM(query) || (query->cond != NULL && query->cond->rownum_upper == 0)) {
        flag |= EX_QUERY_ROWNUM;
    }
    return flag;
}

void sql_init_plan_assist(sql_stmt_t *stmt, plan_assist_t *plan_ass, sql_query_t *query, sql_node_type_t type,
    plan_assist_t *parent)
{
    sql_init_plan_assist_impl(stmt, plan_ass, query, type, parent);
    for (uint32 i = 0; i < plan_ass->table_count; i++) {
        plan_ass->tables[i] = (sql_table_t *)sql_array_get(&query->tables, i);
        plan_ass->plan_tables[i] = plan_ass->tables[i];
        plan_ass->plan_tables[i]->scan_mode = SCAN_MODE_TABLE_FULL;
        plan_ass->plan_tables[i]->scan_flag = 0;
        plan_ass->plan_tables[i]->index = NULL;
        plan_ass->plan_tables[i]->plan_id = (plan_ass->table_count > 1) ? OG_INVALID_ID32 : 0;
        plan_ass->query->filter_infos = NULL;
        /* set table extra attr memory allocator */
        TABLE_CBO_ATTR_OWNER(plan_ass->tables[i]) = query->vmc;
    }
    plan_ass->max_ancestor = get_query_cond_max_ancestor(query);
}

static void build_join_oper_map(sql_array_t *l_tables, sql_join_node_t *join_node, uint8 operator_flag, uint32 step,
    uint8 *join_oper_map)
{
    sql_table_t *r_tab = TABLE_OF_JOIN_LEAF(join_node);
    for (uint32 i = 0; i < l_tables->count; i++) {
        sql_table_t *l_tab = (sql_table_t *)sql_array_get(l_tables, i);
        join_oper_map[step * l_tab->id + r_tab->id] = operator_flag;
        join_oper_map[step * r_tab->id + l_tab->id] = operator_flag;
    }
}

static void generate_join_oper_map(sql_join_node_t *join_node, sql_array_t *l_tables, uint8 operator_flag, uint32 step,
    uint8 *join_oper_map)
{
    switch (join_node->oper) {
        case JOIN_OPER_NONE:
            build_join_oper_map(l_tables, join_node, operator_flag, step, join_oper_map);
            break;
        case JOIN_OPER_HASH:
        case JOIN_OPER_HASH_LEFT:
        case JOIN_OPER_HASH_FULL:
        case JOIN_OPER_HASH_SEMI:
        case JOIN_OPER_HASH_ANTI:
        case JOIN_OPER_HASH_ANTI_NA: {
            sql_join_node_t *hash_node = join_node->hash_left ? join_node->left : join_node->right;
            sql_join_node_t *drive_node = join_node->hash_left ? join_node->right : join_node->left;
            generate_join_oper_map(hash_node, l_tables, operator_flag | (uint8)join_node->oper, step, join_oper_map);
            generate_join_oper_map(drive_node, l_tables, operator_flag, step, join_oper_map);
            break;
        }
        default:
            generate_join_oper_map(join_node->left, l_tables, operator_flag | (uint8)join_node->oper, step,
                join_oper_map);
            generate_join_oper_map(join_node->right, l_tables, operator_flag | (uint8)join_node->oper, step,
                join_oper_map);
            break;
    }
}

static inline void set_table_global_cached(sql_array_t *r_tables)
{
    for (uint32 i = 0; i < r_tables->count; i++) {
        sql_table_t *r_tab = (sql_table_t *)sql_array_get(r_tables, i);
        r_tab->global_cached = OG_TRUE;
    }
}

status_t perfect_tree_and_gen_oper_map(plan_assist_t *pa, uint32 step, sql_join_node_t *join_node)
{
    if (join_node->type == JOIN_TYPE_NONE) {
        return OG_SUCCESS;
    }

    OG_RETURN_IFERR(perfect_tree_and_gen_oper_map(pa, step, join_node->left));
    OG_RETURN_IFERR(perfect_tree_and_gen_oper_map(pa, step, join_node->right));

    if (join_node->oper == JOIN_OPER_NL || join_node->oper == JOIN_OPER_NL_LEFT ||
        join_node->oper == JOIN_OPER_NL_BATCH) {
        set_table_global_cached(&join_node->right->tables);
    }
    generate_join_oper_map(join_node->right, &join_node->left->tables, (uint8)join_node->oper, step, pa->join_oper_map);
    return OG_SUCCESS;
}


status_t sql_make_index_col_map(plan_assist_t *pa, sql_stmt_t *stmt, sql_table_t *table)
{
    if (pa != NULL && pa->vpeek_flag) {
        return OG_SUCCESS;
    }
    uint32 index_col = 0;
    uint32 col_count = knl_get_column_count(table->entry->dc.handle);
    uint32 vcol_count = knl_get_index_vcol_count(table->index);
    uint32 alloc_size = (col_count + vcol_count) * sizeof(uint16);

    if (table->idx_col_map == NULL) {
        OG_RETURN_IFERR(sql_alloc_mem(stmt->context, alloc_size, (void **)&table->idx_col_map));
    }
    if (alloc_size > 0) {
        MEMS_RETURN_IFERR(memset_sp(table->idx_col_map, alloc_size, 0xFF, alloc_size));
    }

    for (uint32 i = 0; i < table->index->column_count; i++) {
        uint16 col_id = table->index->columns[i];
        if (col_id >= DC_VIRTUAL_COL_START) {
            uint32 vcol_id = col_count + index_col++;
            table->idx_col_map[vcol_id] = i;
        } else {
            table->idx_col_map[col_id] = i;
        }
    }
    return OG_SUCCESS;
}

uint32 sql_get_plan_hash_rows(sql_stmt_t *stmt, plan_node_t *plan)
{
    uint32 card = (uint32)plan->rows * OG_HASH_FACTOR;
    if (!stmt->context->opt_by_rbo) {
        card = MIN(card, OG_CBO_MAX_HASH_COUNT);
    } else {
        card = MIN(card, OG_RBO_MAX_HASH_COUNT);
    }
    return card;
}

static inline bool32 select_node_has_hash_join(select_node_t *slct_node)
{
    if (slct_node->type == SELECT_NODE_QUERY) {
        return sql_query_has_hash_join(slct_node->query);
    }
    if (select_node_has_hash_join(slct_node->left)) {
        return OG_TRUE;
    }
    return select_node_has_hash_join(slct_node->right);
}

bool32 sql_query_has_hash_join(sql_query_t *query)
{
    if (query->join_assist.has_hash_oper) {
        return OG_TRUE;
    }
    for (uint32 i = 0; i < query->tables.count; ++i) {
        sql_table_t *table = (sql_table_t *)sql_array_get(&query->tables, i);
        if (OG_IS_SUBSELECT_TABLE(table->type)) {
            if (select_node_has_hash_join(table->select_ctx->root)) {
                return OG_TRUE;
            }
        }
    }
    return OG_FALSE;
}

sql_table_t *sql_get_driver_table(plan_assist_t *plan_ass)
{
    if (plan_ass->top_pa != NULL) {
        plan_ass = plan_ass->top_pa;
    }
    if (plan_ass->plan_count > 0) {
        return plan_ass->plan_tables[0];
    }
    for (uint32 i = 0; i < plan_ass->table_count; ++i) {
        if (plan_ass->tables[i]->is_join_driver) {
            return plan_ass->tables[i];
        }
    }
    return plan_ass->plan_tables[0];
}

bool32 check_stats_empty(cbo_stats_table_t *tab_stats)
{
    return (bool32)(tab_stats == NULL ||
                    tab_stats->col_map == NULL ||
                    (tab_stats->rows == 0 && tab_stats->blocks == 0));
}

bool32 check_table_stats_empty(knl_handle_t handle, sql_table_t *table)
{
    if (table != NULL && table->type == NORMAL_TABLE) {
        dc_entity_t *entity = DC_ENTITY(&table->entry->dc);
        cbo_stats_table_t *tab_stats = knl_get_cbo_table(handle, entity);
        return check_stats_empty(tab_stats);
    }
    return OG_FALSE;
}

// check whether the table is empty and has no column statistics
static bool32 check_table_empty_column_stats(sql_stmt_t *stmt, sql_table_t *table)
{
    cbo_stats_column_t *col_stat = NULL;
    dc_entity_t *entity = DC_ENTITY(&table->entry->dc);
    query_field_t *query_field = NULL;
    bilist_node_t *node = cm_bilist_head(&table->query_fields);

    for (; node != NULL; node = BINODE_NEXT(node)) {
        query_field = BILIST_NODE_OF(query_field_t, node, bilist_node);
        OG_CONTINUE_IFTRUE(!query_field->is_cond_col);

        knl_column_t *column = dc_get_column(entity, query_field->col_id);
        OG_CONTINUE_IFTRUE(COLUMN_IS_LOB(column));

        col_stat = knl_get_cbo_column(&stmt->session->knl_session, entity, query_field->col_id);
        if (col_stat == NULL || col_stat->analyse_time == 0) {
            OG_LOG_DEBUG_INF(
                "[DYNAMIC_SAMPLING] Table[%s] column[%u] has no stats, need column sampling.",
                T2S(&table->name.value), query_field->col_id);
            return OG_TRUE;
        }
    }
    OG_LOG_DEBUG_INF(
        "[DYNAMIC_SAMPLING] Table[%s] all condition columns have stats.",
        T2S(&table->name.value));
    return OG_FALSE;
}

// check whether the table is empty and has no index statistics.
static bool32 check_empty_table_index_stats(sql_table_t *table, knl_index_desc_t *index, cbo_stats_index_t *idx_stats)
{
    // check if the index statistics information exists and has been analyzed.
    OG_RETVALUE_IFTRUE((idx_stats != NULL && idx_stats->analyse_time != 0), OG_FALSE);

    query_field_t *query_field = NULL;
    bilist_node_t *node = cm_bilist_head(&table->query_fields);
    for (; node != NULL; node = BINODE_NEXT(node)) {
        query_field = BILIST_NODE_OF(query_field_t, node, bilist_node);
        OG_CONTINUE_IFTRUE(!query_field->is_cond_col);
        
        for (uint32 i = 0; i < index->column_count; i++) {
            uint32 col_id = index->columns[i];
            knl_column_t *knl_col = knl_get_column(table->entry->dc.handle, col_id);
            OG_RETVALUE_IFTRUE(KNL_COLUMN_IS_VIRTUAL(knl_col), OG_TRUE);
            OG_RETVALUE_IFTRUE((query_field->col_id == col_id), OG_TRUE);
        }
    }
    return OG_FALSE;
}

// Check whether sampling of the table is required.
static bool32 check_table_for_sampling(sql_stmt_t *stmt, sql_table_t *table,
    knl_analyze_dynamic_type_t *dynamic_type)
{
    dc_entity_t *entity = DC_ENTITY(&table->entry->dc);
    cbo_stats_table_t *tab_stat = knl_get_cbo_table(KNL_SESSION(stmt), entity);
    stats_table_mon_t *tab_mon = knl_cbo_get_table_mon(KNL_SESSION(stmt), entity);

    OG_RETVALUE_IFTRUE((tab_stat == NULL), OG_FALSE);
    int64 mod_rows = (tab_mon != NULL) ? (int64)(tab_mon->inserts + tab_mon->updates + tab_mon->deletes) : 0L;
    OG_RETVALUE_IFTRUE((mod_rows <= 0), OG_FALSE);
    OG_RETVALUE_IFTRUE((tab_mon != NULL && tab_mon->is_change), OG_FALSE);

    if (tab_stat->rows == 0) {
        *dynamic_type = STATS_ALL;
        OG_LOG_DEBUG_INF(
            "[DYNAMIC_SAMPLING] Table[%s] has 0 rows, set type to STATS_ALL.",
            T2S(&table->name.value));
        return OG_TRUE;
    }

    // SAMP_MOD_RATE_THRESHOLD = 0.2
    double modRate = (double)mod_rows / tab_stat->rows;
    if (tab_stat->rows <= SMALL_TABLE_SAMPLING_THRD(KNL_SESSION(stmt)) && modRate >= SAMP_MOD_RATE_THRESHOLD) {
        *dynamic_type = STATS_ALL;
        OG_LOG_DEBUG_INF(
            "[DYNAMIC_SAMPLING] Table[%s] is small and modified heavily (modRate=%.2f), set type to STATS_ALL.",
            T2S(&table->name.value), modRate);
        return OG_TRUE;
    }

    OG_LOG_DEBUG_WAR(
        "[DYNAMIC_SAMPLING] Table[%s] modification rate (modRate=%.2f) is below threshold, no sampling needed.",
        T2S(&table->name.value), modRate);
    return OG_FALSE;
}

static bool32 check_dynamic_sampling_table(sql_stmt_t *stmt, sql_table_t *table,
    knl_analyze_dynamic_type_t *dynamic_type)
{
    // If the table has no statistics, perform full sampling.
    if (check_table_stats_empty(KNL_SESSION(stmt), table)) {
        *dynamic_type = STATS_ALL;
        OG_LOG_DEBUG_INF(
            "[DYNAMIC_SAMPLING] Table[%s] has no stats, set type to STATS_ALL.",
            T2S(&table->name.value));
        return OG_TRUE;
    }

    // Only normal tables are eligible for dynamic sampling.
    OG_RETVALUE_IFTRUE((table->type != NORMAL_TABLE), OG_FALSE);

    // Check if the table needs sampling due to data modifications.
    OG_RETVALUE_IFTRUE((check_table_for_sampling(stmt, table, dynamic_type)), OG_TRUE);

    //  Check if the table has statistics for columns used in query conditions.
    if (check_table_empty_column_stats(stmt, table)) {
        *dynamic_type = STATS_COLUMNS;
        return OG_TRUE;
    }

    OG_LOG_DEBUG_WAR(
        "[DYNAMIC_SAMPLING] Table[%s] stats are up-to-date, no need to sample.",
        T2S(&table->name.value));
    return OG_FALSE;
}

// The number of pages in a non temporary table is zero, it will return OG_FALSE
static bool32 calc_normal_table_pages(sql_stmt_t *stmt, sql_table_t *table, uint64 *total_pages)
{
    OG_RETVALUE_IFTRUE((table->type != NORMAL_TABLE), OG_FALSE);
    uint32 pages = 0;
    dc_entity_t *entity = DC_ENTITY(&table->entry->dc);
    OG_RETVALUE_IFTRUE((IS_DUAL_TABLE(&entity->table) || IS_SYS_TABLE(&entity->table)), OG_FALSE);

    if (IS_PART_TABLE(&entity->table)) {
        if (HAS_NO_SCAN_INFO(table) || PART_TABLE_FULL_SCAN(table)) {
            knl_estimate_table_rows(&pages, NULL, KNL_SESSION(stmt), entity, CBO_GLOBAL_PART_NO);
            *total_pages += (uint64)pages;
        } else if (PART_TABLE_EMPTY_SCAN(table)) {
            return OG_FALSE;
        } else if (PART_TABLE_SCAN_ALL_PART_NO_SAVED(table)) {
            for (uint32 i = 0; i < PART_TABLE_SCAN_SAVED_PART_COUNT(table); i++) {
                uint32 part_no = PART_TABLE_SCAN_PART_NO(table, i);
                knl_estimate_table_rows(&pages, NULL, KNL_SESSION(stmt), entity, part_no);
                *total_pages += (uint64)pages;
            }
        } else {
            uint32 max_part_no = knl_get_max_rows_part(entity);
            knl_estimate_table_rows(&pages, NULL, KNL_SESSION(stmt), entity, max_part_no);
            *total_pages += PART_TABLE_SCAN_TOTAL_PART_COUNT(table) * (uint64)pages;
        }
        // If it is an empty scan, return false directly without printing the log.
        OG_LOG_DEBUG_INF(
            "[DYNAMIC_SAMPLING] Table[%s] is partitioned. Calc total_pages=%llu.",
            T2S(&table->name.value), *total_pages);
    } else {
        // If the table is not partitioned, the number of pages is calculated directly.
        knl_estimate_table_rows(&pages, NULL, KNL_SESSION(stmt), entity, CBO_GLOBAL_PART_NO);
        *total_pages += (uint64)pages;
        OG_LOG_DEBUG_INF(
            "[DYNAMIC_SAMPLING] Table[%s] is not partitioned. Calc total_pages=%llu.",
            T2S(&table->name.value), *total_pages);
    }

    if (*total_pages == 0) {
        /*
         * If it is a temporary session table, the sampling is set to the minimum number of sampling pages.
         * If it is not, sampling cannot be performed and false is returned.
         */
        OG_RETVALUE_IFTRUE((!IS_LTT_BY_NAME(entity->table.desc.name) &&
            entity->entry->type == DICT_TYPE_TEMP_TABLE_SESSION), OG_FALSE);
        *total_pages = CBO_MIN_SAMPLING_PAGE;
    }
    return OG_TRUE;
}

static uint32 g_dynamic_sampling_blocks[CBO_MAX_DYN_SAMPLING_LEVEL] = {0, 10, 32, 64, 128, 256, 512, 1024, 4096};
static void calc_sample_ratio(knl_analyze_tab_def_t *def, uint64 total_pages, uint32 level)
{
    if (level == CBO_MAX_DYN_SAMPLING_LEVEL) {
        def->sample_ratio = STATS_MAX_ESTIMATE_PERCENT;
        OG_LOG_DEBUG_INF(
            "[DYNAMIC_SAMPLING] Calc sample ratio: level=%u, total_pages=%llu, sample_ratio=%f (MAX).",
            level, total_pages, def->sample_ratio);
        return;
    }

    uint64 pages = MIN(total_pages, g_dynamic_sampling_blocks[level]);
    def->sample_ratio = MIN(ceil((double)(OG_PERCENT * pages) / total_pages), STATS_MAX_ESTIMATE_PERCENT);

    OG_LOG_DEBUG_INF(
        "[DYNAMIC_SAMPLING] Calc sample ratio: level=%u, total_pages=%llu, sample_page=%llu,sample_ratio=%f.",
        level, total_pages, pages, def->sample_ratio);
}

static status_t execute_with_stat_tracking(sql_stmt_t *stmt, status_t (*execute_func)(knl_handle_t, void*), void *arg)
{
    sql_record_knl_stats_info(stmt);
    status_t status = execute_func(&stmt->session->knl_session, arg);
    sql_reset_knl_stats_info(stmt, status);
    return status;
}

static void log_dynamic_sampling_failure(sql_table_t *table, uint32 part_no)
{
    if (part_no != CBO_GLOBAL_PART_NO) {
        OG_LOG_DEBUG_ERR(
            "[DYNAMIC_SAMPLING] Dynamic sampling failed, user_name=%s, table_name=%s, part_no=%u",
            T2S(&table->user.value), T2S_EX(&table->name.value), part_no);
    } else {
        OG_LOG_DEBUG_ERR(
            "[DYNAMIC_SAMPLING] Dynamic sampling failed, user_name=%s, table_name=%s",
            T2S(&table->user.value), T2S_EX(&table->name.value));
    }
}

static status_t partition_try_sample(sql_stmt_t *stmt, sql_table_t *table,
    knl_analyze_tab_def_t *def, uint32 part_no)
{
    status_t status = execute_with_stat_tracking(stmt,
        (status_t (*)(knl_handle_t, void*))knl_analyze_table_dynamic, def);
    if (status != OG_SUCCESS) {
        log_dynamic_sampling_failure(table, part_no);
    }
    return status;
}

static void get_dynamic_sampling_table_stats(sql_stmt_t *stmt, sql_table_t *table,
    knl_analyze_tab_def_t *def, status_t *status)
{
    OG_LOG_DEBUG_INF(
        "[DYNAMIC_SAMPLING] Start dynamic sampling table[%s], method[%d].",
        T2S(&table->name.value), def->method_opt.option);
    dc_entity_t *entity = DC_ENTITY(&table->entry->dc);
    cbo_stats_table_t *tab_stats = entity->cbo_table_stats;
    
    if (IS_PART_TABLE(&entity->table) && !PART_TABLE_FULL_SCAN(table) && PART_TABLE_SCAN_ALL_PART_NO_SAVED(table)) {
        bool32 is_analyzed = OG_FALSE;

        for (uint32 i = 0; i < PART_TABLE_SCAN_SAVED_PART_COUNT(table); i++) {
            def->part_no = PART_TABLE_SCAN_PART_NO(table, i);
            uint32 pages = 0;
            uint32 rows = 0;

            knl_estimate_table_rows(&pages, &rows, (knl_handle_t)stmt->session, entity, def->part_no);
            OG_CONTINUE_IFTRUE(pages == 0);

            *status = partition_try_sample(stmt, table, def, def->part_no);

            if (*status == OG_SUCCESS) {
                is_analyzed = OG_TRUE;
                break;
            }
        }

        if (!is_analyzed) {
            def->part_no = (tab_stats == NULL) ? 0 : tab_stats->max_part_no;
            *status = partition_try_sample(stmt, table, def, def->part_no);
        }
    } else {
        *status = partition_try_sample(stmt, table, def, CBO_GLOBAL_PART_NO);
    }
}

// Collect and summarize specific column information in the table.
static void get_specified_columns(sql_stmt_t *stmt, sql_table_t *table, knl_analyze_tab_def_t *def)
{
    OG_LOG_DEBUG_INF("[DYNAMIC_SAMPLING] Start get specified columns.");
    query_field_t *query_field = NULL;
    cbo_stats_column_t *col_stat = NULL;
    dc_entity_t *entity = DC_ENTITY(&table->entry->dc);
    bilist_node_t *node = cm_bilist_head(&table->query_fields);
    knl_stats_specified_cols *spec_cols = &def->specify_cols;
    spec_cols->cols_count = 0;

    for (; node != NULL; node = BINODE_NEXT(node)) {
        query_field = BILIST_NODE_OF(query_field_t, node, bilist_node);
        OG_CONTINUE_IFTRUE(!query_field->is_cond_col);
        if (def->dynamic_type == STATS_COLUMNS) {
            knl_column_t *column = dc_get_column(entity, query_field->col_id);
            OG_CONTINUE_IFTRUE(COLUMN_IS_LOB(column));
            col_stat = knl_get_cbo_column(&stmt->session->knl_session, entity, query_field->col_id);
            OG_CONTINUE_IFTRUE(col_stat != NULL && col_stat->analyse_time != 0);
        }
        spec_cols->specified_cols[spec_cols->cols_count] = query_field->col_id;
        spec_cols->cols_count++;
    }
}

static void sql_dynamic_sampling_table(sql_stmt_t *stmt, sql_table_t *table,
    knl_analyze_tab_def_t *def, uint64 total_pages)
{
    def->owner = table->entry->user;
    def->name = table->name.value;
    get_specified_columns(stmt, table, def);

    def->method_opt.option = FOR_SPECIFIED_INDEXED_COLUMNS;
    if (def->dynamic_type == STATS_COLUMNS) {
        def->method_opt.option = FOR_SPECIFIED_COLUMNS;
    }

    status_t status = OG_SUCCESS;
    def->part_no = CBO_GLOBAL_PART_NO;
    get_dynamic_sampling_table_stats(stmt, table, def, &status);

    SQL_LOG_OPTINFO(stmt,
        "[DYNAMIC_SAMPLING] stats_info: table[%u,%s], part_no[%u]; dynamic_sampling_rate=%f, "
        "total_pages=%llu, method_opt=%d, status=%d",
        table->id, T2S(&table->name.value), def->part_no, def->sample_ratio,
        total_pages, def->method_opt.option, status);
}

static void get_dynamic_sampling_index_stats(sql_stmt_t *stmt, sql_table_t *table, knl_index_desc_t *index,
    knl_analyze_index_def_t *def, uint64 total_pages)
{
    def->name.str = index->name;
    def->name.len = (uint32)strlen(index->name);
    def->owner = table->entry->user;
    def->table_name = table->entry->name;
    def->table_owner = table->entry->user;
    OG_LOG_DEBUG_WAR("[DYNAMIC_SAMPLING] Start dynamic sampling table[%s], index[%s].",
        T2S(&table->name.value), index->name);

    status_t status = execute_with_stat_tracking(stmt,
        (status_t (*)(knl_handle_t, void*))knl_analyze_index_dynamic, def);
    if (status != OG_SUCCESS) {
        OG_LOG_DEBUG_ERR(
            "[DYNAMIC_SAMPLING] Dynamic sampling failed, user_name=%s, table_name=%s, index_name=%s",
            T2S(&table->user.value), T2S_EX(&table->name.value), index->name);
    }
    SQL_LOG_OPTINFO(stmt,
        "[DYNAMIC_SAMPLING] stats_info: table[%u,%s], index[%u,%s]; "
        "dynamic_sampling_rate=%f, total_pages=%llu, status=%d",
        table->id, T2S(&table->name.value), index->id, index->name,
        def->sample_ratio, total_pages, status);
}

static void sql_dynamic_sampling_table_indexes(sql_stmt_t *stmt, sql_table_t *table, uint64 total_pages,
    knl_analyze_index_def_t *idx_def, uint32 level)
{
    knl_index_desc_t *index = NULL;
    cbo_stats_index_t *idx_stats = NULL;
    dc_entity_t *entity = DC_ENTITY(&table->entry->dc);
    uint32 idx_count = knl_get_index_count(entity);
    OG_LOG_DEBUG_INF(
        "[DYNAMIC_SAMPLING] Table[%s] has %u indexes, checking for sampling.",
        T2S(&table->name.value), idx_count);
    for (uint32 i = 0; i < idx_count; i++) {
        index = knl_get_index(entity, i);
        OG_CONTINUE_IFTRUE(index->part_idx_invalid);
        idx_stats = knl_get_cbo_index(KNL_SESSION(stmt), entity, index->id);
        OG_CONTINUE_IFTRUE(!check_empty_table_index_stats(table, index, idx_stats));
        stmt->context->dynamic_sampling = level;
        get_dynamic_sampling_index_stats(stmt, table, index, idx_def, total_pages);
    }
}

static status_t sql_init_table_def(knl_analyze_tab_def_t *def)
{
    MEMS_RETURN_IFERR(memset_sp(def, sizeof(knl_analyze_tab_def_t), 0, sizeof(knl_analyze_tab_def_t)));
    def->sample_level = BLOCK_SAMPLE;
    def->part_name = CM_NULL_TEXT;
    def->sample_type = STATS_SPECIFIED_SAMPLE;
    def->is_default = OG_FALSE;
    return OG_SUCCESS;
}

static status_t sql_alloc_tab_idx_def(sql_stmt_t *stmt, knl_analyze_tab_def_t **tab_def,
    knl_analyze_index_def_t **idx_def)
{
    OG_RETURN_IFERR(sql_push(stmt, sizeof(knl_analyze_tab_def_t), (void **)tab_def));
    OG_RETURN_IFERR(sql_init_table_def(*tab_def));
    OG_RETURN_IFERR(sql_push(stmt, sizeof(knl_analyze_index_def_t), (void **)idx_def));
    MEMS_RETURN_IFERR(memset_sp(*idx_def, sizeof(knl_analyze_index_def_t), 0, sizeof(knl_analyze_index_def_t)));
    (*idx_def)->sample_level = BLOCK_SAMPLE;
    (*idx_def)->need_analyzed = OG_TRUE;
    return OG_SUCCESS;
}

// Process each table for dynamic sampling.
static status_t process_table_for_sampling(sql_stmt_t *stmt, plan_assist_t *pa, uint32 level,
    knl_analyze_tab_def_t *tab_def, knl_analyze_index_def_t *idx_def)
{
    for (uint32 i = 0; i < pa->query->tables.count; i++) {
        sql_table_t *table = (sql_table_t *)sql_array_get(&pa->query->tables, i);
        uint64 total_pages = 0;

        OG_CONTINUE_IFTRUE(!calc_normal_table_pages(stmt, table, &total_pages));

        calc_sample_ratio(tab_def, total_pages, level);
        idx_def->sample_ratio = tab_def->sample_ratio / OG_PERCENT;
        
        if (check_dynamic_sampling_table(stmt, table, &tab_def->dynamic_type)) {
            TABLE_CBO_IS_DEAL(table) = OG_FALSE;
            stmt->context->dynamic_sampling = level;
            sql_dynamic_sampling_table(stmt, table, tab_def, total_pages);
            OG_CONTINUE_IFTRUE(tab_def->dynamic_type == STATS_ALL);
        }
        OG_LOG_DEBUG_INF(
            "[DYNAMIC_SAMPLING] Table[%s] dynamic type is not STATS_ALL, checking indexes.",
            T2S(&table->name.value));

        sql_dynamic_sampling_table_indexes(stmt, table, total_pages, idx_def, level);
    }
    return OG_SUCCESS;
}

status_t sql_dynamic_sampling_table_stats(sql_stmt_t *stmt, plan_assist_t *pa)
{
    uint32 level = sql_get_dynamic_sampling_level(stmt);

    OG_RETSUC_IFTRUE(level == 0);
    OG_RETSUC_IFTRUE(pa->type != SQL_SELECT_NODE && pa->type != SQL_QUERY_NODE && pa->type != SQL_MERGE_NODE);
    
    knl_inc_session_ssn(&stmt->session->knl_session);
    OGSQL_SAVE_STACK(stmt);
    knl_analyze_tab_def_t *tab_def = NULL;
    knl_analyze_index_def_t *idx_def = NULL;
    if (sql_alloc_tab_idx_def(stmt, &tab_def, &idx_def) != OG_SUCCESS) {
        OG_LOG_DEBUG_WAR("[DYNAMIC_SAMPLING] Alloc memory failed.");
        OGSQL_RESTORE_STACK(stmt);
        return OG_ERROR;
    }
    
    status_t status = process_table_for_sampling(stmt, pa, level, tab_def, idx_def);
    OGSQL_RESTORE_STACK(stmt);
    return status;
}

#ifdef __cplusplus
}
#endif
