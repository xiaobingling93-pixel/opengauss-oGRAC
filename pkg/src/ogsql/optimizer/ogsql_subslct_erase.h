/* -------------------------------------------------------------------------
 * This file is part of the oGRAC project.
 * Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
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
 * ogsql_subslct_erase.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/optimizer/ogsql_subslct_erase.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SQL_SUBSELECT_ERASE_H__
#define __SQL_SUBSELECT_ERASE_H__

#include "ogsql_stmt.h"
#include "ogsql_expr.h"
#include "ogsql_cond.h"

#ifdef __cplusplus
extern "C" {
#endif

#define OG_GENERATIONS_1 1
#define OG_GENERATIONS_2 2

#define OG_COMPLEX_SQL_FLAG_FULL         0x0000
#define OG_COMPLEX_SQL_FLAG_IGNORE_ORDER 0x0001
#define OG_COMPLEX_SQL_FLAG_IGNORE_AGGR  0x0002
#define OG_COMPLEX_SQL_FLAG_IGNORE_GROUP 0x0004
#define OG_COMPLEX_SQL_FLAG_IGNORE_RMKYE 0x0008  // REMOTE KEY

static inline bool32 og_subslct_is_subtbl(sql_table_t* tbl)
{
    return tbl->type == SUBSELECT_AS_TABLE || tbl->type == VIEW_AS_TABLE;
}

static inline bool32 og_subslct_is_normal_subtbl(sql_table_t* tbl)
{
    return (tbl->type == SUBSELECT_AS_TABLE || tbl->type == VIEW_AS_TABLE) &&
        tbl->subslct_tab_usage == SUBSELECT_4_NORMAL_JOIN;
}

// when the subselect is simple, replace the subselect in subselect table name,
// where the parent select is simple, replace the parent select in parent select table name.
status_t og_transf_select_erase(sql_stmt_t *stmt, sql_query_t *query);
void og_erase_subselect_remove_sel_ctx(sql_stmt_t *stmt, sql_select_t *select);

#ifdef __cplusplus
}
#endif

#endif
