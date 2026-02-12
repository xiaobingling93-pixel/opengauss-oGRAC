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
 * ogsql_hash_mtrl_rewrite.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/optimizer/ogsql_hash_mtrl_rewrite.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __OGSQL_HASH_MTRL_REWRITE_H__
#define __OGSQL_HASH_MTRL_REWRITE_H__

#include "ogsql_stmt.h"
#include "ogsql_expr.h"
#include "ogsql_cond.h"
#include "ogsql_func.h"

#ifdef __cplusplus
extern "C" {
#endif


typedef struct st_column_flags {
    bool32 has_ancestor;
    bool32 has_self;
    bool32 has_subquery;
} column_flags_t;

typedef bool32 (*cond_check_strategy)(cond_node_t *, bool32 *);

status_t og_transf_var_subquery_rewrite(sql_stmt_t *statement, sql_query_t *qry);

#ifdef __cplusplus
}
#endif

#endif