/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd. All rights reserved.
 * This file is part of oGRAC project.
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
 * ogsql_predicate_deliver.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/optimizer/ogsql_predicate_deliver.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef __SQL_PREDICATE_DELIVER_H__
#define __SQL_PREDICATE_DELIVER_H__

#include "ogsql_stmt.h"
#include "ogsql_cond.h"

#ifdef __cplusplus
extern "C" {
#endif
typedef enum en_dlvr_mode {
    DLVR_FILTER_COND,
    DLVR_JOIN_COND,
    DLVR_ALL,
} dlvr_mode_t;

typedef struct st_predicates_node {
    int16 **col_map;
    galist_t cols;
    galist_t values;
} pred_node_t;

typedef struct st_dlvr_info {
    dlvr_mode_t dlvr_mode;
    galist_t pred_nodes;
    galist_t graft_nodes;
} dlvr_info_t;

status_t og_transf_predicate_delivery(sql_stmt_t *statement, sql_query_t *qry);
status_t og_dlvr_predicates_on_join_iter(sql_stmt_t *statement, sql_query_t *qry, sql_join_node_t *jnode);

#ifdef __cplusplus
}
#endif

#endif
