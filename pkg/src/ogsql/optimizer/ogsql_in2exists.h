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
 * src/ogsql/optimizer/ogsql_in2exists.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __SQL_IN2EXISTS_H__
#define __SQL_IN2EXISTS_H__

#include "ogsql_stmt.h"
#include "ogsql_cond.h"

#ifdef __cplusplus
extern "C" {
#endif

#define MAX_IN2EXISTS_ELEM_COUNT 1024

status_t og_transf_in2exists(sql_stmt_t *stmt, sql_query_t *query);
status_t og_transf_optimize_exists(sql_stmt_t *stmt, cond_node_t *cond);
void og_del_parent_refs_in_cond(sql_query_t *query, cond_node_t *cond);
#ifdef __cplusplus
}
#endif

#endif
