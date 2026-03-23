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
 * ogsql_pushdown_orderby.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/optimizer/ogsql_pushdown_orderby.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __OGSQL_PUSHDOWN_ORDERBY_H__
#define __OGSQL_PUSHDOWN_ORDERBY_H__

#include "ogsql_stmt.h"
#include "ogsql_expr.h"

#ifdef __cplusplus
extern "C" {
#endif

status_t og_transf_pushdown_orderby(sql_stmt_t *statement, sql_query_t *qry);
status_t og_handle_pushdown_col(visit_assist_t *v_ast, expr_node_t **exprn);

#ifdef __cplusplus
}
#endif

#endif