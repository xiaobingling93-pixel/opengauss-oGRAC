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
* ogsql_verifier_common.h
*
*
* IDENTIFICATION
* src/ogsql/verifier/ogsql_verifier_common.h
*
* -------------------------------------------------------------------------
*/
#ifndef __SQL_VERIFIER_COMMON_H__
#define __SQL_VERIFIER_COMMON_H__

#include "cm_defs.h"
#include "cm_list.h"
#include "ogsql_context.h"
#include "ogsql_expr_def.h"
#include "ogsql_verifier.h"


#ifdef __cplusplus
extern "C" {
#endif

status_t og_modify_query_cond_col(visit_assist_t *v_ast, expr_node_t **exprn);
bool32 check_column_nullable(sql_query_t *sql_query, var_column_t *var_col);
status_t ogsql_set_rs_strict_null_flag(sql_verifier_t *verf, rs_column_t *rs_col);

#ifdef __cplusplus
}
#endif
#endif