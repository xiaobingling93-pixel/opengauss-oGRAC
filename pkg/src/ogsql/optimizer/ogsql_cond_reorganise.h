/* -------------------------------------------------------------------------
 * This file is part of the oGRAC project.
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
* ogsql_cond_reorganise.h
*
*
* IDENTIFICATION
* src/ctsql/optimizer/ogsql_cond_reorganise.h
*
* -------------------------------------------------------------------------
*/
#ifndef __SQL_COND_REORGANISE_H__
#define __SQL_COND_REORGANISE_H__

#include "ogsql_stmt.h"
#include "ogsql_cond.h"

#ifdef __cplusplus
extern "C" {
#endif

status_t og_transf_cond_reorder(sql_stmt_t *statement, sql_query_t *qry);

#ifdef __cplusplus
}
#endif

#endif