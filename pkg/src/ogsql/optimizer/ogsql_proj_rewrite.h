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
* ogsql_proj_rewrite.h
*
*
* IDENTIFICATION
* src/ogsql/optimizer/ogsql_proj_rewrite.h
*
* -------------------------------------------------------------------------
*/
#ifndef __SQL_PROJ_REWRITE_H__
#define __SQL_PROJ_REWRITE_H__

#include "srv_instance.h"
#include "ogsql_func.h"
#include "ogsql_cond_rewrite.h"

#ifdef __cplusplus
extern "C" {
#endif

#define QUERY_HAS_SINGLE_GROUP_BY(query) ((query)->group_sets->count == 1)
#define NO_NEED_ELIMINATE(query) ((query)->group_sets->count > 1 || (query)->pivot_items != NULL || \
                                  (query)->connect_by_cond != NULL || (query)->has_distinct || \
                                  ((query)->winsort_list->count > 0 && (query)->ssa.count > 0))

status_t og_transf_eliminate_proj_col(sql_stmt_t *statement, sql_query_t *query);

#ifdef __cplusplus
}
#endif

#endif