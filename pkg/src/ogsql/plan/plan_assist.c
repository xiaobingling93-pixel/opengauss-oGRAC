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
 * plan_assist.c
 *
 *
 * IDENTIFICATION
 *      pkg/src/ogsql/plan/plan_assist.c
 *
 * -------------------------------------------------------------------------
 */
#include "plan_join.h"
#include "cbo_base.h"

#ifdef __cplusplus
extern "C" {
#endif

static status_t og_cbo_check_index_cost(sql_stmt_t *statement, sql_query_t *qry,
    ck_type_t check_type, bool32 *is_rewritable)
{
    // 待实现：用hash semi join跟走索引的NL代价做比较
    *is_rewritable = OG_TRUE;
    return OG_SUCCESS;
}

status_t og_check_index_4_rewrite(sql_stmt_t *statement, sql_query_t *qry, ck_type_t type, bool32 *is_rewritable)
{
    if (CBO_ON) {
        status_t status = OG_ERROR;
        SYNC_POINT_GLOBAL_START(CBO_CANNOT_REWRITE_BY_INDEX, (int32 *)is_rewritable, OG_FALSE);
        status = og_cbo_check_index_cost(statement, qry, type, is_rewritable);
        SYNC_POINT_GLOBAL_END;
        return status;
    }
    return OG_SUCCESS;
}

#ifdef __cplusplus
}
#endif
