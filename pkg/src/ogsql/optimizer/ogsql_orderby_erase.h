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
 * ogsql_orderby_erase.h
 *
 *
 * IDENTIFICATION
 *      src/ogsql/optimizer/ogsql_orderby_erase.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef OGSQL_ORDERBY_ERASE_H
#define OGSQL_ORDERBY_ERASE_H
#include "ogsql_stmt.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct st_parent_ancestor_flag {
    uint32 idx;
    uint32 flag;
} parent_ancestor_flag_t;

status_t og_transf_eliminate_orderby(sql_stmt_t *statement, sql_query_t *qry);

#ifdef __cplusplus
}
#endif

#endif
