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
 * ogsql_hash_join_common.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/executor/ogsql_hash_join_common.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __OGSQL_HASH_JOIN_COMMON_H__
#define __OGSQL_HASH_JOIN_COMMON_H__

#include "ogsql_join_comm.h"
#include "ogsql_nl_join.h"

static inline bool32 og_hash_join_sql_cursor_eof(sql_cursor_t *hash_sql_cur)
{
    return hash_sql_cur == NULL || hash_sql_cur->eof;
}

static inline void og_end_hash_join_fetch(sql_cursor_t *sql_cur, plan_node_t *hash_plan_node)
{
    CM_POINTER2(sql_cur, hash_plan_node);
    sql_end_plan_cursor_fetch(sql_cur, hash_plan_node);
    sql_cur->last_table = OG_INVALID_ID32;
}

static inline void og_get_hash_keys(galist_t **out_hash_keys, join_plan_t *join_plan)
{
    CM_POINTER(join_plan);
    if (join_plan->hash_left) {
        *out_hash_keys = join_plan->right_hash.key_items;
    } else {
        *out_hash_keys = join_plan->left_hash.key_items;
    }
}

static inline void og_get_prob_keys(galist_t **out_prob_keys, join_plan_t *join_plan)
{
    CM_POINTER(join_plan);
    if (join_plan->hash_left) {
        *out_prob_keys = join_plan->left_hash.key_items;
    } else {
        *out_prob_keys = join_plan->right_hash.key_items;
    }
}

static inline void og_get_probe_plan(plan_node_t **probe_plan, join_plan_t *join_plan)
{
    CM_POINTER(join_plan);
    if (join_plan->hash_left) {
        *probe_plan = join_plan->right;
    } else {
        *probe_plan = join_plan->left;
    }
}

#endif