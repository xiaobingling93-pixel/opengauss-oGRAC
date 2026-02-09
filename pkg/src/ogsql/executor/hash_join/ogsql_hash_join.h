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
 * ogsql_hash_join.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/executor/hash_join/ogsql_hash_join.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __OGSQL_HASH_JOIN_H__
#define __OGSQL_HASH_JOIN_H__

#include "ogsql_mtrl.h"
#include "ogsql_join_comm.h"
#include "ogsql_hash_join_semi.h"
#include "ogsql_hash_join_fetch.h"
#include "ogsql_nl_join.h"
#define HASH_TABLE_ENTRY (&hash_sql_cur->hash_table_entry)

status_t og_hash_join_execute(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node, bool32 *end);
status_t og_hash_join_exec_right_left(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node,
    bool32 *end);
status_t og_hash_join_exec_full(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node, bool32 *end);
typedef status_t(*vm_insert_t)(bool32 *match_found, hash_segment_t *seg, hash_table_entry_t *table, const char *buf,
                               uint32 size);
typedef status_t(*append_key_t)(char *buf, uint32 *size, mtrl_rowid_t *vm_rowids, uint32 count);

typedef struct {
    vm_insert_t vm_insert;
    append_key_t append_key;
    oper_func_t q_oper;
    bool32 is_full_join;
} mtrl_insert_func_t;

typedef status_t (*hash_join_fetch_t)(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node,
    bool32 *end);
typedef status_t (*hash_join_exec_t)(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node, bool32 *end);
typedef struct st_hash_join_funcs_t {
    join_oper_t oper;
    hash_join_exec_t execute;
    hash_join_fetch_t fetch;
} hash_join_funcs_t;

static const hash_join_funcs_t g_hash_join_funcs[] = {
    { JOIN_OPER_NONE,               NULL,                           NULL                      },
    { JOIN_OPER_NL,                 NULL,                           NULL                      },
    { JOIN_OPER_NL_BATCH,           NULL,                           NULL                      },
    { JOIN_OPER_NL_LEFT,            NULL,                           NULL                      },
    { JOIN_OPER_NL_FULL,            NULL,                           NULL                      },
    { JOIN_OPER_HASH,               og_hash_join_execute,           og_hash_join_fetch            },
    { JOIN_OPER_HASH_LEFT,          og_hash_join_execute,           og_hash_join_fetch_left       },
    { JOIN_OPER_HASH_FULL,          og_hash_join_exec_full,         og_hash_join_fetch_full       },
    { JOIN_OPER_HASH_RIGHT_LEFT,    og_hash_join_exec_right_left,   og_hash_join_fetch_right_left },
    { JOIN_OPER_MERGE,              NULL,                           NULL                      },
    { JOIN_OPER_MERGE_LEFT,         NULL,                           NULL                      },
    { JOIN_OPER_MERGE_FULL,         NULL,                           NULL                      },
    { JOIN_OPER_HASH_SEMI,          og_hash_join_exec_semi,         og_hash_join_fetch_semi   },
    { JOIN_OPER_HASH_ANTI,          og_hash_join_exec_semi,         og_hash_join_fetch_semi   },
    { JOIN_OPER_HASH_ANTI_NA,       og_hash_join_exec_semi,         og_hash_join_fetch_semi   },
    { JOIN_OPER_HASH_RIGHT_SEMI,    og_hash_join_exec_right_semi,   og_hash_join_fetch_right_semi },
    { JOIN_OPER_HASH_RIGHT_ANTI,    og_hash_join_exec_right_anti,   og_hash_join_fetch_anti_right },
    { JOIN_OPER_HASH_RIGHT_ANTI_NA, og_hash_join_exec_right_anti,   og_hash_join_fetch_anti_right },
    { JOIN_OPER_HASH_PAR,           og_hash_join_execute,           og_hash_join_fetch            },
};

/*
 * set_hash_mtrl_cursor
 * get the cursor of materialized hash table
 */
static inline sql_cursor_t *og_hash_join_get_hash_cursor(sql_cursor_t *sql_cur, plan_node_t *plan_node)
{
    uint32_t pos = plan_node->join_p.hj_pos;
    return sql_cur->hash_mtrl.hj_tables[pos];
}

/*
 * free_hash_mtrl_cursor
 */
static inline void og_hash_join_free_mtrl_cursor(sql_stmt_t *statement, sql_cursor_t *sql_cur, plan_node_t *plan_node)
{
    CM_POINTER3(statement, sql_cur, plan_node);
    sql_cursor_t *hash_sql_cur = og_hash_join_get_hash_cursor(sql_cur, plan_node);
    if (hash_sql_cur != NULL) {
        sql_cur->hash_mtrl.hj_tables[plan_node->join_p.hj_pos] = NULL;
        sql_free_cursor(statement, hash_sql_cur);
    }
}

status_t og_hash_join_execute_core(sql_stmt_t *statement, sql_cursor_t *parent_cursor, plan_node_t *plan_node,
    bool32 *end, bool32 is_distinct);
status_t og_hash_join_get_hash_keys_type(sql_stmt_t *statement, sql_cursor_t *hash_cursor, join_plan_t *join_plan,
                                     galist_t *hash_keys, galist_t *probe_keys, join_info_t *hj_info);

// return OG_ERROR if error occurs
#define RET_IFERR_RESTORE_STACK(ret, statement)                                  \
    do {                                                                    \
        status_t _status_ = (ret);                                          \
        if (SECUREC_UNLIKELY(_status_ != OG_SUCCESS)) {                     \
            cm_set_error_pos(__FILE__, __LINE__);                           \
            OGSQL_RESTORE_STACK(statement);                                      \
            return _status_;                                                \
        }                                                                   \
    } while (0)

#endif