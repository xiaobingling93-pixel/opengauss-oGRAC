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
 * ddl_table_attr_parser.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/parser_ddl/ddl_table_attr_parser.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef __DDL_TABLE_ATTR_PARSER_H__
#define __DDL_TABLE_ATTR_PARSER_H__

#include "ogsql_stmt.h"
#include "cm_lex.h"
#include "ddl_partition_parser.h"

#ifdef __cplusplus
extern "C" {
#endif
#define TEMP_TBL_ATTR_PARSED 0x00000002
#define TBLOPTS_EX_AUTO_INCREMENT 0x00000001
#define DDL_MAX_COMMENT_LEN 4000

typedef enum {
    LOB_STORE_PARAM_TABLESPACE,
    LOB_STORE_PARAM_STORAGE_IN_ROW,
} lob_store_param_type_t;

typedef struct {
    lob_store_param_type_t type;
    union {
        char *str_value;
        bool32 bool_value;
    };
} lob_store_param_t;

typedef enum table_attr_type {
    TABLE_ATTR_TABLESPACE,
    TABLE_ATTR_STORAGE,
    TABLE_ATTR_INITRANS,
    TABLE_ATTR_MAXTRANS,
    TABLE_ATTR_PCTFREE,
    TABLE_ATTR_CRMODE,
    TABLE_ATTR_FORMAT,
    TABLE_ATTR_SYSTEM,
    TABLE_ATTR_LOB,
    TABLE_ATTR_ON_COMMIT,
    TABLE_ATTR_APPENDONLY,
    TABLE_ATTR_PARTITION,
    TABLE_ATTR_AUTO_INCREMENT,
    TABLE_ATTR_CHARSET,
    TABLE_ATTR_COLLATE,
    TABLE_ATTR_CACHE,
    TABLE_ATTR_NO_CACHE,
    TABLE_ATTR_LOGGING,
    TABLE_ATTR_NO_LOGGING,
    TABLE_ATTR_COMPRESS,
    TABLE_ATTR_NO_COMPRESS,
    TABLE_ATTR_ORGANIZATION_EXTERNAL
} table_attr_type_t;

typedef struct table_attr {
    table_attr_type_t type;
    union {
        char *str_value;
        int32 int_value;
        int64 int64_value;
        bool32 bool_value;
        void *ptr_value;
        knl_storage_def_t *storage_def;
        struct {
            galist_t *lob_columns;
            char *seg_name;
            galist_t *lob_store_params;
        };
        parser_table_part *partition;
    };
} table_attr_t;

status_t sql_parse_init_auto_increment(sql_stmt_t *stmt, lex_t *lex, int64 *serial_start);
status_t sql_check_organization_column(knl_table_def_t *def);
status_t sql_parse_coalesce_partition(sql_stmt_t *stmt, lex_t *lex, knl_altable_def_t *def);
status_t sql_parse_check_auto_increment(sql_stmt_t *stmt, word_t *word, knl_altable_def_t *def);
status_t sql_parse_appendonly(lex_t *lex, word_t *word, bool32 *appendonly);
status_t sql_parse_organization(sql_stmt_t *stmt, lex_t *lex, word_t *word, knl_ext_def_t *def);
status_t sql_parse_table_attrs(sql_stmt_t *stmt, lex_t *lex, knl_table_def_t *table_def,
                               bool32 *expect_as, word_t *word);
status_t sql_parse_row_format(lex_t *lex, word_t *word, bool8 *csf);
status_t sql_parse_table_compress(sql_stmt_t *stmt, lex_t *lex, uint8 *type, uint8 *algo);
status_t og_parse_table_attrs(sql_stmt_t *stmt, knl_table_def_t *table_def, galist_t *table_attrs);
status_t og_parse_organization(sql_stmt_t *stmt, knl_ext_def_t **extern_def, char *directory, char *location,
    char *record_delimiter, char *fields_terminator);

#ifdef __cplusplus
}
#endif

#endif
