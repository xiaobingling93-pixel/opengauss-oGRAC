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
 * pl_unreserved_kwlist.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/pl/parser/pl_unreserved_kwlist.h
 *
 * -------------------------------------------------------------------------
 */

/* There is deliberately not an #ifndef PL_UNRESERVED_KWLIST_H here. */

/*
 * List of (keyword-name, keyword-token-value) pairs.
 *
 * Be careful not to put the same word into pl_reserved_kwlist.h.  Also be
 * sure that pl_gram.y's unreserved_keyword production agrees with this list.
 *
 * Note: gen_keywordlist.pl requires the entries to appear in ASCII order.
 */

/* name, value */
OG_KEYWORD("absolute", K_ABSOLUTE)
OG_KEYWORD("alias", K_ALIAS)
OG_KEYWORD("alter", K_ALTER)
OG_KEYWORD("array", K_ARRAY)
OG_KEYWORD("as", K_AS)
OG_KEYWORD("backward", K_BACKWARD)
OG_KEYWORD("bulk", K_BULK)
OG_KEYWORD("call", K_CALL)
OG_KEYWORD("catalog_name", K_CATALOG_NAME)
OG_KEYWORD("class_origin", K_CLASS_ORIGIN)
OG_KEYWORD("collect", K_COLLECT)
OG_KEYWORD("column_name", K_COLUMN_NAME)
OG_KEYWORD("commit", K_COMMIT)
OG_KEYWORD("condition", K_CONDITION)
OG_KEYWORD("constant", K_CONSTANT)
OG_KEYWORD("constraint_catalog", K_CONSTRAINT_CATALOG)
OG_KEYWORD("constraint_name", K_CONSTRAINT_NAME)
OG_KEYWORD("constraint_schema", K_CONSTRAINT_SCHEMA)
OG_KEYWORD("continue", K_CONTINUE)
OG_KEYWORD("current", K_CURRENT)
OG_KEYWORD("cursor", K_CURSOR)
OG_KEYWORD("cursor_name", K_CURSOR_NAME)
OG_KEYWORD("debug", K_DEBUG)
OG_KEYWORD("detail", K_DETAIL)
OG_KEYWORD("distinct", K_DISTINCT)
OG_KEYWORD("do", K_DO)
OG_KEYWORD("dump", K_DUMP)
OG_KEYWORD("errcode", K_ERRCODE)
OG_KEYWORD("error", K_ERROR)
OG_KEYWORD("except", K_EXCEPT)
OG_KEYWORD("exceptions", K_EXCEPTIONS)
OG_KEYWORD("false", K_FALSE)
OG_KEYWORD("first", K_FIRST)
OG_KEYWORD("forward", K_FORWARD)
OG_KEYWORD("found", K_FOUND)
OG_KEYWORD("function", K_FUNCTION)
OG_KEYWORD("handler", K_HANDLER)
OG_KEYWORD("hint", K_HINT)
OG_KEYWORD("immediate", K_IMMEDIATE)
OG_KEYWORD("index", K_INDEX)
OG_KEYWORD("info", K_INFO)
OG_KEYWORD("instantiation", K_INSTANTIATION)
OG_KEYWORD("intersect", K_INTERSECT)
OG_KEYWORD("is", K_IS)
OG_KEYWORD("iterate", K_ITERATE)
OG_KEYWORD("last", K_LAST)
OG_KEYWORD("leave", K_LEAVE)
OG_KEYWORD("log", K_LOG)
OG_KEYWORD("merge", K_MERGE)
OG_KEYWORD("message", K_MESSAGE)
OG_KEYWORD("message_text", K_MESSAGE_TEXT)
OG_KEYWORD("multiset", K_MULTISET)
OG_KEYWORD("mysql_errno", K_MYSQL_ERRNO)
OG_KEYWORD("next", K_NEXT)
OG_KEYWORD("no", K_NO)
OG_KEYWORD("notice", K_NOTICE)
OG_KEYWORD("number", K_NUMBER)
OG_KEYWORD("option", K_OPTION)
OG_KEYWORD("package", K_PACKAGE)
OG_KEYWORD("perform", K_PERFORM)
OG_KEYWORD("pg_exception_context", K_PG_EXCEPTION_CONTEXT)
OG_KEYWORD("pg_exception_detail", K_PG_EXCEPTION_DETAIL)
OG_KEYWORD("pg_exception_hint", K_PG_EXCEPTION_HINT)
OG_KEYWORD("pipe", K_PIPE)
OG_KEYWORD("pragma", K_PRAGMA)
OG_KEYWORD("prior", K_PRIOR)
OG_KEYWORD("procedure", K_PROCEDURE)
OG_KEYWORD("query", K_QUERY)
OG_KEYWORD("record", K_RECORD)
OG_KEYWORD("relative", K_RELATIVE)
OG_KEYWORD("release", K_RELEASE)
OG_KEYWORD("repeat", K_REPEAT)
OG_KEYWORD("replace", K_REPLACE)
OG_KEYWORD("resignal", K_RESIGNAL)
OG_KEYWORD("result_oid", K_RESULT_OID)
OG_KEYWORD("returned_sqlstate", K_RETURNED_SQLSTATE)
OG_KEYWORD("reverse", K_REVERSE)
OG_KEYWORD("rollback", K_ROLLBACK)
OG_KEYWORD("row", K_ROW)
OG_KEYWORD("row_count", K_ROW_COUNT)
OG_KEYWORD("rowtype", K_ROWTYPE)
OG_KEYWORD("save", K_SAVE)
OG_KEYWORD("savepoint", K_SAVEPOINT)
OG_KEYWORD("schema_name", K_SCHEMA_NAME)
OG_KEYWORD("scroll", K_SCROLL)
OG_KEYWORD("signal", K_SIGNAL)
OG_KEYWORD("slice", K_SLICE)
OG_KEYWORD("sqlexception", K_SQLEXCEPTION)
OG_KEYWORD("sqlstate", K_SQLSTATE)
OG_KEYWORD("sqlwarning", K_SQLWARNING)
OG_KEYWORD("stacked", K_STACKED)
OG_KEYWORD("subclass_origin", K_SUBCLASS_ORIGIN)
OG_KEYWORD("subtype", K_SUBTYPE)
OG_KEYWORD("sys_refcursor", K_SYS_REFCURSOR)
OG_KEYWORD("table", K_TABLE)
OG_KEYWORD("table_name", K_TABLE_NAME)
OG_KEYWORD("true", K_TRUE)
OG_KEYWORD("type", K_TYPE)
OG_KEYWORD("union", K_UNION)
OG_KEYWORD("until", K_UNTIL)
OG_KEYWORD("use_column", K_USE_COLUMN)
OG_KEYWORD("use_variable", K_USE_VARIABLE)
OG_KEYWORD("variable_conflict", K_VARIABLE_CONFLICT)
OG_KEYWORD("varray", K_VARRAY)
OG_KEYWORD("warning", K_WARNING)
OG_KEYWORD("with", K_WITH)

