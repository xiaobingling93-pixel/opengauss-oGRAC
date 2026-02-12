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
 * pl_reserved_kwlist.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/pl/parser/pl_reserved_kwlist.h
 *
 * -------------------------------------------------------------------------
 */

/* There is deliberately not an #ifndef PL_RESERVED_KWLIST_H here. */

/*
 * List of (keyword-name, keyword-token-value) pairs.
 *
 * Be careful not to put the same word into pl_unreserved_kwlist.h.
 *
 * Note: gen_keywordlist.pl requires the entries to appear in ASCII order.
 */

/* name, value */
OG_KEYWORD("all", K_ALL)
OG_KEYWORD("begin", K_BEGIN)
OG_KEYWORD("by", K_BY)
OG_KEYWORD("case", K_CASE)
OG_KEYWORD("close", K_CLOSE)
OG_KEYWORD("collate", K_COLLATE)
OG_KEYWORD("declare", K_DECLARE)
OG_KEYWORD("default", K_DEFAULT)
OG_KEYWORD("delete", K_DELETE)
OG_KEYWORD("diagnostics", K_DIAGNOSTICS)
OG_KEYWORD("else", K_ELSE)
OG_KEYWORD("elseif", K_ELSIF)
OG_KEYWORD("elsif", K_ELSIF)
OG_KEYWORD("end", K_END)
OG_KEYWORD("exception", K_EXCEPTION)
OG_KEYWORD("execute", K_EXECUTE)
OG_KEYWORD("exit", K_EXIT)
OG_KEYWORD("fetch", K_FETCH)
OG_KEYWORD("for", K_FOR)
OG_KEYWORD("forall", K_FORALL)
OG_KEYWORD("foreach", K_FOREACH)
OG_KEYWORD("from", K_FROM)
OG_KEYWORD("function", K_FUNCTION)
OG_KEYWORD("get", K_GET)
OG_KEYWORD("goto", K_GOTO)
OG_KEYWORD("if", K_IF)
OG_KEYWORD("in", K_IN)
OG_KEYWORD("insert", K_INSERT)
OG_KEYWORD("into", K_INTO)
OG_KEYWORD("limit", K_LIMIT)
OG_KEYWORD("loop", K_LOOP)
OG_KEYWORD("move", K_MOVE)
OG_KEYWORD("not", K_NOT)
OG_KEYWORD("null", K_NULL)
OG_KEYWORD("of", K_OF)
OG_KEYWORD("open", K_OPEN)
OG_KEYWORD("or", K_OR)
OG_KEYWORD("out", K_OUT)
OG_KEYWORD("procedure", K_PROCEDURE)
OG_KEYWORD("raise", K_RAISE)
OG_KEYWORD("ref", K_REF)
OG_KEYWORD("return", K_RETURN)
OG_KEYWORD("select", K_SELECT)
OG_KEYWORD("strict", K_STRICT)
OG_KEYWORD("then", K_THEN)
OG_KEYWORD("to", K_TO)
OG_KEYWORD("update", K_UPDATE)
OG_KEYWORD("using", K_USING)
OG_KEYWORD("when", K_WHEN)
OG_KEYWORD("while", K_WHILE)
