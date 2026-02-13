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
 * pl_scanner.h
 *
 *
 * IDENTIFICATION
 * src/ogsql/pl/parser/pl_scanner.h
 *
 * -------------------------------------------------------------------------
 */
#include "pl_compiler.h"
#include "ogsql_expr_def.h"
#include "cm_text.h"
#include "expr_parser.h"

#include "pl_gram.h" /* must be after parser/scanner.h */

#include "gramparse.h"

#define LENGTH_OF_DOT_AND_STR_END 4
#define INT32_STRING_SIZE 12
/*
 * A word about keywords:
 *
 * We keep reserved and unreserved keywords in separate headers.  Be careful
 * not to put the same word in both headers.  Also be sure that pl_gram.y's
 * unreserved_keyword production agrees with the unreserved header.  The
 * reserved keywords are passed to the core scanner, so they will be
 * recognized before (and instead of) any variable name.  Unreserved
 * words are checked for separately, after determining that the identifier
 * isn't a known variable name.  If plpgsql_IdentifierLookup is DECLARE then
 * no variable names will be recognized, so the unreserved words always work.
 * (Note in particular that this helps us avoid reserving keywords that are
 * only needed in DECLARE sections.)
 *
 * In certain contexts it is desirable to prefer recognizing an unreserved
 * keyword over recognizing a variable name.  Those cases are handled in
 * gram.y using tok_is_keyword().
 *
 * For the most part, the reserved keywords are those that start a PL/pgSQL
 * statement (and so would conflict with an assignment to a variable of the
 * same name).	We also don't sweat it much about reserving keywords that
 * are reserved in the core grammar.  Try to avoid reserving other words.
 */

/*
 * Lists of keyword (name, token-value, category) entries.
 *
 * !!WARNING!!: These lists must be sorted by ASCII name, because binary
 *		 search is used to locate entries.
 *
 * Be careful not to put the same word in both lists.  Also be sure that
 * gram.y's unreserved_keyword production agrees with the second list.
 */

/* ScanKeywordList lookup data for PL/pgSQL keywords */
#include "pl_reserved_kwlist_d.h"
#include "pl_unreserved_kwlist_d.h"

/* Token codes for PL/pgSQL keywords */
#define OG_KEYWORD(kwname, value) value,

static const uint16 ReservedPLKeywordTokens[] = {
#include "pl_reserved_kwlist.h"
};
/* static const uint16 UnreservedPLKeywordTokens[] = {
#include "pl_unreserved_kwlist.h"
}; */

#undef OG_KEYWORD

/* static const struct PlsqlKeywordValue keywordsValue = {
    .procedure = K_PROCEDURE,
    .function = K_FUNCTION,
    .begin = K_BEGIN,
    .select = K_SELECT,
    .update = K_UPDATE,
    .insert = K_INSERT,
    .Delete = K_DELETE,
    .merge = K_MERGE
}; */

/* Auxiliary data about a token (other than the token type) */
typedef struct {
    YYSTYPE lval; /* semantic information */
    YYLTYPE lloc; /* offset in scanbuf */
    int leng;     /* length in bytes */
} TokenAuxData;

status_t pl_parser(sql_stmt_t *stmt, text_t *src)
{
    core_yyscan_t yyscanner;
    base_yy_extra_type yyextra;
    int parse_rc;

    yyscanner = scanner_init((sql_text_t *)src, &yyextra.core_yy_extra, &ReservedPLKeywords, ReservedPLKeywordTokens,
        stmt);

    parse_rc = plsql_yyparse(yyscanner);
    if (parse_rc != 0) {
        return OG_ERROR;
    }

    scanner_finish(yyscanner);
    return OG_SUCCESS;
}

static int internal_yylex(TokenAuxData* auxdata, core_yyscan_t yyscanner)
{
    int token;

    errno_t rc = memset_s(auxdata, sizeof(TokenAuxData), 0, sizeof(TokenAuxData));
    knl_securec_check(rc);

    token = core_yylex(&auxdata->lval.core_yystype, &auxdata->lloc, yyscanner);
    auxdata->leng = ct_yyget_leng(yyscanner);

    return token;
}

int plsql_yylex(core_yyscan_t yyscanner)
{
    int token;
    TokenAuxData aux;

    token = internal_yylex(&aux, yyscanner);
    if (token == IDENT) {
        token = T_WORD;
    }

    plsql_yylval = aux.lval;
    plsql_yylloc = aux.lloc;
    return token;
}

void plsql_yyerror(core_yyscan_t yyscanner, const char* message)
{
    OG_THROW_ERROR(ERR_SQL_SYNTAX_ERROR, message);
    return;
}
