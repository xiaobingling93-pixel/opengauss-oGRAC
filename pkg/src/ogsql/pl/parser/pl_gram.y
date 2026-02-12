%{

#include "pl_compiler.h"
#include "ast_cl.h"
#include "typedef_cl.h"
#include "base_compiler.h"
#include "decl_cl.h"
#include "lines_cl.h"
#include "pl_udt.h"
#include "pl_gram.h"
#include "gramparse.h"
#include "dml_parser.h"
#include "pl_dc.h"

/* Location tracking support --- simpler than bison's default */

#define YYLLOC_DEFAULT(Current, Rhs, N) \
    do { \
        if (N) \
            (Current) = (Rhs)[1]; \
        else \
            (Current) = (Rhs)[0]; \
    } while (0)

#ifdef YYLEX_PARAM
# define YYLEX yylex (YYLEX_PARAM)
#else
# define YYLEX yylex (yyscanner)
#endif

#define MAX_SQL_LEN 1024

extern int plsql_yylex(core_yyscan_t yyscanner);
extern void plsql_yyerror(core_yyscan_t yyscanner, const char* message);
static status_t read_sql_construct(sql_stmt_t *stmt, text_t *src, pl_line_normal_t *line, core_yyscan_t yyscanner);
static status_t read_return_sql_construct(sql_stmt_t *stmt, text_t *src, pl_line_return_t *line,
    core_yyscan_t yyscanner);
static status_t make_type_word(pl_compiler_t *compiler, type_word_t **type, char *str,
    galist_t *typemode, source_location_t loc);
static status_t check_sql_expr(sql_stmt_t *stmt, sql_text_t *sql, sql_select_t **select_ctx);

union YYSTYPE;					/* need forward reference for tok_is_keyword */

%}

%expect 0
%name-prefix "plsql_yy"
%locations

%parse-param {core_yyscan_t yyscanner}
%lex-param   {core_yyscan_t yyscanner}

%union {
    core_YYSTYPE core_yystype;
    const char *keyword;
    PLword word;
    expr_node_t *node;
    text_t *text;
    type_word_t *type;
    char *str;
}

%type <keyword>	unreserved_keyword
%type <node> assign_var
%type <text> expr_until_semi
%type <type> decl_datatype
%type <str> decl_varname

%token <str>    IDENT FCONST SCONST XCONST Op CmpOp COMMENTSTRING SET_USER_IDENT SET_IDENT UNDERSCORE_CHARSET FCONST_F FCONST_D
                OPER_CAT OPER_LSHIFT OPER_RSHIFT
%token <ival>   ICONST PARAM

%token            LEX_ERROR_TOKEN
%token            TYPECAST ORA_JOINOP DOT_DOT COLON_EQUALS PARA_EQUALS SET_IDENT_SESSION SET_IDENT_GLOBAL NULLS_FIRST NULLS_LAST
%token <str>      SIZE_B SIZE_KB SIZE_MB SIZE_GB SIZE_TB SIZE_PB SIZE_EB

%token <word>   T_WORD

%token <keyword>	K_ABSOLUTE
%token <keyword>	K_ALIAS
%token <keyword>	K_ALL
%token <keyword>	K_ALTER
%token <keyword>	K_ARRAY
%token <keyword>	K_AS
%token <keyword>	K_BACKWARD
%token <keyword>	K_BEGIN
%token <keyword>	K_BULK
%token <keyword>	K_BY
%token <keyword>        K_CALL
%token <keyword>	K_CASE
%token <keyword>	K_CATALOG_NAME
%token <keyword>	K_CLASS_ORIGIN
%token <keyword>	K_CLOSE
%token <keyword>	K_COLLATE
%token <keyword>	K_COLLECT
%token <keyword>	K_COLUMN_NAME
%token <keyword>	K_COMMIT
%token <keyword>	K_CONDITION
%token <keyword>	K_CONSTANT
%token <keyword>	K_CONSTRAINT_CATALOG
%token <keyword>	K_CONSTRAINT_NAME
%token <keyword>	K_CONSTRAINT_SCHEMA
%token <keyword>	K_CONTINUE
%token <keyword>	K_CURRENT
%token <keyword>	K_CURSOR
%token <keyword>	K_CURSOR_NAME
%token <keyword>	K_DEBUG
%token <keyword>	K_DECLARE
%token <keyword>	K_DEFAULT
%token <keyword>	K_DELETE
%token <keyword>	K_DETAIL
%token <keyword>	K_DETERMINISTIC
%token <keyword>	K_DIAGNOSTICS
%token <keyword>	K_DISTINCT
%token <keyword>        K_DO
%token <keyword>	K_DUMP
%token <keyword>	K_ELSE
%token <keyword>	K_ELSIF
%token <keyword>	K_END
%token <keyword>	K_ERRCODE
%token <keyword>	K_ERROR
%token <keyword>    K_EXCEPT
%token <keyword>	K_EXCEPTION
%token <keyword>	K_EXCEPTIONS
%token <keyword>	K_EXECUTE
%token <keyword>	K_EXIT
%token <keyword>	K_FALSE
%token <keyword>	K_FETCH
%token <keyword>	K_FIRST
%token <keyword>	K_FOR
%token <keyword>	K_FORALL
%token <keyword>	K_FOREACH
%token <keyword>	K_FORWARD
%token <keyword>	K_FOUND
%token <keyword>	K_FROM
%token <keyword>	K_FUNCTION
%token <keyword>	K_GET
%token <keyword>	K_GOTO
%token <keyword>	K_HANDLER
%token <keyword>	K_HINT
%token <keyword>	K_IF
%token <keyword>	K_IMMEDIATE
%token <keyword>    K_INSTANTIATION
%token <keyword>	K_IN
%token <keyword>	K_INDEX
%token <keyword>	K_INFO
%token <keyword>	K_INSERT
%token <keyword>	K_INTERSECT
%token <keyword>	K_INTO
%token <keyword>	K_IS
%token <keyword>        K_ITERATE
%token <keyword>	K_LAST
%token <keyword>        K_LEAVE
%token <keyword>	K_LIMIT
%token <keyword>	K_LOG
%token <keyword>	K_LOOP
%token <keyword>    K_MERGE
%token <keyword>	K_MESSAGE
%token <keyword>	K_MESSAGE_TEXT
%token <keyword>	K_MOVE
%token <keyword>    K_MULTISET
%token <keyword>    K_MULTISETS
%token <keyword>    K_MYSQL_ERRNO
%token <keyword>    K_NUMBER
%token <keyword>	K_NEXT
%token <keyword>	K_NO
%token <keyword>	K_NOT
%token <keyword>	K_NOTICE
%token <keyword>	K_NULL
%token <keyword>	K_OF
%token <keyword>	K_OPEN
%token <keyword>	K_OPTION
%token <keyword>	K_OR
%token <keyword>	K_OUT
%token <keyword>        K_PACKAGE
%token <keyword>	K_PERFORM
%token <keyword>	K_PIPE
%token <keyword>	K_PG_EXCEPTION_CONTEXT
%token <keyword>	K_PG_EXCEPTION_DETAIL
%token <keyword>	K_PG_EXCEPTION_HINT
%token <keyword>	K_PRAGMA
%token <keyword>	K_PRIOR
%token <keyword>	K_PROCEDURE
%token <keyword>	K_QUERY
%token <keyword>	K_RAISE
%token <keyword>	K_RECORD
%token <keyword>	K_REF
%token <keyword>	K_RELATIVE
%token <keyword>	K_RELEASE
%token <keyword>	K_REPEAT
%token <keyword>	K_REPLACE
%token <keyword>	K_RESULT_OID
%token <keyword>	K_RESIGNAL
%token <keyword>	K_RETURN
%token <keyword>	K_RETURNED_SQLSTATE
%token <keyword>	K_REVERSE
%token <keyword>	K_ROLLBACK
%token <keyword>	K_ROW
%token <keyword>	K_ROWTYPE
%token <keyword>	K_ROW_COUNT
%token <keyword>	K_SAVE
%token <keyword>	K_SAVEPOINT
%token <keyword>	K_SCHEMA_NAME
%token <keyword>	K_SELECT
%token <keyword>	K_SCROLL
%token <keyword>	K_SIGNAL
%token <keyword>	K_SLICE
%token <keyword>	K_SQLEXCEPTION
%token <keyword>	K_SQLSTATE
%token <keyword>	K_SQLWARNING
%token <keyword>	K_STACKED
%token <keyword>	K_STRICT
%token <keyword>	K_SUBCLASS_ORIGIN
%token <keyword>	K_SUBTYPE
%token <keyword>	K_SYS_REFCURSOR
%token <keyword>	K_TABLE
%token <keyword>	K_TABLE_NAME
%token <keyword>	K_THEN
%token <keyword>	K_TO
%token <keyword>	K_TRUE
%token <keyword>	K_TYPE
%token <keyword>	K_UNION
%token <keyword>	K_UNTIL
%token <keyword>	K_UPDATE
%token <keyword>	K_USE_COLUMN
%token <keyword>	K_USE_VARIABLE
%token <keyword>	K_USING
%token <keyword>	K_VARIABLE_CONFLICT
%token <keyword>	K_VARRAY
%token <keyword>	K_WARNING
%token <keyword>	K_WHEN
%token <keyword>	K_WHILE
%token <keyword>	K_WITH

%%

pl_body:
            pl_function
        ;

pl_function:
            pl_block
        ;

pl_block:
            declare_sect_b K_BEGIN
                {
                    pl_compiler_t *compiler = (pl_compiler_t*)og_yyget_extra(yyscanner)->core_yy_extra.stmt->pl_compiler;
                    pl_line_begin_t *line = NULL;
                    text_t block_name = CM_NULL_TEXT;
                    plc_alloc_line(compiler, sizeof(pl_line_begin_t), LINE_BEGIN, (pl_line_ctrl_t **)&line);
                    plc_push(compiler, (pl_line_ctrl_t *)line, &block_name);
                    plc_convert_typedecl(compiler, compiler->decls);
                    line->decls = compiler->decls;
                    line->type_decls = compiler->type_decls;
                    line->name = NULL;
                    if (compiler->body == NULL) {
                        compiler->body = line;
                    }
                }
            proc_sect K_END T_WORD ';'
                {
                    pl_compiler_t *compiler = (pl_compiler_t*)og_yyget_extra(yyscanner)->core_yy_extra.stmt->pl_compiler;
                    if (strncmp($6.ident, compiler->obj->name.str, compiler->obj->name.len) != 0) {
                        YYABORT;
                    }
                    pl_line_ctrl_t *line = NULL;
                    pl_line_begin_t *begin_line = (pl_line_begin_t *)plc_get_current_beginln(compiler);

                    plc_alloc_line(compiler, sizeof(pl_line_ctrl_t), LINE_END, (pl_line_ctrl_t **)&line);
                    begin_line->end = line;
                }
        ;

proc_sect:
            proc_stmts
        ;

proc_stmts:
            proc_stmt
            | proc_stmts proc_stmt
        ;

proc_stmt:
            pl_block ';'
            | label_stmts
        ;

label_stmts:
            label_stmt
        ;

label_stmt:
            stmt_assign
            | stmt_return
        ;

stmt_assign:
            assign_var COLON_EQUALS expr_until_semi
                {
                    pl_line_normal_t *line = NULL;
                    sql_stmt_t *stmt = og_yyget_extra(yyscanner)->core_yy_extra.stmt;
                    pl_compiler_t *compiler = (pl_compiler_t*)stmt->pl_compiler;

                    plc_alloc_line(compiler, sizeof(pl_line_normal_t), LINE_SETVAL, (pl_line_ctrl_t **)&line);
                    line->left = $1;
                    read_sql_construct(stmt, $3, line, yyscanner);
                    plc_clone_expr_node(compiler, &line->left);
                    plc_clone_expr_tree(compiler, &line->expr);
                    plc_clone_cond_tree(compiler, &line->cond);
                }
        ;

stmt_return:
            K_RETURN expr_until_semi
                {
                    pl_line_return_t *line = NULL;
                    sql_stmt_t *stmt = og_yyget_extra(yyscanner)->core_yy_extra.stmt;
                    pl_compiler_t *compiler = (pl_compiler_t*)stmt->pl_compiler;
                    plv_decl_t *decl = NULL;
                    plv_id_t ret_vid = {
                        .block = 0,
                        .id = 0,
                        .input_id = 0
                    };

                    plc_alloc_line(compiler, sizeof(pl_line_return_t), LINE_RETURN, (pl_line_ctrl_t **)&line);
                    read_return_sql_construct(stmt, $2, line, yyscanner);

                    decl = plc_find_param_by_id(compiler, ret_vid);
                    if (decl == NULL) {
                        YYABORT;
                    }
                    OG_RETURN_IFERR(plc_verify_expr(compiler, line->expr));
                    OG_RETURN_IFERR(plc_clone_expr_tree(compiler, &line->expr));
                    if (!sql_is_skipped_expr(line->expr)) {
                        plc_verify_stack_var_assign(compiler, decl, line->expr);
                    }
                }
        ;

expr_until_semi:
                {
                    text_t *expr_src = NULL;
                    int	tok = YYLEX;
                    int begin = yylloc.offset;
                    sql_stmt_t *stmt = og_yyget_extra(yyscanner)->core_yy_extra.stmt;

                    if (sql_stack_alloc(stmt, sizeof(text_t), (void**)&expr_src) != OG_SUCCESS) {
                        YYABORT;
                    }

                    while (tok != ';') {
                        tok = YYLEX;
                    }

                    expr_src->str = og_yyget_extra(yyscanner)->core_yy_extra.scanbuf + begin;
                    expr_src->len = yylloc.offset - begin;
                    $$ = expr_src;
                }
        ;


assign_var:
            T_WORD
                {
                    text_t name;
                    cm_str2text($1.ident, &name);
                    expr_node_t *expr = NULL;
                    sql_stmt_t *stmt = og_yyget_extra(yyscanner)->core_yy_extra.stmt;
                    pl_compiler_t *compiler = (pl_compiler_t*)stmt->pl_compiler;
                    sql_alloc_mem(compiler->stmt->context, sizeof(expr_node_t), (void **)&expr);
                    expr->owner = NULL;
                    expr->type = EXPR_NODE_PROC;
                    expr->unary = UNARY_OPER_NONE;
                    expr->word.func.user_func_first = OG_FALSE;

                    uint32 types = PLV_TYPE | PLV_VARIANT_AND_CUR;
                    plv_decl_t *decl = NULL;
                    plc_variant_name_t variant_name;
                    char block_name_buf[OG_NAME_BUFFER_SIZE];
                    char name_buf[OG_NAME_BUFFER_SIZE];
                    plc_var_type_t type;

                    PLC_INIT_VARIANT_NAME(&variant_name, block_name_buf, name_buf, OG_FALSE, types);
                    type = PLC_NORMAL_VAR;
                    variant_name.block_name.len = 0;
                    plc_concat_text_upper_by_type(&variant_name.name, OG_MAX_NAME_LEN, &name, WORD_TYPE_VARIANT);

                    if (type == PLC_NORMAL_VAR || type == PLC_TRIGGER_VAR) {
                        plc_find_block_decl(compiler, &variant_name, &decl);
                    }

                    plc_build_var_address(stmt, decl, expr, UDT_STACK_ADDR);
                    $$ = expr;
                }
        ;

declare_sect_b:
            decl_stmts
            | /* EMPTY */
        ;

decl_stmts:
            decl_stmt
            | decl_stmts decl_stmt
        ;

decl_stmt:
            decl_varname decl_datatype
                {
                    pl_compiler_t *compiler = (pl_compiler_t*)og_yyget_extra(yyscanner)->core_yy_extra.stmt->pl_compiler;
                    galist_t *decls = compiler->decls;
                    plv_decl_t *decl = NULL;

                    cm_galist_new(decls, sizeof(plv_decl_t), (void **)&decl);
                    decl->vid.block = (int16)compiler->stack.depth;
                    decl->vid.id = (uint16)(decls->count - 1); // not overflow
                    cm_str2text($1, &decl->name);
                    decl->type = PLV_VAR;

                    plc_bison_compile_type(compiler, PLC_PMODE(decl->drct), &decl->variant.type, $2);
                }
        ;

decl_datatype:
                {
                    /* make a type_word_t */
                    int tok = YYLEX;
                    source_location_t loc = yylloc.loc;
                    char *typename = NULL;
                    type_word_t *type = NULL;
                    pl_compiler_t *compiler = (pl_compiler_t*)og_yyget_extra(yyscanner)->core_yy_extra.stmt->pl_compiler;

                    if (tok == T_WORD) {
                        typename = yylval.word.ident;
                    }

                    tok = YYLEX;

                    if (tok != ';') {
                        YYABORT;
                    }

                    if (make_type_word(compiler, &type, typename, NULL, loc) != OG_SUCCESS) {
                        YYABORT;
                    }

                    $$ = type;
                }

decl_varname:
            T_WORD
                {
                    pl_compiler_t *compiler = (pl_compiler_t*)og_yyget_extra(yyscanner)->core_yy_extra.stmt->pl_compiler;
                    text_t name;
                    cm_str2text($1.ident, &name);
                    bool32 result = OG_FALSE;
                    plc_check_duplicate(compiler->decls, (text_t *)&name, $1.quoted, &result);

                    if (result) {
                        YYABORT;
                    }
                    $$ = $1.ident;
                }
            | unreserved_keyword
                {
                    pl_compiler_t *compiler = (pl_compiler_t*)og_yyget_extra(yyscanner)->core_yy_extra.stmt->pl_compiler;
                    char *tmp = strdup($1);
                    text_t name;
                    cm_str2text(tmp, &name);
                    bool32 result = OG_FALSE;
                    plc_check_duplicate(compiler->decls, (text_t *)&name, OG_FALSE, &result);

                    if (result) {
                        YYABORT;
                    }
                    $$ = tmp;
                }
        ;

unreserved_keyword:
                            K_ABSOLUTE
                | K_ALIAS
                | K_ALTER
                | K_ARRAY
                | K_BACKWARD
                | K_CALL
                | K_CATALOG_NAME
                | K_CLASS_ORIGIN
                | K_COLUMN_NAME
                | K_COMMIT
                | K_CONDITION
                | K_CONSTANT
                | K_CONSTRAINT_CATALOG
                | K_CONSTRAINT_NAME
                | K_CONSTRAINT_SCHEMA
                | K_CONTINUE
                | K_CURRENT
                | K_CURSOR_NAME
                | K_DEBUG
                | K_DETAIL
                | K_DISTINCT
                | K_DUMP
                | K_ERRCODE
                | K_ERROR
                | K_EXCEPT
                | K_EXCEPTIONS
                | K_FIRST
                | K_FORWARD
                | K_HINT
                | K_INDEX
                | K_INFO
                | K_INTERSECT
                | K_IS
                | K_LAST
                | K_LOG
                | K_MERGE
                | K_MESSAGE
                | K_MESSAGE_TEXT
                | K_MULTISET
                | K_MYSQL_ERRNO
                | K_NEXT
                | K_NO
                | K_NOTICE
                | K_OPTION
                | K_PACKAGE
                | K_INSTANTIATION
                | K_PG_EXCEPTION_CONTEXT
                | K_PG_EXCEPTION_DETAIL
                | K_PG_EXCEPTION_HINT
                | K_PIPE
                | K_PRIOR
                | K_QUERY
                | K_RECORD
                | K_RELATIVE
                | K_RESIGNAL
                | K_RESULT_OID
                | K_RETURNED_SQLSTATE
                | K_REVERSE
                | K_ROLLBACK
                | K_ROW
                | K_ROW_COUNT
                | K_ROWTYPE
                | K_SAVE
                | K_SCHEMA_NAME
                | K_SCROLL
                | K_SIGNAL
                | K_SLICE
                | K_SQLSTATE
                | K_STACKED
                | K_SUBCLASS_ORIGIN
                | K_SYS_REFCURSOR
                | K_TABLE
                | K_TABLE_NAME
                | K_UNION
                | K_USE_COLUMN
                | K_USE_VARIABLE
                | K_VARIABLE_CONFLICT
                | K_VARRAY
                | K_WARNING
                | K_WITH
        ;

%%

static status_t read_return_sql_construct(sql_stmt_t *stmt, text_t *src, pl_line_return_t *line,
    core_yyscan_t yyscanner)
{
    char str[MAX_SQL_LEN] = { 0 };
    text_t sql;
    sql.str = str;
    sql_select_t *select_ctx = NULL;

    status_t ret = snprintf_s(str, MAX_SQL_LEN, MAX_SQL_LEN - 1,
        "SELECT %.*s", (int)src->len, src->str);
    knl_securec_check_ss(ret);
    sql.len = strlen("SELECT ") + src->len;

    if (check_sql_expr(stmt, (sql_text_t*)&sql, &select_ctx) != OG_SUCCESS) {
        return OG_ERROR;
    }

    line->expr = ((query_column_t*)cm_galist_get(select_ctx->first_query->columns, 0))->expr;
    return OG_SUCCESS;
}

static status_t read_sql_construct(sql_stmt_t *stmt, text_t *src, pl_line_normal_t *line, core_yyscan_t yyscanner)
{
    char str[MAX_SQL_LEN] = { 0 };
    text_t sql;
    sql.str = str;
    sql_select_t *select_ctx = NULL;
    pl_compiler_t *compiler = stmt->pl_compiler;

    status_t ret = snprintf_s(str, MAX_SQL_LEN, MAX_SQL_LEN - 1,
        "SELECT %.*s", (int)src->len, src->str);
    knl_securec_check_ss(ret);
    sql.len = strlen("SELECT ") + src->len;

    if (check_sql_expr(stmt, (sql_text_t*)&sql, &select_ctx) == OG_SUCCESS) {
        /* expr */
        line->expr = ((query_column_t*)cm_galist_get(select_ctx->first_query->columns, 0))->expr;
        OG_RETURN_IFERR(plc_verify_setval(compiler, line->left, line->expr));
        return OG_SUCCESS;
    }

    /* cond */
    ret = memset_sp(str, MAX_SQL_LEN, 0, MAX_SQL_LEN);
    knl_securec_check(ret);

    ret = snprintf_s(str, MAX_SQL_LEN, MAX_SQL_LEN - 1,
        "SELECT CASE WHEN %.*s THEN TRUE ELSE FALSE END", (int)src->len, src->str);
    knl_securec_check_ss(ret);
    sql.len = strlen("SELECT CASE WHEN  THEN TRUE ELSE FALSE END ") + src->len;
    
    if (check_sql_expr(stmt, (sql_text_t*)&sql, &select_ctx) != OG_SUCCESS) {
        return OG_ERROR;
    }
    case_expr_t * case_expr = (case_expr_t*)(((query_column_t*)cm_galist_get(select_ctx->first_query->columns, 0))->expr->root->value.v_pointer);
    line->cond = ((case_pair_t*)cm_galist_get(&case_expr->pairs, 0))->when_cond;

    OG_RETURN_IFERR(plc_verify_cond(compiler, line->cond));

    return OG_SUCCESS;
}

static status_t check_sql_expr(sql_stmt_t *stmt, sql_text_t *sql, sql_select_t **select_ctx)
{
    if (raw_parser(stmt, sql, (void**)select_ctx) != OG_SUCCESS) {
        return OG_ERROR;
    }

    return OG_SUCCESS;
}

static status_t make_type_word(pl_compiler_t *compiler, type_word_t **type, char *str,
    galist_t *typemode, source_location_t loc)
{
    if (sql_alloc_mem(compiler->stmt->context, sizeof(type_word_t), (void **)type) != OG_SUCCESS) {
        return OG_ERROR;
    }
    (*type)->str = str;
    (*type)->typemode = typemode;
    (*type)->loc = loc;
    return OG_SUCCESS;
}
