#ifndef SCANBUF_EXTRA_SIZE
#define SCANBUF_EXTRA_SIZE (2)
#endif

#ifndef LITERAL_SIZE
#define LITERAL_SIZE (1024)
#endif

/*
 * Constant data exported from this file.  This array maps from the
 * zero-based keyword numbers returned by ScanKeywordLookup to the
 * Bison token numbers needed by gram.y.  This is exported because
 * callers need to pass it to scanner_init, if they are using the
 * standard keyword list ScanKeywords.
 */
#define OG_KEYWORD(kwname, value, category) (value),

const uint16 b_format_ScanKeywordTokens[] = {
#include "kwlist.h"
};

/*
 *  The following macro will inject DIALECT_B_FORMAT_SQL value
 *  as the first token in the string being parsed.
 *  We use this mechanism to choose different dialects
 *  within the parser.  See the corresponding code
 *  in scanner_init()
 */
#undef YY_USER_INIT

#define YY_USER_INIT                         \
    do                                       \
    {                                        \
        if ( !yyg->yy_start )                \
            yyg->yy_start = 1;               \
        if ( !yyin )                         \
            yyin = stdin;                    \
        if ( !yyout )                                                \
            yyout = stdout;                                          \
        if ( !YY_CURRENT_BUFFER ) {                                  \
            yyensure_buffer_stack (yyscanner);                       \
            YY_CURRENT_BUFFER_LVALUE =                               \
                yy_create_buffer(yyin, YY_BUF_SIZE, yyscanner);      \
        }                                                            \
        yy_load_buffer_state(yyscanner);                             \
        return DIALECT_B_FORMAT_SQL;                                 \
    } while (0)

static int ct_yylex_init(yyscan_t* ptr_yy_globals, sql_stmt_t *stmt);
