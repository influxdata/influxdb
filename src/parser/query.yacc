%{
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "query_types.h"

expression *create_expression(expression *left, char op, expression *right) {
  expression *expr = malloc(sizeof(expression));
  expr->left = left;
  expr->op = op;
  expr->right = right;
  return expr;
}

%}

%union {
  char                  character;
  char*                 string;
  int                   integer;
  condition*            condition;
  bool_expression*      bool_expression;
  expression*           expression;
  value_array*          value_array;
  value*                v;
}

// debugging
%debug

// better error/location reporting
%locations
%error-verbose

// declare that we want a reentrant parser
%define      api.pure
%parse-param {query *q}
%parse-param {void *scanner}
%lex-param   {void *scanner}

// define types of tokens (terminals)
%token          SELECT FROM WHERE EQUAL GROUP BY FIRST LAST
%token <string> STRING_VALUE INT_VALUE NAME REGEX_OP REGEX_STRING

// define the precendence of these operators
%left  OR
%left  AND
%nonassoc <string> OPERATION_EQUAL OPERATION_NE OPERATION_GT OPERATION_LT OPERATION_LE OPERATION_GE
%left  <character> '+' '-'
%left  <character> '*' '/'

// define the types of the non-terminals
%type <v>               FROM_CLAUSE
%type <condition>       WHERE_CLAUSE
%type <value_array>     COLUMN_NAMES
%type <string>          BOOL_OPERATION
%type <condition>       CONDITION
%type <bool_expression> BOOL_EXPRESSION
%type <value_array>     VALUES
%type <v>               VALUE
%type <v>               FUNCTION_CALL
%type <expression>      EXPRESSION
%type <value_array>     GROUP_BY_CLAUSE
%type <integer>         LIMIT

// the initial token
%start                  QUERY

// destructors are used to free up memory in case of an error
%destructor { free_value($$); } <v>
%destructor { if ($$) free_condition($$); } <condition>
%destructor { free($$); } <string>
%destructor { free_expression($$); } <expression>
%destructor { if ($$) free_value_array($$); } <value_array>

// grammar
%%
QUERY:
        SELECT COLUMN_NAMES FROM_CLAUSE GROUP_BY_CLAUSE WHERE_CLAUSE LIMIT ';'
        {
          q->c = $2;
          q->f = $3;
          q->group_by = $4;
          q->where_condition = $5;
          q->limit = $6;
        }
        |
        SELECT COLUMN_NAMES FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT ';'
        {
          q->c = $2;
          q->f = $3;
          q->where_condition = $4;
          q->group_by = $5;
          q->limit = $6;
        }

LIMIT:
        FIRST INT_VALUE
        {
          $$ = atoi($2);
          free($2);
        }
        |
        LAST INT_VALUE
        {
          $$ = -atoi($2);
          free($2);
        }
        |
        {
          $$ = 0;
        }

VALUES:
        VALUE
        {
          $$ = malloc(sizeof(value_array));
          $$->size = 1;
          $$->elems = malloc(sizeof(value*));
          $$->elems[0] = $1;
        }
        |
        VALUES ',' VALUE
        {
          size_t new_size = $1->size + 1;
          $1->elems = realloc($$->elems, sizeof(value*) * new_size);
          $1->elems[$1->size] = $3;
          $1->size = new_size;
          $$ = $1;
        }

GROUP_BY_CLAUSE:
        GROUP BY VALUES
        {
          $$ = $3;
        }
        |
        {
          $$ = NULL;
        }

COLUMN_NAMES: VALUES

FROM_CLAUSE: FROM VALUE
{
  $$ = $2;
}

WHERE_CLAUSE:
        WHERE CONDITION
        {
          $$ = $2;
        }
        |
        {
          $$ = NULL;
        }

FUNCTION_CALL:
        NAME '(' ')'
        {
          $$ = malloc(sizeof(value));
          $$->name = $1;
          $$->args = malloc(sizeof(array));
          $$->args->size = 0;
          $$->args->elems = NULL;
        }
        |
        NAME '(' VALUES ')'
        {
          $$ = malloc(sizeof(value));
          $$->name = $1;
          $$->args = $3;
        }

VALUE:
        STRING_VALUE
        {
          $$ = malloc(sizeof(value));
          $$->name = $1;
          $$->args = NULL;
        }
        |
        INT_VALUE
        {
          $$ = malloc(sizeof(value));
          $$->name = $1;
          $$->args = NULL;
        }
        |
        '*'
        {
          $$ = malloc(sizeof(value));
          $$->name = strdup("*");
          $$->args = NULL;
        }
        |
        NAME
        {
          $$ = malloc(sizeof(value));
          $$->name = $1;
          $$->args = NULL;
        }
        |
        FUNCTION_CALL

EXPRESSION:
        VALUE
        {
          $$ = malloc(sizeof(expression));
          $$->left = $1;
          $$->op = '\0';
          $$->right = NULL;
        }
        |
        '(' EXPRESSION ')'
        {
          $$ = $2;
        }
        |
        EXPRESSION '*' EXPRESSION { $$ = create_expression($1, $2, $3); }
        |
        EXPRESSION '/' EXPRESSION { $$ = create_expression($1, $2, $3); }
        |
        EXPRESSION '+' EXPRESSION { $$ = create_expression($1, $2, $3); }
        |
        EXPRESSION '-' EXPRESSION { $$ = create_expression($1, $2, $3); }

BOOL_EXPRESSION:
        EXPRESSION
        {
          $$ = malloc(sizeof(bool_expression));
          $$->left = $1;
          $$->op = NULL;
          $$->right = NULL;
        }
        |
        EXPRESSION BOOL_OPERATION EXPRESSION
        {
          $$ = malloc(sizeof(bool_expression));
          $$->left = $1;
          $$->op = $2;
          $$->right = $3;
        }
        |
        EXPRESSION REGEX_OP REGEX_STRING
        {
          $$ = malloc(sizeof(bool_expression));
          $$->left = $1;
          $$->op = $2;
          value *v = malloc(sizeof(value));
          v->name = $3;
          v->args = NULL;
          $$->right = malloc(sizeof(expression));
          $$->right->left = v;
          $$->right->op = '\0';
          $$->right->right = NULL;
        }

CONDITION:
        BOOL_EXPRESSION
        {
          $$ = malloc(sizeof(condition));
          $$->is_bool_expression = TRUE;
          $$->left = $1;
          $$->op = NULL;
          $$->right = NULL;
        }
        |
        '(' CONDITION ')'
        {
          $$ = $2;
        }
        |
        CONDITION AND CONDITION
        {
          $$ = malloc(sizeof(condition));
          $$->is_bool_expression = FALSE;
          $$->left = $1;
          $$->op = "AND";
          $$->right = $3;
        }
        |
        CONDITION OR CONDITION
        {
          $$ = malloc(sizeof(condition));
          $$->is_bool_expression = FALSE;
          $$->left = $1;
          $$->op = "OR";
          $$->right = $3;
        }

BOOL_OPERATION:
        OPERATION_EQUAL
        |
        OPERATION_NE
        |
        OPERATION_GT
        |
        OPERATION_LT
        |
        OPERATION_GE
        |
        OPERATION_LE

%%
void *yy_scan_string(char *, void *);
void yy_delete_buffer(void *, void *);

query
parse_query(char *const query_s)
{
  query q;
  q.error = NULL;
  /* yydebug = 1; */
  void *scanner;
  yylex_init(&scanner);
  void *buffer = yy_scan_string(query_s, scanner);
  yyparse (&q, scanner);
  yy_delete_buffer(buffer, scanner);
  yylex_destroy(scanner);
  return q;
}

int yyerror(YYLTYPE *locp, query *q, void *s, char *err) {
  q->error = malloc(sizeof(error));
  q->error->err = strdup(err);
  q->error->line = locp->last_line;
  q->error->column = locp->last_column;
}
