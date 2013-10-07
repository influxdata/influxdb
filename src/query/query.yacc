%{
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "query_types.h"

void free_array(array *array);
void free_value_array(value_array *array);
void free_value(value *value);
void free_expression(expression *expr);
void free_bool_expression(bool_expression *expr);
void free_condition(condition *condition);
void free_from_clause(from *from);
void free_error (error *error);

%}

%union {
  char character;
  char *string;
  array *arr;
  int i;
  from *f;
  condition *condition;
  bool_expression *bool_expression;
  expression *expression;
  value_array *value_array;
  value *v;
}

// debugging
%debug

// declare that we want a reentrant parser
%define      api.pure
%error-verbose
%locations
%parse-param {query *q}
%parse-param {void *scanner}
%lex-param   {void *scanner}

%token          SELECT FROM WHERE EQUAL GROUP_BY
%token <string> STRING_VALUE NAME

// define the precendence of these operators
%left  OR
%left  AND
%nonassoc <string> OPERATION_EQUAL OPERATION_NE OPERATION_GT OPERATION_LT OPERATION_LE OPERATION_GE
%left  <character> '+' '-'
%left  <character> '*' '/'

%type <f>               FROM_CLAUSE
%type <string>          TABLE_NAME
%type <condition>       WHERE_CLAUSE
%type <value_array>     COLUMN_NAMES
%type <string>          BOOL_OPERATION
%type <character>       ARITHMETIC_OPERATION
%type <condition>       CONDITION
%type <bool_expression> BOOL_EXPRESSION
%type <value_array>     VALUES
%type <v>               VALUE
%type <v>               FUNCTION_CALL
%type <expression>      EXPRESSION
%type <value_array>     GROUP_BY_CLAUSE
%start                  QUERY

%destructor { free_value($$); } <v>
%destructor { free_condition($$); } <condition>
%destructor { free_array($$); } <arr>
%destructor { free_from_clause($$); } <f>
%destructor { free($$); } <string>
%destructor { free_expression($$); } <expression>
%destructor { free_value_array($$); } <value_array>

%%
QUERY:
        SELECT COLUMN_NAMES FROM_CLAUSE GROUP_BY_CLAUSE WHERE_CLAUSE ';'
        {
          q->c = $2;
          q->f = $3;
          q->group_by = $4;
          q->where_condition = $5;
        }
        |
        SELECT COLUMN_NAMES FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE ';'
        {
          q->c = $2;
          q->f = $3;
          q->where_condition = $4;
          q->group_by = $5;
        }
        |
        SELECT COLUMN_NAMES FROM_CLAUSE ';'
        {
          q->c = $2;
          q->f = $3;
          q->group_by = NULL;
          q->where_condition = NULL;
        }
        |
        SELECT COLUMN_NAMES FROM_CLAUSE WHERE_CLAUSE ';'
        {
          q->c = $2;
          q->f = $3;
          q->group_by = NULL;
          q->where_condition = $4;
        }
        |
        SELECT COLUMN_NAMES FROM_CLAUSE GROUP_BY_CLAUSE ';'
        {
          q->c = $2;
          q->f = $3;
          q->group_by = $4;
          q->where_condition = NULL;
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
        GROUP_BY VALUES
        {
          $$ = $2;
        }

COLUMN_NAMES: VALUES

FROM_CLAUSE: FROM TABLE_NAME
{
  $$ = malloc(sizeof(from));
  $$->table = $2;
}

WHERE_CLAUSE: WHERE CONDITION
{
  $$ = $2;
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
        VALUE ARITHMETIC_OPERATION VALUE
        {
          $$ = malloc(sizeof(expression));
          $$->left = $1;
          $$->op = $2;
          $$->right = $3;
        }

ARITHMETIC_OPERATION:
        '+'
        |
        '-'
        |
        '*'
        |
        '/'

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

TABLE_NAME: NAME

%%
void *yy_scan_string(char *, void *);
void yy_delete_buffer(void *, void *);

void
free_array(array *array)
{
  int i;
  for (i = 0; i < array->size; i++)
    free(array->elems[i]);
  free(array->elems);
  free(array);
}

void
free_value_array(value_array *array)
{
  int i;
  for (i = 0; i < array->size; i++)
    free_value(array->elems[i]);
  free(array->elems);
  free(array);
}

void
free_value(value *value)
{
  free(value->name);
  if (value->args) free_value_array(value->args);
  free(value);
}

void
free_expression(expression *expr)
{
  free_value(expr->left);
  if (expr->right) free_value(expr->right);
  free(expr);
}

void
free_bool_expression(bool_expression *expr)
{
  free_expression(expr->left);
  if (expr->op) free(expr->op);
  if (expr->right) free_expression(expr->right);
  free(expr);
}

void
free_condition(condition *condition)
{
  if (condition->is_bool_expression) {
    free_bool_expression((bool_expression*) condition->left);
  } else {
    free_condition(condition->left);
    free_condition(condition->right);
  }
  free(condition);
}

void
free_from_clause(from *from)
{
  free(from->table);
  free(from);
}

void
free_error (error *error)
{
  free(error->err);
  free(error);
}

void
close_query (query *q)
{
  if (q->error) {
    free_error(q->error);
    return;
  }

  // free the columns
  free_value_array(q->c);

  if (q->where_condition) {
    free_condition(q->where_condition);
  }

  if (q->group_by) {
    free_value_array(q->group_by);
  }

  // free the from clause
  free_from_clause(q->f);
}

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
