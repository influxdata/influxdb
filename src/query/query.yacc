%{
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "query_types.h"
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
  value *v;
}

// debugging
%debug

// declare that we want a reentrant parser
%define      api.pure
%parse-param {query *q}
%parse-param {void *scanner}
%lex-param   {void *scanner}

%token          SELECT FROM WHERE EQUAL
%token <string> OPERATION_EQUAL OPERATION_NE OPERATION_GT OPERATION_LT OPERATION_LE OPERATION_GE STRING_VALUE NAME
%left  <character> '+' '-'
%left  <character> '*' '/'
%left  <string> OR
%left  <string> AND

%type <f>               FROM_CLAUSE
%type <string>          TABLE_NAME
%type <condition>       WHERE_CLAUSE
%type <arr>             COLUMN_NAMES
%type <string>          BOOL_OPERATION
%type <character>       ARITHMETIC_OPERATION
%type <condition>       CONDITION
%type <bool_expression> BOOL_EXPRESSION
%type <arr>             STRING_VALUES
%type <v>               VALUE
%type <v>               FUNCTION_CALL
%type <expression>      EXPRESSION
%start                  QUERY

%%
QUERY:
        SELECT COLUMN_NAMES FROM_CLAUSE WHERE_CLAUSE ';'
        {
          q->c = $2;
          q->f = $3;
          q->where_condition = $4;
        }
        |
        SELECT COLUMN_NAMES FROM_CLAUSE ';'
        {
          q->c = $2;
          q->f = $3;
          q->where_condition = NULL;
        }

COLUMN_NAMES:
        NAME
        {
          $$ = malloc(sizeof(array));
          $$->elems = NULL;
          $$->size = 0;
          size_t new_size = $$->size+1;
          $$->elems = realloc($$->elems, new_size*sizeof(char*));
          $$->elems[$$->size] = $1;
          $$->size = new_size;
        }
        |
        COLUMN_NAMES ',' NAME
        {
          size_t new_size = $1->size+1;
          $1->elems = realloc($1->elems, new_size);
          $1->elems[$1->size] = $3;
          $1->size = new_size;
          $$ = $1;
        }

FROM_CLAUSE: FROM TABLE_NAME
{
  $$ = malloc(sizeof(from));
  $$->table = $2;
}

WHERE_CLAUSE: WHERE CONDITION
{
  $$ = $2;
}

STRING_VALUES:
        STRING_VALUE
        {
          $$ = malloc(sizeof(array));
          $$->elems = realloc($$->elems, sizeof(char*) * 1);
          $$->elems[0] = $1;
          $$->size = 1;
        }
        |
        STRING_VALUES ',' STRING_VALUE
        {
          size_t new_size = $1->size + 1;
          $1->elems = realloc($1->elems, sizeof(char*) * new_size);
          $1->elems[$1->size] = $3;
          $1->size = new_size;
          $$ = $1;
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
        NAME '(' STRING_VALUES ')'
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
        BOOL_EXPRESSION AND BOOL_EXPRESSION
        {
          $$ = malloc(sizeof(condition));
          $$->left = $1;
          $$->op = "AND";
          $$->right = $3;
        }
        |
        BOOL_EXPRESSION OR  BOOL_EXPRESSION
        {
          $$ = malloc(sizeof(condition));
          $$->left = $1;
          $$->op = "OR";
          $$->right = $3;
        }
        |
        BOOL_EXPRESSION
        {
          $$ = malloc(sizeof(condition));
          $$->left = $1;
          $$->op = NULL;
          $$->right = NULL;
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
free_value(value *value)
{
  free(value->name);
  if (value->args) free_array(value->args);
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
close_query (query *q)
{
  // free the columns
  int i;
  for (i = 0; i < q->c->size; i++) {
    free(q->c->elems[i]);
  }
  free(q->c->elems);
  free(q->c);

  // TODO: free the where conditions
  if (q->where_condition) {
    free_bool_expression(q->where_condition->left);
    if (q->where_condition->op) free(q->where_condition->op);
    if (q->where_condition->right) free_bool_expression(q->where_condition->right);
    free(q->where_condition);
  }

  // free the from clause
  free(q->f->table);
  free(q->f);
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

int yyerror(query *q, void *s, char *err) {
  q->error = strdup(err);
}
