%{
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "query_types.h"
%}

%union {
  char *string;
  array *arr;
  int i;
  from *f;
  where *w;
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
%token <string> NAME STRING_VALUE
%token <i>      INT_VALUE
%token          OPERATION_EQUAL OPERATION_NE OPERATION_GT OPERATION_LT OPERATION_LE OPERATION_GE

%type <f>       FROM_CLAUSE
%type <string>  TABLE_NAME
%type <string>  FIELD_NAME
%type <w>       WHERE_CLAUSE
%type <v>       FIELD_VALUE
%type <arr>     COLUMN_NAMES
%type <i>       OPERATION
%start          QUERY

%%
QUERY: SELECT COLUMN_NAMES FROM_CLAUSE WHERE_CLAUSE ';'
{
  q->c = $2;
  q->f = $3;
  q->w = $4;
}

COLUMN_NAMES:
        NAME
        {
          $$ = malloc(sizeof(array));
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

WHERE_CLAUSE: WHERE FIELD_NAME OPERATION FIELD_VALUE
{
  $$ = malloc(sizeof(where));
  $$->column_name = $2;
  $$->op = $3;
  $$->v = $4;
}

OPERATION:
        OPERATION_EQUAL
        {
          $$ = OP_EQ;
        }
        |
        OPERATION_NE
        {
          $$ = OP_NE;
        }
        |
        OPERATION_GT
        {
          $$ = OP_GT;
        }
        |
        OPERATION_LT
        {
          $$ = OP_LT;
        }
        |
        OPERATION_GE
        {
          $$ = OP_GE;
        }
        |
        OPERATION_LE
        {
          $$ = OP_LE;
        }

TABLE_NAME: NAME
FIELD_NAME: NAME
{
  $$ = $1;
}
FIELD_VALUE:
        STRING_VALUE
        {
          $$ = malloc(sizeof(value));
          $$->svalue = $1;
        }
        |
        INT_VALUE
        {
          $$ = malloc(sizeof(value));
          $$->ivalue = $1;
        }

%%
void *yy_scan_string(char *, void *);
void yy_delete_buffer(void *, void *);

void
close_query (query *q)
{
  free(q->w->column_name);
  free(q->w->v->svalue);
  free(q->w->v);
  free(q->w);

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
