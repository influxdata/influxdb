%{
#include <stdio.h>
#include <string.h>
#include "query_types.h"
%}

%union {
  char *string;
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

%type <f>       FROM_CLAUSE
%type <string>  TABLE_NAME
%type <string>  FIELD_NAME
%type <w>       WHERE_CLAUSE
%type <v>       FIELD_VALUE
%start          QUERY

%%
QUERY: SELECT FROM_CLAUSE WHERE_CLAUSE ';'
{
  q->f = $2;
  q->w = $3;
}

FROM_CLAUSE: FROM TABLE_NAME
{
  $$ = malloc(sizeof(from));
  $$->table = $2;
}

WHERE_CLAUSE: WHERE FIELD_NAME '=' FIELD_VALUE
{
  $$ = malloc(sizeof(where));
  $$->column_name = $2;
  $$->op = OP_EQUAL;
  $$->v = $4;
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
