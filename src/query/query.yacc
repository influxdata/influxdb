%{
#include <stdio.h>
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
close_query (query *q) {
  free(q->w->column_name);
  free(q->w->v->svalue);
  free(q->w->v);
  free(q->w);

  free(q->f->table);
  free(q->f);
}

int
main(int argc, char **argv) {
  /* yydebug = 1; */
  void *scanner;
  yylex_init(&scanner);
  query q;
  void *buffer = yy_scan_string("select from t where foo = '5' ;", scanner);
  yyparse (&q, scanner);
  yy_delete_buffer(buffer, scanner);
  printf("table name: %s\n", q.f->table);
  printf("where column: %s, value: %s\n", q.w->column_name, q.w->v->svalue);
  yylex_destroy(scanner);
  close_query(&q);
  return 0;
}

int yyerror(query *q, void *s, char *err) {
  fprintf(stderr, "error: %s\n", err);
}
