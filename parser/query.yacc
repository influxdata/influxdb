%{
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include "query_types.h"

value *create_value(char *name, int type, char is_case_insensitive, value_array *args) {
  value *v = malloc(sizeof(value));
  v->name = name;
  v->value_type = type;
  v->is_case_insensitive = is_case_insensitive;
  v->args = args;
  v->alias = NULL;
  return v;
}

value *create_expression_value(char *operator, size_t size, ...) {
  value *v = malloc(sizeof(value));
  v->name = operator;
  v->alias = NULL;
  v->value_type = VALUE_EXPRESSION;
  v->is_case_insensitive = FALSE;
  v->args = malloc(sizeof(value_array));
  v->args->size = size;
  v->args->elems = malloc(sizeof(value*) * size);
  va_list ap;
  va_start(ap, size);

  int i;
  for (i = 0; i < size; i++) {
    value *x = va_arg(ap, value*);
    v->args->elems[i] = x;
  }
  va_end(ap);
  return v;
}

%}

%union {
  char                  character;
  char*                 string;
  int                   integer;
  condition*            condition;
  value_array*          value_array;
  value*                v;
  from_clause*          from_clause;
  into_clause*          into_clause;
  query*                query;
  select_query*         select_query;
  delete_query*         delete_query;
  drop_series_query*    drop_series_query;
  drop_query*           drop_query;
  groupby_clause*       groupby_clause;
  table_name_array*     table_name_array;
  struct {
    int limit;
    char ascending;
  } limit_and_order;
}

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
%token          SELECT DELETE FROM WHERE EQUAL GROUP BY LIMIT ORDER ASC DESC MERGE INNER JOIN AS LIST SERIES INTO CONTINUOUS_QUERIES CONTINUOUS_QUERY DROP DROP_SERIES EXPLAIN
%token <string> STRING_VALUE INT_VALUE FLOAT_VALUE BOOLEAN_VALUE TABLE_NAME SIMPLE_NAME INTO_NAME REGEX_OP
%token <string>  NEGATION_REGEX_OP REGEX_STRING INSENSITIVE_REGEX_STRING DURATION

// define the precedence of these operators
%left  OR
%left  AND
%nonassoc <string> OPERATION_EQUAL OPERATION_NE OPERATION_GT OPERATION_LT OPERATION_LE OPERATION_GE OPERATION_IN
%left  <character> '+' '-'
%left  <character> '*' '/'

// define the types of the non-terminals
%type <from_clause>       FROM_CLAUSE
%type <condition>         WHERE_CLAUSE
%type <value_array>       COLUMN_NAMES
%type <string>            BOOL_OPERATION ALIAS_CLAUSE
%type <condition>         CONDITION
%type <v>                 BOOL_EXPRESSION
%type <value_array>       VALUES
%type <v>                 VALUE TABLE_VALUE SIMPLE_TABLE_VALUE TABLE_NAME_VALUE SIMPLE_NAME_VALUE INTO_VALUE INTO_NAME_VALUE
%type <table_name_array>  SIMPLE_TABLE_VALUES
%type <v>                 WILDCARD REGEX_VALUE DURATION_VALUE FUNCTION_CALL
%type <groupby_clause>    GROUP_BY_CLAUSE
%type <integer>           LIMIT_CLAUSE
%type <character>         ORDER_CLAUSE
%type <into_clause>       INTO_CLAUSE
%type <limit_and_order>   LIMIT_AND_ORDER_CLAUSES
%type <query>             QUERY
%type <delete_query>      DELETE_QUERY
%type <drop_series_query> DROP_SERIES_QUERY
%type <select_query>      SELECT_QUERY
%type <drop_query>        DROP_QUERY
%type <select_query>      EXPLAIN_QUERY

// the initial token
%start                    ALL_QUERIES

// destructors are used to free up memory in case of an error
%destructor { free_value($$); } <v>
%destructor { free_from_clause($$); } <from_clause>
%destructor { if ($$) free_condition($$); } <condition>
%destructor { free($$); } <string>
%destructor { free_expression($$); } <expression>
%destructor { if ($$) free_value_array($$); } <value_array>
%destructor { free_groupby_clause($$); } <groupby_clause>
%destructor { close_query($$); free($$); } <query>

// grammar
%%
ALL_QUERIES:
        QUERY
        {
          *q = *$1;
          free($1);
        }
        |
        QUERY ';'
        {
          *q = *$1;
          free($1);
        }
        |
        QUERY ';' ALL_QUERIES
        {
          *q = *$1;
          free($1);
        }

QUERY:
        SELECT_QUERY
        {
          $$ = calloc(1, sizeof(query));
          $$->select_query = $1;
        }
        |
        DELETE_QUERY
        {
          $$ = calloc(1, sizeof(query));
          $$->delete_query = $1;
        }
        |
        DROP_QUERY
        {
          $$ = calloc(1, sizeof(query));
          $$->drop_query = $1;
        }
        |
        LIST SERIES
        {
          $$ = calloc(1, sizeof(query));
          $$->list_series_query = TRUE;
        }
        |
        DROP_SERIES_QUERY
        {
          $$ = calloc(1, sizeof(query));
          $$->drop_series_query = $1;
        }
        |
        LIST CONTINUOUS_QUERIES
        {
          $$ = calloc(1, sizeof(query));
          $$->list_continuous_queries_query = TRUE;
        }
        |
        EXPLAIN_QUERY
        {
          $$ = calloc(1, sizeof(query));
          $$->select_query = $1;
        }

DROP_QUERY:
        DROP CONTINUOUS_QUERY INT_VALUE
        {
          $$ = calloc(1, sizeof(drop_query));
          $$->id = atoi($3);
          free($3);
        }

DELETE_QUERY:
        DELETE FROM_CLAUSE WHERE_CLAUSE
        {
          $$ = calloc(1, sizeof(delete_query));
          $$->from_clause = $2;
          $$->where_condition = $3;
        }

DROP_SERIES_QUERY:
        DROP_SERIES SIMPLE_TABLE_VALUE
        {
          $$ = malloc(sizeof(drop_series_query));
          $$->name = $2;
        }

EXPLAIN_QUERY:
        EXPLAIN SELECT_QUERY
        {
          $$ = $2;
          $$->explain = TRUE;
        }

SELECT_QUERY:
        SELECT COLUMN_NAMES FROM_CLAUSE GROUP_BY_CLAUSE WHERE_CLAUSE LIMIT_AND_ORDER_CLAUSES INTO_CLAUSE
        {
          $$ = calloc(1, sizeof(select_query));
          $$->c = $2;
          $$->from_clause = $3;
          $$->group_by = $4;
          $$->where_condition = $5;
          $$->limit = $6.limit;
          $$->ascending = $6.ascending;
          $$->into_clause = $7;
          $$->explain = FALSE;
        }
        |
        SELECT COLUMN_NAMES FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_AND_ORDER_CLAUSES INTO_CLAUSE
        {
          $$ = calloc(1, sizeof(select_query));
          $$->c = $2;
          $$->from_clause = $3;
          $$->where_condition = $4;
          $$->group_by = $5;
          $$->limit = $6.limit;
          $$->ascending = $6.ascending;
          $$->into_clause = $7;
          $$->explain = FALSE;
        }

LIMIT_AND_ORDER_CLAUSES:
        ORDER_CLAUSE LIMIT_CLAUSE
        {
          $$.limit = $2;
          $$.ascending = $1;
        }
        |
        LIMIT_CLAUSE ORDER_CLAUSE
        {
          $$.limit = $1;
          $$.ascending = $2;
        }

ORDER_CLAUSE:
        ORDER ASC
        {
          $$ = TRUE;
        }
        |
        ORDER DESC
        {
          $$ = FALSE;
        }
        |
        {
          $$ = FALSE;
        }

LIMIT_CLAUSE:
        LIMIT INT_VALUE
        {
          $$ = atoi($2);
          free($2);
        }
        |
        {
          $$ = -1;
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
          $$ = malloc(sizeof(groupby_clause));
          $$->elems = $3;
          $$->fill_function = NULL;
        }
        |
        GROUP BY VALUES FUNCTION_CALL
        {
          $$ = malloc(sizeof(groupby_clause));
          $$->elems = $3;
          $$->fill_function = $4;
        }
        |
        {
          $$ = NULL;
        }

INTO_CLAUSE:
        INTO INTO_VALUE
        {
          $$ = malloc(sizeof(into_clause));
          $$->target = $2;
          $$->backfill_function = NULL;
        }
        |
        INTO INTO_VALUE FUNCTION_CALL
        {
          $$ = malloc(sizeof(into_clause));
          $$->target = $2;
          $$->backfill_function = $3;
        }
        |
        {
          $$ = NULL;
        }

COLUMN_NAMES:
        VALUES

ALIAS_CLAUSE:
        AS SIMPLE_TABLE_VALUE
        {
          $$ = $2->name;
          free($2);
        }
        |
        {
          $$ = NULL;
        }

FROM_CLAUSE:
        FROM TABLE_VALUE
        {
          $$ = malloc(sizeof(from_clause));
          $$->names = malloc(sizeof(table_name_array));
          $$->names->elems = malloc(sizeof(table_name*));
          $$->names->size = 1;
          $$->names->elems[0] = malloc(sizeof(table_name));
          $$->names->elems[0]->name = $2;
          $$->names->elems[0]->alias = NULL;
          $$->from_clause_type = FROM_ARRAY;
        }
        |
        FROM SIMPLE_TABLE_VALUES
        {
          $$ = malloc(sizeof(from_clause));
          $$->names = $2;
          $$->from_clause_type = FROM_ARRAY;
        }
        |
        FROM SIMPLE_TABLE_VALUE
        {
          $$ = malloc(sizeof(from_clause));
          $$->names = malloc(sizeof(table_name_array));
          $$->names->elems = malloc(sizeof(table_name*));
          $$->names->size = 1;
          $$->names->elems[0] = malloc(sizeof(table_name));
          $$->names->elems[0]->name = $2;
          $$->names->elems[0]->alias = NULL;
          $$->from_clause_type = FROM_ARRAY;
        }
        |
        FROM SIMPLE_TABLE_VALUE MERGE SIMPLE_TABLE_VALUE
        {
          $$ = malloc(sizeof(from_clause));
          $$->names = malloc(sizeof(table_name_array));
          $$->names->elems = malloc(2 * sizeof(table_name*));
          $$->names->size = 2;
          $$->names->elems[0] = malloc(sizeof(table_name));
          $$->names->elems[0]->name = $2;
          $$->names->elems[0]->alias = NULL;
          $$->names->elems[1] = malloc(sizeof(table_name));
          $$->names->elems[1]->name = $4;
          $$->names->elems[1]->alias = NULL;
          $$->from_clause_type = FROM_MERGE;
        }
        |
        FROM SIMPLE_TABLE_VALUE ALIAS_CLAUSE INNER JOIN SIMPLE_TABLE_VALUE ALIAS_CLAUSE
        {
          $$ = malloc(sizeof(from_clause));
          $$->names = malloc(sizeof(table_name_array));
          $$->names->elems = malloc(2 * sizeof(value*));
          $$->names->size = 2;
          $$->names->elems[0] = malloc(sizeof(table_name));
          $$->names->elems[0]->name = $2;
          $$->names->elems[0]->alias = $3;
          $$->names->elems[1] = malloc(sizeof(table_name));
          $$->names->elems[1]->name = $6;
          $$->names->elems[1]->alias = $7;
          $$->from_clause_type = FROM_INNER_JOIN;
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
        SIMPLE_NAME '(' ')'
        {
          $$ = create_value($1, VALUE_FUNCTION_CALL, FALSE, NULL);
        }
        |
        SIMPLE_NAME '(' VALUES ')'
        {
          $$ = create_value($1, VALUE_FUNCTION_CALL, FALSE, $3);
        }

VALUE:
        STRING_VALUE
        {
          $$ = create_value($1, VALUE_STRING, FALSE, NULL);
        }
        |
        INT_VALUE
        {
          $$ = create_value($1, VALUE_INT, FALSE, NULL);
        }
        |
        '-' INT_VALUE
        {
          size_t len = strlen($2) + 2;
          char *new_value = malloc(len);
          new_value[0] = '-';
          strncpy(new_value+1, $2, len-1);
          free($2);
          $$ = create_value(new_value, VALUE_INT, FALSE, NULL);
        }
        |
        FLOAT_VALUE
        {
          $$ = create_value($1, VALUE_FLOAT, FALSE, NULL);
        }
        |
        '-' FLOAT_VALUE
        {
          size_t len = strlen($2) + 2;
          char *new_value = malloc(len);
          new_value[0] = '-';
          strncpy(new_value+1, $2, len-1);
          free($2);
          $$ = create_value(new_value, VALUE_FLOAT, FALSE, NULL);
        }
        |
        BOOLEAN_VALUE
        {
          $$ = create_value($1, VALUE_BOOLEAN, FALSE, NULL);
        }
        |
        DURATION_VALUE
        {
          $$ = $1;
          $$->alias = NULL;
        }
        |
        '-' DURATION_VALUE
        {
          char *name = calloc(1, strlen($2->name) + 2);
          strcat(name, "-");
          strcat(name, $2->name);
          free($2->name);
          $2->name = name;
          $$ = $2;
          $$->alias = NULL;
        }
        |
        SIMPLE_NAME_VALUE
        {
          $$ = $1;
          $$->alias = NULL;
        }
        |
        WILDCARD
        {
          $$ = $1;
          $$->alias = NULL;
        }
        |
        TABLE_NAME_VALUE
        {
          $$ = $1;
          $$->alias = NULL;
        }
        |
        FUNCTION_CALL
        {
          $$ = $1;
          $$->alias = NULL;
        }
        |
        '(' VALUE ')'
        {
          $$ = $2;
        }
        |
        '(' VALUE ')' AS SIMPLE_NAME
        {
          $$ = $2;
          $$->alias = $5;
        }
        |
        FUNCTION_CALL AS SIMPLE_NAME
        {
          $$ = $1;
          $$->alias = $3;
        }
        |
        VALUE '*' VALUE { $$ = create_expression_value(strdup("*"), 2, $1, $3); }
        |
        VALUE '/' VALUE { $$ = create_expression_value(strdup("/"), 2, $1, $3); }
        |
        VALUE '+' VALUE { $$ = create_expression_value(strdup("+"), 2, $1, $3); }
        |
        VALUE '-' VALUE { $$ = create_expression_value(strdup("-"), 2, $1, $3); }

TABLE_VALUE:
        SIMPLE_NAME_VALUE | TABLE_NAME_VALUE | REGEX_VALUE

SIMPLE_TABLE_VALUE:
        SIMPLE_NAME_VALUE | TABLE_NAME_VALUE

SIMPLE_TABLE_VALUES:
        SIMPLE_TABLE_VALUE
        {
          $$ = malloc(sizeof(table_name_array));
          $$->size = 1;
          $$->elems = malloc(sizeof(table_name*));
          $$->elems[0] = malloc(sizeof(table_name));
          $$->elems[0]->name = $1;
          $$->elems[0]->alias = NULL;
        }
        |
        SIMPLE_TABLE_VALUES ',' SIMPLE_TABLE_VALUE
        {
          size_t new_size = $1->size + 1;
          $1->elems = realloc($$->elems, sizeof(table_name*) * new_size);
          $1->elems[$1->size] = malloc(sizeof(table_name));
          $1->elems[$1->size]->name = $3;
          $1->elems[$1->size]->alias = NULL;
          $1->size = new_size;
          $$ = $1;
        }

INTO_VALUE:
        SIMPLE_NAME_VALUE | TABLE_NAME_VALUE | INTO_NAME_VALUE

DURATION_VALUE:
        DURATION
        {
          $$ = create_value($1, VALUE_DURATION, FALSE, NULL);
        }

SIMPLE_NAME_VALUE:
        SIMPLE_NAME
        {
          $$ = create_value($1, VALUE_SIMPLE_NAME, FALSE, NULL);
        }

WILDCARD:
        '*'
        {
          char *name = strdup("*");
          $$ = create_value(name, VALUE_WILDCARD, FALSE, NULL);
        }

INTO_NAME_VALUE:
        INTO_NAME
        {
          $$ = create_value($1, VALUE_INTO_NAME, FALSE, NULL);
        }

TABLE_NAME_VALUE:
        TABLE_NAME
        {
          $$ = create_value($1, VALUE_TABLE_NAME, FALSE, NULL);
        }

REGEX_VALUE:
        REGEX_STRING
        {
          $$ = create_value($1, VALUE_REGEX, FALSE, NULL);
        }
        |
        INSENSITIVE_REGEX_STRING
        {
          $$ = create_value($1, VALUE_REGEX, TRUE, NULL);
        }

BOOL_EXPRESSION:
        VALUE
        |
        VALUE BOOL_OPERATION VALUE
        {
          $$ = create_expression_value($2, 2, $1, $3);
        }
        |
        VALUE OPERATION_IN '(' VALUES ')'
        {
          $$ = create_expression_value($2, 1, $1);
          $$->args->elems = realloc($$->args->elems, sizeof(value*) * ($4->size + 1));
          memcpy($$->args->elems + 1, $4->elems, $4->size * sizeof(value*));
          $$->args->size = $4->size + 1;
          free($4->elems);
          free($4);
        }
        |
        VALUE REGEX_OP REGEX_VALUE
        {
          $$ = create_expression_value($2, 2, $1, $3);
        }
        |
        VALUE NEGATION_REGEX_OP REGEX_VALUE
        {
          $$ = create_expression_value($2, 2, $1, $3);
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
  query q = {NULL, NULL, NULL, NULL, FALSE, FALSE, NULL};
  void *scanner;
  yylex_init(&scanner);
#ifdef DEBUG
  yydebug = 1;
  yyset_debug(1, scanner);
#endif
  void *buffer = yy_scan_string(query_s, scanner);
  yyparse (&q, scanner);
  yy_delete_buffer(buffer, scanner);
  yylex_destroy(scanner);
  return q;
}

int yyerror(YYLTYPE *locp, query *q, void *s, char *err) {
  q->error = malloc(sizeof(error));
  q->error->err = strdup(err);
  q->error->first_line = locp->first_line;
  q->error->first_column = locp->first_column;
  q->error->last_line = locp->last_line;
  q->error->last_column = locp->last_column;
}
