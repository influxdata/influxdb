%{
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "query_types.h"

#if !defined(__APPLE_CC__) && defined(__x86_64__)
__asm__(".symver memcpy,memcpy@GLIBC_2.2.5");
#endif

expression *create_expression(expression *left, char op, expression *right) {
  expression *expr = malloc(sizeof(expression));
  expr->left = left;
  expr->op = op;
  expr->right = right;
  return expr;
}

value *create_value(char *name, int type, char is_case_insensitive, value_array *args) {
  value *v = malloc(sizeof(value));
  v->name = name;
  v->value_type = type;
  v->is_case_insensitive = is_case_insensitive;
  v->args = args;
  return v;
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
  from_clause*          from_clause;
  struct {
    int limit;
    char ascending;
  } limit_and_order;
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
%token          SELECT FROM WHERE EQUAL GROUP BY FIRST LAST LIMIT ORDER ASC DESC MERGE INNER JOIN
%token <string> STRING_VALUE INT_VALUE FLOAT_VALUE TABLE_NAME SIMPLE_NAME REGEX_OP REGEX_STRING INSENSITIVE_REGEX_STRING DURATION

// define the precendence of these operators
%left  OR
%left  AND
%nonassoc <string> OPERATION_EQUAL OPERATION_NE OPERATION_GT OPERATION_LT OPERATION_LE OPERATION_GE
%left  <character> '+' '-'
%left  <character> '*' '/'

// define the types of the non-terminals
%type <from_clause>     FROM_CLAUSE
%type <condition>       WHERE_CLAUSE
%type <value_array>     COLUMN_NAMES
%type <string>          BOOL_OPERATION
%type <condition>       CONDITION
%type <bool_expression> BOOL_EXPRESSION
%type <value_array>     VALUES
%type <v>               VALUE TABLE_VALUE SIMPLE_TABLE_VALUE TABLE_NAME_VALUE SIMPLE_NAME_VALUE
%type <v>               WILDCARD REGEX_VALUE DURATION_VALUE FUNCTION_CALL
%type <expression>      EXPRESSION
%type <value_array>     GROUP_BY_CLAUSE
%type <integer>         LIMIT_CLAUSE
%type <character>       ORDER_CLAUSE
%type <limit_and_order> LIMIT_AND_ORDER_CLAUSES

// the initial token
%start                  QUERY

// destructors are used to free up memory in case of an error
%destructor { free_value($$); } <v>
%destructor { free_from_clause($$); } <from_clause>
%destructor { if ($$) free_condition($$); } <condition>
%destructor { free($$); } <string>
%destructor { free_expression($$); } <expression>
%destructor { if ($$) free_value_array($$); } <value_array>

// grammar
%%
QUERY:
        SELECT COLUMN_NAMES FROM_CLAUSE GROUP_BY_CLAUSE WHERE_CLAUSE LIMIT_AND_ORDER_CLAUSES ';'
        {
          q->c = $2;
          q->from_clause = $3;
          q->group_by = $4;
          q->where_condition = $5;
          q->limit = $6.limit;
          q->ascending = $6.ascending;
        }
        |
        SELECT COLUMN_NAMES FROM_CLAUSE WHERE_CLAUSE GROUP_BY_CLAUSE LIMIT_AND_ORDER_CLAUSES ';'
        {
          q->c = $2;
          q->from_clause = $3;
          q->where_condition = $4;
          q->group_by = $5;
          q->limit = $6.limit;
          q->ascending = $6.ascending;
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

COLUMN_NAMES:
        VALUES

FROM_CLAUSE:
        FROM TABLE_VALUE
        {
          $$ = malloc(sizeof(from_clause));
          $$->names = malloc(sizeof(value_array));
          $$->names->elems = malloc(sizeof(value*));
          $$->names->size = 1;
          $$->names->elems[0] = $2;
          $$->from_clause_type = FROM_ARRAY;
        }
        |
        FROM SIMPLE_TABLE_VALUE MERGE SIMPLE_TABLE_VALUE
        {
          $$ = malloc(sizeof(from_clause));
          $$->names = malloc(sizeof(value_array));
          $$->names->elems = malloc(2 * sizeof(value*));
          $$->names->size = 2;
          $$->names->elems[0] = $2;
          $$->names->elems[1] = $4;
          $$->from_clause_type = FROM_MERGE;
        }
        |
        FROM SIMPLE_TABLE_VALUE INNER JOIN SIMPLE_TABLE_VALUE
        {
          $$ = malloc(sizeof(from_clause));
          $$->names = malloc(sizeof(value_array));
          $$->names->elems = malloc(2 * sizeof(value*));
          $$->names->size = 2;
          $$->names->elems[0] = $2;
          $$->names->elems[1] = $5;
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
        FLOAT_VALUE
        {
          $$ = create_value($1, VALUE_FLOAT, FALSE, NULL);
        }
        |
        DURATION_VALUE
        |
        SIMPLE_NAME_VALUE
        |
        WILDCARD
        |
        TABLE_NAME_VALUE
        |
        FUNCTION_CALL

TABLE_VALUE:
        SIMPLE_NAME_VALUE | TABLE_NAME_VALUE | REGEX_VALUE

SIMPLE_TABLE_VALUE:
        SIMPLE_NAME_VALUE | TABLE_NAME_VALUE

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
        EXPRESSION REGEX_OP REGEX_VALUE
        {
          $$ = malloc(sizeof(bool_expression));
          $$->left = $1;
          $$->op = $2;
          $$->right = malloc(sizeof(expression));
          $$->right->left = $3;
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
