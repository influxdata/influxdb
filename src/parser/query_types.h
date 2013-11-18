#include <stddef.h>

#define FALSE 0
#define TRUE !FALSE

typedef struct {
  size_t size;
  char **elems;
} array;

struct value_t;

typedef struct {
  size_t size;
  struct value_t **elems;
} value_array;

typedef struct value_t {
  char *name;
  enum {
    VALUE_REGEX,
    VALUE_INT,
    VALUE_FLOAT,
    VALUE_STRING,
    VALUE_TABLE_NAME,
    VALUE_SIMPLE_NAME,
    VALUE_DURATION,
    VALUE_WILDCARD,
    VALUE_FUNCTION_CALL
  } value_type;
  char is_case_insensitive;
  value_array *args;
} value;

typedef struct expression_t {
  void *left;                           /* this can be a *value or *expression */
  char op;                              /* +, -, *, / or \0 if there's no right operand */
  struct expression_t *right;
} expression;

typedef struct {
  expression *left;
  char *op;                             /* ==, !=, <, >, <=, >= or NULL if there is no right operand */
  expression *right;
} bool_expression;

typedef struct condition_t {
  char is_bool_expression;
  void *left;
  char* op;                             /* AND, OR or NULL if there's no right operand */
  struct condition_t *right;
} condition;

typedef struct {
  int line;
  int column;
  char *err;
} error;

typedef struct {
  value *name;
  char *alias;
} table_name;

typedef struct {
  size_t size;
  table_name **elems;
} table_name_array;

typedef struct {
  enum {
    FROM_ARRAY,
    FROM_MERGE,
    FROM_INNER_JOIN
  } from_clause_type;
  // in case of merge or join, it's guaranteed that the names array
  // will have two table names only and they aren't regex.
  table_name_array *names;
} from_clause;

typedef struct {
  value_array *c;
  from_clause *from_clause;
  value_array *group_by;
  condition *where_condition;
  error *error;
  int limit;
  char ascending;
} query;

// some funcs for freeing our types
void free_array(array *array);
void free_value_array(value_array *array);
void free_value(value *value);
void free_expression(expression *expr);
void free_bool_expression(bool_expression *expr);
void free_condition(condition *condition);
void free_error (error *error);

// this is the api that is used in GO
query parse_query(char *const query_s);
void  close_query (query *q);

