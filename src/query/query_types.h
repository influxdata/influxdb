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
  value_array *args;
} value;

typedef struct {
  value *left;
  char op;                              /* +, -, *, / or \0 if there's no right operand */
  value *right;
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
  value_array *c;
  value *f;
  value_array *group_by;
  condition *where_condition;
  error *error;
  int limit;
} query;

query parse_query(char *const query_s);
void  close_query (query *q);
