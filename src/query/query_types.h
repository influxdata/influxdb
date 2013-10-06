#include <stddef.h>

typedef struct {
  char *table;
} from;

typedef enum {
  OP_EQ,
  OP_NE,
  OP_GT,
  OP_LT,
  OP_GE,
  OP_LE
} operation_t;

typedef struct {
  size_t size;
  char **elems;
} array;

typedef struct {
  int  ivalue;
  char *svalue;
} value;

typedef struct {
  char *column_name;
  operation_t op;
  value *v;
} where;

typedef struct {
  array *c;
  from *f;
  where *w;
  char *error;
} query;

query parse_query(char *const query_s);
void  close_query (query *q);
