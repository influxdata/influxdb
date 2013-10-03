typedef struct {
  char *table;
} from;

typedef enum {
  OP_EQUAL
} operation_t;

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
  from *f;
  where *w;
  char *error;
} query;

query parse_query(char *const query_s);
void  close_query (query *q);
