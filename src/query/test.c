#include <stdio.h>
#include "query_types.h"

int
main(int argc, char **argv)
{
  query q = parse_query("select from t where foo = '5' ;");
  printf("table name: %s\n", q.f->table);
  printf("where column: %s, value: %s\n", q.w->column_name, q.w->v->svalue);
  close_query(&q);
  return 0;
}

