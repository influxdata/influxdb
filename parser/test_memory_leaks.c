// +build ignore

#include "query_types.h"
#include <stdio.h>

int
main(int argc, char **argv)
{
  char *qs[] = {
    "select count(*) from users.events group_by user_email,time(1h) where time>now()-1d;",
    "explain select users.events group_by user_email,time(1h) where time>now()-1d;",
    "select * from foo where time < -1s",
    "select * from merge(/.*/) where time < -1s",
    "list series /",
    "select count(*) from users.events group_by user_email,time(1h) where time >> now()-1d;",
    "select value from t where c == 5 and b == 6;",
    "select value from t where c == '5';",
    "select value from cpu.idle where value > 90 and (time > now() - 1d or value > 80) and time < now() - 1w;",
    "select email from users.events where email =~ /gmail\\\\.com/i and time>now()-2d;",
    "select email from users.events where email in ('jvshahid@gmail.com')",
    "select * from foobar limit",
    "list continuous queries;",
    "list series /foo/ bar",
    "select -1 * value from t where c == 5 and b == 6;",
    "select value from cpu.idle where value > 90 and (time > now() - 1d or value > 80) and time < now() - 1w last 10;",
    "drop series foobar",
    "drop continuous query 5;",
    "select count(bar) as the_count from users.events group_by user_email,time(1h);",
    "select email from users.events as events where email === /gmail\\\\.com/i and time>now()-2d;",
    "select value from t where c = '5';",
    "select * from foo into bar;",
  };

  int i;
  for (i = 0; i < sizeof(qs) / sizeof(char*); i++) {
    // test freeing on close for different types of queries
    queries q = parse_query(qs[i]);
    close_queries(&q);
  }

  printf("Checked %d queries for memory leak\n", i);

  return 0;
}
